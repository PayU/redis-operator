package controllers

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/pkg/errors"

	dbv1 "github.com/PayU/redis-operator/api/v1"
	rediscli "github.com/PayU/redis-operator/controllers/rediscli"
	"github.com/PayU/redis-operator/controllers/view"
	clusterData "github.com/PayU/redis-operator/data"
)

func (r *RedisClusterReconciler) NewRedisClusterView(redisCluster *dbv1.RedisCluster) (*view.RedisClusterView, error) {
	v := &view.RedisClusterView{}
	pods, e := r.getRedisClusterPods(redisCluster)
	if e != nil {
		fmt.Printf("Error fetching pods viw %+v\n", e.Error())
		return v, e
	}
	v.CreateView(pods, r.RedisCLI)
	return v, nil
}

//type NodeNames [2]string // 0: node name, 1: leader name

func (r *RedisClusterReconciler) getLeaderIP(followerIP string) (string, error) {
	info, _, err := r.RedisCLI.Info(followerIP)
	if err != nil {
		return "", err
	}
	return info.Replication["master_host"], nil
}

// Returns the node name and leader name from a pod
func (r *RedisClusterReconciler) getRedisNodeNamesFromIP(namespace string, podIP string) (string, string, error) {
	pod, err := r.getPodByIP(namespace, podIP)
	if err != nil {
		return "", "", err
	}
	return pod.Labels["node-name"], pod.Labels["leader-name"], err
}

// Returns a mapping between node names and IPs
func (r *RedisClusterReconciler) getNodeIPs(redisCluster *dbv1.RedisCluster) (map[string]string, error) {
	nodeIPs := make(map[string]string)
	pods, err := r.getRedisClusterPods(redisCluster)
	if err != nil {
		return nil, err
	}
	for _, pod := range pods {
		nodeIPs[pod.Labels["node-name"]] = pod.Status.PodIP
	}
	return nodeIPs, nil
}

func (r *RedisClusterReconciler) createNewRedisCluster(redisCluster *dbv1.RedisCluster) error {
	r.Log.Info("Creating new cluster...")

	if _, err := r.createRedisService(redisCluster); err != nil {
		return err
	}

	if err := r.initializeCluster(redisCluster); err != nil {
		return err
	}
	r.Log.Info("[OK] Redis cluster initialized successfully")
	return nil
}

func (r *RedisClusterReconciler) initializeFollowers(redisCluster *dbv1.RedisCluster) error {
	r.Log.Info("Initializing followers...")
	leadersView, err := r.NewRedisClusterView(redisCluster)
	if err != nil {
		return err
	}

	followersNamesToLeaderNodes := make(map[string]*view.PodView)
	for _, leaderNode := range leadersView.PodsViewByName {
		for i := 1; i <= redisCluster.Spec.LeaderFollowersCount; i++ {
			followersNamesToLeaderNodes[leaderNode.Pod.Labels["node-name"]+"-"+strconv.Itoa(i)] = leaderNode
		}
	}

	err = r.addFollowers(redisCluster, followersNamesToLeaderNodes)
	if err != nil {
		return err
	}

	r.Log.Info("[OK] Redis followers initialized successfully")
	return nil
}

func (r *RedisClusterReconciler) initializeCluster(redisCluster *dbv1.RedisCluster) error {
	var leaderNames []string
	// leaders are created first to increase the chance they get scheduled on different
	// AZs when using soft affinity rules
	for leaderNumber := 0; leaderNumber < redisCluster.Spec.LeaderCount; leaderNumber++ {
		leaderNames = append(leaderNames, "redis-node-"+strconv.Itoa(leaderNumber))
	}

	newLeaderPods, err := r.createRedisLeaderPods(redisCluster, leaderNames...)
	if err != nil {
		return err
	}

	var nodeIPs []string
	for _, leaderPod := range newLeaderPods {
		r.RedisCLI.Flushall(leaderPod.Status.PodIP)
		r.RedisCLI.ClusterReset(leaderPod.Status.PodIP)
		nodeIPs = append(nodeIPs, leaderPod.Status.PodIP)
	}

	if _, err := r.waitForPodReady(newLeaderPods...); err != nil {
		return err
	}

	if err := r.waitForRedis(nodeIPs...); err != nil {
		return err
	}

	if _, err = r.RedisCLI.ClusterCreate(nodeIPs); err != nil {
		return err
	}

	return r.waitForClusterCreate(nodeIPs)
}

// Make a new Redis node join the cluster as a follower and wait until data sync is complete
func (r *RedisClusterReconciler) replicateLeader(followerIP string, leaderIP string) error {
	r.Log.Info(fmt.Sprintf("Replicating leader: %s->%s", followerIP, leaderIP))
	leaderID, err := r.RedisCLI.MyClusterID(leaderIP)
	if err != nil {
		return err
	}

	followerID, err := r.RedisCLI.MyClusterID(followerIP)
	if err != nil {
		return err
	}

	if stdout, err := r.RedisCLI.AddFollower(followerIP, leaderIP, leaderID); err != nil {
		if !strings.Contains(stdout, "All nodes agree about slots configuration") {
			return err
		}
	}

	if err = r.waitForRedisMeet(leaderIP, followerIP); err != nil {
		return err
	}

	r.Log.Info(fmt.Sprintf("Replication successful"))

	if err = r.waitForRedisReplication(leaderIP, leaderID, followerID); err != nil {
		return err
	}

	return r.waitForRedisSync(followerIP)
}

// Triggers a failover command on the specified node and waits for the follower
// to become leader
func (r *RedisClusterReconciler) doFailover(followerIP string, opt string) error {
	r.Log.Info(fmt.Sprintf("Running 'cluster failover %s' on %s", opt, followerIP))
	_, err := r.RedisCLI.ClusterFailover(followerIP, opt)
	if err != nil {
		return err
	}
	if err := r.waitForManualFailover(followerIP); err != nil {
		return err
	}
	return nil
}

// Changes the role of a leader with one of its healthy followers
// Returns the IP of the promoted follower
// leaderIP: IP of leader that will be turned into a follower
// opt: the type of failover operation ('', 'force', 'takeover')
// followerIP (optional): followers that should be considered for the failover process
func (r *RedisClusterReconciler) doLeaderFailover(leaderIP string, opt string, followerIPs ...string) (string, error) {
	var promotedFollowerIP string
	leaderID, err := r.RedisCLI.MyClusterID(leaderIP)
	if err != nil {
		return "", err
	}

	r.Log.Info(fmt.Sprintf("Starting manual failover on leader: %s(%s)", leaderIP, leaderID))

	if len(followerIPs) != 0 {
		for _, followerIP := range followerIPs {
			if _, pingErr := r.RedisCLI.Ping(followerIP); pingErr == nil {
				promotedFollowerIP = followerIP
				break
			}
		}
	} else {
		followers, _, err := r.RedisCLI.ClusterReplicas(leaderIP, leaderID)
		if err != nil {
			return "", err
		}
		if len(*followers) == 0 {
			return "", errors.Errorf("Attempted FAILOVER on a leader (%s) with no followers. This case is not supported yet.", leaderIP)
		}
		for _, follower := range *followers {
			if !follower.IsFailing() {
				promotedFollowerIP = strings.Split(follower.Addr, ":")[0]
			}
		}
	}

	if err := r.doFailover(promotedFollowerIP, opt); err != nil {
		return "", err
	}

	r.Log.Info(fmt.Sprintf("[OK] Leader failover successful for (%s). New leader: (%s)", leaderIP, promotedFollowerIP))
	return promotedFollowerIP, nil
}

// Recreates a leader based on a replica that took its place in a failover process;
// the old leader pod must be already deleted
func (r *RedisClusterReconciler) recreateLeader(redisCluster *dbv1.RedisCluster, promotedFollowerIP string) error {
	nodeName, oldLeaderName, err := r.getRedisNodeNamesFromIP(redisCluster.Namespace, promotedFollowerIP)
	if err != nil {
		return err
	}
	r.Log.Info(fmt.Sprintf("Recreating leader [%s] using node [%s]", oldLeaderName, nodeName))

	newLeaderPods, err := r.createRedisLeaderPods(redisCluster, oldLeaderName)
	if err != nil {
		return err
	}
	newLeaderIP := newLeaderPods[0].Status.PodIP

	newLeaderPods, err = r.waitForPodReady(newLeaderPods...)
	if err != nil {
		return err
	}

	if err := r.waitForRedis(newLeaderIP); err != nil {
		return err
	}

	if err = r.replicateLeader(newLeaderIP, promotedFollowerIP); err != nil {
		return err
	}

	r.Log.Info("Leader replication successful")

	if _, err = r.doLeaderFailover(promotedFollowerIP, "", newLeaderIP); err != nil {
		return err
	}

	r.Log.Info(fmt.Sprintf("[OK] Leader [%s] recreated successfully; new IP: [%s]", oldLeaderName, newLeaderIP))
	return nil
}

func (r *RedisClusterReconciler) reCreateFollowerslessLeader(redisCluster *dbv1.RedisCluster, leaderName string, healthyLeaderIp string) error {
	r.Log.Info(fmt.Sprintf("Recreating followersless leader [%s]", leaderName))
	newLeaderPods, err := r.createRedisLeaderPods(redisCluster, leaderName)
	if err != nil {
		return err
	}
	newLeaderIP := newLeaderPods[0].Status.PodIP

	newLeaderPods, err = r.waitForPodReady(newLeaderPods...)
	if err != nil {
		return err
	}

	if err := r.waitForRedis(newLeaderIP); err != nil {
		return err
	}

	if _, err := r.RedisCLI.AddLeader(newLeaderIP, healthyLeaderIp); err != nil {
		return err
	}

	r.Log.Info(fmt.Sprintf("[OK] Leader [%s] recreated successfully; new IP: [%s]", leaderName, newLeaderIP))
	return nil
}

// Adds one or more follower pods to the cluster
func (r *RedisClusterReconciler) addFollowers(redisCluster *dbv1.RedisCluster, followersNamesToLeaderNodes map[string]*view.PodView) error {
	var wg sync.WaitGroup
	wg.Add(len(followersNamesToLeaderNodes))
	for followerName, leaderNode := range followersNamesToLeaderNodes {
		go r.addFollower(redisCluster, followerName, leaderNode, &wg)
	}
	return nil
}

func (r *RedisClusterReconciler) addFollower(redisCluster *dbv1.RedisCluster, followerName string, leaderNode *view.PodView, wg *sync.WaitGroup) {
	defer wg.Done()
	newFollowerPods, e := r.createRedisFollowerPods(redisCluster, map[string]*view.PodView{followerName: leaderNode})
	if e != nil {
		r.Log.Error(e, "Could not create redis follower pods")
		return
	}

	newFollowerPods, e = r.waitForPodReady(newFollowerPods...)
	if e != nil {
		r.Log.Error(e, "Error while waiting for pods to be ready")
		return
	}

	if e = r.waitForRedis(newFollowerPods[0].Status.PodIP); e != nil {
		r.Log.Error(e, "Error while waiting for pods to be ready")
		return
	}

	r.Log.Info(fmt.Sprintf("Replicating: %s %s", followerName, leaderNode.Name))
	if e = r.replicateLeader(newFollowerPods[0].Status.PodIP, leaderNode.Ip); e != nil {
		r.Log.Error(e, "Error while waiting for pods to be ready")
		return
	}
}

// Removes all nodes the cluster node table entries with IDs of nodes not available
// Recives the list of healthy cluster nodes (Redis is reachable and has cluster mode on)
func (r *RedisClusterReconciler) forgetLostNodes(redisCluster *dbv1.RedisCluster) error {
	v, err := r.NewRedisClusterView(redisCluster)
	if err != nil {
		println("Cloud not retrieve new cluster view")
		return err
	}
	healthyIps := make([]string, 0)
	lostIds := make([]string, 0)
	visitedById := make(map[string]bool)

	for _, node := range v.PodsViewByName {
		clusterNodes, _, err := r.RedisCLI.ClusterNodes(node.Ip)
		if err != nil {
			// retry ?
			lostIds = append(lostIds, node.NodeId)
		} else {
			healthyIps = append(healthyIps, node.Ip)
		}
		visitedById[node.NodeId] = true
		for _, tableNode := range *clusterNodes {
			isLost := strings.Contains(tableNode.Flags, "fail")
			if _, declaredAsLost := visitedById[tableNode.ID]; !declaredAsLost && isLost {
				lostIds = append(lostIds, tableNode.ID)
				visitedById[tableNode.ID] = true
			}
		}
	}

	fmt.Printf("List of lost nodes ids: %+v\n", lostIds)
	fmt.Printf("List of healthy nodes ips: %+v\n", healthyIps)
	var wg sync.WaitGroup
	wg.Add(len(healthyIps))
	for _, id := range lostIds {
		for _, ip := range healthyIps {
			go r.forgetNode(ip, id, &wg)
		}
		wg.Wait()
	}
	data, _ := json.MarshalIndent(v.ToPrintableForm(), "", "")
	clusterData.SaveRedisClusterView(data)
	return nil
}

func (r *RedisClusterReconciler) forgetNode(healthyIp string, idToForget string, wg *sync.WaitGroup) {
	defer wg.Done()
	r.RedisCLI.ClusterForget(healthyIp, idToForget)
}

// Handles the failover process for a leader. Waits for automatic failover, then
// attempts a forced failover and eventually a takeover
// Returns the ip of the promoted follower
func (r *RedisClusterReconciler) handleFailover(redisCluster *dbv1.RedisCluster, leaderName string, reachableFollowersNames []string, v *view.RedisClusterView) (string, error) {
	var promotedPodIP string = ""

	promotedPodIP, err := r.waitForFailover(redisCluster, leaderName, reachableFollowersNames, v)
	if err == nil && promotedPodIP != "" {
		return promotedPodIP, nil
	}
	r.Log.Info(fmt.Sprintf("[WARN] Automatic failover failed for leader [%s]. Attempting forced failover.", leaderName))
	// Automatic failover failed. Attempt to force failover on a healthy follower.
	for _, reachableFollowerName := range reachableFollowersNames {
		follower, exists := v.PodsViewByName[reachableFollowerName]
		if !exists {
			continue
		}
		if forcedFailoverErr := r.doFailover(follower.Ip, "force"); forcedFailoverErr != nil {
			if rediscli.IsFailoverNotOnReplica(forcedFailoverErr) {
				r.Log.Info(fmt.Sprintf("Forced failover successful on [%s](%s)", reachableFollowerName, follower.Ip))
				promotedPodIP = follower.Ip
				break
			}
			r.Log.Error(forcedFailoverErr, fmt.Sprintf("[WARN] Failed forced attempt to make node [%s](%s) leader", reachableFollowerName, follower.Ip))
		} else {
			r.Log.Info(fmt.Sprintf("Forced failover successful on [%s](%s)", reachableFollowerName, follower.Ip))
			promotedPodIP = follower.Ip
			break
		}
	}

	if promotedPodIP != "" {
		return promotedPodIP, nil
	}

	var forcedFailoverErr error

	// Forced failover failed. Attempt to takeover on a healthy follower.
	for _, reachableFollowerName := range reachableFollowersNames {
		follower, exists := v.PodsViewByName[reachableFollowerName]
		if !exists {
			continue
		}
		if forcedFailoverErr = r.doFailover(follower.Pod.Status.PodIP, "takeover"); forcedFailoverErr != nil {
			if rediscli.IsFailoverNotOnReplica(forcedFailoverErr) {
				r.Log.Info(fmt.Sprintf("Takeover successful on [%s](%s)", reachableFollowerName, follower.Ip))
				promotedPodIP = follower.Ip
				break
			}
			r.Log.Error(forcedFailoverErr, fmt.Sprintf("[WARN] Failed takeover attempt to make node [%s](%s) leader", reachableFollowerName, follower.Ip))
		} else {
			r.Log.Info(fmt.Sprintf("Takeover successful on [%s](%s)", reachableFollowerName, follower.Ip))
			promotedPodIP = follower.Pod.Status.PodIP
			break
		}
	}
	if len(promotedPodIP) > 0 {
		return promotedPodIP, nil
	}
	return "", forcedFailoverErr
}

func (r *RedisClusterReconciler) recoverCluster(redisCluster *dbv1.RedisCluster) error {
	r.Log.Info("Getting expected cluster view...")
	expectedView, err := r.GetExpectedView(cluster)
	if err != nil {
		return err
	}
	r.Log.Info("Getting actual cluster view...")
	view, err := r.NewRedisClusterView(redisCluster)
	if err != nil {
		return err
	}
	r.Log.Info(fmt.Sprintf("Removing non reachable pods...number of non reachable pods: %d", len(view.NonReachable)))
	for _, nonReachablePod := range view.NonReachable {
		deletedPod, _ := r.deletePodsByIP(nonReachablePod.Namespace, nonReachablePod.Ip)
		if len(deletedPod) > 0 {
			view.Terminating = append(view.Terminating, deletedPod...)
		}
	}
	r.Log.Info(fmt.Sprintf("Waiting for terminating pods...number of terminating pods: %d", len(view.NonReachable)))
	for _, terminatingPod := range view.Terminating {
		r.waitForPodDelete(terminatingPod)
	}

	r.Log.Info("Matching expected cluster view against actual cluster view...")
	missingLeaders, missingFollowers := r.matchViewWithExpected(view, expectedView)
	r.Log.Info(fmt.Sprintf("Missing leaders: %d, Missing followers: %d", len(missingLeaders), len(missingFollowers)))

	r.recoverLeaders(redisCluster, view, missingLeaders)
	r.recoverFollowers(redisCluster, missingFollowers)

	complete, err := r.isClusterComplete(redisCluster)
	if err != nil || !complete {
		return errors.Errorf("Cluster recovery not complete")
	}

	return r.forgetLostNodes(redisCluster)
}

func (r *RedisClusterReconciler) recoverLeaders(redisCluster *dbv1.RedisCluster, view *view.RedisClusterView, missingLeaders map[string][]string) {
	// Missing leaders holds missing leaders names and list of their *existing/reachable* followers names
	r.Log.Info("Recovering leaders...")
	for missingLeader, reachableFollowers := range missingLeaders {
		if len(reachableFollowers) > 0 {
			promotedIp, err := r.handleFailover(redisCluster, missingLeader, reachableFollowers, view)
			if err != nil {
				r.Log.Error(err, fmt.Sprintf("Could not failover leader [%s]\n", missingLeader))
				continue
			}
			err = r.recreateLeader(redisCluster, promotedIp)
			if err != nil {
				r.Log.Error(err, fmt.Sprintf("Could not re create leader [%s]\n", missingLeader))
				continue
			}
		} else {
			// find healthy leader, execute node addition by using its ip
			for _, node := range view.PodsViewByName {
				if node.IsLeader {
					err := r.reCreateFollowerslessLeader(redisCluster, missingLeader, node.Ip)
					if err != nil {
						r.Log.Error(err, fmt.Sprintf("Could not re create leader [%s]\n", missingLeader))
						continue
					}
					// reshard
					// rebalance
					break
				}
			}

		}
	}
}

func (r *RedisClusterReconciler) recoverFollowers(redisCluster *dbv1.RedisCluster, followersNamesToLeaderNodes map[string]*view.PodView) {
	// Missing followers holds missing followers names and their matching leader node
	r.Log.Info("Recovering followers...")
	if err := r.addFollowers(redisCluster, followersNamesToLeaderNodes); err != nil {
		r.Log.Error(err, fmt.Sprintf("Could not re create missing followers"))
	}
}

func (r *RedisClusterReconciler) matchViewWithExpected(v *view.RedisClusterView, expected map[string]string) (missingLeaders map[string][]string, missingFollowers map[string]*view.PodView) {
	missingLeaders = make(map[string][]string)        // holds missing leaders names and list of their *existing/reachable* followers names
	missingFollowers = make(map[string]*view.PodView) // holds missing followers names and their matching *existing/reachable* leader node
	for followerName, leaderName := range expected {
		if _, exists := v.PodsViewByName[leaderName]; !exists {
			if _, declaredMissing := missingLeaders[leaderName]; !declaredMissing {
				missingLeaders[leaderName] = make([]string, 0)
			}
		}
		if followerName != leaderName {
			if _, exists := v.PodsViewByName[followerName]; !exists {
				if leader, exists := v.PodsViewByName[leaderName]; exists {
					missingFollowers[followerName] = leader
				}
			} else {
				if _, leaderIsMissing := missingLeaders[leaderName]; leaderIsMissing {
					missingLeaders[leaderName] = append(missingLeaders[leaderName], followerName)
				}
			}
		}
	}
	return missingLeaders, missingFollowers
}

func (r *RedisClusterReconciler) updateFollower(redisCluster *dbv1.RedisCluster, followerIp string, followerName string, leaderNode *view.PodView) error {

	deletedPods, err := r.deletePodsByIP(redisCluster.Namespace, followerIp)
	if err != nil {
		return err
	} else {
		if err := r.waitForPodDelete(deletedPods...); err != nil {
			return err
		}
	}

	r.forgetLostNodes(redisCluster)

	r.Log.Info(fmt.Sprintf("Starting to add follower. Follower name: (%s) LeaderName: (%s)", followerName, leaderNode.Name))
	if err := r.addFollowers(redisCluster, map[string]*view.PodView{followerName: leaderNode}); err != nil {
		return err
	}

	return nil
}

func (r *RedisClusterReconciler) updateLeader(redisCluster *dbv1.RedisCluster, leaderIP string) error {
	// TODO handle the case where a leader has no followers
	promotedFollowerIP, err := r.doLeaderFailover(leaderIP, "")
	if err != nil {
		return err
	}

	if deletedPods, err := r.deletePodsByIP(redisCluster.Namespace, leaderIP); err != nil {
		return err
	} else {
		if err := r.waitForPodDelete(deletedPods...); err != nil {
			return err
		}
	}

	if err := r.forgetLostNodes(redisCluster); err != nil {
		return err
	}

	if err := r.recreateLeader(redisCluster, promotedFollowerIP); err != nil {
		return err
	}
	return nil
}

func (r *RedisClusterReconciler) updateCluster(redisCluster *dbv1.RedisCluster) error {
	v, err := r.NewRedisClusterView(redisCluster)
	if err != nil {
		return err
	}
	r.Log.Info("Cluster view: %+v\n", v.ToPrintableForm())
	r.Log.Info("Updating...")
	for _, node := range v.PodsViewByName {
		podUpToDate, err := r.isPodUpToDate(redisCluster, &node.Pod)
		if err != nil {
			return err
		}
		if !podUpToDate {
			if node.IsLeader {
				if err = r.updateLeader(redisCluster, node.Ip); err != nil {
					// >>> TODO the logic of checking if a leader pod (first N pods) is indeed a Redis leader must be handled separately
					if rediscli.IsNodeIsNotMaster(err) {
						if _, errDel := r.deletePodsByIP(node.Namespace, node.Ip); errDel != nil {
							return errDel
						}
					}
					return err
				}
			} else {
				if leader, exists := v.PodsViewByName[node.LeaderName]; exists {
					if err = r.updateFollower(redisCluster, node.Ip, node.Name, leader); err != nil {
						return err
					}
				} else {
					return errors.Errorf("Could not update follower node: %s due to missing leader: %s", node.Name, node.LeaderName)
				}
			}
		} else { //todo should be only followers?
			if _, pollErr := r.waitForPodReady(node.Pod); pollErr != nil {
				return pollErr
			}
			if pollErr := r.waitForRedis(node.Ip); pollErr != nil {
				return pollErr
			}
		}
	}
	r.forgetLostNodes(redisCluster)
	data, _ := json.MarshalIndent(v.ToPrintableForm(), "", "")
	clusterData.SaveRedisClusterView(data)
	return nil
}

// TODO replace with a readyness probe on the redis container
func (r *RedisClusterReconciler) waitForRedis(nodeIPs ...string) error {
	for _, nodeIP := range nodeIPs {
		r.Log.Info("Waiting for Redis on " + nodeIP)
		if nodeIP == "" {
			return errors.Errorf("Missing IP")
		}
		if pollErr := wait.PollImmediate(2*r.Config.Times.RedisPingCheckInterval, 5*r.Config.Times.RedisPingCheckTimeout, func() (bool, error) {
			reply, err := r.RedisCLI.Ping(nodeIP)
			if err != nil {
				return false, err
			}
			if strings.ToLower(strings.TrimSpace(reply)) != "pong" {
				return false, nil
			}
			return true, nil
		}); pollErr != nil {
			return pollErr
		}
	}
	return nil
}

func (r *RedisClusterReconciler) waitForClusterCreate(leaderIPs []string) error {
	r.Log.Info("Waiting for cluster create execution to complete...")
	return wait.Poll(2*r.Config.Times.ClusterCreateInterval, 5*r.Config.Times.ClusterCreateTimeout, func() (bool, error) {
		for _, leaderIP := range leaderIPs {
			clusterInfo, _, err := r.RedisCLI.ClusterInfo(leaderIP)
			if err != nil {
				return false, err
			}
			if clusterInfo.IsClusterFail() {
				return false, nil
			}
			clusterNodes, _, err := r.RedisCLI.ClusterNodes(leaderIP)
			if err != nil {
				return false, err
			}
			if len(*clusterNodes) != len(leaderIPs) {
				return false, nil
			}
		}
		return true, nil
	})
}

// Safe to be called with both followers and leaders, the call on a leader will be ignored
func (r *RedisClusterReconciler) waitForRedisSync(nodeIP string) error {
	r.Log.Info("Waiting for SYNC to start on " + nodeIP)
	if err := wait.PollImmediate(2*r.Config.Times.SyncStartCheckInterval, 5*r.Config.Times.SyncStartCheckTimeout, func() (bool, error) {
		redisInfo, _, err := r.RedisCLI.Info(nodeIP)
		if err != nil {
			return false, err
		}

		syncStatus := redisInfo.GetSyncStatus()
		if syncStatus == "" {
			return false, nil
		}

		return true, nil
	}); err != nil {
		if err.Error() != wait.ErrWaitTimeout.Error() {
			return err
		}
		r.Log.Info(fmt.Sprintf("[WARN] Timeout waiting for SYNC process to start on %s", nodeIP))
	}

	return wait.PollImmediate(2*r.Config.Times.SyncCheckInterval, 5*r.Config.Times.SyncCheckTimeout, func() (bool, error) {
		redisInfo, _, err := r.RedisCLI.Info(nodeIP)
		if err != nil {
			return false, err
		}
		syncStatus := redisInfo.GetSyncStatus()
		if syncStatus != "" {
			// after acquiring the ETA we should use it instead of a constant for waiting
			loadStatusETA := redisInfo.GetLoadETA()
			if loadStatusETA != "" {
				r.Log.Info(fmt.Sprintf("Node %s LOAD ETA: %s", nodeIP, loadStatusETA))
			} else {
				r.Log.Info(fmt.Sprintf("Node %s SYNC status: %s", nodeIP, syncStatus))
			}
			return false, nil
		}

		r.Log.Info(fmt.Sprintf("Node %s is synced", nodeIP))
		return true, nil
	})
}

func (r *RedisClusterReconciler) waitForRedisLoad(nodeIP string) error {
	r.Log.Info(fmt.Sprintf("Waiting for node %s to start LOADING", nodeIP))
	if err := wait.PollImmediate(2*r.Config.Times.LoadStartCheckInterval, 5*r.Config.Times.LoadStartCheckTimeout, func() (bool, error) {
		redisInfo, _, err := r.RedisCLI.Info(nodeIP)
		if err != nil {
			return false, err
		}

		loadStatusETA := redisInfo.GetLoadETA()
		if loadStatusETA == "" {
			return false, nil
		}

		r.Log.Info(fmt.Sprintf("node %s started to load", nodeIP))
		return true, nil
	}); err != nil {
		if err.Error() != wait.ErrWaitTimeout.Error() {
			return err
		}
		r.Log.Info(fmt.Sprintf("[WARN] timeout waiting for LOADING process to start on node %s", nodeIP))
	}

	// waiting for loading process to finish
	return wait.PollImmediate(2*r.Config.Times.LoadCheckInterval, 5*r.Config.Times.LoadCheckTimeout, func() (bool, error) {
		redisInfo, _, err := r.RedisCLI.Info(nodeIP)
		if err != nil {
			return false, err
		}

		loadStatusETA := redisInfo.GetLoadETA()
		if loadStatusETA != "" {
			r.Log.Info(fmt.Sprintf("node %s LOAD ETA: %s", nodeIP, loadStatusETA))
			return false, nil
		}

		r.Log.Info(fmt.Sprintf("node %s is fully loaded", nodeIP))
		return true, nil
	})
}

func (r *RedisClusterReconciler) waitForRedisReplication(leaderIP string, leaderID string, followerID string) error {
	r.Log.Info(fmt.Sprintf("Waiting for CLUSTER REPLICATION (%s, %s)", leaderIP, followerID))
	return wait.PollImmediate(2*r.Config.Times.RedisClusterReplicationCheckInterval, 5*r.Config.Times.RedisClusterReplicationCheckTimeout, func() (bool, error) {
		replicas, _, err := r.RedisCLI.ClusterReplicas(leaderIP, leaderID)
		if err != nil {
			return false, err
		}
		for _, replica := range *replicas {
			if replica.ID == followerID {
				return true, nil
			}
		}
		return false, nil
	})
}

func (r *RedisClusterReconciler) waitForRedisMeet(nodeIP string, newNodeIP string) error {
	r.Log.Info(fmt.Sprintf("Waiting for CLUSTER MEET (%s, %s)", nodeIP, newNodeIP))
	return wait.PollImmediate(3*r.Config.Times.RedisClusterMeetCheckInterval, 10*r.Config.Times.RedisClusterMeetCheckTimeout, func() (bool, error) {
		clusterNodes, _, err := r.RedisCLI.ClusterNodes(nodeIP)
		if err != nil {
			return false, err
		}
		for _, node := range *clusterNodes {
			if strings.Split(node.Addr, ":")[0] == newNodeIP {
				return true, nil
			}
		}
		return false, nil
	})
}

// Waits for a specified pod to be marked as master
func (r *RedisClusterReconciler) waitForManualFailover(podIP string) error {
	r.Log.Info(fmt.Sprintf("Waiting for [%s] to become leader", podIP))
	return wait.PollImmediate(2*r.Config.Times.RedisManualFailoverCheckInterval, 5*r.Config.Times.RedisManualFailoverCheckTimeout, func() (bool, error) {
		info, _, err := r.RedisCLI.Info(podIP)
		if err != nil {
			return false, err
		}
		if info.Replication["role"] == "master" {
			return true, nil
		}
		return false, nil
	})
}

// Waits for Redis to pick a new leader
// Returns the IP of the promoted follower
func (r *RedisClusterReconciler) waitForFailover(redisCluster *dbv1.RedisCluster, leaderName string, reachableFollowersNames []string, v *view.RedisClusterView) (string, error) {
	r.Log.Info(fmt.Sprintf("Waiting for leader [%s] failover", leaderName))
	var promotedFollowerIP string

	return promotedFollowerIP, wait.PollImmediate(2*r.Config.Times.RedisAutoFailoverCheckInterval, 5*r.Config.Times.RedisAutoFailoverCheckTimeout, func() (bool, error) {
		for _, reachableFollowerName := range reachableFollowersNames {
			follower, exists := v.PodsViewByName[reachableFollowerName]
			if !exists {
				continue
			}
			info, _, err := r.RedisCLI.Info(follower.Ip)
			if err != nil {
				continue
			}

			if info.Replication["role"] == "master" {
				promotedFollowerIP = follower.Ip
				return true, nil
			}
		}
		return false, nil
	})
}

func (r *RedisClusterReconciler) isPodUpToDate(redisCluster *dbv1.RedisCluster, pod *corev1.Pod) (bool, error) {
	for _, container := range pod.Spec.Containers {
		for _, crContainer := range redisCluster.Spec.RedisPodSpec.Containers {
			if crContainer.Name == container.Name {
				if !reflect.DeepEqual(container.Resources, crContainer.Resources) || crContainer.Image != container.Image {
					return false, nil
				}
			}
		}
	}
	return true, nil
}

// Checks if the image declared by the custom resource is the same as the image in the pods
func (r *RedisClusterReconciler) isClusterUpToDate(redisCluster *dbv1.RedisCluster) (bool, error) {
	pods, err := r.getRedisClusterPods(redisCluster)
	if err != nil {
		return false, err
	}
	for _, pod := range pods {
		podUpdated, err := r.isPodUpToDate(redisCluster, &pod)
		if err != nil {
			return false, err
		}
		if !podUpdated {
			return false, nil
		}
	}
	return true, nil
}

func (r *RedisClusterReconciler) isClusterComplete(redisCluster *dbv1.RedisCluster) (bool, error) {
	expectedView, err := r.GetExpectedView(cluster)
	if err != nil {
		return false, err
	}
	view, err := r.NewRedisClusterView(redisCluster)
	if err != nil {
		return false, err
	}

	missingLeaders, missingFollowers := r.matchViewWithExpected(view, expectedView)
	isComplete := len(missingLeaders) == 0 && len(missingFollowers) == 0

	data, _ := json.MarshalIndent(view.ToPrintableForm(), "", "")
	clusterData.SaveRedisClusterView(data)

	return isComplete, nil
}
