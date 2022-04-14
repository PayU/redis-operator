package controllers

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

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

type NodeNames [2]string // 0: node name, 1: leader name

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
	leaderPods, err := r.getRedisClusterPods(redisCluster, "leader")
	if err != nil {
		return err
	}

	var nodeNames []NodeNames
	for _, leaderPod := range leaderPods {
		for i := 1; i <= redisCluster.Spec.LeaderFollowersCount; i++ {
			nodeNames = append(nodeNames, NodeNames{leaderPod.Labels["node-name"] + "-" + strconv.Itoa(i), leaderPod.Labels["node-name"]})
		}
	}

	err = r.addFollowers(redisCluster, nodeNames...)
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

func (r *RedisClusterReconciler) reCreateFollowerslessLeader(redisCluster *dbv1.RedisCluster, leaderName string) error {
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

	r.Log.Info(fmt.Sprintf("[OK] Leader [%s] recreated successfully; new IP: [%s]", leaderName, newLeaderIP))
	return nil
}

// Adds one or more follower pods to the cluster
func (r *RedisClusterReconciler) addFollowers(redisCluster *dbv1.RedisCluster, nodeNames ...NodeNames) error {
	if len(nodeNames) == 0 {
		return errors.Errorf("Failed to add followers - no node names: (%s)", nodeNames)
	}

	newFollowerPods, err := r.createRedisFollowerPods(redisCluster, nodeNames...)
	if err != nil {
		return err
	}

	nodeIPs, err := r.getNodeIPs(redisCluster)
	if err != nil {
		return err
	}

	pods, err := r.waitForPodReady(newFollowerPods...)
	if err != nil {
		return err
	}

	for _, followerPod := range pods {
		if err := r.waitForRedis(followerPod.Status.PodIP); err != nil {
			return err
		}
		if len(nodeIPs[followerPod.Labels["leader-name"]]) > 0 {
			r.Log.Info(fmt.Sprintf("Replicating: %s %s", followerPod.Name, followerPod.Labels["leader-name"]))
			if err = r.replicateLeader(followerPod.Status.PodIP, nodeIPs[followerPod.Labels["leader-name"]]); err != nil {
				return err
			}
		}
	}
	return nil
}

// Removes all nodes the cluster node table entries with IDs of nodes not available
// Recives the list of healthy cluster nodes (Redis is reachable and has cluster mode on)
func (r *RedisClusterReconciler) forgetLostNodes(redisCluster *dbv1.RedisCluster) error {
	v, e := r.NewRedisClusterView(redisCluster)
	if e != nil {
		println("Cloud not retrieve new cluster view")
		return e
	}
	healthyIps := make([]string, 0)
	lostIds := make([]string, 0)
	visitedById := make(map[string]bool)
	for _, node := range v.PodsViewByName {
		healthyIps = append(healthyIps, node.Ip)
		for _, tableNode := range node.ClusterNodesTable {
			if _, declaredAsLost := visitedById[tableNode.Id]; !declaredAsLost && !tableNode.IsReachable {
				visitedById[tableNode.Id] = true
				lostIds = append(lostIds, tableNode.Id)
			}
		}
	}
	fmt.Printf("New cluster view: %+v\n", v.ToPrintableForm())
	fmt.Printf("List of lost nodes ids: %+v\n", lostIds)
	fmt.Printf("List of healthy nodes ips: %+v\n", healthyIps)
	for _, id := range lostIds {
		for _, ip := range healthyIps {
			r.RedisCLI.ClusterForget(ip, id)
		}
	}
	data, _ := json.MarshalIndent(v.ToPrintableForm(), "", "")
	clusterData.SaveRedisClusterView(data)
	return nil
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
	expectedView, err := r.GetExpectedView(cluster)
	if err != nil {
		return err
	}
	view, err := r.NewRedisClusterView(redisCluster)
	if err != nil {
		return err
	}
	for _, nonReachablePod := range view.NonReachable {
		deletedPod, _ := r.deletePodsByIP(nonReachablePod.Namespace, nonReachablePod.Ip)
		if len(deletedPod) > 0 {
			view.Terminating = append(view.Terminating, deletedPod...)
		}
	}
	for _, terminatingPod := range view.Terminating {
		r.waitForPodDelete(terminatingPod)
	}

	if len(view.PodsViewByName) == 0 {
		return r.handleInitializingCluster(redisCluster)
	}

	missingLeaders, missingFollowers := r.matchViewWithExpected(view, expectedView)

	// Missing leaders holds missing leaders names and list of their *existing/reachable* followers names
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
			err = r.reCreateFollowerslessLeader(redisCluster, missingLeader)
			if err != nil {
				r.Log.Error(err, fmt.Sprintf("Could not re create leader [%s]\n", missingLeader))
				continue
			}
		}
	}

	// Missing followers holds missing followers names and their matching leader name
	// At this point all missing leaders already re created
	if len(missingFollowers) > 0 {
		if err := r.addFollowers(redisCluster, missingFollowers...); err != nil {
			r.Log.Error(err, fmt.Sprintf("Could not re create missing followers [%+v]\n", missingFollowers))
		}
	}

	complete, err := r.isClusterComplete(redisCluster)
	if err != nil || !complete {
		return errors.Errorf("Cluster recovery not complete")
	}

	return r.forgetLostNodes(redisCluster)
}

func (r *RedisClusterReconciler) matchViewWithExpected(v *view.RedisClusterView, expected map[string]string) (missingLeaders map[string][]string, missingFollowers []NodeNames) {
	missingLeaders = make(map[string][]string) // holds missing leaders names and list of their *existing/reachable* followers names
	missingFollowers = make([]NodeNames, 0)    // holds missing followers names and their matching leader name
	for followerName, leaderName := range expected {
		if _, exists := v.PodsViewByName[leaderName]; !exists {
			if _, declaredMissing := missingLeaders[leaderName]; !declaredMissing {
				missingLeaders[leaderName] = make([]string, 0)
			}
		}
		if followerName != leaderName {
			if _, exists := v.PodsViewByName[followerName]; !exists {
				missingFollowers = append(missingFollowers, NodeNames{followerName, leaderName})
			} else {
				if _, leaderIsMissing := missingLeaders[leaderName]; leaderIsMissing {
					missingLeaders[leaderName] = append(missingLeaders[leaderName], followerName)
				}
			}
		}
	}
	return missingLeaders, missingFollowers
}

func (r *RedisClusterReconciler) updateFollower(redisCluster *dbv1.RedisCluster, followerIP string) error {
	pod, err := r.getPodByIP(redisCluster.Namespace, followerIP)
	if err != nil {
		return err
	}

	deletedPods, err := r.deletePodsByIP(redisCluster.Namespace, followerIP)
	if err != nil {
		return err
	} else {
		if err := r.waitForPodDelete(deletedPods...); err != nil {
			return err
		}
	}

	if err := r.forgetLostNodes(redisCluster); err != nil {
		return err
	}

	r.Log.Info(fmt.Sprintf("Starting to add follower: (%s %s)", pod.Labels["node-name"], pod.Labels["leader-name"]))
	if err := r.addFollowers(redisCluster, NodeNames{pod.Labels["node-name"], pod.Labels["leader-name"]}); err != nil {
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
				if err = r.updateFollower(redisCluster, node.Ip); err != nil {
					return err
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
	if e := r.forgetLostNodes(redisCluster); err != nil {
		return e
	}
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
