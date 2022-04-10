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

var EMPTY struct{}

// Representation of a cluster, each element contains information about a leader
type RedisClusterView []LeaderNode

type LeaderNode struct {
	Pod         *corev1.Pod
	NodeName    string
	RedisID     string
	Failed      bool
	Terminating bool
	Followers   []FollowerNode
}

type FollowerNode struct {
	Pod         *corev1.Pod
	NodeName    string
	LeaderName  string
	RedisID     string
	Failed      bool
	Terminating bool
}

func (v *RedisClusterView) String() string {
	result := ""
	for _, leader := range *v {
		leaderStatus := "ok"
		leaderPodStatus := "up"
		if leader.Pod == nil {
			leaderPodStatus = "down"
		} else if leader.Terminating {
			leaderPodStatus = "terminating"
		}
		if leader.Failed {
			leaderStatus = "fail"
		}
		result = result + fmt.Sprintf("Leader: %s(%s,%s)-[", leader.NodeName, leaderPodStatus, leaderStatus)
		for _, follower := range leader.Followers {
			status := "ok"
			podStatus := "up"
			if follower.Pod == nil {
				podStatus = "down"
			} else if follower.Terminating {
				podStatus = "terminating"
			}
			if follower.Failed {
				status = "fail"
			}
			result = result + fmt.Sprintf("%s(%s,%s)", follower.NodeName, podStatus, status)
		}
		result += "]"
	}
	return result
}

func (r *RedisClusterReconciler) NewRedisClusterView2(redisCluster *dbv1.RedisCluster) (*view.RedisClusterView, error) {
	v := &view.RedisClusterView{
		PodsViewByName:  make(map[string]*view.PodView),
		NodeIdToPodName: make(map[string]string),
	}
	pods, e := r.getRedisClusterPods(redisCluster)
	if e != nil {
		fmt.Printf("Error fetching pods viw %+v\n", e.Error())
		return v, e
	}
	v.CreateView(pods, r.RedisCLI)
	return v, nil
}

// Returns a list with all the IPs of the Redis nodes
func (v *RedisClusterView) IPs() []string {
	var ips []string
	for _, leader := range *v {
		if leader.Pod != nil {
			ips = append(ips, leader.Pod.Status.PodIP)
		}
		for _, follower := range leader.Followers {
			if follower.Pod != nil {
				ips = append(ips, follower.Pod.Status.PodIP)
			}
		}
	}
	return ips
}

// Returns a list with all the IPs of the Redis nodes that are healthy
// A nod eis healthy if Redis can be reached and is in cluster mode
func (r *RedisClusterView) HealthyNodeIPs() []string {
	var ips []string
	for _, leader := range *r {
		if leader.Pod != nil && !(leader.Failed || leader.Terminating) {
			ips = append(ips, leader.Pod.Status.PodIP)
		}
		for _, follower := range leader.Followers {
			if follower.Pod != nil && !(follower.Failed || follower.Terminating) {
				ips = append(ips, follower.Pod.Status.PodIP)
			}
		}
	}
	return ips
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
	v, e := r.NewRedisClusterView2(redisCluster)
	if e != nil {
		println("Cloud not retrieve new cluster view")
		return e
	}
	healthyIps := make([]string, 0)
	lostIds := make([]string, 0)
	visitedById := make(map[string]bool)
	for _, node := range v.PodsViewByName {
		if !node.IsReachable {
			if _, declaredAsLost := visitedById[node.NodeId]; !declaredAsLost {
				visitedById[node.NodeId] = true
				lostIds = append(lostIds, node.NodeId)
			}
			continue
		} else {
			healthyIps = append(healthyIps, node.Ip)
		}
		for _, tableNode := range node.ClusterNodesTable {
			if _, declaredAsLost := visitedById[tableNode.Id]; !declaredAsLost && !tableNode.IsReachable {
				visitedById[tableNode.Id] = true
				lostIds = append(lostIds, tableNode.Id)
			}
		}
	}
	fmt.Printf("New cluster view: %+v\n", v.ToPrintableForm())
	fmt.Printf("New view list of lost nodes: %+v\n", lostIds)
	fmt.Printf("New view list of healthy nodes: %+v\n", healthyIps)
	for _, id := range lostIds {
		r.forgetNode(healthyIps, id)
	}
	data, _ := json.MarshalIndent(v.ToPrintableForm(), "", "")
	clusterData.SaveRedisClusterView(data)
	return nil
}

// Removes a node from the cluster nodes table of all specified nodes
// nodeIPs: 	list of active node IPs
// removedID: ID of node to be removed
func (r *RedisClusterReconciler) forgetNode(nodeIPs []string, removedID string) error {
	var wg sync.WaitGroup
	errs := make(chan error, len(nodeIPs))

	for _, nodeIP := range nodeIPs {
		wg.Add(1)
		go func(ip string, wg *sync.WaitGroup) {
			defer wg.Done()
			r.Log.Info(fmt.Sprintf("Running cluster FORGET with: %s %s", ip, removedID))
			if _, err := r.RedisCLI.ClusterForget(ip, removedID); err != nil {
				// TODO we should catch here the error thrown when the ID was already removed
				errs <- err
			}
		}(nodeIP, &wg)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// Handles the failover process for a leader. Waits for automatic failover, then
// attempts a forced failover and eventually a takeover
// Returns the ip of the promoted follower
func (r *RedisClusterReconciler) handleFailover(redisCluster *dbv1.RedisCluster, leaderName string, v *view.RedisClusterView) (string, error) {
	leader := v.PodsViewByName[leaderName]
	var promotedPodIP string = ""

	promotedPodIP, err := r.waitForFailover(redisCluster, leaderName, v)
	if err == nil && promotedPodIP != "" {
		return promotedPodIP, nil
	}
	r.Log.Info(fmt.Sprintf("[WARN] Automatic failover failed for leader [%s]. Attempting forced failover.", leader.Name))
	// Automatic failover failed. Attempt to force failover on a healthy follower.
	for _, followerName := range leader.FollowersByName {
		if follower, exists := v.PodsViewByName[followerName]; exists && follower.IsReachable {
			if forcedFailoverErr := r.doFailover(follower.Ip, "force"); forcedFailoverErr != nil {
				if rediscli.IsFailoverNotOnReplica(forcedFailoverErr) {
					r.Log.Info(fmt.Sprintf("Forced failover successful on [%s](%s)", followerName, follower.Ip))
					promotedPodIP = follower.Ip
					break
				}
				r.Log.Error(forcedFailoverErr, fmt.Sprintf("[WARN] Failed forced attempt to make node [%s](%s) leader", followerName, follower.Ip))
			} else {
				r.Log.Info(fmt.Sprintf("Forced failover successful on [%s](%s)", followerName, follower.Ip))
				promotedPodIP = follower.Ip
				break
			}
		}
	}

	if promotedPodIP != "" {
		return promotedPodIP, nil
	}

	// Forced failover failed. Attempt to takeover on a healthy follower.
	for _, followerName := range leader.FollowersByName {
		if follower, exists := v.PodsViewByName[followerName]; exists && follower.IsReachable {
			if forcedFailoverErr := r.doFailover(follower.Pod.Status.PodIP, "takeover"); forcedFailoverErr != nil {
				if rediscli.IsFailoverNotOnReplica(forcedFailoverErr) {
					r.Log.Info(fmt.Sprintf("Takeover successful on [%s](%s)", followerName, follower.Ip))
					promotedPodIP = follower.Ip
					break
				}
				r.Log.Error(forcedFailoverErr, fmt.Sprintf("[WARN] Failed takeover attempt to make node [%s](%s) leader", followerName, follower.Ip))
			} else {
				r.Log.Info(fmt.Sprintf("Takeover successful on [%s](%s)", followerName, follower.Ip))
				promotedPodIP = follower.Pod.Status.PodIP
				break
			}
		}
	}
	return promotedPodIP, nil
}

func (r *RedisClusterReconciler) recoverCluster(redisCluster *dbv1.RedisCluster) error {
	v, e := r.NewRedisClusterView2(redisCluster)
	if e != nil {
		println("Cloud not retrieve new cluster view")
		return e
	}

	fmt.Printf("New cluster view: %+v\n", v.ToPrintableForm())

	var missingFollowers []NodeNames
	var failedFollowerIPs []string
	var terminatingFollowerIPs []string
	var terminatingFollowerPods []corev1.Pod

	for _, node := range v.PodsViewByName {
		if node.IsLeader {
			if !node.IsReachable {
				if !node.Exists {
					promotedPodIP, err := r.handleFailover(redisCluster, node.Name, v)
					if promotedPodIP == "" || err != nil {
						return errors.New("Leader lost, not followers to replicate data from.")
					}
					if node.IsTerminating {
						if err = r.waitForPodDelete(node.Pod); err != nil {
							return errors.Errorf("Failed to wait for leader pod to be deleted %s: %v", node.Name, err)
						}
					} else {
						_, err := r.deletePodsByIP(node.Namespace, node.Ip)
						if err != nil {
							return err
						}
						if err = r.waitForPodDelete(node.Pod); err != nil {
							return err
						}
					}
					if err := r.recreateLeader(redisCluster, promotedPodIP); err != nil {
						return err
					}
				} else {
					// pod exists and might be terminating
					for _, followerName := range node.FollowersByName {
						if follower, exists := v.PodsViewByName[followerName]; exists && follower.IsReachable {
							clusterNodes, _, err := r.RedisCLI.ClusterNodes(follower.Ip)
							if err != nil {
								continue
							}
							for _, clusterNode := range *clusterNodes {
								if clusterNode.ID == follower.NodeId && strings.Contains(clusterNode.Flags, "master") {
									if err := r.recreateLeader(redisCluster, follower.Ip); err != nil {
										return err
									}
									break
								}
							}
						}
					}
				}
			} else {
				for _, followerName := range node.FollowersByName {
					if _, exists := v.PodsViewByName[followerName]; !exists {
						missingFollowers = append(missingFollowers, NodeNames{followerName, node.Name})
					}
				}
			}
		} else {
			if node.IsTerminating {
				terminatingFollowerPods = append(terminatingFollowerPods, node.Pod)
				terminatingFollowerIPs = append(terminatingFollowerIPs, node.Ip)
				missingFollowers = append(missingFollowers, NodeNames{node.Name, node.LeaderName})
			} else if !node.IsReachable {
				failedFollowerIPs = append(failedFollowerIPs, node.Ip)
				missingFollowers = append(missingFollowers, NodeNames{node.Name, node.LeaderName})
			}
		}
	}
	deletedPods, err := r.deletePodsByIP(redisCluster.Namespace, failedFollowerIPs...)
	if err != nil {
		return err
	}
	podsToWaitFor := append(terminatingFollowerPods, deletedPods...)
	if err = r.waitForPodDelete(podsToWaitFor...); err != nil {
		return err
	}

	if len(missingFollowers) > 0 {
		if err := r.addFollowers(redisCluster, missingFollowers...); err != nil {
			return err
		}
	}
	r.forgetLostNodes(redisCluster)
	complete, err := r.isClusterComplete(redisCluster)
	if err != nil || !complete {
		return errors.Errorf("Cluster recovery not complete")
	}
	data, _ := json.MarshalIndent(v.ToPrintableForm(), "", "")
	clusterData.SaveRedisClusterView(data)
	return nil
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
	v, err := r.NewRedisClusterView2(redisCluster)
	if err != nil {
		return err
	}
	r.Log.Info("Cluster view: %+v\n", v.ToPrintableForm())
	r.Log.Info("Updating...")
	for _, node := range v.PodsViewByName {
		if node.Exists {
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
func (r *RedisClusterReconciler) waitForFailover(redisCluster *dbv1.RedisCluster, leaderName string, v *view.RedisClusterView) (string, error) {
	leader := v.PodsViewByName[leaderName]
	r.Log.Info(fmt.Sprintf("Waiting for leader [%s] failover", leaderName))
	legitFollowerFound := false
	var promotedFollowerIP string

	for _, followerName := range leader.FollowersByName {
		if follower, exists := v.PodsViewByName[followerName]; exists && follower.IsReachable {
			legitFollowerFound = true
		}
	}

	if !legitFollowerFound {
		return "", errors.Errorf("Failing leader [%s] lost all followers. Recovery unsupported.", leaderName)
	}

	return promotedFollowerIP, wait.PollImmediate(2*r.Config.Times.RedisAutoFailoverCheckInterval, 5*r.Config.Times.RedisAutoFailoverCheckTimeout, func() (bool, error) {
		for _, followerName := range leader.FollowersByName {
			follower, exists := v.PodsViewByName[followerName]
			if !exists || !follower.IsReachable {
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
	v, e := r.NewRedisClusterView2(redisCluster)
	if e != nil {
		return false, e
	}

	for _, node := range v.PodsViewByName {
		if node.IsTerminating {
			r.Log.Info(fmt.Sprintf("Found terminating node: %+v, IsLeader: %+v", node.Name, node.IsLeader))
			return false, nil
		}
		if !node.IsReachable {
			r.Log.Info(fmt.Sprintf("Found non reachable node: %+v, IsLeader: %+v", node.Name, node.IsLeader))
			return false, nil
		}
	}

	data, _ := json.MarshalIndent(v.ToPrintableForm(), "", "")
	clusterData.SaveRedisClusterView(data)

	return true, nil
}
