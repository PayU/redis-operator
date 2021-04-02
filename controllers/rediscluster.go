package controllers

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/pkg/errors"

	dbv1 "github.com/PayU/Redis-Operator/api/v1"
	rediscli "github.com/PayU/Redis-Operator/controllers/rediscli"
)

var EMPTY struct{}

// Representation of a cluster, each element contains information about a leader
type RedisClusterView []LeaderNode

type LeaderNode struct {
	Info        *rediscli.RedisInfo
	Pod         *corev1.Pod
	NodeNumber  string
	Failed      bool
	Terminating bool
	Followers   []FollowerNode
}

type FollowerNode struct {
	Info         *rediscli.RedisInfo
	Pod          *corev1.Pod
	NodeNumber   string
	LeaderNumber string
	Failed       bool
	Terminating  bool
}

// In-place sort of a cluster view - ascending alphabetical order by node number
func (v *RedisClusterView) Sort() {
	sort.Slice(*v, func(i, j int) bool {
		return (*v)[i].NodeNumber < (*v)[j].NodeNumber
	})
	for _, leader := range *v {
		sort.Slice(leader.Followers, func(i, j int) bool {
			return leader.Followers[i].NodeNumber < leader.Followers[j].NodeNumber
		})
	}
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
		result = result + fmt.Sprintf("Leader: %s(%s,%s)-[", leader.NodeNumber, leaderPodStatus, leaderStatus)
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
			result = result + fmt.Sprintf("%s(%s,%s)", follower.NodeNumber, podStatus, status)
		}
		result = result + "]"
	}
	return result
}

func (r *RedisClusterReconciler) NewRedisClusterView(redisCluster *dbv1.RedisCluster) (*RedisClusterView, error) {
	var cv RedisClusterView

	pods, err := r.getRedisClusterPods(redisCluster)
	if err != nil {
		return nil, err
	}

	for i := 0; i < redisCluster.Spec.LeaderCount; i++ {
		leader := LeaderNode{Pod: nil, NodeNumber: strconv.Itoa(i), Info: nil, Failed: true, Terminating: false, Followers: nil}
		for j := 1; j <= redisCluster.Spec.LeaderFollowersCount; j++ {
			follower := FollowerNode{Pod: nil, NodeNumber: strconv.Itoa(j), LeaderNumber: strconv.Itoa(i), Info: nil, Failed: true, Terminating: false}
			leader.Followers = append(leader.Followers, follower)
		}
		cv = append(cv, leader)
	}

	for i, pod := range pods {
		nn, err := strconv.Atoi(pod.Labels["node-number"])
		if err != nil {
			return nil, errors.Errorf("Failed to parse node-number label: %s (%s)", pod.Labels["node-number"], pod.Name)
		}
		ln, err := strconv.Atoi(pod.Labels["leader-number"])
		if err != nil {
			return nil, errors.Errorf("Failed to parse leader-number label: %s (%s)", pod.Labels["leader-number"], pod.Name)
		}
		if nn == 0 {
			cv[ln].Pod = &pods[i]
			cv[ln].NodeNumber = pod.Labels["node-number"]
			if pod.ObjectMeta.DeletionTimestamp != nil {
				cv[ln].Terminating = true
			} else {
				if pod.Status.PodIP != "" {
					clusterInfo, err := r.RedisCLI.ClusterInfo(pod.Status.PodIP)
					if err == nil && clusterInfo != nil && (*clusterInfo)["cluster_state"] == "ok" {
						cv[ln].Failed = false
					}
					info, err := r.RedisCLI.Info(pod.Status.PodIP)
					if err != nil || info == nil {
						return nil, errors.Errorf("Could not get node info while building cluster view for %s", pod.Status.PodIP)
					}
					cv[ln].Info = info
				}
			}
		} else {
			cv[ln].Followers[nn-1].Pod = &pods[i]
			cv[ln].Followers[nn-1].NodeNumber = pod.Labels["node-number"]
			cv[ln].Followers[nn-1].LeaderNumber = pod.Labels["leader-number"]
			if pod.ObjectMeta.DeletionTimestamp != nil {
				cv[ln].Followers[nn-1].Terminating = true
			} else {
				if pod.Status.PodIP != "" {
					clusterInfo, err := r.RedisCLI.ClusterInfo(pod.Status.PodIP)
					if err == nil && clusterInfo != nil && (*clusterInfo)["cluster_state"] == "ok" {
						cv[ln].Followers[nn-1].Failed = false
					}
				}
			}
		}
	}
	return &cv, nil
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

type NodeNumbers [2]string // 0: leader/group number, 1: node number

func (r *RedisClusterReconciler) getLeaderIP(followerIP string) (string, error) {
	info, err := r.RedisCLI.Info(followerIP)
	if err != nil {
		return "", err
	}
	return info.Replication["master_host"], nil
}

// Returns the leader number and node number from a pod
func (r *RedisClusterReconciler) getRedisNodeNumbersFromIP(namespace string, podIP string) (string, string, error) {
	pod, err := r.getPodByIP(namespace, podIP)
	if err != nil {
		return "", "", err
	}
	return pod.Labels["leader-number"], pod.Labels["node-number"], err
}

// Returns a mapping between node numbers and IPs
// return: map["leaderNumber-nodeNumber"]: "IP"
func (r *RedisClusterReconciler) getRedisNodeIPs(redisCluster *dbv1.RedisCluster) (map[string]string, error) {
	nodeIPs := make(map[string]string)
	pods, err := r.getRedisClusterPods(redisCluster)
	if err != nil {
		return nil, err
	}
	for _, pod := range pods {
		nodeIPs[fmt.Sprintf("%s-%s", pod.Labels["leader-number"], pod.Labels["node-number"])] = pod.Status.PodIP
	}
	return nodeIPs, nil
}

func (r *RedisClusterReconciler) createNewRedisCluster(redisCluster *dbv1.RedisCluster) error {
	r.Log.Info("Creating new cluster...")

	if _, err := r.createRedisSettingConfigMap(redisCluster); err != nil {
		return err
	}

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

	var nodeNumbers []NodeNumbers
	for _, leaderPod := range leaderPods {
		for i := 1; i <= redisCluster.Spec.LeaderFollowersCount; i++ {
			nodeNumbers = append(nodeNumbers, NodeNumbers{leaderPod.Labels["leader-number"], strconv.Itoa(i)})
		}
	}

	err = r.addFollowers(redisCluster, nodeNumbers...)
	if err != nil {
		return err
	}

	r.Log.Info("[OK] Redis followers initialized successfully")
	return nil
}

func (r *RedisClusterReconciler) initializeCluster(redisCluster *dbv1.RedisCluster) error {
	var leaderNumbers []string
	// leaders are created first to increase the chance they get scheduled on different
	// AZs when using soft affinity rules
	for leaderNumber := 0; leaderNumber < redisCluster.Spec.LeaderCount; leaderNumber++ {
		leaderNumbers = append(leaderNumbers, strconv.Itoa(leaderNumber))
	}

	newLeaderPods, err := r.createRedisLeaderPods(redisCluster, leaderNumbers...)
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

// Triggeres a failover command on the specified node and waits for the follower
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
		for i, followerIP := range followerIPs {
			if _, pingErr := r.RedisCLI.Ping(followerIP); pingErr == nil {
				promotedFollowerIP = followerIPs[i]
				break
			}
		}
	} else {
		followers, err := r.RedisCLI.ClusterReplicas(leaderIP, leaderID)
		if err != nil {
			return "", err
		}
		if len(*followers) == 0 {
			return "", errors.Errorf("Attempted FAILOVER on a leader (%s) with no followers. This case is not supported yet.", leaderIP)
		}
		for i := range *followers {
			if !(*followers)[i].IsFailing() {
				promotedFollowerIP = strings.Split((*followers)[i].Addr, ":")[0]
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
	nodeNumber, oldLeaderNumber, err := r.getRedisNodeNumbersFromIP(redisCluster.Namespace, promotedFollowerIP)
	if err != nil {
		return err
	}
	r.Log.Info(fmt.Sprintf("Recreating leader [%s] using node [%s]", oldLeaderNumber, nodeNumber))

	newLeaderPods, err := r.createRedisLeaderPods(redisCluster, oldLeaderNumber)
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

	r.Log.Info(fmt.Sprintf("[OK] Leader [%s] recreated successfully; new IP: [%s]", oldLeaderNumber, newLeaderIP))
	return nil
}

// Adds one or more follower pods to the cluster
func (r *RedisClusterReconciler) addFollowers(redisCluster *dbv1.RedisCluster, nodeNumbers ...NodeNumbers) error {
	if len(nodeNumbers) == 0 {
		return errors.Errorf("Failed to add followers - no node numbers: (%s)", nodeNumbers)
	}

	newFollowerPods, err := r.createRedisFollowerPods(redisCluster, nodeNumbers...)
	if err != nil {
		return err
	}

	nodeIPs, err := r.getRedisNodeIPs(redisCluster)
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
		index := fmt.Sprintf("%s-%s", followerPod.Labels["leader-number"], "0")
		r.Log.Info(fmt.Sprintf("[%s] replicating redis-node-%s(%s)", followerPod.Name, index, nodeIPs[index]))
		if err = r.replicateLeader(followerPod.Status.PodIP, nodeIPs[index]); err != nil {
			return err
		}
	}
	return nil
}

// Removes all nodes the cluster node table entries with IDs of nodes not available
// Recives the list of healthy cluster nodes (Redis is reachable and has cluster mode on)
func (r *RedisClusterReconciler) forgetLostNodes(redisCluster *dbv1.RedisCluster) error {

	clusterView, err := r.NewRedisClusterView(redisCluster)
	if err != nil {
		return err
	}

	lostNodeIDSet := make(map[string]struct{})
	nodeMap := make(map[string]string)

	r.Log.Info("Forgetting lost nodes...")
	healthyNodeIPs := clusterView.HealthyNodeIPs()

	for _, healthyNodeIP := range healthyNodeIPs {
		healthyNodeID, err := r.RedisCLI.MyClusterID(healthyNodeIP)
		if err != nil {
			r.Log.Error(err, fmt.Sprintf("Could not reach node %s", healthyNodeIP))
			return err
		}
		nodeMap[healthyNodeIP] = healthyNodeID
	}

	for healthyNodeIP := range nodeMap {
		nodeTable, err := r.RedisCLI.ClusterNodes(healthyNodeIP)
		if err != nil || nodeTable == nil || len(*nodeTable) == 0 {
			r.Log.Info(fmt.Sprintf("[WARN] Could not forget lost nodes on node %s", healthyNodeIP))
			continue
		}
		for _, node := range *nodeTable {
			lost := true
			for _, id := range nodeMap {
				if node.ID == id {
					lost = false
					break
				}
			}
			if lost {
				lostNodeIDSet[node.ID] = EMPTY
			}
		}
	}

	for id := range lostNodeIDSet {
		r.forgetNode(healthyNodeIPs, id)
	}
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
				// TODO we should chatch here the error thrown when the ID was already removed
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

func (r *RedisClusterReconciler) cleanupNodeList(podIPs []string) error {
	var wg sync.WaitGroup
	errs := make(chan error, len(podIPs))

	r.Log.Info(fmt.Sprintf("Cleanning up: %v", podIPs))

	for _, podIP := range podIPs {
		wg.Add(1)
		go func(ip string, wg *sync.WaitGroup) {
			defer wg.Done()
			clusterNodes, err := r.RedisCLI.ClusterNodes(ip)
			if err != nil {
				// TODO node is not reachable => nothing to clean; we could consider throwing an error instead
				return
			}
			for _, clusterNode := range *clusterNodes {
				if clusterNode.IsFailing() {
					// TODO opportunity for higher concurrency - spawn a routine for each ClusterForget command
					if _, err := r.RedisCLI.ClusterForget(ip, clusterNode.ID); err != nil {
						errs <- err
						return
					}
				}
			}
		}(podIP, &wg)
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
func (r *RedisClusterReconciler) handleFailover(redisCluster *dbv1.RedisCluster, leader *LeaderNode) (string, error) {
	var promotedPodIP string = ""

	promotedPodIP, err := r.waitForFailover(redisCluster, leader)
	if err != nil || promotedPodIP == "" {
		r.Log.Info(fmt.Sprintf("[WARN] Automatic failover failed for leader [%s]. Attempting forced failover.", leader.NodeNumber))
	} else {
		return promotedPodIP, nil
	}

	// Automatic failover failed. Attempt to force failover on a healthy follower.
	for _, follower := range leader.Followers {
		if follower.Pod != nil && !follower.Failed {
			if _, pingErr := r.RedisCLI.Ping(follower.Pod.Status.PodIP); pingErr == nil {
				if forcedFailoverErr := r.doFailover(follower.Pod.Status.PodIP, "force"); forcedFailoverErr != nil {
					if rediscli.IsFailoverNotOnReplica(forcedFailoverErr) {
						r.Log.Info(fmt.Sprintf("Forced failover successful on [%s](%s)", follower.NodeNumber, follower.Pod.Status.PodIP))
						promotedPodIP = follower.Pod.Status.PodIP
						break
					}
					r.Log.Error(forcedFailoverErr, fmt.Sprintf("[WARN] Failed forced attempt to make node [%s](%s) leader", follower.NodeNumber, follower.Pod.Status.PodIP))
				} else {
					r.Log.Info(fmt.Sprintf("Forced failover successful on [%s](%s)", follower.NodeNumber, follower.Pod.Status.PodIP))
					promotedPodIP = follower.Pod.Status.PodIP
					break
				}
			}
		}
	}

	if promotedPodIP != "" {
		return promotedPodIP, nil
	}

	// Forced failover failed. Attempt to takeover on a healthy follower.
	for _, follower := range leader.Followers {
		if follower.Pod != nil && !follower.Failed {
			if _, pingErr := r.RedisCLI.Ping(follower.Pod.Status.PodIP); pingErr == nil {
				if forcedFailoverErr := r.doFailover(follower.Pod.Status.PodIP, "takeover"); forcedFailoverErr != nil {
					if rediscli.IsFailoverNotOnReplica(forcedFailoverErr) {
						r.Log.Info(fmt.Sprintf("Takeover successful on [%s](%s)", follower.NodeNumber, follower.Pod.Status.PodIP))
						promotedPodIP = follower.Pod.Status.PodIP
						break
					}
					r.Log.Error(forcedFailoverErr, fmt.Sprintf("[WARN] Failed takeover attempt to make node [%s](%s) leader", follower.NodeNumber, follower.Pod.Status.PodIP))
				} else {
					r.Log.Info(fmt.Sprintf("Takeover successful on [%s](%s)", follower.NodeNumber, follower.Pod.Status.PodIP))
					promotedPodIP = follower.Pod.Status.PodIP
					break
				}
			}
		}
	}
	return promotedPodIP, nil
}

func (r *RedisClusterReconciler) recoverCluster(redisCluster *dbv1.RedisCluster) error {
	var runLeaderRecover bool = false
	clusterView, err := r.NewRedisClusterView(redisCluster)
	if err != nil {
		return err
	}

	r.Log.Info(clusterView.String())
	for i, leader := range *clusterView {
		if leader.Failed {
			runLeaderRecover = true

			if leader.Terminating {
				if err = r.waitForPodDelete(*leader.Pod); err != nil {
					return errors.Errorf("Failed to wait for leader pod to be deleted %s: %v", leader.NodeNumber, err)
				}
			}

			promotedPodIP, err := r.handleFailover(redisCluster, &(*clusterView)[i])
			if err != nil {
				return err
			}

			if leader.Pod != nil && !leader.Terminating {
				_, err := r.deletePodsByIP(redisCluster.Namespace, leader.Pod.Status.PodIP)
				if err != nil {
					return err
				}
				if err = r.waitForPodDelete(*leader.Pod); err != nil {
					return err
				}
			}

			if err := r.forgetLostNodes(redisCluster); err != nil {
				return err
			}

			if err := r.recreateLeader(redisCluster, promotedPodIP); err != nil {
				return err
			}
		}
	}

	if runLeaderRecover {
		// we fetch again the cluster view in case the state has changed
		// since the last check (before handling the failed leaders)
		clusterView, err = r.NewRedisClusterView(redisCluster)
		if err != nil {
			return err
		}

		r.Log.Info(clusterView.String())
	}

	for _, leader := range *clusterView {
		var missingFollowers []NodeNumbers
		var failedFollowerIPs []string
		var terminatingFollowerIPs []string
		var terminatingFollowerPods []corev1.Pod

		for _, follower := range leader.Followers {
			if follower.Pod == nil {
				missingFollowers = append(missingFollowers, NodeNumbers{follower.LeaderNumber, follower.NodeNumber})
			} else if follower.Terminating {
				terminatingFollowerPods = append(terminatingFollowerPods, *follower.Pod)
				terminatingFollowerIPs = append(terminatingFollowerIPs, follower.Pod.Status.PodIP)
				missingFollowers = append(missingFollowers, NodeNumbers{follower.LeaderNumber, follower.NodeNumber})
			} else if follower.Failed {
				failedFollowerIPs = append(failedFollowerIPs, follower.Pod.Status.PodIP)
				missingFollowers = append(missingFollowers, NodeNumbers{follower.LeaderNumber, follower.NodeNumber})
			}
		}
		deletedPods, err := r.deletePodsByIP(redisCluster.Namespace, failedFollowerIPs...)
		if err != nil {
			return err
		}
		if err = r.waitForPodDelete(append(terminatingFollowerPods, deletedPods...)...); err != nil {
			return err
		}

		if len(missingFollowers) > 0 {
			if err := r.forgetLostNodes(redisCluster); err != nil {
				return err
			}
			if err := r.addFollowers(redisCluster, missingFollowers...); err != nil {
				return err
			}
		}
	}

	complete, err := r.isClusterComplete(redisCluster)
	if err != nil || !complete {
		return errors.Errorf("Cluster recovery not complete")
	}
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

	r.Log.Info(fmt.Sprintf("Starting to add follower: (%s %s)", pod.Labels["leader-number"], pod.Labels["node-number"]))
	if err := r.addFollowers(redisCluster, NodeNumbers{pod.Labels["leader-number"], pod.Labels["node-number"]}); err != nil {
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
	clusterView, err := r.NewRedisClusterView(redisCluster)
	if err != nil {
		return err
	}
	r.Log.Info(clusterView.String())
	r.Log.Info("Updating...")
	for _, leader := range *clusterView {
		for _, follower := range leader.Followers {
			podUpToDate, err := r.isPodUpToDate(redisCluster, follower.Pod)
			if err != nil {
				return err
			}
			if !podUpToDate {
				if err = r.updateFollower(redisCluster, follower.Pod.Status.PodIP); err != nil {
					return err
				}
			} else {
				if _, pollErr := r.waitForPodReady(*follower.Pod); pollErr != nil {
					return pollErr
				}
				if pollErr := r.waitForRedis(follower.Pod.Status.PodIP); pollErr != nil {
					return pollErr
				}
			}
		}
		podUpToDate, err := r.isPodUpToDate(redisCluster, leader.Pod)
		if err != nil {
			return err
		}
		if !podUpToDate {
			if err = r.updateLeader(redisCluster, leader.Pod.Status.PodIP); err != nil {
				// >>> TODO the logic of checking if a leader pod (frst N pods) is indeed a Redis leader must be handled separately
				if rediscli.IsNodeIsNotMaster(err) {
					if _, errDel := r.deletePodsByIP(redisCluster.Namespace, leader.Pod.Status.PodIP); errDel != nil {
						return errDel
					}
				}
				return err
			}
		}
	}
	if err = r.cleanupNodeList(clusterView.HealthyNodeIPs()); err != nil {
		return err
	}
	return nil
}

// TODO replace with a readyness probe on the redis container
func (r *RedisClusterReconciler) waitForRedis(nodeIPs ...string) error {
	for _, nodeIP := range nodeIPs {
		r.Log.Info("Waiting for Redis on " + nodeIP)
		if nodeIP == "" {
			return errors.Errorf("Missing IP")
		}
		if pollErr := wait.PollImmediate(genericCheckInterval, genericCheckTimeout, func() (bool, error) {
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
	return wait.Poll(clusterCreateInterval, clusterCreateTimeout, func() (bool, error) {
		for _, leaderIP := range leaderIPs {
			clusterInfo, err := r.RedisCLI.ClusterInfo(leaderIP)
			if err != nil {
				return false, err
			}
			if clusterInfo.IsClusterFail() {
				return false, nil
			}
			clusterNodes, err := r.RedisCLI.ClusterNodes(leaderIP)
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
	if err := wait.PollImmediate(syncCheckInterval, syncCheckTimeout, func() (bool, error) {
		redisInfo, err := r.RedisCLI.Info(nodeIP)
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

	return wait.PollImmediate(genericCheckInterval, genericCheckTimeout, func() (bool, error) {
		redisInfo, err := r.RedisCLI.Info(nodeIP)
		if err != nil {
			return false, err
		}
		syncStatus := redisInfo.GetSyncStatus()
		if syncStatus != "" {
			// after aquiring the ETA we should use it instead of genericCheckTimeout constant for waiting
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
	if err := wait.PollImmediate(loadCheckInterval, loadCheckTimeout, func() (bool, error) {
		redisInfo, err := r.RedisCLI.Info(nodeIP)
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
	return wait.PollImmediate(genericCheckInterval, genericCheckTimeout, func() (bool, error) {
		redisInfo, err := r.RedisCLI.Info(nodeIP)
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
	return wait.PollImmediate(genericCheckInterval, genericCheckTimeout, func() (bool, error) {
		replicas, err := r.RedisCLI.ClusterReplicas(leaderIP, leaderID)
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
	return wait.PollImmediate(genericCheckInterval, genericCheckTimeout, func() (bool, error) {
		clusterNodes, err := r.RedisCLI.ClusterNodes(nodeIP)
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
	return wait.PollImmediate(genericCheckInterval, genericCheckTimeout, func() (bool, error) {
		info, err := r.RedisCLI.Info(podIP)
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
func (r *RedisClusterReconciler) waitForFailover(redisCluster *dbv1.RedisCluster, leader *LeaderNode) (string, error) {
	r.Log.Info(fmt.Sprintf("Waiting for leader [%s] failover", leader.NodeNumber))
	failedFollowers := 0
	var promotedFollowerIP string

	for _, follower := range leader.Followers {
		if follower.Failed {
			failedFollowers++
		}
	}

	if failedFollowers == redisCluster.Spec.LeaderFollowersCount {
		return "", errors.Errorf("Failing leader [%s] lost all followers. Recovery unsupported.", leader.NodeNumber)
	}

	return promotedFollowerIP, wait.PollImmediate(genericCheckInterval, genericCheckTimeout, func() (bool, error) {
		for _, follower := range leader.Followers {
			if follower.Failed {
				continue
			}

			info, err := r.RedisCLI.Info(follower.Pod.Status.PodIP)
			if err != nil {
				continue
			}

			if info.Replication["role"] == "master" {
				promotedFollowerIP = follower.Pod.Status.PodIP
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
	clusterView, err := r.NewRedisClusterView(redisCluster)
	if err != nil {
		return false, err
	}
	for _, leader := range *clusterView {
		if leader.Terminating {
			r.Log.Info("Found terminating leader: " + leader.NodeNumber)
			return false, nil
		}
		if leader.Failed {
			r.Log.Info("Found failed leader: " + leader.NodeNumber)
			return false, nil
		}
		for _, follower := range leader.Followers {
			if follower.Terminating {
				r.Log.Info("Found terminating follower: " + follower.NodeNumber)
				return false, nil
			}
			if follower.Failed {
				r.Log.Info("Found failed follower: " + follower.NodeNumber)
				return false, nil
			}
		}
	}
	return true, nil
}

func (r *RedisClusterReconciler) isClusterScaling(redisCluster *dbv1.RedisCluster) (bool, error) {
	// use 'cluster info' to get the cluster size and check against the RDC value
	clusterView, err := r.NewRedisClusterView(redisCluster)
	if err != nil {
		return false, err
	}
	info, err := r.RedisCLI.ClusterInfo((*clusterView)[0].Pod.Status.PodIP)
	if err != nil {
		return false, err
	}
	// >>>>>>>>>>>>>>> how do I differentiae between failed replicas and unscaled replicas
	if (*info)["cluster_size"] != strconv.Itoa(redisCluster.Spec.LeaderCount) {
		return true, nil
	}
	return false, nil
}

func (r *RedisClusterReconciler) doResharding() {}

func (r *RedisClusterReconciler) scaleCluster(redisCluster *dbv1.RedisCluster) error {
	clusterView, err := r.NewRedisClusterView(redisCluster)
	clusterView.Sort()
	if err != nil {
		return err
	}
	if len(*clusterView) < 1 {
		return errors.Errorf("Cluster view empty while scaling")
	}

	if redisCluster.Spec.LeaderCount < len(*clusterView) {
		// Do scale down:
		// determine how many nodes need to be scaled down
		// the nodes that should be deleted (nodes with the highest number or node from AZ with most leaders?)
		// redis-cli cluster nodes
		// get hash ranges of leader to be removed + IDs of all leaders
		// https://redis.io/commands/cluster-slots
		// https://redis.io/commands/cluster-nodes
		// trigger resharding
		// 'lock' all the leader pods with a finalizer so they can't be deleted during resharding
		// ** the resharding can take a long time
		// check if leader is empty
		// remove all replicas
		// remove leader
		var dst []string
		nodesToScaleCount := len(*clusterView) - redisCluster.Spec.LeaderCount
		clusterNodes, err := r.RedisCLI.ClusterNodes((*clusterView)[0].Pod.Status.PodIP)
		if err != nil {
			return err
		}

		for _, node := range (*clusterView)[:len(*clusterView)-nodesToScaleCount] {
			dst = append(dst, node.Info.Server["run_id"])
		}

		for _, scaledNode := range (*clusterView)[len(*clusterView)-nodesToScaleCount:] {
			for _, node := range *clusterNodes {
				if node.ID == scaledNode.Info.Server["run_id"] {
					r.redistributeSlots(node.Slots, scaledNode.Pod.Status.PodIP, scaledNode.Info.Server["run_id"], dst)
					break
				}
			}
		}
		// set finalizer so the pod is not removed during scaling
	} else if redisCluster.Spec.LeaderCount > len(*clusterView) {
		// Do scale up:
		// create new leader pod
		// add the pod to the cluster
		// 'lock' all the leader pods with a finalizer so they can't be deleted during resharding
		// ** the resharding can take a long time
		// run the reshard command
		nodesToScaleCount := redisCluster.Spec.LeaderCount - len(*clusterView)
		newLeaderPods, err := r.createRedisLeaderPods(redisCluster, strconv.Itoa(nodesToScaleCount))
		if err != nil {
			return err
		}

		if _, err := r.waitForPodReady(newLeaderPods...); err != nil {
			return err
		}

		var nodeIPs []string
		for _, leaderPod := range newLeaderPods {
			nodeIPs = append(nodeIPs, leaderPod.Status.PodIP)
		}

		if err := r.waitForRedis(nodeIPs...); err != nil {
			return err
		}

		for _, newNodeIP := range nodeIPs {
			r.RedisCLI.AddNode(newNodeIP, (*clusterView)[0].Pod.Status.PodIP)
			// >>>>>>>>>> reshard
		}
	}
	return nil
}

// Uses redis-cli reshard method to distribute evenly the slot list from the source to all destination nodes
// slots: list of slots and slot ranges
// srcIP: IP of the node from which slots will be taken
// src: Redis node ID from which slots will be taken
// dst: list of Redis node IDs receiving the slots
func (r *RedisClusterReconciler) redistributeSlots(slots []string, srcIP string, src string, dst []string) error {
	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> check if this command can run concurrently for multiple distinct source nodes and same destination nodes <<<<<<<<<<<<<<<
	totalSlots := 0
	type Range struct {
		Start int
		End   int
		Size  int
	}
	var numericSlots []Range
	for _, slot := range slots {
		rangeDelim := strings.Index(slot, "-")
		isMigrating := strings.Index(slot, "<-") != -1 || strings.Index(slot, "->") != -1
		if isMigrating {
			return errors.Errorf("Can't redistribute slots during an ongoing migration (slots: %v | source: %s | destination: %v)", slots, src, dst)
		}
		if rangeDelim == -1 {
			rangeStart, err := strconv.Atoi(slot)
			if err != nil {
				return err
			}
			numericSlots = append(numericSlots, Range{rangeStart, -1, 1})
			totalSlots++
		} else {
			rangeStart, err := strconv.Atoi(slot[:rangeDelim])
			if err != nil {
				return err
			}
			rangeEnd, err := strconv.Atoi(slot[rangeDelim+1:])
			if err != nil {
				return err
			}
			rangeSize := (rangeEnd - rangeStart + 1)
			numericSlots = append(numericSlots, Range{rangeStart, rangeEnd, rangeSize})
			totalSlots += rangeSize
		}
	}
	var slotAllocation int = totalSlots / len(dst)
	fmt.Printf("Relocating %d slots to each of the other leaders: %v\n", slotAllocation, dst)
	for i := 0; i < len(dst)-1; i++ {
		r.RedisCLI.Reshard(srcIP, src, dst[i], strconv.Itoa(slotAllocation))
		totalSlots -= slotAllocation
	}
	r.RedisCLI.Reshard(srcIP, src, dst[len(dst)-1], strconv.Itoa(totalSlots))
	return nil
}
