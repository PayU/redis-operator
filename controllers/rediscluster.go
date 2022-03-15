package controllers

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/pkg/errors"

	dbv1 "github.com/PayU/redis-operator/api/v1"
	rediscli "github.com/PayU/redis-operator/controllers/rediscli"
	clusterData "github.com/PayU/redis-operator/data"
)

var EMPTY struct{}

// Representation of a cluster, each element contains information about a leader
type RedisClusterView []LeaderNode

type LeaderNode struct {
	Pod         *corev1.Pod
	NodeNumber  string
	RedisID     string
	Failed      bool
	Terminating bool
	Followers   []FollowerNode
}

type FollowerNode struct {
	Pod          *corev1.Pod
	NodeNumber   string
	LeaderNumber string
	RedisID      string
	Failed       bool
	Terminating  bool
}

type RedisPodView struct {
	RedisNodeId string
	IP          string
	Port        string
	IsLeader    bool
}

type ClusterScaleView struct {
	CurrentLeaderCount             int
	CurrentFollowersPerLeaderCount int
	CurrentPodCount                int
	NewLeaderCount                 int
	NewFollowersPreLeaderCount     int
	NewPodCount                    int
	PodIndexToPodView              map[string]*RedisPodView
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
		result += "]"
	}
	return result
}

func (r *RedisClusterReconciler) NewRedisClusterView(redisCluster *dbv1.RedisCluster) (*RedisClusterView, error) {
	var cv RedisClusterView

	pods, err := r.getRedisClusterPods(redisCluster)
	if err != nil {
		return nil, err
	}

	followerCounter := redisCluster.Spec.LeaderCount
	for i := 0; i < redisCluster.Spec.LeaderCount; i++ {
		leader := LeaderNode{Pod: nil, NodeNumber: strconv.Itoa(i), RedisID: "", Failed: true, Terminating: false, Followers: nil}
		for j := 0; j < redisCluster.Spec.LeaderFollowersCount; j++ {
			follower := FollowerNode{Pod: nil, NodeNumber: strconv.Itoa(followerCounter), LeaderNumber: strconv.Itoa(i), RedisID: "", Failed: true, Terminating: false}
			leader.Followers = append(leader.Followers, follower)
			followerCounter++
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
		if nn == ln {
			cv[ln].Pod = &pods[i]
			cv[ln].NodeNumber = pod.Labels["node-number"]
			if pod.ObjectMeta.DeletionTimestamp != nil {
				cv[ln].Terminating = true
			} else {
				if pod.Status.PodIP != "" {
					clusterInfo, _, err := r.RedisCLI.ClusterInfo(pod.Status.PodIP)
					if err == nil && clusterInfo != nil && (*clusterInfo)["cluster_state"] == "ok" {
						cv[ln].Failed = false
					}
				}
			}
		} else {
			index := (nn - redisCluster.Spec.LeaderCount) % redisCluster.Spec.LeaderFollowersCount
			cv[ln].Followers[index].Pod = &pods[i]
			cv[ln].Followers[index].NodeNumber = pod.Labels["node-number"]
			cv[ln].Followers[index].LeaderNumber = pod.Labels["leader-number"]
			if pod.ObjectMeta.DeletionTimestamp != nil {
				cv[ln].Followers[index].Terminating = true
			} else {
				if pod.Status.PodIP != "" {
					clusterInfo, _, err := r.RedisCLI.ClusterInfo(pod.Status.PodIP)
					if err == nil && clusterInfo != nil && (*clusterInfo)["cluster_state"] == "ok" {
						cv[ln].Followers[index].Failed = false
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

type NodeNumbers [2]string // 0: node number, 1: leader number

func (r *RedisClusterReconciler) getLeaderIP(followerIP string) (string, error) {
	info, _, err := r.RedisCLI.Info(followerIP)
	if err != nil {
		return "", err
	}
	return info.Replication["master_host"], nil
}

// Returns the node number and leader number from a pod
func (r *RedisClusterReconciler) getRedisNodeNumbersFromIP(namespace string, podIP string) (string, string, error) {
	pod, err := r.getPodByIP(namespace, podIP)
	if err != nil {
		return "", "", err
	}
	return pod.Labels["node-number"], pod.Labels["leader-number"], err
}

// Returns a mapping between node numbers and IPs
func (r *RedisClusterReconciler) getNodeIPs(redisCluster *dbv1.RedisCluster) (map[string]string, error) {
	nodeIPs := make(map[string]string)
	pods, err := r.getRedisClusterPods(redisCluster)
	if err != nil {
		return nil, err
	}
	for _, pod := range pods {
		nodeIPs[pod.Labels["node-number"]] = pod.Status.PodIP
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

	var nodeNumbers []NodeNumbers
	nodeNumber := redisCluster.Spec.LeaderCount // first node numbers are reserved for leaders
	for _, leaderPod := range leaderPods {
		for i := 0; i < redisCluster.Spec.LeaderFollowersCount; i++ {
			nodeNumbers = append(nodeNumbers, NodeNumbers{strconv.Itoa(nodeNumber), leaderPod.Labels["node-number"]})
			nodeNumber++
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

	newLeaderPods, err := r.CreateRedisLeaderPods(redisCluster, leaderNumbers...)
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
		for i, followerIP := range followerIPs {
			if _, pingErr := r.RedisCLI.Ping(followerIP); pingErr == nil {
				promotedFollowerIP = followerIPs[i]
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

	newLeaderPods, err := r.CreateRedisLeaderPods(redisCluster, oldLeaderNumber)
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
		r.Log.Info(fmt.Sprintf("Replicating: %s %s", followerPod.Name, "redis-node-"+followerPod.Labels["leader-number"]))
		if err = r.replicateLeader(followerPod.Status.PodIP, nodeIPs[followerPod.Labels["leader-number"]]); err != nil {
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
		nodeTable, _, err := r.RedisCLI.ClusterNodes(healthyNodeIP)
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
	data, _ := json.MarshalIndent(clusterView, "", "")
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

func (r *RedisClusterReconciler) cleanupNodeList(podIPs []string) error {
	var wg sync.WaitGroup
	errs := make(chan error, len(podIPs))

	r.Log.Info(fmt.Sprintf("Cleaning up: %v", podIPs))

	for _, podIP := range podIPs {
		wg.Add(1)
		go func(ip string, wg *sync.WaitGroup) {
			defer wg.Done()
			clusterNodes, _, err := r.RedisCLI.ClusterNodes(ip)
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
				missingFollowers = append(missingFollowers, NodeNumbers{follower.NodeNumber, follower.LeaderNumber})
			} else if follower.Terminating {
				terminatingFollowerPods = append(terminatingFollowerPods, *follower.Pod)
				terminatingFollowerIPs = append(terminatingFollowerIPs, follower.Pod.Status.PodIP)
				missingFollowers = append(missingFollowers, NodeNumbers{follower.NodeNumber, follower.LeaderNumber})
			} else if follower.Failed {
				failedFollowerIPs = append(failedFollowerIPs, follower.Pod.Status.PodIP)
				missingFollowers = append(missingFollowers, NodeNumbers{follower.NodeNumber, follower.LeaderNumber})
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
	data, _ := json.MarshalIndent(clusterView, "", "")
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

	r.Log.Info(fmt.Sprintf("Starting to add follower: (%s %s)", pod.Labels["node-number"], pod.Labels["leader-number"]))
	if err := r.addFollowers(redisCluster, NodeNumbers{pod.Labels["node-number"], pod.Labels["leader-number"]}); err != nil {
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
				// >>> TODO the logic of checking if a leader pod (first N pods) is indeed a Redis leader must be handled separately
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
	data, _ := json.MarshalIndent(clusterView, "", "")
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
		if pollErr := wait.PollImmediate(r.Config.Times.RedisPingCheckInterval, r.Config.Times.RedisPingCheckTimeout, func() (bool, error) {
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
	return wait.Poll(r.Config.Times.ClusterCreateInterval, r.Config.Times.ClusterCreateTimeout, func() (bool, error) {
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
	if err := wait.PollImmediate(r.Config.Times.SyncStartCheckInterval, r.Config.Times.SyncStartCheckTimeout, func() (bool, error) {
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

	return wait.PollImmediate(r.Config.Times.SyncCheckInterval, r.Config.Times.SyncCheckTimeout, func() (bool, error) {
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
	if err := wait.PollImmediate(r.Config.Times.LoadStartCheckInterval, r.Config.Times.LoadStartCheckTimeout, func() (bool, error) {
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
	return wait.PollImmediate(r.Config.Times.LoadCheckInterval, r.Config.Times.LoadCheckTimeout, func() (bool, error) {
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
	return wait.PollImmediate(r.Config.Times.RedisClusterReplicationCheckInterval, r.Config.Times.RedisClusterReplicationCheckTimeout, func() (bool, error) {
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
	return wait.PollImmediate(r.Config.Times.RedisClusterMeetCheckInterval, r.Config.Times.RedisClusterMeetCheckTimeout, func() (bool, error) {
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
	return wait.PollImmediate(r.Config.Times.RedisManualFailoverCheckInterval, r.Config.Times.RedisManualFailoverCheckTimeout, func() (bool, error) {
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

	return promotedFollowerIP, wait.PollImmediate(r.Config.Times.RedisAutoFailoverCheckInterval, r.Config.Times.RedisAutoFailoverCheckTimeout, func() (bool, error) {
		for _, follower := range leader.Followers {
			if follower.Failed {
				continue
			}

			info, _, err := r.RedisCLI.Info(follower.Pod.Status.PodIP)
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
	data, _ := json.MarshalIndent(clusterView, "", "")
	clusterData.SaveRedisClusterView(data)

	return true, nil
}

func (r *RedisClusterReconciler) PrintForTests(redisCluster *dbv1.RedisCluster) error {
	v, e := r.extractClusterInfo(redisCluster)
	println("LEADER COUNT: ", v.CurrentLeaderCount)
	println("POD COUNT: ", v.CurrentPodCount)
	fmt.Println("PODS VIEW: ", v.PodIndexToPodView)
	return e
}

func (r *RedisClusterReconciler) ScaleHorizontally(redisCluster *dbv1.RedisCluster) error {
	clusterScaleView, e := r.extractClusterInfo(redisCluster)
	if e != nil {
		r.Log.Error(e, "Cluster scale-view creation failure. Could not extract cluster data.\n", e)
		return e
	}
	if r.isScaleRequired(clusterScaleView) {
		r.Log.Info("Horizontal scale: Attempting to scale leaders...")
		if e = r.scaleLeaders(clusterScaleView, redisCluster); e != nil {
			r.Log.Error(e, "Horizontal scale: Failed. Could not manage leaders scale re-arrangement.\n", e)
			return e
		}
		r.Log.Info("Horizontal scale: 1 out of 3 steps succeeded. Attempting to rebalance cluster...")
		e = r.requestRebalance(clusterScaleView)
		if e != nil {
			r.Log.Error(e, "Horizontal scale: Failed. Could not perform cluster rebalance.\n", e)
			return e
		}
		r.Log.Info("Horizontal scale: 2 out of 3 steps succeeded. Attempting to align followers to the new state...")
		if e = r.scaleFollowers(clusterScaleView, redisCluster); e != nil {
			r.Log.Error(e, "Horizontal scale: Completed improperly. Attempt to align followers to the new state was interrupted.")
			return e
		}
		r.Log.Info("Horizontal scale: 3 out of 3 steps succeeded.")
	}
	return nil
}

func (r *RedisClusterReconciler) extractClusterInfo(redisCluster *dbv1.RedisCluster) (*ClusterScaleView, error) {
	clusterView, e := r.NewRedisClusterView(redisCluster)
	if e != nil {
		return nil, e
	}

	clusterScaleView := &ClusterScaleView{
		CurrentLeaderCount:         len(*clusterView),
		CurrentPodCount:            len(clusterView.IPs()),
		NewLeaderCount:             redisCluster.Spec.LeaderCount,
		NewFollowersPreLeaderCount: redisCluster.Spec.LeaderFollowersCount,
		PodIndexToPodView:          make(map[string]*RedisPodView),
	}

	clusterScaleView.CurrentFollowersPerLeaderCount = (clusterScaleView.CurrentPodCount / clusterScaleView.CurrentLeaderCount) - 1
	clusterScaleView.NewPodCount = clusterScaleView.NewLeaderCount * (clusterScaleView.NewFollowersPreLeaderCount + 1)

	for _, leader := range *clusterView {
		if leader.Pod != nil {
			nodeId, _, e := r.getRedisNodeNumbersFromIP(redisCluster.Namespace, leader.Pod.Status.PodIP)
			if e != nil {
				return nil, e
			}
			nodeClusterId, e := r.RedisCLI.MyClusterID(leader.Pod.Status.PodIP)
			if e != nil {
				return nil, e
			}
			clusterScaleView.PodIndexToPodView[nodeId] = &RedisPodView{
				RedisNodeId: nodeClusterId,
				IP:          leader.Pod.Status.PodIP,
				Port:        r.RedisCLI.Port,
				IsLeader:    true,
			}
			for _, follower := range leader.Followers {
				if follower.Pod != nil {
					nodeId, _, e = r.getRedisNodeNumbersFromIP(redisCluster.Namespace, follower.Pod.Status.PodIP)
					if e != nil {
						return nil, e
					}
					nodeClusterId, e = r.RedisCLI.MyClusterID(follower.Pod.Status.PodIP)
					if e != nil {
						return nil, e
					}
					clusterScaleView.PodIndexToPodView[nodeId] = &RedisPodView{
						RedisNodeId: nodeClusterId,
						IP:          follower.Pod.Status.PodIP,
						Port:        r.RedisCLI.Port,
						IsLeader:    false,
					}
				}
			}
		}
	}
	return clusterScaleView, nil
}

func (r *RedisClusterReconciler) isScaleRequired(v *ClusterScaleView) bool {
	const MINIMUM_LEADERS_COUNT = 3
	if v.NewLeaderCount < MINIMUM_LEADERS_COUNT {
		r.Log.Info("[WARN] Horizontal scale: New leaders count was set to an unsupported value: %v, minimum number of leaders is %v, Attempt to perform scale horizontally won't be triggered", v.NewLeaderCount, MINIMUM_LEADERS_COUNT)
		return false
	}
	return v.CurrentLeaderCount != v.NewLeaderCount || v.CurrentFollowersPerLeaderCount != v.NewFollowersPreLeaderCount
}

func (r *RedisClusterReconciler) requestRebalance(v *ClusterScaleView) error {
	scaleUp := v.CurrentLeaderCount < v.NewLeaderCount
	scaleDown := v.CurrentLeaderCount > v.NewLeaderCount
	var e error
	if scaleUp {
		_, e = r.RedisCLI.ClusterRebalance(v.PodIndexToPodView["0"].IP, "--cluster-use-empty-masters")
	} else if scaleDown {
		_, e = r.RedisCLI.ClusterRebalance(v.PodIndexToPodView["0"].IP)
	}
	// Rebalance is needed only if number of leaders changed.
	return e
}

func (r *RedisClusterReconciler) scaleLeaders(v *ClusterScaleView, redisCluster *dbv1.RedisCluster) error {
	scaleUp := v.CurrentLeaderCount < v.NewLeaderCount
	scaleDown := v.CurrentLeaderCount > v.NewLeaderCount
	if scaleUp {
		if e := r.scaleUpLeaders(v, redisCluster); e != nil {
			return e
		}
	} else if scaleDown {
		if e := r.scaleDownLeaders(v); e != nil {
			return e
		}
	}
	return nil
}

func (r *RedisClusterReconciler) scaleUpLeaders(v *ClusterScaleView, redisCluster *dbv1.RedisCluster) error {
	for i := v.CurrentLeaderCount; i < v.NewLeaderCount; i++ {
		removedNode := v.PodIndexToPodView[string(i)]
		r.forgetRedisNodeAndDeletePod(removedNode, v)
		newLeaders, e := r.CreateRedisLeaderPods(redisCluster, string(i))
		if e != nil || len(newLeaders) == 0 {
			// re try loop
		}
		newLeader := newLeaders[0]
		newLeaderIp := newLeader.Status.PodIP
		servingLeadereIp := v.PodIndexToPodView["0"].IP
		_, e = r.RedisCLI.ClusterMeet(servingLeadereIp, newLeaderIp, r.RedisCLI.Port)
		if e != nil {
			// re try loop
		}
	}
	return nil
}

func (r *RedisClusterReconciler) scaleDownLeaders(v *ClusterScaleView) error {
	const NODES_AGREE = "[OK] All nodes agree about slots configuration."
	const SLOTS_COVERED = "[OK] All 16384 slots covered."
	LOWER_BOUND_FOR_TARGET_NODE := 0
	UPPER_BOUND_FOR_TARGET_NODE := v.NewLeaderCount - 1
	targetNode := LOWER_BOUND_FOR_TARGET_NODE
	for i := v.NewLeaderCount; i < v.CurrentLeaderCount; i++ {
		fromNodeId := v.PodIndexToPodView[string(i)].RedisNodeId
		toNodeId := v.PodIndexToPodView[string(targetNode)].RedisNodeId
		defaultNumOfSlots := ""
		servingLeadereIp := v.PodIndexToPodView["0"].IP
		r.RedisCLI.ClusterReshard(fromNodeId, toNodeId, defaultNumOfSlots, servingLeadereIp)
		if stdout, e := r.RedisCLI.ClusterCheck(servingLeadereIp); e == nil && strings.Contains(stdout, NODES_AGREE) && strings.Contains(stdout, SLOTS_COVERED) {
			slotOwnersMap, _, e := r.RedisCLI.ClusterSlots(servingLeadereIp)
			if e != nil {
				// re-try loop
			}
			if _, found := slotOwnersMap[fromNodeId]; found {
				return errors.New("Could not perform leaders scale down, reshard request was interrupted")
			}
			r.forgetRedisNodeAndDeletePod(v.PodIndexToPodView[string(i)], v)
			if targetNode == UPPER_BOUND_FOR_TARGET_NODE {
				targetNode = LOWER_BOUND_FOR_TARGET_NODE
			} else {
				targetNode++
			}
		} else {
			return errors.New("Could not perform leaders scale down, reshard request was interrupted")
		}
	}
	return nil
}

func (r *RedisClusterReconciler) scaleFollowers(v *ClusterScaleView, redisCluster *dbv1.RedisCluster) error {
	scaledUp := v.NewLeaderCount > v.CurrentLeaderCount
	var firstFollower int
	if scaledUp {
		firstFollower = v.NewLeaderCount
	} else {
		firstFollower = v.CurrentLeaderCount
	}
	if v.NewFollowersPreLeaderCount > 0 {
		for i := firstFollower; i < v.CurrentPodCount; i++ {
			newLeaderIdx := v.NewLeaderCount % i
			oldLeaderIdx := v.CurrentLeaderCount % i
			newNodeCount := v.NewLeaderCount*(v.NewFollowersPreLeaderCount+1) - 1
			if newLeaderIdx != oldLeaderIdx || i > newNodeCount {
				podView := v.PodIndexToPodView[string(i)]
				r.forgetRedisNodeAndDeletePod(podView, v)
			}
		}
	} else {
		for i := firstFollower; i < v.CurrentPodCount; i++ {
			podView := v.PodIndexToPodView[string(i)]
			r.forgetRedisNodeAndDeletePod(podView, v)
		}
	}
	return r.recoverCluster(redisCluster)
}

func (r *RedisClusterReconciler) forgetRedisNodeAndDeletePod(p *RedisPodView, v *ClusterScaleView) {
	for _, clusterPodView := range v.PodIndexToPodView {
		if clusterPodView != p {
			_, e := r.RedisCLI.ClusterForget(clusterPodView.IP, p.RedisNodeId)
			if e != nil {
				// re try loop
			}
		}
	}
	_, e := r.deletePodsByIP("", p.IP)
	if e != nil {
		// re try loop
	}
}
