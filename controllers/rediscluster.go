package controllers

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/pkg/errors"

	dbv1 "github.com/PayU/Redis-Operator/api/v1"
)

// Representation of a cluster, each element contains information about a leader
type RedisClusterView []LeaderNode

type LeaderNode struct {
	Pod        *corev1.Pod
	NodeNumber string
	RedisID    string
	Failed     bool
	Followers  []FollowerNode
}

type FollowerNode struct {
	Pod          *corev1.Pod
	NodeNumber   string
	LeaderNumber string
	RedisID      string
	Failed       bool
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
		}
		if leader.Failed {
			leaderStatus = "fail"
		}
		result = result + fmt.Sprintf("Leader: %s(%s,%s)-[", leader.NodeNumber, leaderPodStatus, leaderStatus)
		for _, follower := range leader.Followers {
			status := "ok"
			podStatus := "up"
			if follower.Pod == nil {
				status = "down"
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

	followerCounter := redisCluster.Spec.LeaderCount
	for i := 0; i < redisCluster.Spec.LeaderCount; i++ {
		leader := LeaderNode{Pod: nil, NodeNumber: strconv.Itoa(i), RedisID: "", Failed: true, Followers: nil}
		for j := 0; j < redisCluster.Spec.LeaderFollowersCount; j++ {
			follower := FollowerNode{Pod: nil, NodeNumber: strconv.Itoa(followerCounter), LeaderNumber: strconv.Itoa(i), RedisID: "", Failed: true}
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
			clusterInfo, err := r.RedisCLI.ClusterInfo(pod.Status.PodIP)
			if err == nil && (*clusterInfo)["cluster_state"] == "ok" {
				cv[ln].Failed = false
			}
		} else {
			index := (nn - redisCluster.Spec.LeaderCount) % redisCluster.Spec.LeaderFollowersCount
			cv[ln].Followers[index].Pod = &pods[i]
			cv[ln].Followers[index].NodeNumber = pod.Labels["node-number"]
			cv[ln].Followers[index].LeaderNumber = pod.Labels["leader-number"]
			clusterInfo, err := r.RedisCLI.ClusterInfo(pod.Status.PodIP)
			if err == nil && (*clusterInfo)["cluster_state"] == "ok" {
				cv[ln].Followers[index].Failed = false
			}
		}
	}
	return &cv, nil
}

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

type NodeNumbers [2]string // 0: node number, 1: leader number

func (r *RedisClusterReconciler) getLeaderIP(followerIP string) (string, error) {
	info, err := r.RedisCLI.Info(followerIP)
	if err != nil {
		return "", err
	}
	return info.Replication["master_host"], nil
}

// Returns the node number and leader number from a pod
func (r *RedisClusterReconciler) getRedisNodeNumbersFromIP(podIP string) (string, string, error) {
	pod, err := r.getPodByIP(podIP)
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

	if _, err := r.createRedisSettingConfigMap(redisCluster); err != nil {
		return err
	}

	if _, err := r.createRedisService(redisCluster); err != nil {
		return err
	}

	if _, err := r.createRedisHeadlessService(redisCluster); err != nil {
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

	if err = r.RedisCLI.ClusterCreate(nodeIPs); err != nil {
		return err
	}

	return r.waitForClusterCreate(nodeIPs)
}

// Make a new Redis node join the cluster as a follower and wait until data sync is complete
func (r *RedisClusterReconciler) replicateLeader(followerIP string, leaderIP string) error {
	leaderID, err := r.RedisCLI.MyClusterID(leaderIP)
	if err != nil {
		return err
	}

	followerID, err := r.RedisCLI.MyClusterID(followerIP)
	if err != nil {
		return err
	}

	_ = r.RedisCLI.DelFollower(leaderIP, followerID)

	if _, err := r.RedisCLI.ClusterReset(followerIP); err != nil {
		return err
	}

	if err = r.RedisCLI.AddFollower(followerIP, leaderIP, leaderID); err != nil {
		return err
	}

	if err = r.waitForRedisMeet(leaderIP, followerIP); err != nil {
		return err
	}

	return r.waitForRedisReplication(leaderIP, leaderID, followerID)
}

// Changes the role of a leader with one of its followers
// Returns the IP of the promoted follower
func (r *RedisClusterReconciler) doFailover(leaderIP string, followerIP ...string) (string, error) {
	var promotedFollowerIP string
	leaderID, err := r.RedisCLI.MyClusterID(leaderIP)
	if err != nil {
		return "", err
	}

	if len(followerIP) != 0 {
		promotedFollowerIP = followerIP[0]
	} else {
		replicas, err := r.RedisCLI.ClusterReplicas(leaderIP, leaderID)
		if err != nil {
			return "", err
		}
		if len(*replicas) == 0 {
			return "", errors.Errorf("Attempted FAILOVER on a leader (%s) with no followers. This case is not supported yet.", leaderIP)
		}
		promotedFollowerIP = strings.Split((*replicas)[0].Addr, ":")[0]
	}

	r.Log.Info("Starting FAILOVER on node " + promotedFollowerIP)
	_, err = r.RedisCLI.ClusterFailover(promotedFollowerIP)
	if err != nil {
		return "", err
	}
	if err = r.waitForRedisSync(promotedFollowerIP); err != nil {
		return "", err
	}
	if err = r.waitForFailover(promotedFollowerIP); err != nil {
		return "", err
	}
	r.Log.Info(fmt.Sprintf("[OK] Leader failover successful (%s,%s)", leaderIP, promotedFollowerIP))
	return promotedFollowerIP, nil
}

// Recreates a leader based on a replica that took its place in a failover process;
// the old leader pod must be already deleted
func (r *RedisClusterReconciler) recreateLeader(redisCluster *dbv1.RedisCluster, promotedFollowerIP string) error {
	nodeNumber, oldLeaderNumber, err := r.getRedisNodeNumbersFromIP(promotedFollowerIP)
	if err != nil {
		return err
	}
	r.Log.Info(fmt.Sprintf("Recreating leader [%s] using node [%s]", oldLeaderNumber, nodeNumber))

	newLeaderPods, err := r.createRedisLeaderPods(redisCluster, oldLeaderNumber)
	if err != nil {
		return err
	}
	newLeaderIP := newLeaderPods[0].Status.PodIP

	if err = r.replicateLeader(newLeaderIP, promotedFollowerIP); err != nil {
		return err
	}

	r.Log.Info("Leader replication successful")

	if _, err = r.doFailover(promotedFollowerIP, newLeaderIP); err != nil {
		return err
	}

	r.Log.Info(fmt.Sprintf("[OK] Leader [%s] recreated successfully; new IP: [%s]", oldLeaderNumber, newLeaderIP))
	return nil
}

// Adds a follower pod to the cluster
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

	for _, followerPod := range newFollowerPods {
		if pollErr := r.waitForPodReady(&followerPod); pollErr != nil {
			return pollErr
		}
		r.Log.Info(fmt.Sprintf("Replicating: %s %s", followerPod.Name, "redis-node-"+followerPod.Labels["leader-number"]))
		if err = r.replicateLeader(followerPod.Status.PodIP, nodeIPs[followerPod.Labels["leader-number"]]); err != nil {
			return err
		}
	}
	return nil
}

// Removes one or more follower nodes from the cluster
func (r *RedisClusterReconciler) removeFollowers(followerIPs ...string) error {
	var pods []corev1.Pod
	for _, followerIP := range followerIPs {
		followerID, err := r.RedisCLI.MyClusterID(followerIP)
		if err == nil {
			err = r.RedisCLI.DelFollower(followerIP, followerID)
			if err != nil && !(r.isConnectionLost(err) || r.isUnknownNode(err)) {
				return err
			}
		}

		followerPod, err := r.getPodByIP(followerIP)
		if err != nil {
			return err
		}

		pods = append(pods, followerPod)

		if err := r.Delete(context.Background(), &followerPod); err != nil {
			return err
		}
	}

	r.Log.Info(fmt.Sprintf("Waiting to remove follower pods %v", followerIPs))
	return r.waitForPodDelete(pods...)
}

func (r *RedisClusterReconciler) cleanupNodeList(podIPs []string) error {
	r.Log.Info("Cleaning up node lists...")
	for _, ip := range podIPs {
		clusterNodes, err := r.RedisCLI.ClusterNodes(ip)
		if err != nil {
			continue
		}
		for _, clusterNode := range *clusterNodes {
			if clusterNode.IsFailing() {
				if _, err := r.RedisCLI.ClusterForget(ip, clusterNode.ID); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (r *RedisClusterReconciler) recoverCluster(redisCluster *dbv1.RedisCluster) error {
	clusterView, err := r.NewRedisClusterView(redisCluster)
	if err != nil {
		return err
	}
	r.Log.Info(clusterView.String())
	if err = r.cleanupNodeList(clusterView.IPs()); err != nil {
		return err
	}
	for _, leader := range *clusterView {
		if leader.Failed {
			fixed := false
			for _, follower := range leader.Followers {
				if follower.Pod != nil {
					info, err := r.RedisCLI.Info(follower.Pod.Status.PodIP)
					if err != nil {
						continue
					}
					if info.Replication["role"] == "master" {
						if err := r.recreateLeader(redisCluster, follower.Pod.Status.PodIP); err != nil {
							return err
						}
						fixed = true
						break
					}
				}
			}
			if !fixed {
				return errors.Errorf("*** Permanent loss of leader node %s - recovery unsupported", leader.NodeNumber)
			}
		} else {
			var missingFollowers []NodeNumbers
			var failedFollowers []string
			for _, follower := range leader.Followers {
				if follower.Failed {
					if follower.Pod != nil {
						failedFollowers = append(failedFollowers, follower.Pod.Status.PodIP)
					}
					missingFollowers = append(missingFollowers, NodeNumbers{follower.NodeNumber, follower.LeaderNumber})
				}
			}
			if len(failedFollowers) > 0 {
				if err := r.removeFollowers(failedFollowers...); err != nil {
					return err
				}
			}
			if len(missingFollowers) > 0 {
				if err := r.addFollowers(redisCluster, missingFollowers...); err != nil {
					return err
				}
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
	pod, err := r.getPodByIP(followerIP)
	if err != nil {
		return err
	}

	if err := r.removeFollowers(followerIP); err != nil {
		return err
	}

	r.Log.Info(fmt.Sprintf("Starting to add follower: (%s %s)", pod.Labels["node-number"], pod.Labels["leader-number"]))
	if err := r.addFollowers(redisCluster, NodeNumbers{pod.Labels["node-number"], pod.Labels["leader-number"]}); err != nil {
		return err
	}

	return nil
}

func (r *RedisClusterReconciler) updateLeader(redisCluster *dbv1.RedisCluster, leaderIP string) error {
	promotedFollowerIP, err := r.doFailover(leaderIP)
	if err != nil {
		return err
	}

	if err := r.removeFollowers(leaderIP); err != nil {
		return err
	}

	if err := r.recreateLeader(redisCluster, promotedFollowerIP); err != nil {
		return err
	}
	return nil
}

func (r *RedisClusterReconciler) updateCluster(redisCluster *dbv1.RedisCluster) error {
	redisClusterView, err := r.NewRedisClusterView(redisCluster)
	if err != nil {
		return err
	}
	for _, leader := range *redisClusterView {
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
				if pollErr := r.waitForPodReady(follower.Pod); pollErr != nil {
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
				return err
			}
		} else {
			if pollErr := r.waitForPodReady(leader.Pod); pollErr != nil {
				return pollErr
			}
		}
	}
	if err = r.cleanupNodeList(redisClusterView.IPs()); err != nil {
		return err
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
	r.Log.Info("Waiting for SYNC on " + nodeIP)
	return wait.PollImmediate(genericCheckInterval, genericCheckTimeout, func() (bool, error) {
		redisInfo, err := r.RedisCLI.Info(nodeIP)
		if err != nil {
			return false, err
		}
		syncStatus := redisInfo.GetSyncStatus()
		if syncStatus != "" {
			return false, nil
		}
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

func (r *RedisClusterReconciler) waitForFailover(podIP string) error {
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

func (r *RedisClusterReconciler) isPodUpToDate(redisCluster *dbv1.RedisCluster, pod *corev1.Pod) (bool, error) {
	for _, container := range pod.Spec.Containers {
		if container.Name == "redis-container" {
			if container.Image != redisCluster.Spec.Image {
				return false, nil
			}
			if !reflect.DeepEqual(container.Resources, redisCluster.Spec.RedisContainerResources) {
				return false, nil
			}
		}
	}
	if pod.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution[0].PodAffinityTerm.TopologyKey != redisCluster.Spec.Affinity.ZoneTopologyKey ||
		pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0].TopologyKey != redisCluster.Spec.Affinity.HostTopologyKey {
		return false, nil
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
		if leader.Failed {
			r.Log.Info("Found failed leader: " + leader.NodeNumber)
			return false, nil
		}
		for _, follower := range leader.Followers {
			if follower.Failed {
				r.Log.Info("Found failed follower: " + follower.NodeNumber)
				return false, nil
			}
		}
	}
	return true, nil
}

func (r *RedisClusterReconciler) isConnectionLost(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "server closed the connection") ||
		strings.Contains(strings.ToLower(err.Error()), "connection refused")
}

func (r *RedisClusterReconciler) isUnknownNode(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "unknown node")
}
