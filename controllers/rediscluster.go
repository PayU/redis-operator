package controllers

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/pkg/errors"

	dbv1 "github.com/PayU/redis-operator/api/v1"
	rediscli "github.com/PayU/redis-operator/controllers/rediscli"
	view "github.com/PayU/redis-operator/controllers/view"
)

type ScaleType int

const (
	ScaleUpLeaders ScaleType = iota
	ScaleUpFollowers
	ScaleDownLeaders
	ScaleDownFollowers
)

func (s ScaleType) String() string {
	return [...]string{"ScaleUpLeaders", "ScaleUpFollowers", "ScaleDownLeaders", "ScaleDownFollowers"}[s]
}

func (r *RedisClusterReconciler) NewRedisClusterView(redisCluster *dbv1.RedisCluster) (*view.RedisClusterView, error) {
	v := &view.RedisClusterView{}
	pods, e := r.getRedisClusterPods(redisCluster)
	if e != nil {
		r.Log.Error(e, "Could not fetch cluster pods list for cluster view")
		return v, e
	}
	v.CreateView(pods, r.RedisCLI)
	return v, nil
}

func (r *RedisClusterReconciler) createNewRedisCluster(redisCluster *dbv1.RedisCluster) error {
	r.Log.Info("Creating new cluster...")

	if _, err := r.createRedisService(redisCluster); err != nil {
		return err
	}

	if err := r.initializeLeaders(redisCluster); err != nil {
		return err
	}
	r.Log.Info("[OK] Redis cluster initialized successfully")
	return nil
}

func (r *RedisClusterReconciler) initializeFollowers(redisCluster *dbv1.RedisCluster) error {
	r.Log.Info("Initializing followers...")
	v, err := r.NewRedisClusterView(redisCluster)
	if err != nil {
		return err
	}

	followers := make(map[string]*view.NodeView)
	for _, n := range r.RedisClusterStateView.Nodes {
		if n.Name != n.LeaderName {
			leader, exists := v.Nodes[n.LeaderName]
			if !exists {
				return errors.New(fmt.Sprintf("Missing leader detected: [%s], aborting followers initialization process\n", leader.Name))
			}
			followers[n.Name] = leader
		}
	}
	r.addFollowers(redisCluster, followers)
	r.Log.Info("[OK] Redis followers initialized successfully")
	return nil
}

func (r *RedisClusterReconciler) initializeLeaders(redisCluster *dbv1.RedisCluster) error {
	var leaderNames []string
	// leaders are created first to increase the chance they get scheduled on different
	// AZs when using soft affinity rules

	for _, n := range r.RedisClusterStateView.Nodes {
		if n.Name == n.LeaderName {
			leaderNames = append(leaderNames, n.Name)
		}
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

	if err := r.waitForClusterCreate(nodeIPs); err != nil {
		return err
	}

	for _, n := range r.RedisClusterStateView.Nodes {
		if n.Name == n.LeaderName {
			n.NodeState = view.NodeOK
		}
	}
	r.RedisClusterStateView.ClusterState = view.ClusterOK
	return nil
}

// Make a new Redis node join the cluster as a follower and wait until data sync is complete
func (r *RedisClusterReconciler) replicateLeader(followerName string, followerIP string, leaderIP string) error {
	followerID, err := r.RedisCLI.MyClusterID(followerIP)
	if err != nil {
		return err
	}

	leaderID, err := r.RedisCLI.MyClusterID(leaderIP)
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

	r.RedisClusterStateView.Nodes[followerName].NodeState = view.SyncNode
	return r.waitForRedisSync(followerIP)
}

func (r *RedisClusterReconciler) AddNewLeaderUsingReplica(newNode *view.NodeView, replica *view.NodeView, mutex *sync.Mutex) error {
	mutex.Lock()
	r.RedisClusterStateView.Nodes[newNode.Name].NodeState = view.AddNode
	mutex.Unlock()
	if stdout, err := r.RedisCLI.AddFollower(newNode.Ip, replica.Ip, replica.Id); err != nil {
		if !strings.Contains(stdout, "All nodes agree about slots configuration") {
			r.Log.Error(err, fmt.Sprintf("Error occured during process of adding new leader [%s] using replica [%s]", newNode.Name, replica.Name))
			return err
		}
	}
	if err := r.waitForRedisMeet(replica.Ip, newNode.Ip); err != nil {
		r.Log.Error(err, fmt.Sprintf("Error occured during process of adding new leader [%s] using replica [%s]", newNode.Name, replica.Name))
		return err
	}

	newNodeId, err := r.RedisCLI.MyClusterID(newNode.Ip)
	if err != nil {
		r.Log.Error(err, fmt.Sprintf("Error occured during retrieving new node id from redis pod [%s, %s]", newNode.Name, newNode.Ip))
	}
	newNode.Id = newNodeId

	r.ReplicateRedisNode(replica, newNode, mutex)
	return nil
}

func (r *RedisClusterReconciler) ReplicateRedisNode(replicateFrom *view.NodeView, replicateTo *view.NodeView, mutex *sync.Mutex) {
	mutex.Lock()
	r.RedisClusterStateView.Nodes[replicateTo.Name].NodeState = view.ReplicateNode
	mutex.Unlock()
	if err := r.waitForRedisReplication(replicateFrom.Ip, replicateFrom.Id, replicateTo.Id); err != nil {
		r.Log.Error(err, fmt.Sprintf("Redis node replication has failed for [%s]->[%s]", replicateFrom.Name, replicateTo.Name))
		return
	}
	r.SyncRedisNode(replicateTo.Name, replicateTo.Ip, mutex)
}

func (r *RedisClusterReconciler) SyncRedisNode(nodeName string, nodeIp string, mutex *sync.Mutex) {
	mutex.Lock()
	r.RedisClusterStateView.Nodes[nodeName].NodeState = view.SyncNode
	mutex.Unlock()
	if e := r.waitForRedisSync(nodeIp); e != nil {
		r.Log.Error(e, fmt.Sprintf("Redis node sync has failed for [%s]", nodeName))
		return
	}
	mutex.Lock()
	r.RedisClusterStateView.Nodes[nodeName].NodeState = view.NodeOK
	mutex.Unlock()
}

// Triggers a failover command on the specified node and waits for the follower
// to become leader
func (r *RedisClusterReconciler) doFailover(promotedNodeIp string, opt string) error {
	r.Log.Info(fmt.Sprintf("Running 'cluster failover %s' on %s", opt, promotedNodeIp))
	_, err := r.RedisCLI.ClusterFailover(promotedNodeIp, opt)
	if err != nil {
		return err
	}
	if err := r.waitForManualFailover(promotedNodeIp); err != nil {
		return err
	}
	return nil
}

// Changes the role of a leader with one of its healthy followers
// Returns the IP of the promoted follower
// leaderIP: IP of leader that will be turned into a follower
// opt: the type of failover operation ('', 'force', 'takeover')
// followerIP (optional): followers that should be considered for the failover process
func (r *RedisClusterReconciler) doLeaderFailover(leaderIP string, opt string, followerIPs []string) (string, error) {
	for _, followerIP := range followerIPs {
		e := r.attemptToFailOver(followerIP, opt)
		if e != nil {
			continue
		}
		return followerIP, nil
	}

	leaderID, e := r.RedisCLI.MyClusterID(leaderIP)
	if e != nil {
		return "", errors.New(fmt.Sprintf("Failed to perform failover from leader [%s], Error: %v", leaderIP, e.Error()))
	}
	followers, _, e := r.RedisCLI.ClusterReplicas(leaderIP, leaderID)
	if e != nil {
		return "", errors.New(fmt.Sprintf("Failed to perform failover from leader [%s], Error: %v", leaderIP, e.Error()))
	}
	if len(*followers) > 0 {
		for _, follower := range *followers {
			if !follower.IsFailing() {
				followerIP := strings.Split(follower.Addr, ":")[0]
				e := r.attemptToFailOver(followerIP, opt)
				if e != nil {
					continue
				}
				return followerIP, nil
			}
		}
	}
	return "", errors.New(fmt.Sprintf("Failed to perform failover from leader [%s]", leaderIP))
}

func (r *RedisClusterReconciler) attemptToFailOver(followerIP string, opt string) error {
	_, e := r.RedisCLI.Ping(followerIP)
	if e != nil {
		r.Log.Info(fmt.Sprintf("[Warning] Attempt to failover: ping to node ip [%s] failed", followerIP))
		return e
	}
	e = r.doFailover(followerIP, opt)
	if e != nil {
		r.Log.Info(fmt.Sprintf("[Warning] Attempt to failover with node ip [%s] failed", followerIP))
		return e
	}
	r.Log.Info(fmt.Sprintf("[OK] Attempt to failover succeeded. [%s] is a leader", followerIP))
	return nil
}

// Recreates a leader based on a replica that took its place in a failover process;
// the old leader pod must be already deleted
func (r *RedisClusterReconciler) recreateLeader(redisCluster *dbv1.RedisCluster, promotedFollowerIP string, oldLeaderName string) error {
	r.Log.Info(fmt.Sprintf("Recreating leader [%s] using node [%s]", oldLeaderName, promotedFollowerIP))

	newLeaderPods, err := r.createRedisLeaderPods(redisCluster, oldLeaderName)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
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

	if err = r.replicateLeader(oldLeaderName, newLeaderIP, promotedFollowerIP); err != nil {
		return err
	}

	r.Log.Info("Leader replication successful")

	if _, err = r.doLeaderFailover(promotedFollowerIP, "", []string{newLeaderIP}); err != nil {
		return err
	}

	r.RedisClusterStateView.Nodes[oldLeaderName].NodeState = view.NodeOK
	r.Log.Info(fmt.Sprintf("[OK] Leader [%s] recreated successfully; new IP: [%s]", oldLeaderName, newLeaderIP))
	return nil
}

func (r *RedisClusterReconciler) deleteRemovedNodes(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView, healthyServerIp string, removeFromMap bool) {
	toDeleteFromMap := []string{}
	// first delete followers
	for _, n := range r.RedisClusterStateView.Nodes {
		if n.NodeState == view.DeleteNode && n.Name != n.LeaderName {
			if node, exists := v.Nodes[n.Name]; exists {
				r.deleteNode(healthyServerIp, node)
				if removeFromMap {
					toDeleteFromMap = append(toDeleteFromMap, n.Name)
				}
			}
		}
	}
	// second delete leaders
	for _, n := range r.RedisClusterStateView.Nodes {
		if n.NodeState == view.DeleteNode {
			if node, exists := v.Nodes[n.Name]; exists {
				r.deleteNode(healthyServerIp, node)
				if removeFromMap {
					toDeleteFromMap = append(toDeleteFromMap, n.Name)
				}
			}
		}
	}
	// third delete pods that been removed from map but interrupted during deletion
	for _, p := range v.Nodes {
		if _, exists := r.RedisClusterStateView.Nodes[p.Name]; !exists {
			r.deletePod(p.Pod)
		}
	}
	// four remove from map in case of need
	for _, d := range toDeleteFromMap {
		delete(r.RedisClusterStateView.Nodes, d)
	}
}

func (r *RedisClusterReconciler) deleteNode(healthyServerIp string, n *view.NodeView) {
	r.RedisCLI.DelNode(healthyServerIp, n.Id)
	r.deletePod(n.Pod)
	r.RedisClusterStateView.Nodes[n.Name].NodeState = view.CreateNode
}

func (r *RedisClusterReconciler) addLeaders(redisCluster *dbv1.RedisCluster, healthyLeaderIp string, leaderNames []string) error {
	leaders, e := r.createRedisLeaderPods(redisCluster, leaderNames...)
	if e != nil || len(leaders) == 0 {
		r.Log.Error(e, "Could not add new leaders")
		return e
	}
	var wg sync.WaitGroup
	wg.Add(len(leaders))
	for _, leader := range leaders {
		r.RedisClusterStateView.Nodes[leader.Name].NodeState = view.AddNode
		go r.addLeader(redisCluster, leader, healthyLeaderIp, &wg)
	}
	wg.Wait()
	return nil
}

func (r *RedisClusterReconciler) addLeader(redisCluster *dbv1.RedisCluster, leaderPod corev1.Pod, healthyServerIp string, wg *sync.WaitGroup) {
	defer wg.Done()
	leader, e := r.waitForPodReady(leaderPod)
	if e != nil || len(leader) == 0 {
		r.Log.Error(e, fmt.Sprintf("Error while waiting for pod [%s] to be ready\n", leaderPod.Name))
		return
	}
	leaderPod = leader[0]
	e = r.waitForRedis(leaderPod.Status.PodIP)
	if e != nil {
		r.Log.Error(e, fmt.Sprintf("Error while waiting for pod [%s] to be ready\n", leaderPod.Name))
		return
	}
	_, e = r.RedisCLI.AddLeader(leaderPod.Status.PodIP, healthyServerIp)
	if e != nil {
		r.Log.Error(e, fmt.Sprintf("Error while adding pod [%s] to redis cluster, healthy node ip: [%s]", leaderPod.Name, healthyServerIp))
		return
	}
	r.RedisClusterStateView.Nodes[leaderPod.Name].NodeState = view.NodeOK
}

// Adds one or more follower pods to the cluster
func (r *RedisClusterReconciler) addFollowers(redisCluster *dbv1.RedisCluster, missingFollowerToLeaderNode map[string]*view.NodeView) {
	var followerPods []corev1.Pod
	createOpts := []client.CreateOption{client.FieldOwner("redis-operator-controller")}
	for followerName, leader := range missingFollowerToLeaderNode {
		pod, e := r.makeFollowerPod(redisCluster, followerName, leader.Name)
		if e != nil {
			r.Log.Error(e, fmt.Sprintf("Could not make follower pod [%s]", followerName))
			r.deletePod(&pod)
			continue
		}
		e = r.Create(context.Background(), &pod, createOpts...)
		if e != nil {
			if !strings.Contains(e.Error(), "already exists") {
				r.Log.Error(e, fmt.Sprintf("Could not create follower pod [%s]", followerName))
				r.deletePod(&pod)
				continue
			}
		}
		r.Log.Info(fmt.Sprintf("New follower pod [%s] created", followerName))
		followerPods = append(followerPods, pod)
		n, exists := r.RedisClusterStateView.Nodes[followerName]
		if exists {
			n.NodeState = view.AddNode
		} else {
			r.RedisClusterStateView.Nodes[followerName] = &view.NodeStateView{
				Name:       followerName,
				LeaderName: leader.Name,
				NodeState:  view.AddNode,
			}
		}
	}
	followers, e := r.waitForPodNetworkInterface(followerPods...)
	if e != nil {
		r.Log.Error(e, fmt.Sprintf("Error occured during waiting to pod network interface"))
		r.deletePods(followerPods)
		return
	}
	var wg sync.WaitGroup
	var mutex = &sync.Mutex{}
	wg.Add(len(followers))
	for _, follower := range followers {
		leader := missingFollowerToLeaderNode[follower.Name]
		go r.addFollower(redisCluster, follower, leader.Name, leader.Ip, &wg, mutex)
	}
	wg.Wait()
}

func (r *RedisClusterReconciler) addFollower(redisCluster *dbv1.RedisCluster, followerPod corev1.Pod, leaderName string, leaderIp string, wg *sync.WaitGroup, mutex *sync.Mutex) {
	defer wg.Done()
	newFollower, e := r.waitForPodReady(followerPod)
	if e != nil {
		r.Log.Error(e, fmt.Sprintf("Error while waiting for pod [%s] to be ready\n", followerPod.Name))
		return
	}
	if e = r.waitForRedis(newFollower[0].Status.PodIP); e != nil {
		r.Log.Error(e, fmt.Sprintf("Error while waiting for pod [%s] to be ready\n", followerPod.Name))
		return
	}

	follower := newFollower[0]
	mutex.Lock()
	n, exists := r.RedisClusterStateView.Nodes[follower.Name]
	if exists {
		n.NodeState = view.ReplicateNode
	} else {
		r.RedisClusterStateView.Nodes[follower.Name] = &view.NodeStateView{
			Name:       followerPod.Name,
			LeaderName: leaderName,
			NodeState:  view.ReplicateNode,
		}
	}

	mutex.Unlock()
	r.Log.Info(fmt.Sprintf("Replicating: %s %s", follower.Name, leaderName))

	if e = r.replicateLeader(follower.Name, follower.Status.PodIP, leaderIp); e != nil {
		r.Log.Error(e, fmt.Sprintf("Error while waiting for pod to be replicated [%s]->[%s]", leaderIp, follower.Status.PodIP))
		return
	}
	mutex.Lock()
	n, exists = r.RedisClusterStateView.Nodes[follower.Name]
	if exists {
		n.NodeState = view.NodeOK
	} else {
		r.RedisClusterStateView.Nodes[follower.Name] = &view.NodeStateView{
			Name:       followerPod.Name,
			LeaderName: leaderName,
			NodeState:  view.NodeOK,
		}
	}
	mutex.Unlock()
}

// Removes all nodes the cluster node table entries with IDs of nodes not available
// Recives the list of healthy cluster nodes (Redis is reachable and has cluster mode on)
func (r *RedisClusterReconciler) forgetLostNodes(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) error {
	healthyIps := make([]string, 0)
	lostIds := make([]string, 0)
	visitedById := make(map[string]bool)

	for _, node := range v.Nodes {
		podIp := node.Ip
		nodeId := node.Id
		clusterNodes, _, err := r.RedisCLI.ClusterNodes(podIp)
		if err != nil {
			lostIds = append(lostIds, nodeId)
		} else {
			healthyIps = append(healthyIps, podIp)
		}
		visitedById[nodeId] = true
		for _, tableNode := range *clusterNodes {
			isLost := strings.Contains(tableNode.Flags, "fail")
			if _, declaredAsLost := visitedById[tableNode.ID]; !declaredAsLost && isLost {
				lostIds = append(lostIds, tableNode.ID)
				visitedById[tableNode.ID] = true
			}
		}
	}

	if len(lostIds) > 0 {
		r.Log.Info(fmt.Sprintf("List of lost nodes ids: %v", lostIds))
		var forgetByAllWG sync.WaitGroup
		forgetByAllWG.Add(len(lostIds))
		for _, id := range lostIds {
			r.forgetNodeByAllHealthyIps(healthyIps, id, &forgetByAllWG)
		}
		forgetByAllWG.Wait()
	}

	return nil
}

func (r *RedisClusterReconciler) forgetNodeByAllHealthyIps(healthyIps []string, nodeId string, wg *sync.WaitGroup) {
	defer wg.Done()
	var forgetByOneWG sync.WaitGroup
	forgetByOneWG.Add(len(healthyIps))
	for _, ip := range healthyIps {
		go r.forgetNode(ip, nodeId, &forgetByOneWG)
	}
	forgetByOneWG.Wait()
}

func (r *RedisClusterReconciler) forgetNode(healthyIp string, idToForget string, wg *sync.WaitGroup) {
	defer wg.Done()
	r.RedisCLI.ClusterForget(healthyIp, idToForget)
}

// Handles the failover process for a leader. Waits for automatic failover, then
// attempts a forced failover and eventually a takeover
// Returns the ip of the promoted follower
func (r *RedisClusterReconciler) handleFailover(redisCluster *dbv1.RedisCluster, leaderName string, reachableFollowers []corev1.Pod) (corev1.Pod, error) {
	promotedPod, err := r.waitForFailover(redisCluster, leaderName, reachableFollowers)
	if err == nil {
		return promotedPod, nil
	}

	r.Log.Info(fmt.Sprintf("[WARN] Automatic failover failed for leader [%s]. Attempting forced failover.", leaderName))
	// Automatic failover failed. Attempt to force failover on a healthy follower.
	for _, follower := range reachableFollowers {
		followerIP := follower.Status.PodIP
		if forcedFailoverErr := r.doFailover(followerIP, "force"); forcedFailoverErr != nil {
			if rediscli.IsFailoverNotOnReplica(forcedFailoverErr) {
				r.Log.Info(fmt.Sprintf("Forced failover successful on [%s](%s)", follower.Name, followerIP))
				return follower, nil
			}
			r.Log.Error(forcedFailoverErr, fmt.Sprintf("[WARN] Failed forced attempt to make node [%s](%s) leader", follower.Name, followerIP))
		} else {
			r.Log.Info(fmt.Sprintf("Forced failover successful on [%s](%s)", follower.Name, followerIP))
			return follower, nil
		}
	}

	var forcedFailoverErr error
	// Forced failover failed. Attempt to takeover on a healthy follower.
	for _, follower := range reachableFollowers {
		followerIP := follower.Status.PodIP
		if forcedFailoverErr = r.doFailover(follower.Status.PodIP, "takeover"); forcedFailoverErr != nil {
			if rediscli.IsFailoverNotOnReplica(forcedFailoverErr) {
				r.Log.Info(fmt.Sprintf("Takeover successful on [%s](%s)", follower.Name, followerIP))
				return follower, nil
			}
			r.Log.Error(forcedFailoverErr, fmt.Sprintf("[WARN] Failed takeover attempt to make node [%s](%s) leader", follower.Name, followerIP))
		} else {
			r.Log.Info(fmt.Sprintf("Takeover successful on [%s](%s)", follower.Name, follower.Status.PodIP))
			return follower, nil
		}
	}
	return corev1.Pod{}, forcedFailoverErr
}

func (r *RedisClusterReconciler) recoverCluster(redisCluster *dbv1.RedisCluster) error {

	r.Log.Info("Getting cluster view...")
	v, err := r.NewRedisClusterView(redisCluster)
	if err != nil {
		return err
	}

	failingPodsCleared := r.waitForNonReachablePodsTermination(redisCluster, v)
	if failingPodsCleared {
		return nil
	}

	r.Log.Info("Validating cluster state...")
	recoveryRequired, err := r.RecoverRedisCluster(redisCluster, v)
	if err != nil {
		return err
	}

	if !recoveryRequired {
		r.Log.Info("Validating leaders state...")
		recoveryRequired, err := r.RecoverLeaders(redisCluster, v)
		if err != nil {
			return err
		}
		if !recoveryRequired {
			r.Log.Info("Validating followers state...")
			recoveryRequired, _ = r.RecoverFollowers(redisCluster, v)
		}
	}

	if !recoveryRequired {
		complete, err := r.isClusterComplete(redisCluster, v)
		recoveryComplete := complete && err == nil
		if err != nil {
			r.Log.Error(err, "Could not perform cluster-complete validation")
		}
		r.Log.Info(fmt.Sprintf("Recovery complete: %v", recoveryComplete))
		if recoveryComplete {
			redisCluster.Status.ClusterState = string(Ready)
		}
	}
	defer r.updateClusterStateView(redisCluster)
	return nil
}

func (r *RedisClusterReconciler) recoverInterruptedFlows(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) {
	for _, n := range r.RedisClusterStateView.Nodes {
		if n.NodeState == view.ReshardNode {
			// faind leader
			// reshard
			// change state
			return
		}
		masterReplicaIp := ""
		for _, clusterNode := range v.Nodes {
			if clusterNode.LeaderName == n.LeaderName {
				info, _, err := r.RedisCLI.Info(clusterNode.Ip)
				if err != nil || info == nil {
					continue
				}
				if info.Replication["role"] == "master" {
					masterReplicaIp = clusterNode.Ip
					break
				}
			}
		}
		if len(masterReplicaIp) > 0 {
			switch n.NodeState {
			case view.ReplicateNode:
				// replicate
				// sync
				// if leader then failover
				break
			case view.SyncNode:
				// sync
				// if leader then failover
				break
			case view.FailoverNode:
				// failover
				break
			}
		} else {
			n.NodeState = view.DeleteNode
		}
	}
}

func (r *RedisClusterReconciler) RecoverRedisCluster(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) (bool, error) {
	s := r.RedisClusterStateView.ClusterState
	r.Log.Info(fmt.Sprintf("Cluster state: %v", s))
	switch s {
	case view.ClusterOK:
		return false, nil
	case view.ClusterFix:
		healthyLeaderName := r.findHealthyLeader(v)
		if len(healthyLeaderName) == 0 {
			return true, errors.New("Could not find healthy reachable leader to serve cluster fix request")
		}
		healthyLeaderIp := v.Nodes[healthyLeaderName].Ip
		_, stdout, e := r.RedisCLI.ClusterFix(healthyLeaderIp)
		if e != nil && !strings.Contains(e.Error(), "[OK] All 16384 slots covered") && !strings.Contains(stdout, "[OK] All 16384 slots covered") {
			return true, e
		}
		r.RedisClusterStateView.ClusterState = view.ClusterRebalance
		return true, nil
	case view.ClusterRebalance:
		healthyLeaderName := r.findHealthyLeader(v)
		if len(healthyLeaderName) == 0 {
			return true, errors.New("Could not find healthy reachable leader to serve cluster rebalance request")
		}
		healthyLeaderIp := v.Nodes[healthyLeaderName].Ip
		rebalanced, _, e := r.RedisCLI.ClusterRebalance(healthyLeaderIp, true)
		if !rebalanced || e != nil {
			r.RedisClusterStateView.ClusterState = view.ClusterFix
			println(r.RedisClusterStateView.ClusterState)
			return true, e
		}
		r.RedisClusterStateView.ClusterState = view.ClusterOK
		return true, nil
	}
	return false, nil
}

func (r *RedisClusterReconciler) waitForNonReachablePodsTermination(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) bool {
	terminatingPods := []corev1.Pod{}
	nonReachablePods := []corev1.Pod{}
	for _, node := range v.Nodes {
		pod := node.Pod
		if pod.Status.Phase == "Terminating" {
			terminatingPods = append(terminatingPods, *pod)
			continue
		}
		clusterInfo, _, e := r.RedisCLI.ClusterInfo(pod.Status.PodIP)
		if e != nil || (*clusterInfo) == nil || (*clusterInfo)["cluster_state"] != "ok" {
			nonReachablePods = append(nonReachablePods, *pod)
			continue
		}
	}
	if len(nonReachablePods) > 0 {
		r.Log.Info(fmt.Sprintf("Removing non reachable pods...number of non reachable pods: %d", len(nonReachablePods)))
		deletedPods, _ := r.deletePods(nonReachablePods)
		if len(deletedPods) > 0 {
			terminatingPods = append(terminatingPods, deletedPods...)
		}
	}

	if len(terminatingPods) > 0 {
		r.Log.Info(fmt.Sprintf("Waiting for terminating pods...number of terminating pods: %d", len(terminatingPods)))
		for _, terminatingPod := range terminatingPods {
			r.waitForPodDelete(terminatingPod)
		}
	}
	return len(terminatingPods)+len(nonReachablePods) > 0
}

func (r *RedisClusterReconciler) RecoverFollowers(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) (bool, error) {
	missingFollowerToLeaderNode := make(map[string]*view.NodeView)
	for _, n := range r.RedisClusterStateView.Nodes {
		if n.Name != n.LeaderName {
			if _, exists := v.Nodes[n.Name]; !exists || n.NodeState == view.CreateNode {
				leaderNode, leaderExists := v.Nodes[n.LeaderName]
				if leaderExists {
					missingFollowerToLeaderNode[n.Name] = leaderNode
				}
			}
		}
	}
	r.addFollowers(redisCluster, missingFollowerToLeaderNode)
	return len(missingFollowerToLeaderNode) == 0, nil
}

func (r *RedisClusterReconciler) updateFollower(redisCluster *dbv1.RedisCluster, followerPod corev1.Pod) error {

	deletedPods, err := r.deletePods([]corev1.Pod{followerPod})
	if err != nil {
		return err
	} else {
		if err := r.waitForPodDelete(deletedPods...); err != nil {
			return err
		}
	}

	followerName := followerPod.Name
	leaderName := followerPod.Labels["leader-name"]
	r.Log.Info(fmt.Sprintf("Starting to add follower. Follower name: (%s) LeaderName: (%s)", followerName, leaderName))

	leaderPod, e := r.getRedisClusterPodsByLabel(redisCluster, "node-name", leaderName)
	if e != nil || len(leaderPod) == 0 {
		r.Log.Error(e, fmt.Sprintf("Error while retrieving leader pod [%s]\n", leaderName))
		return e
	}

	var wg sync.WaitGroup
	mutex := &sync.Mutex{}
	wg.Add(1)
	r.addFollower(redisCluster, followerPod, leaderName, leaderPod[0].Status.PodIP, &wg, mutex)
	if err != nil {
		return err
	}
	wg.Wait()
	return nil
}

func (r *RedisClusterReconciler) updateLeader(redisCluster *dbv1.RedisCluster, leaderIP string, v *view.RedisClusterView) error {
	promotedFollowerIP, err := r.doLeaderFailover(leaderIP, "", []string{})
	if err != nil {
		return err
	}

	var leader *view.NodeView
	for _, n := range v.Nodes {
		if n.Ip == promotedFollowerIP {
			leader = n
			break
		}
	}

	if leader == nil {
		return errors.New("Could not find pod name for ip " + promotedFollowerIP)
	}

	if deletedPods, err := r.deletePods([]corev1.Pod{*leader.Pod}); err != nil {
		return err
	} else {
		if err := r.waitForPodDelete(deletedPods...); err != nil {
			return err
		}
	}
	if err := r.recreateLeader(redisCluster, promotedFollowerIP, leader.Name); err != nil {
		return err
	}
	return nil
}

func (r *RedisClusterReconciler) updateCluster(redisCluster *dbv1.RedisCluster) error {
	r.Log.Info("Updating Cluster Pods...")
	v, err := r.NewRedisClusterView(cluster)
	if err != nil {
		return err
	}
	for _, node := range v.Nodes {
		podUpToDate, err := r.isPodUpToDate(redisCluster, node.Pod)
		if err != nil {
			return err
		}
		if !podUpToDate {
			if node.Name == node.LeaderName {
				if err = r.updateLeader(redisCluster, node.Ip, v); err != nil {
					if rediscli.IsNodeIsNotMaster(err) {
						if _, errDel := r.deletePods([]corev1.Pod{*node.Pod}); errDel != nil {
							return errDel
						}
					}
					return err
				}
			} else {
				if err = r.updateFollower(redisCluster, *node.Pod); err != nil {
					return err
				}
			}
		} else {
			if _, pollErr := r.waitForPodReady(*node.Pod); pollErr != nil {
				return pollErr
			}
			if pollErr := r.waitForRedis(node.Ip); pollErr != nil {
				return pollErr
			}
		}
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
		if pollErr := wait.PollImmediate(3*r.Config.Times.RedisPingCheckInterval, 10*r.Config.Times.RedisPingCheckTimeout, func() (bool, error) {
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
	return wait.Poll(3*r.Config.Times.ClusterCreateInterval, 10*r.Config.Times.ClusterCreateTimeout, func() (bool, error) {
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

		if redisInfo != nil {
			syncStatus := redisInfo.GetSyncStatus()
			if syncStatus == "" {
				return false, nil
			}
		} else {
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
	if err := wait.PollImmediate(3*r.Config.Times.LoadStartCheckInterval, 10*r.Config.Times.LoadStartCheckTimeout, func() (bool, error) {
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
	return wait.PollImmediate(3*r.Config.Times.LoadCheckInterval, 10*r.Config.Times.LoadCheckTimeout, func() (bool, error) {
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
	return wait.PollImmediate(3*r.Config.Times.RedisClusterReplicationCheckInterval, 10*r.Config.Times.RedisClusterReplicationCheckTimeout, func() (bool, error) {
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
	return wait.PollImmediate(3*r.Config.Times.RedisManualFailoverCheckInterval, 10*r.Config.Times.RedisManualFailoverCheckTimeout, func() (bool, error) {
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
func (r *RedisClusterReconciler) waitForFailover(redisCluster *dbv1.RedisCluster, leaderName string, reachableFollowers []corev1.Pod) (corev1.Pod, error) {
	r.Log.Info(fmt.Sprintf("Waiting for leader [%s] failover", leaderName))
	var promotedFollower corev1.Pod

	return promotedFollower, wait.PollImmediate(2*r.Config.Times.RedisAutoFailoverCheckInterval, 2*r.Config.Times.RedisAutoFailoverCheckTimeout, func() (bool, error) {
		for _, follower := range reachableFollowers {
			info, _, err := r.RedisCLI.Info(follower.Status.PodIP)
			if err != nil {
				continue
			}

			if info.Replication["role"] == "master" {
				promotedFollower = follower
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
func (r *RedisClusterReconciler) isClusterUpToDate(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) (bool, error) {
	for _, node := range v.Nodes {
		pod := node.Pod
		podUpdated, err := r.isPodUpToDate(redisCluster, pod)
		if err != nil {
			return false, err
		}
		if !podUpdated {
			return false, nil
		}
	}
	return true, nil
}

func (r *RedisClusterReconciler) isClusterComplete(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) (bool, error) {

	if len(v.Nodes) == 0 {
		redisCluster.Status.ClusterState = string(Reset)
		return false, nil
	}

	nonHealthyNodes := map[string]view.NodeState{}

	for _, n := range r.RedisClusterStateView.Nodes {
		if _, exists := v.Nodes[n.Name]; !exists && n.NodeState != view.DeleteNode {
			n.NodeState = view.CreateNode
			nonHealthyNodes[n.Name] = n.NodeState
		} else if n.NodeState != view.NodeOK {
			nonHealthyNodes[n.Name] = n.NodeState
		}
	}
	isComplete := r.RedisClusterStateView.ClusterState == view.ClusterOK && len(nonHealthyNodes) == 0

	r.Log.Info(fmt.Sprintf("Is cluster complete: %v", isComplete))
	if !isComplete {
		r.Log.Info(fmt.Sprintf("Unhealthy nodes report: %+v", nonHealthyNodes))
	}

	healthyLeaderName := r.findHealthyLeader(v)
	if h, exists := v.Nodes[healthyLeaderName]; exists {
		r.deleteRemovedNodes(redisCluster, v, h.Ip, false)
	}

	return isComplete, nil
}

func (r *RedisClusterReconciler) RecoverLeaders(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) (bool, error) {
	leadersWithResponsiveFollowers, leadersWithoutFollowers, e := r.GetMissingLeaders(redisCluster, v)
	if e != nil {
		r.Log.Error(e, fmt.Sprintf("Could not retrieve missing leaders list for redis cluster"))
		return true, nil
	}

	e = r.RecreateLeaderWithoutReplicas(redisCluster, leadersWithoutFollowers, v)
	if e != nil {
		r.Log.Error(e, fmt.Sprintf("Could not  process failed"))
	}

	successfulFailovers := r.FailOverMissingLeaders(redisCluster, leadersWithResponsiveFollowers)
	if len(successfulFailovers) > 0 {
		leaderNameToReplicaNode := map[string]*view.NodeView{}
		for missingLeaderName, promotedReplicaPod := range successfulFailovers {
			promotedReplica, exists := v.Nodes[promotedReplicaPod.Name]
			if !exists {
				r.Log.Error(nil, "Could not retrieve promoted redis node [%s] to handle [%s] failover", promotedReplicaPod.Name, missingLeaderName)
				continue
			}
			leaderNameToReplicaNode[missingLeaderName] = promotedReplica
		}
		r.CreateNewLeadersUsingReplica(redisCluster, leaderNameToReplicaNode)
	}
	recoveryRequired := len(leadersWithResponsiveFollowers)+len(leadersWithoutFollowers) > 0
	return recoveryRequired, nil
}

func (r *RedisClusterReconciler) CreateNewLeadersUsingReplica(redisCluster *dbv1.RedisCluster, leaderNameToReplicaNode map[string]*view.NodeView) {
	newPods := []corev1.Pod{}
	for leaderName, replica := range leaderNameToReplicaNode {
		n, exists := r.RedisClusterStateView.Nodes[leaderName]
		if exists {
			n.NodeState = view.CreateNode
		} else {
			r.RedisClusterStateView.Nodes[leaderName] = &view.NodeStateView{
				Name:       leaderName,
				LeaderName: leaderName,
				NodeState:  view.CreateNode,
			}
		}

		r.Log.Info(fmt.Sprintf("Creating leader node [%s] using replica [%s]", leaderName, replica.Name))
		pods, err := r.createRedisLeaderPods(redisCluster, leaderName)
		if err != nil || len(pods) == 0 {
			r.Log.Error(err, fmt.Sprintf("Error while creating new pod [%s]", leaderName))
			r.deletePods(pods)
			continue
		}
		newPods = append(newPods, pods[0])
	}
	pods, err := r.waitForPodReady(newPods...)
	if err != nil || len(newPods) == 0 {
		r.Log.Error(err, fmt.Sprintf("Error while waiting for new pods to be ready %v", newPods))
		r.deletePods(newPods)
		return
	}
	newPodsIps := []string{}
	for _, newPod := range pods {
		if len(newPod.Status.PodIP) > 0 {
			newPodsIps = append(newPodsIps, newPod.Status.PodIP)
		}
	}
	err = r.waitForRedis(newPodsIps...)
	if err != nil {
		r.Log.Error(err, fmt.Sprintf("Error while waiting for new pods ready %v", newPodsIps))
		r.deletePods(pods)
		return
	}

	var wg sync.WaitGroup
	mutex := &sync.Mutex{}
	wg.Add(len(newPods))
	for _, newPod := range newPods {
		replica := leaderNameToReplicaNode[newPod.Name]
		go r.addPodAsClusterLeaderNode(newPod, replica, &wg, mutex)
	}
	wg.Wait()
	return
}

func (r *RedisClusterReconciler) addPodAsClusterLeaderNode(newPod corev1.Pod, replica *view.NodeView, wg *sync.WaitGroup, mutex *sync.Mutex) {
	defer wg.Done()
	newNode := &view.NodeView{
		Name:        newPod.Name,
		Id:          "",
		Namespace:   replica.Namespace,
		Ip:          newPod.Status.PodIP,
		LeaderName:  replica.LeaderName,
		IsLeader:    true,
		IsReachable: true,
		Pod:         &newPod,
	}
	err := r.AddNewLeaderUsingReplica(newNode, replica, mutex)
	if err != nil {
		r.Log.Error(err, fmt.Sprintf("Error while adding new leader [%s] using replica [%s]", newNode.Name, replica.Name))
		r.deletePod(&newPod)
		return
	}
	if _, err = r.doLeaderFailover(replica.Ip, "", []string{newNode.Ip}); err != nil {
		r.Log.Error(err, fmt.Sprintf("Error while failing-over new leader [%s] using replica [%s]", newNode.Name, replica.Name))
		mutex.Lock()
		n, exists := r.RedisClusterStateView.Nodes[newNode.Name]
		if exists {
			n.NodeState = view.FailoverNode
		} else {
			r.RedisClusterStateView.Nodes[newNode.Name] = &view.NodeStateView{
				Name:       newNode.Name,
				LeaderName: newNode.Name,
				NodeState:  view.FailoverNode,
			}
		}
		mutex.Unlock()
		return
	}
	r.Log.Info(fmt.Sprintf("[OK] Leader [%s] recreated successfully; new IP: [%s]", newNode.Name, newNode.Ip))
	mutex.Lock()
	n, exists := r.RedisClusterStateView.Nodes[newNode.Name]
	if exists {
		n.NodeState = view.NodeOK
	} else {
		r.RedisClusterStateView.Nodes[newNode.Name] = &view.NodeStateView{
			Name:       newNode.Name,
			LeaderName: newNode.Name,
			NodeState:  view.NodeOK,
		}
	}
	mutex.Unlock()
}

func (r *RedisClusterReconciler) GetMissingLeaders(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) (leadersWithResponsiveFollowers map[string][]corev1.Pod, leadersWithoutFollowers []string, e error) {
	leadersWithResponsiveFollowers = make(map[string][]corev1.Pod)
	leadersWithoutFollowers = make([]string, 0)

	for _, n := range r.RedisClusterStateView.Nodes {
		if n.Name == n.LeaderName {
			if _, exists := v.Nodes[n.Name]; !exists || n.NodeState == view.CreateNode {
				replicaPodsList, e := r.getRedisClusterPodsByLabel(redisCluster, "leader-name", n.Name)
				if e != nil {
					return leadersWithResponsiveFollowers, leadersWithoutFollowers, e
				}
				for _, replicaPod := range replicaPodsList {
					if replicaPod.Name != n.Name && replicaPod.Status.Phase == "Running" {
						if _, declaredMissing := leadersWithResponsiveFollowers[n.Name]; declaredMissing {
							leadersWithResponsiveFollowers[n.Name] = append(leadersWithResponsiveFollowers[n.Name], replicaPod)
						} else {
							leadersWithResponsiveFollowers[n.Name] = []corev1.Pod{replicaPod}
						}
					}
				}
				if _, hasReplicas := leadersWithResponsiveFollowers[n.Name]; !hasReplicas {
					leadersWithoutFollowers = append(leadersWithoutFollowers, n.Name)
				}
			}
		}
	}
	return leadersWithResponsiveFollowers, leadersWithoutFollowers, nil
}

func (r *RedisClusterReconciler) FailOverMissingLeaders(redisCluster *dbv1.RedisCluster, followersMap map[string][]corev1.Pod) (successfulFailovers map[string]corev1.Pod) {
	successfulFailovers = make(map[string]corev1.Pod)
	for leader, followers := range followersMap {
		assertMissing, _ := r.getRedisClusterPodsByLabel(redisCluster, "node-name", leader)
		if len(assertMissing) > 0 {
			continue
		}
		if len(followers) > 0 {
			hasPromotedFollower := false
			for _, follower := range followers {
				ip := follower.Status.PodIP
				info, _, e := r.RedisCLI.Info(ip)
				if e != nil {
					continue
				}
				if info.Replication["role"] == "master" {
					successfulFailovers[leader] = follower
					hasPromotedFollower = true
					break
				}
			}
			if !hasPromotedFollower {
				promoted, e := r.handleFailover(redisCluster, leader, followers)
				if e == nil {
					successfulFailovers[leader] = promoted
				}
			}
		}
	}
	return successfulFailovers
}

func (r *RedisClusterReconciler) RecreateLeaders(redisCluster *dbv1.RedisCluster, successfulFailovers map[string]string) {
	var wg sync.WaitGroup
	wg.Add(len(successfulFailovers))
	for oldLeaderName, promotedFollowerIp := range successfulFailovers {
		go r.RecreateLeader(redisCluster, oldLeaderName, promotedFollowerIp, &wg)
	}
	wg.Wait()
}

func (r *RedisClusterReconciler) RecreateLeader(redisCluster *dbv1.RedisCluster, oldLeaderName string, promotedFollowerIp string, wg *sync.WaitGroup) {
	defer wg.Done()
	r.recreateLeader(redisCluster, promotedFollowerIp, oldLeaderName)
}

func (r *RedisClusterReconciler) RecreateLeaderWithoutReplicas(redisCluster *dbv1.RedisCluster, leadersWithoutReplicas []string, v *view.RedisClusterView) error {
	if len(leadersWithoutReplicas) == 0 {
		return nil
	}
	healthyLeaderName := r.findHealthyLeader(v)
	if len(healthyLeaderName) == 0 {
		return errors.New("Could not find healthy reachable leader to serve the fix request")
	}
	healthyLeaderIp := v.Nodes[healthyLeaderName].Ip
	r.RedisClusterStateView.ClusterState = view.ClusterFix
	_, _, e := r.RedisCLI.ClusterFix(healthyLeaderIp)
	if e != nil {
		stdout, err := r.RedisCLI.ClusterCheck(healthyLeaderIp)
		if err != nil || !strings.Contains(stdout, "[OK] All 16384 slots covered") {
			return e
		}
	}
	r.RedisClusterStateView.ClusterState = view.ClusterOK
	e = r.addLeaders(redisCluster, healthyLeaderIp, leadersWithoutReplicas)
	if e != nil {
		return e
	}
	r.RedisClusterStateView.ClusterState = view.ClusterRebalance
	successful, _, e := r.RedisCLI.ClusterRebalance(healthyLeaderIp, true)
	if !successful || e != nil {
		r.Log.Error(nil, fmt.Sprintf("Could not perform cluster rebalance with ip [%s]", healthyLeaderIp))
		return e
	}
	r.RedisClusterStateView.ClusterState = view.ClusterOK
	return nil
}

func (r *RedisClusterReconciler) findHealthyLeader(v *view.RedisClusterView, exclude ...map[string]bool) (name string) {
	for _, node := range v.Nodes {
		if node.IsLeader && node.IsReachable {
			if len(exclude) > 0 {
				skipNode := false
				for _, excludeMap := range exclude {
					if _, excludeNode := excludeMap[node.Name]; excludeNode {
						skipNode = true
						break
					}
				}
				if skipNode {
					continue
				}
			}
			if n, exists := r.RedisClusterStateView.Nodes[node.Name]; exists && n.NodeState == view.NodeOK {
				return node.Name
			}
		}
	}
	return ""
}

func (r *RedisClusterReconciler) isScaleRequired(redisCluster *dbv1.RedisCluster) (bool, ScaleType) {
	leaders := 0
	followers := 0
	for _, n := range r.RedisClusterStateView.Nodes {
		if n.Name == n.LeaderName {
			leaders++
		} else {
			followers++
		}
	}
	leadersBySpec := redisCluster.Spec.LeaderCount
	followersBySpec := leadersBySpec * redisCluster.Spec.LeaderFollowersCount
	isRequired := (leaders != leadersBySpec) || (followers != followersBySpec)
	var scaleType ScaleType
	if leaders < leadersBySpec {
		scaleType = ScaleUpLeaders
	} else if leaders > leadersBySpec {
		scaleType = ScaleDownLeaders
	} else if followers < followersBySpec {
		scaleType = ScaleUpFollowers
	} else if followers > followersBySpec {
		scaleType = ScaleDownFollowers
	}
	return isRequired, scaleType
}

func (r *RedisClusterReconciler) scaleCluster(redisCluster *dbv1.RedisCluster) error {
	v, e := r.NewRedisClusterView(redisCluster)
	if e != nil {
		return e
	}
	_, scaleType := r.isScaleRequired(redisCluster)
	switch scaleType {
	case ScaleUpLeaders:
		e = r.scaleUpLeaders(redisCluster, v)
		break
	case ScaleDownLeaders:
		e = r.scaleDownLeaders(redisCluster, v)
		break
	case ScaleUpFollowers:
		e = r.scaleUpFollowers(redisCluster, v)
		break
	case ScaleDownFollowers:
		e = r.scaleDownFollowers(redisCluster, v)
		break
	}
	return e
}

func (r *RedisClusterReconciler) scaleUpLeaders(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) error {
	healthyLeaderName := r.findHealthyLeader(v)
	if len(healthyLeaderName) == 0 {
		return errors.New("Could not find healthy reachable leader to serve scale up leaders request")
	}
	healthyLeaderIp := v.Nodes[healthyLeaderName].Ip
	leaders := r.leadersCount()
	leadersBySpec := redisCluster.Spec.LeaderCount
	newLeadersNames := []string{}
	for i := leaders; i < leadersBySpec; i++ {
		name := "redis-node-" + fmt.Sprint(i)
		newLeadersNames = append(newLeadersNames, name)
		r.RedisClusterStateView.Nodes[name] = &view.NodeStateView{
			Name:       name,
			LeaderName: name,
			NodeState:  view.CreateNode,
		}
		for j := 1; i < redisCluster.Spec.LeaderFollowersCount+1; j++ {
			followerName := name + "-" + fmt.Sprint(j)
			r.RedisClusterStateView.Nodes[name] = &view.NodeStateView{
				Name:       followerName,
				LeaderName: name,
				NodeState:  view.CreateNode,
			}
		}
	}
	r.RedisClusterStateView.ClusterState = view.ClusterRebalance
	e := r.addLeaders(redisCluster, healthyLeaderIp, newLeadersNames)
	if e != nil {
		return e
	}
	time.Sleep(3 * time.Second)
	successful, _, e := r.RedisCLI.ClusterRebalance(healthyLeaderIp, true)
	if !successful || e != nil {
		r.Log.Error(nil, fmt.Sprintf("Could not perform cluster rebalance with ip [%s]", healthyLeaderIp))
		return e
	}
	r.RedisClusterStateView.ClusterState = view.ClusterOK
	return nil
}

func (r *RedisClusterReconciler) scaleDownLeaders(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) error {
	leaders := r.leadersCount()
	leadersBySpec := redisCluster.Spec.LeaderCount

	excludedLeaders := map[string]bool{}
	for i := leadersBySpec; i < leaders; i++ {
		leaderName := "redis-node-" + fmt.Sprint(i)
		excludedLeaders[leaderName] = true
	}

	healthyLeaderName := r.findHealthyLeader(v, excludedLeaders)
	if len(healthyLeaderName) == 0 {
		return errors.New("Could not find healthy reachable leader to serve scale down request")
	}
	targetLeaderName := r.findHealthyLeader(v, excludedLeaders, map[string]bool{healthyLeaderName: true})
	if len(targetLeaderName) == 0 {
		return errors.New("Could not find healthy reachable leader to serve scale down request")
	}
	healthyLeaderIp := v.Nodes[healthyLeaderName].Ip
	targetLeaderId := v.Nodes[targetLeaderName].Id

	for leaderName, _ := range excludedLeaders {
		if leaderToRemove, exists := v.Nodes[leaderName]; exists {
			if n, exists := r.RedisClusterStateView.Nodes[leaderToRemove.Name]; exists {
				n.NodeState = view.ReshardNode
			} else {
				r.RedisClusterStateView.Nodes[leaderToRemove.Name] = &view.NodeStateView{
					Name:       leaderName,
					LeaderName: leaderName,
					NodeState:  view.ReshardNode,
				}
			}
			err := r.reshardLeaderCheckCoverage(healthyLeaderIp, targetLeaderId, leaderToRemove)
			if err != nil {
				r.RedisClusterStateView.ClusterState = view.ClusterFix
				return err
			}
		} else {
			r.RedisClusterStateView.ClusterState = view.ClusterFix
			success, _, err := r.RedisCLI.ClusterFix(healthyLeaderIp)
			if err != nil || !success {
				return err
			}
			r.RedisClusterStateView.ClusterState = view.ClusterOK
		}
		for _, node := range v.Nodes {
			if node.LeaderName == leaderName {
				if n, exists := r.RedisClusterStateView.Nodes[node.Name]; exists {
					n.NodeState = view.DeleteNode
				}
			}
		}
	}
	r.deleteRemovedNodes(redisCluster, v, healthyLeaderIp, true)
	return nil
}

func (r *RedisClusterReconciler) scaleUpFollowers(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) error {
	leadersToFollowerCount := r.numOfFollowersPerLeader(v)
	followersBySpec := redisCluster.Spec.LeaderFollowersCount
	for leaderName, followerCount := range leadersToFollowerCount {
		for i := followerCount + 1; i <= followersBySpec; i++ {
			name := leaderName + "-" + fmt.Sprint(i)
			r.RedisClusterStateView.Nodes[name] = &view.NodeStateView{
				Name:       name,
				LeaderName: leaderName,
				NodeState:  view.CreateNode,
			}
		}
	}
	return nil
}

func (r *RedisClusterReconciler) scaleDownFollowers(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) error {
	leadersToFollowerCount := r.numOfFollowersPerLeader(v)
	followersBySpec := redisCluster.Spec.LeaderFollowersCount
	for leaderName, followerCount := range leadersToFollowerCount {
		for i := followersBySpec + 1; i <= followerCount; i++ {
			name := leaderName + "-" + fmt.Sprint(i)
			if _, exists := r.RedisClusterStateView.Nodes[name]; exists {
				r.RedisClusterStateView.Nodes[name].NodeState = view.DeleteNode
			}
		}
	}
	healthyLeaderName := r.findHealthyLeader(v)
	if len(healthyLeaderName) == 0 {
		return errors.New("Could not find healthy reachable leader to serve the fix request")
	}
	healthyLeaderIp := v.Nodes[healthyLeaderName].Ip
	r.deleteRemovedNodes(redisCluster, v, healthyLeaderIp, true)
	return nil
}

func (r *RedisClusterReconciler) reshardLeaderCheckCoverage(healthyLeaderIp string, targetLeaderId string, leaderToRemove *view.NodeView) error {
	maxSlotsPerLeader := 16384
	success, _, e := r.RedisCLI.ClusterReshard(healthyLeaderIp, leaderToRemove.Id, targetLeaderId, maxSlotsPerLeader)
	if e != nil || !success {
		return e
	}
	emptyLeadersIds, fullCoverage, e := r.CheckClusterAndCoverage(healthyLeaderIp)
	if !fullCoverage || e != nil {
		return e
	}
	if _, leaderHasZeroSlots := emptyLeadersIds[leaderToRemove.Id]; !leaderHasZeroSlots {
		return errors.New(fmt.Sprintf("Could not perform reshard operation for leader %s  %s", leaderToRemove.Ip, leaderToRemove.Id))
	}
	return nil
}

func (r *RedisClusterReconciler) CheckClusterAndCoverage(nodeIp string) (emptyLeadersIds map[string]bool, fullyCovered bool, err error) {
	emptyLeadersIds = map[string]bool{}
	clusterCheckResult, err := r.RedisCLI.ClusterCheck(nodeIp)
	if err != nil {
		return emptyLeadersIds, false, err
	}
	slotsConfigurationFormat := "[OK] All nodes agree about slots configuration"
	allSlotsCoveredFormat := "[OK] All 16384 slots covered"
	zeroSlotsPerMasterFormat := "M:\\s*(\\w*|\\d*)\\s*\\d+\\.\\d+\\.\\d+\\.\\d+:\\d+\\s*slots:\\s*\\(0 slots\\)\\s*master"
	c := regexp.MustCompile(zeroSlotsPerMasterFormat)
	matchingSubstrings := c.FindAllStringSubmatch(clusterCheckResult, -1)
	for _, match := range matchingSubstrings {
		if len(match) > 1 {
			captureId := match[1]
			emptyLeadersIds[captureId] = true
		}
	}

	if strings.Contains(clusterCheckResult, slotsConfigurationFormat) && strings.Contains(clusterCheckResult, allSlotsCoveredFormat) {
		r.Log.Info(fmt.Sprintf("[OK] All slots are covered, empty leaders list contains leaders that it is safe now to remove: %v", emptyLeadersIds))
		return emptyLeadersIds, true, nil
	}
	return emptyLeadersIds, false, errors.New(fmt.Sprintf("Cluster check validation failed, command stdout result: %v", clusterCheckResult))
}

func (r *RedisClusterReconciler) leadersCount() int {
	leaders := 0
	for _, n := range r.RedisClusterStateView.Nodes {
		if n.Name == n.LeaderName {
			leaders++
		}
	}
	return leaders
}

func (r *RedisClusterReconciler) numOfFollowersPerLeader(v *view.RedisClusterView) map[string]int {
	followersPerLeader := map[string]int{}
	for _, node := range v.Nodes {
		if _, contained := followersPerLeader[node.LeaderName]; !contained {
			followersPerLeader[node.LeaderName] = 0
		}
		if !node.IsLeader {
			followersPerLeader[node.LeaderName]++
		}
	}
	return followersPerLeader
}
