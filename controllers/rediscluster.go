package controllers

import (
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"

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

var (
	memorySizeFormat = "\\s*(\\d+\\.*\\d*)\\w+"
	comp             = regexp.MustCompile(memorySizeFormat)
)

func (s ScaleType) String() string {
	return [...]string{"ScaleUpLeaders", "ScaleUpFollowers", "ScaleDownLeaders", "ScaleDownFollowers"}[s]
}

func (r *RedisClusterReconciler) NewRedisClusterView(redisCluster *dbv1.RedisCluster) (*view.RedisClusterView, bool) {
	r.Log.Info("Getting cluster view...")
	v := &view.RedisClusterView{}
	pods, e := r.getRedisClusterPods(redisCluster)
	if e != nil {
		r.Log.Error(e, "Could not fetch cluster pods list for cluster view")
		return v, false
	}
	e = v.CreateView(pods, r.RedisCLI)
	if e != nil {
		if strings.Contains(e.Error(), "Non reachable node found") {
			r.Log.Info("[Warn] Non reachable nodes found during view creation, re-attempting reconcile loop...")
		} else {
			r.Log.Info("[Warn] Could not get view for api view update, Error: %v", e.Error())
		}
		return v, false
	}
	return v, true
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
			r.RedisClusterStateView.SetNodeState(n.Name, n.LeaderName, view.NodeOK)
		}
	}
	r.RedisClusterStateView.ClusterState = view.ClusterOK
	return nil
}

func (r *RedisClusterReconciler) failOverToReplica(leaderName string, v *view.RedisClusterView) (promotedReplica *view.NodeView, err error) {
	for _, n := range v.Nodes {
		if n != nil {
			if n.LeaderName == leaderName && n.Name != leaderName {
				err := r.attemptToFailOver(n.Ip)
				if err != nil {
					continue
				}
				return n, nil
			}
		}
	}
	return nil, nil
}

func (r *RedisClusterReconciler) attemptToFailOver(followerIP string, opt ...string) error {
	_, e := r.RedisCLI.Ping(followerIP)
	if e != nil {
		r.Log.Info(fmt.Sprintf("[Warning] Attempt to failover: ping to node ip [%s] failed", followerIP))
		return e
	}
	e = r.doFailover(followerIP, opt...)
	if e != nil {
		r.Log.Info(fmt.Sprintf("[Warning] Attempt to failover with node ip [%s] failed", followerIP))
		return e
	}
	r.Log.Info(fmt.Sprintf("[OK] Attempt to failover succeeded. [%s] is a leader", followerIP))
	return nil
}

// Triggers a failover command on the specified node and waits for the follower
// to become leader
func (r *RedisClusterReconciler) doFailover(promotedNodeIp string, opt ...string) error {
	r.Log.Info(fmt.Sprintf("Running 'cluster failover %s' on %s", opt, promotedNodeIp))
	_, err := r.RedisCLI.ClusterFailover(promotedNodeIp, opt...)
	if err != nil {
		return err
	}
	return r.waitForManualFailover(promotedNodeIp)
}

func (r *RedisClusterReconciler) cleanMapFromNodesToRemove(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) {
	r.Log.Info("Cleaning state map from nodes that shuold be removed...")
	healthyLeaderName, found := r.findHealthyLeader(v)
	if !found {
		r.Log.Error(errors.New(""), "Could not find healthy leader ip, aborting remove and delete operation...")
		return
	}
	healthyServerIp := v.Nodes[healthyLeaderName].Ip
	podsToDelete := []corev1.Pod{}
	toDeleteFromMap := []string{}

	// first remove followers (followers get errored when forgetting masters)
	for _, n := range r.RedisClusterStateView.Nodes {
		if n.Name != n.LeaderName {
			node, exists := v.Nodes[n.Name]
			if n.NodeState == view.DeleteNode {
				if exists && node != nil {
					r.removeNode(healthyServerIp, node)
					podsToDelete = append(podsToDelete, node.Pod)
				}
				toDeleteFromMap = append(toDeleteFromMap, n.Name)
			}
			if n.NodeState == view.DeleteNodeKeepInMap {
				if exists && node != nil {
					r.removeNode(healthyServerIp, node)
					podsToDelete = append(podsToDelete, node.Pod)
				}
				n.NodeState = view.CreateNode
			}
		}
	}

	// second remove leaders
	for _, n := range r.RedisClusterStateView.Nodes {
		if n.Name == n.LeaderName {
			node, exists := v.Nodes[n.Name]
			if n.NodeState == view.DeleteNode {
				if exists && node != nil {
					r.removeNode(healthyServerIp, node)
					podsToDelete = append(podsToDelete, node.Pod)
				}
				toDeleteFromMap = append(toDeleteFromMap, n.Name)
			}
			if n.NodeState == view.DeleteNodeKeepInMap {
				if exists && node != nil {
					r.removeNode(healthyServerIp, node)
					podsToDelete = append(podsToDelete, node.Pod)
				}
				n.NodeState = view.CreateNode
			}
		}
	}

	// third delete pods
	r.waitForAllNodesAgreeAboutSlotsConfiguration(v, redisCluster)
	deletedPods, err := r.deletePods(podsToDelete)
	if err != nil {
		r.Log.Error(err, "Error while attempting to delete removed pods")
		return
	}
	r.waitForPodDelete(deletedPods...)

	// four detect if there exists pods that are not reported in state map
	for _, node := range v.Nodes {
		if node != nil {
			if _, existsInMap := r.RedisClusterStateView.Nodes[node.Name]; !existsInMap {
				isMaster, err := r.checkIfMaster(node.Ip)
				if err != nil || !isMaster {
					r.deletePod(node.Pod)
					return
				}
				r.waitForAllNodesAgreeAboutSlotsConfiguration(v, redisCluster)
				r.scaleDownSingleUnit(node.Name, map[string]bool{node.Name: true}, v)
				r.RedisClusterStateView.ClusterState = view.ClusterRebalance
			}
		}
	}

	// five delete from map if necessary
	for _, d := range toDeleteFromMap {
		delete(r.RedisClusterStateView.Nodes, d)
	}

	r.Log.Info("Done processing nodes to be removed")
}

func (r *RedisClusterReconciler) removeNode(healthyServerIp string, n *view.NodeView) error {
	_, err := r.RedisCLI.Ping(n.Ip)
	if err != nil {
		return err
	}
	r.Log.Info(fmt.Sprintf("Removing node [%s] from all tables...", n.Id))
	_, err = r.RedisCLI.DelNode(healthyServerIp, n.Id)
	if err != nil {
		return err
	}
	r.Log.Info(fmt.Sprintf("Waiting for node [%s:%s] removal to be completed...", n.Ip, n.Id))
	if pollErr := wait.PollImmediate(r.Config.Times.RedisRemoveNodeCheckInterval, r.Config.Times.RedisRemoveNodeTimeout, func() (bool, error) {
		clusterNodes, _, err := r.RedisCLI.ClusterNodes(n.Ip)
		if err != nil {
			if strings.Contains(err.Error(), "Redis is loading the dataset in memory") {
				return false, nil
			}
			return true, err
		}
		if len(*clusterNodes) == 1 {
			return true, nil
		}
		return false, nil
	}); pollErr != nil {
		return pollErr
	}
	return nil
}

func (r *RedisClusterReconciler) waitForAllNodesAgreeAboutSlotsConfiguration(v *view.RedisClusterView, redisCluster ...*dbv1.RedisCluster) {
	r.Log.Info("Waiting for all cluster nodes to agree about slots configuration...")
	if len(redisCluster) > 0 {
		newView, ok := r.NewRedisClusterView(redisCluster[0])
		if ok {
			v = newView
		}
	}
	var nodes *rediscli.RedisClusterNodes
	nonResponsive := map[string]bool{}
	agreed := map[string]bool{}
	if pollErr := wait.PollImmediate(r.Config.Times.RedisNodesAgreeAboutSlotsConfigCheckInterval, r.Config.Times.RedisNodesAgreeAboutSlotsConfigTimeout, func() (bool, error) {
		agreement := true
		for _, n := range v.Nodes {
			if !agreement {
				break
			}
			if n == nil {
				continue
			}
			if _, nr := nonResponsive[n.Name]; nr {
				continue
			}
			if _, agree := agreed[n.Name]; agree {
				continue
			}
			stdout, err := r.RedisCLI.ClusterCheck(n.Ip)
			if err != nil {
				nonResponsive[n.Name] = true
				continue
			}
			if strings.Contains(stdout, "[OK] All nodes agree about slots configuration") {
				agreed[n.Name] = true
			} else {
				agreement = false
			}
			if agreement {
				nodesTable, _, err := r.RedisCLI.ClusterNodes(n.Ip)
				if err != nil {
					nonResponsive[n.Name] = true
					continue
				}
				if nodesTable == nil {
					continue
				}
				if nodes == nil {
					nodes = nodesTable
				}else{
					if len(*nodesTable) > len(*nodes){
						nodes = nodesTable
						agreement = false
					}
				}
			}
		}
		return agreement, nil
	}); pollErr != nil {
		r.Log.Info("[Warn] Error occured during waiting for cluster nodes to agree about slots configuration, performing CLUSTER REBALANCE might need to be followed by CLUSTER FIX")
	}
}

func (r *RedisClusterReconciler) addLeaderNodes(redisCluster *dbv1.RedisCluster, healthyServerIp string, leaderNames []string, v *view.RedisClusterView) error {
	if len(leaderNames) == 0 {
		return nil
	}
	leaders, e := r.createRedisLeaderPods(redisCluster, leaderNames...)
	if e != nil || len(leaders) == 0 {
		r.deletePods(leaders)
		r.Log.Error(e, "Could not add new leaders")
		return e
	}
	readyNodes := []corev1.Pod{}
	var wg sync.WaitGroup
	mutex := &sync.Mutex{}
	wg.Add(len(leaders))
	for _, leader := range leaders {
		go func(leader corev1.Pod) {
			defer wg.Done()
			if r.preperNewRedisNode(leader, mutex) {
				mutex.Lock()
				readyNodes = append(readyNodes, leader)
				mutex.Unlock()
			}
		}(leader)
	}
	wg.Wait()
	for _, leader := range readyNodes {
		r.waitForAllNodesAgreeAboutSlotsConfiguration(v)
		r.joindNewLeaderToCluster(leader, healthyServerIp, mutex)
	}
	r.waitForAllNodesAgreeAboutSlotsConfiguration(v)
	return nil
}

func (r *RedisClusterReconciler) preperNewRedisNode(pod corev1.Pod, mutex *sync.Mutex) bool {
	newLeader, e := r.waitForPodReady(pod)
	if e != nil || len(newLeader) == 0 {
		message := fmt.Sprintf("Error while waiting for pod [%s] to be read", pod.Name)
		r.handleCreateErrByDeleteGracefully(pod.Name, pod, mutex, message, e)
		return false
	}
	leaderPod := newLeader[0]
	e = r.waitForRedis(leaderPod.Status.PodIP)
	if e != nil {
		message := fmt.Sprintf("Error while waiting for pod [%s] to be ready", leaderPod.Name)
		r.handleCreateErrByDeleteGracefully(pod.Name, leaderPod, mutex, message, e)
		return false
	}
	r.RedisCLI.Flushall(leaderPod.Status.PodIP)
	r.RedisCLI.ClusterReset(leaderPod.Status.PodIP)
	return true
}

func (r *RedisClusterReconciler) joindNewLeaderToCluster(pod corev1.Pod, healthyServerIp string, mutex *sync.Mutex) {
	r.Log.Info(fmt.Sprintf("Adding new leader: [%s]", pod.Name))
	_, e := r.RedisCLI.AddLeader(pod.Status.PodIP, healthyServerIp)
	if e != nil {
		message := fmt.Sprintf("Error while adding pod [%s] to redis cluster, healthy node ip: [%s]", pod.Name, healthyServerIp)
		r.handleCreateErrByDeleteGracefully(pod.Name, pod, mutex, message, e)
		return
	}
	e = r.waitForRedisMeet(pod.Status.PodIP)
	if e != nil {
		message := fmt.Sprintf("Error while adding pod [%s] to redis cluster, healthy node ip: [%s]", pod.Name, healthyServerIp)
		r.handleCreateErrByDeleteGracefully(pod.Name, pod, mutex, message, e)
		return
	}
}

func (r *RedisClusterReconciler) handleCreateErrByDeleteGracefully(name string, leaderPod corev1.Pod, mutex *sync.Mutex, message string, e error) {
	r.deletePod(leaderPod)
	r.RedisClusterStateView.LockResourceAndRemoveFromMap(name, mutex)
	r.Log.Error(e, message)
}

// Removes all nodes the cluster node table entries with IDs of nodes not available
// Recives the list of healthy cluster nodes (Redis is reachable and has cluster mode on)
func (r *RedisClusterReconciler) forgetLostNodes(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) bool {
	r.Log.Info("Forgetting lost nodes...")
	healthyNodes := map[string]string{}
	lostIds := map[string]bool{}
	for _, node := range v.Nodes {
		if node == nil {
			continue
		}
		if _, declaredLost := lostIds[node.Id]; declaredLost {
			continue
		}
		ipsToNodesTable, err := r.ClusterNodesWaitForRedisLoadDataSetInMemory(node.Ip)
		nodesTable, exists := ipsToNodesTable[node.Ip]
		if err != nil || !exists || nodesTable == nil {
			continue
		}
		healthyNodes[node.Name] = node.Ip
		for _, tableNode := range *nodesTable {
			if strings.Contains(tableNode.Flags, "fail") {
				lostIds[tableNode.ID] = true
			}
		}
	}
	if len(lostIds) > 0 {
		r.Log.Info(fmt.Sprintf("List of healthy nodes: %v", healthyNodes))
		r.Log.Info(fmt.Sprintf("List of lost nodes ids: %v", lostIds))
		failingForgets := r.runForget(lostIds, healthyNodes, map[string]string{})
		if len(failingForgets) > 0 {
			waitIfFails := 20 * time.Second
			time.Sleep(waitIfFails)
			for name, id := range failingForgets {
				node, exists := v.Nodes[name]
				if exists && node != nil {
					_, err := r.RedisCLI.ClusterForget(node.Ip, id)
					if err != nil {
						r.deletePod(node.Pod)
					}
				}
			}
			r.runForget(lostIds, healthyNodes, failingForgets)
		}
		r.Log.Info(fmt.Sprintf("Cluster FORGET sent for [%v] lost nodes", len(lostIds)))
	}
	return len(lostIds) > 0
}

func (r *RedisClusterReconciler) runForget(lostIds map[string]bool, healthyNodes map[string]string, ignore map[string]string) map[string]string{
	podsToDelete := map[string]string{}
	var wg sync.WaitGroup
	waitIfFails := 20 * time.Second
	mutex := &sync.Mutex{}
	wg.Add(len(lostIds) * len(healthyNodes))
	for id, _ := range lostIds {
		for name, ip := range healthyNodes {
			go func(ip string, id string) {
				defer wg.Done()
				if _, toIgnore := ignore[name]; toIgnore {
					return
				}
				_, err := r.RedisCLI.ClusterForget(ip, id)
				if err != nil {
					if strings.Contains(err.Error(), "Can't forget my master") {
						mutex.Lock()
						r.Log.Info(fmt.Sprintf("[Warn] node [%v] is not able to forget [%v] properly, additional attempt to forget will be performed within [%v], additional failure to forget [%v] will lead to node [%v] deletion", ip, id, waitIfFails, id, ip))
						podsToDelete[name] = id
						mutex.Unlock()
					}
				}
			}(ip, id)
		}
	}
	wg.Wait()
	return podsToDelete
}

func (r *RedisClusterReconciler) recoverCluster(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) error {

	failingPodsCleared := r.waitForNonReachablePodsTermination(redisCluster, v)
	if failingPodsCleared {
		r.Log.Info("[Warn] Terminating pods detcted...")
		return nil
	}

	lostNodesDetcted := r.forgetLostNodes(redisCluster, v)
	if lostNodesDetcted {
		r.Log.Info("[Warn] Lost nodes detcted on some of cluster nodes...")
		return nil
	}

	r.Log.Info("Validating cluster state...")
	recoveryRequired, err := r.recoverRedisCluster(redisCluster, v)
	if err != nil || recoveryRequired {
		return err
	}

	r.Log.Info("Recovering non healthy nodes...")
	recoveryRequired = r.recoverNodes(redisCluster, v)
	if recoveryRequired {
		return nil
	}

	complete, err := r.isClusterHealthy(redisCluster, v)
	recoveryComplete := complete && err == nil
	if err != nil {
		r.Log.Error(err, "Could not perform cluster-complete validation")
	}
	r.Log.Info(fmt.Sprintf("Recovery complete: %v", recoveryComplete))
	if recoveryComplete {
		redisCluster.Status.ClusterState = string(Ready)
	}

	return nil
}

func (r *RedisClusterReconciler) detectLossOfLeadersWithAllReplicas(v *view.RedisClusterView) []string {
	missing := []string{}
	reportedMissing := map[string]bool{}
	var wg sync.WaitGroup
	mutex := &sync.Mutex{}
	for _, n := range r.RedisClusterStateView.Nodes {
		if n.Name != n.LeaderName {
			continue
		}
		wg.Add(1)
		go func(n *view.NodeStateView) {
			defer wg.Done()
			_, exists := v.Nodes[n.Name]
			if exists || n.NodeState == view.DeleteNode || n.NodeState == view.ReshardNode || n.NodeState == view.DeleteNodeKeepInMap || n.NodeState == view.ReshardNodeKeepInMap {
				return
			}
			_, hasPromotedReplica := r.findPromotedMasterReplica(n.LeaderName, v)
			if hasPromotedReplica {
				return
			}
			mutex.Lock()
			_, markedMissing := reportedMissing[n.LeaderName]
			if markedMissing {
				return
			}
			missing = append(missing, n.LeaderName)
			r.RedisClusterStateView.SetNodeState(n.LeaderName, n.LeaderName, view.CreateNode)
			mutex.Unlock()
		}(n)
	}
	wg.Wait()
	return missing
}

func (r *RedisClusterReconciler) findPromotedMasterReplica(leaderName string, v *view.RedisClusterView) (*view.NodeView, bool) {
	for _, node := range v.Nodes {
		if node == nil {
			continue
		}
		if leaderName != node.LeaderName {
			continue
		}
		info, _, err := r.RedisCLI.Info(node.Ip)
		if err != nil || info == nil || info.Replication["role"] != "master" {
			continue
		}
		ipsToNodesTable, err := r.ClusterNodesWaitForRedisLoadDataSetInMemory(node.Ip)
		nodesTable, exists := ipsToNodesTable[node.Ip]
		if err != nil || !exists || nodesTable == nil {
			continue
		}
		if len(*nodesTable) == 1 {
			continue
		}
		return node, true
	}
	return nil, false
}

func (r *RedisClusterReconciler) retrievePodForProcessing(name string, leaderName string, pods map[string]corev1.Pod, v *view.RedisClusterView) (corev1.Pod, bool) {
	pod := corev1.Pod{}
	newPod, justCreated := pods[name]
	existingNode, exists := v.Nodes[name]
	if justCreated {
		pod = newPod
	} else {
		if exists {
			pod = existingNode.Pod
		} else {
			return pod, false
		}
	}
	return pod, true
}

func (r *RedisClusterReconciler) checkStateMissAlignments(n *view.NodeStateView, v *view.RedisClusterView, mutex *sync.Mutex) bool {
	skip := n.NodeState == view.DeleteNode || n.NodeState == view.ReshardNode || n.NodeState == view.DeleteNodeKeepInMap || n.NodeState == view.ReshardNodeKeepInMap
	if skip {
		return false
	}
	node, exists := v.Nodes[n.Name]
	if !exists {
		r.RedisClusterStateView.LockResourceAndSetNodeState(n.Name, n.LeaderName, view.CreateNode, mutex)
		return false
	}
	ipsToNodesTable, err := r.ClusterNodesWaitForRedisLoadDataSetInMemory(node.Ip)
	nodesTable, exists := ipsToNodesTable[node.Ip]
	if err != nil || !exists || nodesTable == nil {
		r.RedisClusterStateView.LockResourceAndSetNodeState(n.Name, n.LeaderName, view.DeleteNodeKeepInMap, mutex)
		return false
	}
	if len(*nodesTable) == 1 {
		r.RedisClusterStateView.LockResourceAndSetNodeState(n.Name, n.LeaderName, view.AddNode, mutex)
		return false
	}
	if _, leaderInMap := r.RedisClusterStateView.Nodes[n.LeaderName]; !leaderInMap {
		node, exists := v.Nodes[n.Name]
		if !exists || node == nil {
			r.RedisClusterStateView.LockResourceAndSetNodeState(n.Name, n.LeaderName, view.DeleteNode, mutex)
			return false
		}
		isMaster, err := r.checkIfMaster(node.Ip)
		if err != nil || !isMaster {
			r.RedisClusterStateView.LockResourceAndSetNodeState(n.Name, n.LeaderName, view.DeleteNode, mutex)
			return false
		}
		r.RedisClusterStateView.LockResourceAndSetNodeState(n.Name, n.LeaderName, view.ReshardNode, mutex)
		return false
	}
	if n.Name == n.LeaderName {
		isMaster, err := r.checkIfMaster(node.Ip)
		if err != nil {
			return false
		}
		if !isMaster {
			r.RedisClusterStateView.LockResourceAndSetNodeState(n.Name, n.LeaderName, view.FailoverNode, mutex)
			return false
		}
	}
	return true
}

func (r *RedisClusterReconciler) removeSoloLeaders(v *view.RedisClusterView) {
	for _, node := range v.Nodes {
		if node == nil {
			continue
		}
		ipsToNodesTables, err := r.ClusterNodesWaitForRedisLoadDataSetInMemory(node.Ip)
		nodesTable, exists := ipsToNodesTables[node.Ip]
		if err != nil || !exists || nodesTable == nil || len(*nodesTable) == 1 {
			r.deletePod(node.Pod)
		}
	}
}

func (r *RedisClusterReconciler) handleLossOfLeaderWithAllReplicas(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) bool {
	r.Log.Info("Checking for potential data loss..")
	missingLeadersWithLossOfReplicas := r.detectLossOfLeadersWithAllReplicas(v)
	if len(missingLeadersWithLossOfReplicas) > 0 {
		r.removeSoloLeaders(v)
		r.Log.Info("[Warn] Loss of leader with all of his replica detected, mitigating with CLUSTER FIX...")
		r.RedisClusterStateView.ClusterState = view.ClusterFix
		healthyLeaderName, found := r.findHealthyLeader(v)
		if !found {
			return true
		}
		healthyLeader, exists := v.Nodes[healthyLeaderName]
		if !exists {
			return true
		}
		_, stdout, e := r.RedisCLI.ClusterFix(healthyLeader.Ip)
		if e != nil && !strings.Contains(e.Error(), "[OK] All 16384 slots covered") && !strings.Contains(stdout, "[OK] All 16384 slots covered") {
			return true
		}
		r.RedisClusterStateView.ClusterState = view.ClusterRebalance
		r.waitForAllNodesAgreeAboutSlotsConfiguration(v, redisCluster)
		rebalanced, _, e := r.RedisCLI.ClusterRebalance(healthyLeader.Ip, true)
		if !rebalanced || e != nil {
			r.RedisClusterStateView.ClusterState = view.ClusterFix
			return true
		}
		r.addLeaderNodes(redisCluster, healthyLeader.Ip, missingLeadersWithLossOfReplicas, v)
		return true
	}
	r.Log.Info("[OK] At least one leader exdists for each set of replicas")
	return false
}

func (r *RedisClusterReconciler) detectExisintgLeadersAndReplicasWithFullDataLoss(v *view.RedisClusterView) bool{
	// If node is created thinking there exists other temp-master to replicate from, during its waiting for pod-create the other single replica fails
	// it will get stuck in "CreateNode/AddNode" state forever, cluster will be in [recover] mode forever
	// indication:
	// If all the nodes with the same leader name has len of nodes == 1
	// reshard & delete them all, it will be detected later as case of loss leaders with all followers, and recovered by FIX -> REBALANCE 
	lost := []string{}
	var wg sync.WaitGroup
	mutex := &sync.Mutex{}
	for _, n := range v.Nodes {
		if n.Name != n.LeaderName {
			continue
		}
		k, existsInMap := r.RedisClusterStateView.Nodes[n.Name]
		if !existsInMap || k.NodeState == view.DeleteNode || k.NodeState == view.DeleteNodeKeepInMap || k.NodeState == view.ReshardNode || k.NodeState == view.ReshardNodeKeepInMap{
			continue
		}
		wg.Add(1)
		go func(n *view.NodeView) {
			defer wg.Done()
			setOK := false
			nodes, _, err := r.RedisCLI.ClusterNodes(n.Ip)
			if err != nil || nodes == nil {
				return
			}
			if len(*nodes) > 2 {
				return
			}
			for _, node := range v.Nodes {
				if node.LeaderName != n.LeaderName || node.Name == n.Name {
					continue
				}
				nodes, _, err := r.RedisCLI.ClusterNodes(node.Ip)
				if err != nil || nodes == nil {
					continue
				}
				if len(*nodes) > 2 {
					setOK = true
					break
				}
			}
			if !setOK {
				// At this point: n has one recognized node in his table, all the existing nodes with same leader name has one recognized node in their table
				// Mitigation: all of them need to be resharded, but kept in map
				mutex.Lock()
				lost = append(lost, n.LeaderName)
				if r.RedisClusterStateView.ClusterState != view.ClusterFix {
					r.RedisClusterStateView.ClusterState = view.ClusterFix
				}
				mutex.Unlock()
			}
		}(n)
	}
	wg.Wait()
	for _, l := range lost {
		r.Log.Info(fmt.Sprintf("[Warn] A case of existing nodes with complete loss of data detected, leader name: [%v], all replicas of this leader will be resharded and deleted", l))
		for _, node := range v.Nodes {
			if node.LeaderName == l {
				r.RedisClusterStateView.SetNodeState(node.Name, node.LeaderName, view.ReshardNodeKeepInMap)
			}
		}
	}
	return len(lost) > 0
}

func (r *RedisClusterReconciler) recoverNodes(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) bool {
	if r.handleLossOfLeaderWithAllReplicas(redisCluster, v) {
		return true
	}
	if r.detectExisintgLeadersAndReplicasWithFullDataLoss(v) {
		return true
	}
	// must be synchrounous
	if r.handleInterruptedScaleFlows(redisCluster, v) {
		return true
	}
	actionRequired := false
	mutex := &sync.Mutex{}
	r.Log.Info("Detecting missing nodes in cluster...")
	pods := r.createMissingRedisPods(redisCluster, v)
	if len(pods) == 0 {
		r.Log.Info("[OK] No missing nodes detected")
	}
	var wg sync.WaitGroup
	// can be asynchrounous
	r.Log.Info("Mitigating failures and interrupted flows...")
	for _, n := range r.RedisClusterStateView.Nodes {
		if n.NodeState == view.NodeOK {
			if r.checkStateMissAlignments(n, v, mutex) {
				continue
			}
		}
		wg.Add(1)
		go func(n *view.NodeStateView) {
			defer wg.Done()
			pod, proceed := r.retrievePodForProcessing(n.Name, n.LeaderName, pods, v)
			if !proceed {
				return
			}
			op := r.handleInterruptedClusterHealthFlow(redisCluster, n, pod, v, mutex)
			if op {
				mutex.Lock()
				actionRequired = true
				mutex.Unlock()
			}

		}(n)
	}
	wg.Wait()
	if !actionRequired {
		r.Log.Info("[OK] Cluster nodes are healthy")
	}
	return actionRequired
}

func (r *RedisClusterReconciler) handleInterruptedScaleFlows(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) bool {
	actionRequired := false
	r.Log.Info("Checking if sharding process have been interrupted for some nodes...")
	for _, n := range r.RedisClusterStateView.Nodes {
		switch n.NodeState {
		case view.ReshardNode:
			actionRequired = true
			r.scaleDownLeader(n.Name, n.LeaderName, map[string]bool{n.Name: true}, v)
			break
		case view.ReshardNodeKeepInMap:
			actionRequired = true
			r.scaleDownLeaderKeepInMap(n.Name, n.LeaderName, map[string]bool{n.Name: true}, v)
			break
		case view.NewEmptyNode:
			actionRequired = true
			r.recoverFromNewEmptyNode(n.Name, v)
			break
		}
	}
	if actionRequired {
		r.RedisClusterStateView.ClusterState = view.ClusterRebalance
	} else {
		r.Log.Info("[OK] Previous sharding requests ended successfully")
	}
	return actionRequired
}

func (r *RedisClusterReconciler) handleInterruptedClusterHealthFlow(redisCluster *dbv1.RedisCluster, n *view.NodeStateView, pod corev1.Pod, v *view.RedisClusterView, mutex *sync.Mutex) bool {

	promotedMasterReplica, hasPromotedReplica := r.findPromotedMasterReplica(n.LeaderName, v)
	if !hasPromotedReplica || promotedMasterReplica == nil {
		return true
	}

	if promotedMasterReplica.Name == n.Name {
		r.RedisClusterStateView.LockResourceAndSetNodeState(n.Name, n.LeaderName, view.NodeOK, mutex)
	}

	m := &view.MissingNodeView{
		Name:              n.Name,
		LeaderName:        n.LeaderName,
		CurrentMasterName: promotedMasterReplica.Name,
		CurrentMasterId:   promotedMasterReplica.Id,
		CurrentMasterIp:   promotedMasterReplica.Ip,
	}
	var err error
	actionRequired := false

	switch n.NodeState {
	case view.AddNode:
		actionRequired = true
		mutex.Lock()
		r.waitForAllNodesAgreeAboutSlotsConfiguration(v)
		mutex.Unlock()
		err = r.recoverFromAddNode(pod, m, mutex)
		break
	case view.ReplicateNode:
		actionRequired = true
		err = r.recoverFromReplicateNode(pod.Status.PodIP, m, mutex)
		break
	case view.SyncNode:
		actionRequired = true
		err = r.recoverFromSyncNode(pod.Status.PodIP, m, mutex)
		break
	case view.FailoverNode:
		actionRequired = true
		err = r.recoverFromFailOver(pod.Status.PodIP, m, mutex)
		break
	}

	if err != nil {
		r.forgetLostNodes(redisCluster, v)
		r.RedisClusterStateView.ClusterState = view.ClusterFix
	}

	return actionRequired
}

func (r *RedisClusterReconciler) recoverFromNewEmptyNode(name string, v *view.RedisClusterView) {
	if n, exists := v.Nodes[name]; exists && n != nil {
		ipsToNodesTables, err := r.ClusterNodesWaitForRedisLoadDataSetInMemory(n.Ip)
		nodesTable, exists := ipsToNodesTables[n.Ip]
		if err != nil || !exists || nodesTable == nil || len(*nodesTable) <= 1 {
			r.RedisClusterStateView.SetNodeState(n.Name, n.LeaderName, view.DeleteNodeKeepInMap)
			return
		}
		for _, tableNode := range *nodesTable {
			if tableNode.ID == n.Id {
				if len(tableNode.Slots) > 0 {
					r.RedisClusterStateView.SetNodeState(n.Name, n.LeaderName, view.NodeOK)
				}
			}
		}
	}
}

func (r *RedisClusterReconciler) recoverFromAddNode(p corev1.Pod, m *view.MissingNodeView, mutex *sync.Mutex) error {
	masterIp := m.CurrentMasterIp
	masterId := m.CurrentMasterId
	newPodIp := p.Status.PodIP

	ipsToNodesTables, err := r.ClusterNodesWaitForRedisLoadDataSetInMemory(newPodIp)
	nodesTable, exists := ipsToNodesTables[newPodIp]
	if err != nil || !exists || nodesTable == nil {
		return err
	}
	if len(*nodesTable) == 1 {
		warnMsg := "[WARN] This failure might be an indication for additional failures that appeared in cluster during recovering process, try to wait for/induce FORGET of failing nodes and re-attempt reconcile loop"
		r.Log.Info(fmt.Sprintf("Adding new redis node [%s], current master [%s]", m.Name, m.CurrentMasterName))
		mutex.Lock()
		r.RedisClusterStateView.SetNodeState(m.Name, m.LeaderName, view.AddNode)
		_, err = r.RedisCLI.AddFollower(newPodIp, masterIp, masterId)
		mutex.Unlock()
		if err != nil {
			r.Log.Error(err, fmt.Sprintf("Could not perform ADD NODE [%s] to cluster", m.Name))
			r.Log.Info(warnMsg)
			r.deletePod(p)
			return err
		}
		r.Log.Info(fmt.Sprintf("Waiting for master node [%s] to meet [%s]", m.CurrentMasterName, m.Name))
		err = r.waitForRedisMeet(newPodIp)
		if err != nil {
			r.Log.Error(err, fmt.Sprintf("Error while waiting for cluster to meet [%s]", m.Name))
			r.Log.Info(warnMsg)
			return err
		}
	}
	return r.recoverFromReplicateNode(p.Status.PodIP, m, mutex)
}

func (r *RedisClusterReconciler) recoverFromReplicateNode(podIp string, m *view.MissingNodeView, mutex *sync.Mutex) error {
	if m.CurrentMasterIp == podIp {
		r.RedisClusterStateView.LockResourceAndSetNodeState(m.Name, m.LeaderName, view.NodeOK, mutex)
		return nil
	}
	newPodId, err := r.RedisCLI.MyClusterID(podIp)
	if err != nil {
		return err
	}
	err = r.waitForRedisReplication(m.CurrentMasterName, m.CurrentMasterIp, m.CurrentMasterId, m.Name, newPodId)
	if err != nil {
		r.Log.Error(err, fmt.Sprintf("Error while waiting for node [%s] replication ", m.Name))
		return err
	}
	return r.recoverFromSyncNode(podIp, m, mutex)
}

func (r *RedisClusterReconciler) recoverFromSyncNode(podIp string, m *view.MissingNodeView, mutex *sync.Mutex) error {
	r.RedisClusterStateView.LockResourceAndSetNodeState(m.Name, m.LeaderName, view.SyncNode, mutex)
	err := r.waitForRedisSync(m, podIp)
	if err != nil {
		r.Log.Error(err, fmt.Sprintf("Error while waiting for node [%s] sync process ", m.Name))
		return err
	}
	return r.recoverFromFailOver(podIp, m, mutex)
}

func (r *RedisClusterReconciler) recoverFromFailOver(podIp string, m *view.MissingNodeView, mutex *sync.Mutex) error {
	if m.Name == m.LeaderName {
		r.Log.Info(fmt.Sprintf("Performing failover for node [%s]", m.Name))
		r.RedisClusterStateView.LockResourceAndSetNodeState(m.Name, m.LeaderName, view.FailoverNode, mutex)
		err := r.doFailover(podIp, "")
		if err != nil {
			r.Log.Info(fmt.Sprintf("[Warning] Attempt to failover with node [%s:%s] failed", m.Name, podIp))
			return err
		}
	}
	r.RedisClusterStateView.LockResourceAndSetNodeState(m.Name, m.LeaderName, view.NodeOK, mutex)
	return nil
}

func (r *RedisClusterReconciler) detectNodeTableMissalignments(v *view.RedisClusterView) bool {
	r.Log.Info("Detecting nodes table missalignments...")
	missalignments := []string{}
	for _, node := range v.Nodes {
		if node == nil {
			continue
		}
		if node.IsLeader {
			continue
		}
		leaderNode, leaderPodExists := v.Nodes[node.LeaderName]
		if !leaderPodExists || leaderNode == nil {
			continue
		}
		ipsToNodeTables, err := r.ClusterNodesWaitForRedisLoadDataSetInMemory(node.Ip, leaderNode.Ip)
		if err != nil {
			continue
		}
		if followerNodeTable, exists := ipsToNodeTables[node.Ip]; !exists || followerNodeTable == nil || len(*followerNodeTable) <= 1 {
			continue
		}
		if leaderNodeTable, exists := ipsToNodeTables[leaderNode.Ip]; !exists || leaderNodeTable == nil || len(*leaderNodeTable) <= 1 {
			continue
		}
		isFollowerMaster, err := r.checkIfMaster(node.Ip)
		if err != nil || !isFollowerMaster {
			continue
		}
		isLeaderMaster, err := r.checkIfMaster(leaderNode.Ip)
		if err != nil {
			continue
		}
		if !isLeaderMaster {
			r.RedisClusterStateView.SetNodeState(node.LeaderName, node.LeaderName, view.FailoverNode)
			continue
		}
		// At this point: pod which we concider follower, serves as master in the context of the cluster, as well its leader exists and serves as master in the context of cluster:
		// they are responsible for different sets of slots instead of being replicas of each other.
		// This is an indication for corner case of rebalance request that accidentally included the follower into the rebalance before the decision about its role was made (probably interrupt during cluster meet)
		missalignments = append(missalignments, node.Name)
	}

	if len(missalignments) > 0 {
		r.Log.Info(fmt.Sprintf("[Warn] Detected a case of missalignment between expected follower role to its part in cluster, nodes: %v", missalignments))
	} else {
		r.Log.Info("[OK] No missalignments been discovered")
	}

	for _, missAlignedNode := range missalignments {
		r.scaleDownSingleUnit(missAlignedNode, map[string]bool{missAlignedNode: true}, v)
	}
	return len(missalignments) > 0
}

func (r *RedisClusterReconciler) recoverRedisCluster(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) (bool, error) {
	s := r.RedisClusterStateView.ClusterState
	r.Log.Info(fmt.Sprintf("Cluster state: %v", s))
	switch s {
	case view.ClusterOK:
		return r.detectNodeTableMissalignments(v), nil
	case view.ClusterFix:
		healthyLeaderName, found := r.findHealthyLeader(v)
		if !found {
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
		r.removeSoloLeaders(v)
		r.waitForAllNodesAgreeAboutSlotsConfiguration(v)
		healthyLeaderName, found := r.findHealthyLeader(v)
		if !found {
			return true, errors.New("Could not find healthy reachable leader to serve cluster rebalance request")
		}
		healthyLeaderIp := v.Nodes[healthyLeaderName].Ip
		rebalanced, _, e := r.RedisCLI.ClusterRebalance(healthyLeaderIp, true)
		if !rebalanced || e != nil {
			r.RedisClusterStateView.ClusterState = view.ClusterFix
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
		if node == nil {
			continue
		}
		pod := node.Pod
		if pod.Status.Phase == "Terminating" {
			terminatingPods = append(terminatingPods, pod)
			continue
		}
		clusterInfo, _, e := r.RedisCLI.ClusterInfo(pod.Status.PodIP)
		if e != nil || (*clusterInfo) == nil {
			nonReachablePods = append(nonReachablePods, pod)
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

func (r *RedisClusterReconciler) updateCluster(redisCluster *dbv1.RedisCluster) error {
	r.Log.Info("Updating Cluster Pods...")
	v, ok := r.NewRedisClusterView(redisCluster)
	if !ok {
		return errors.New("Could not perform redis cluster update")
	}
	updatingPodsAtOnce := 0
	for _, n := range v.Nodes {
		if updatingPodsAtOnce >= r.Config.Thresholds.MaxToleratedPodsUpdateAtOnce {
			return nil
		}
		if n == nil {
			continue
		}
		if n.Name == n.LeaderName {
			podUpToDate, err := r.isPodUpToDate(redisCluster, n.Pod)
			if err != nil {
				continue
			}
			if !podUpToDate {
				updatingPodsAtOnce++
				promotedReplica, err := r.failOverToReplica(n.Name, v)
				if err != nil || promotedReplica == nil {
					continue
				}
				r.deletePod(n.Pod)
			}
		}
	}
	if updatingPodsAtOnce > 0 {
		return nil
	}

	for _, n := range v.Nodes {
		if updatingPodsAtOnce >= r.Config.Thresholds.MaxToleratedPodsUpdateAtOnce {
			return nil
		}
		if n == nil {
			continue
		}
		if n.Name != n.LeaderName {
			podUpToDate, err := r.isPodUpToDate(redisCluster, n.Pod)
			if err != nil {
				continue
			}
			if !podUpToDate {
				r.deletePod(n.Pod)
				updatingPodsAtOnce++
			}
		}
	}
	return nil
}

// TODO replace with a readyness probe on the redis container
func (r *RedisClusterReconciler) waitForRedis(nodeIPs ...string) error {
	for _, nodeIP := range nodeIPs {
		if nodeIP == "" {
			return errors.Errorf("Missing IP")
		}
		if pollErr := wait.PollImmediate(r.Config.Times.RedisPingCheckInterval, r.Config.Times.RedisPingCheckTimeout, func() (bool, error) {
			reply, err := r.RedisCLI.Ping(nodeIP)
			if err != nil {
				return true, err
			}
			if strings.Compare(reply, "PONG") == 0 {
				return true, nil
			}
			return false, nil
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
				if strings.Contains(err.Error(), "Redis is loading the dataset in memory") {
					return false, nil
				}
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
func (r *RedisClusterReconciler) waitForRedisSync(m *view.MissingNodeView, nodeIP string) error {
	r.Log.Info(fmt.Sprintf("Waiting for SYNC to start on [%s:%s]", m.Name, nodeIP))
	return wait.PollImmediate(r.Config.Times.SyncCheckInterval, r.Config.Times.SyncCheckTimeout, func() (bool, error) {
		stdoutF, err := r.RedisCLI.Role(nodeIP)
		if err != nil {
			if strings.Contains(err.Error(), "LOADING Redis is loading the dataset in memory") {
				return false, nil
			}
			return false, err
		}
		stdoutL, err := r.RedisCLI.Role(m.CurrentMasterIp)
		if err != nil {
			if strings.Contains(err.Error(), "LOADING Redis is loading the dataset in memory") {
				return false, nil
			}
			return false, err
		}
		if !strings.Contains(stdoutF, m.CurrentMasterIp) || !strings.Contains(stdoutL, nodeIP) {
			return false, nil
		}
		infoF, _, err := reconciler.RedisCLI.Info(nodeIP)
		if err != nil || infoF == nil {
			if strings.Contains(err.Error(), "LOADING Redis is loading the dataset in memory") {
				return false, nil
			}
			return false, err
		}
		infoL, _, err := reconciler.RedisCLI.Info(m.CurrentMasterIp)
		if err != nil || infoL == nil {
			if strings.Contains(err.Error(), "LOADING Redis is loading the dataset in memory") {
				return false, nil
			}
			return false, err
		}
		memorySizeF := infoF.Memory["used_memory_human"]
		memorySizeL := infoL.Memory["used_memory_human"]

		mF := comp.FindAllStringSubmatch(memorySizeF, -1)
		var memF float64 = 0
		for _, match := range mF {
			if len(match) > 1 {
				memF, _ = strconv.ParseFloat(match[1], 64)
			}
		}
		mL := comp.FindAllStringSubmatch(memorySizeL, -1)
		var memL float64 = 0
		for _, match := range mL {
			if len(match) > 0 {
				memL, _ = strconv.ParseFloat(match[1], 64)
			}
		}
		dbsizeF, stdoutF, err := r.RedisCLI.DBSIZE(nodeIP)
		if err != nil {
			return false, err
		}
		dbsizeL, stdoutL, err := r.RedisCLI.DBSIZE(m.CurrentMasterIp)
		if err != nil {
			return false, err
		}

		var dbSizeMatch int64 = 100
		var memSizeMatch float64 = 100.0

		if dbsizeL > 0 {
			dbSizeMatch = (dbsizeF * 100) / dbsizeL
		}

		if memL > 0 {
			memSizeMatch = (memF * 100) / memL
			memSizeMatch = roundFloatToPercision(memSizeMatch, 3)
		}

		r.Log.Info(fmt.Sprintf("Checking sync on master [%v] to replica [%v]: Memory size (%v, %v, %v%v), DB size (%v, %v, %v%v)", m.CurrentMasterIp, nodeIP, memorySizeL, memorySizeF, memSizeMatch, "% match", dbsizeL, dbsizeF, dbSizeMatch, "% match"))
		return dbSizeMatch >= int64(r.Config.Thresholds.SyncMatchThreshold), nil
	})
}

func (r *RedisClusterReconciler) waitForRedisReplication(leaderName string, leaderIP string, leaderID string, followerName string, followerID string) error {
	r.Log.Info(fmt.Sprintf("Waiting for CLUSTER REPLICATION [%s:%s]->[%s:%s]", leaderName, leaderID, followerName, followerID))
	return wait.PollImmediate(r.Config.Times.RedisClusterReplicationCheckInterval, r.Config.Times.RedisClusterReplicationCheckTimeout, func() (bool, error) {
		replicas, _, err := r.RedisCLI.ClusterReplicas(leaderIP, leaderID)
		if err != nil {
			return true, err
		}
		for _, replica := range *replicas {
			if replica.ID == followerID {
				return true, nil
			}
		}
		return false, nil
	})
}

func (r *RedisClusterReconciler) waitForRedisMeet(newNodeIP string) error {
	return wait.PollImmediate(r.Config.Times.RedisClusterMeetCheckInterval, r.Config.Times.RedisClusterMeetCheckTimeout, func() (bool, error) {
		clusterNodes, _, err := r.RedisCLI.ClusterNodes(newNodeIP)
		if err != nil {
			if strings.Contains(err.Error(), "Redis is loading the dataset in memory") {
				return false, nil
			}
			return true, err
		}
		if len(*clusterNodes) > 2 {
			return true, nil
		}
		return false, nil
	})
}

// Waits for a specified pod to be marked as master
func (r *RedisClusterReconciler) waitForManualFailover(podIP string) error {
	r.Log.Info(fmt.Sprintf("Waiting for [%s] to become leader", podIP))
	return wait.PollImmediate(r.Config.Times.RedisManualFailoverCheckInterval, r.Config.Times.RedisManualFailoverCheckTimeout, func() (bool, error) {
		isMaster, err := r.checkIfMaster(podIP)
		if err != nil {
			return true, err
		}
		return isMaster, nil
	})
}

// Waits for Redis to pick a new leader
// Returns the IP of the promoted follower
func (r *RedisClusterReconciler) waitForFailover(redisCluster *dbv1.RedisCluster, leaderName string, reachableFollowers []corev1.Pod) (corev1.Pod, error) {
	r.Log.Info(fmt.Sprintf("Waiting for leader [%s] failover", leaderName))
	var promotedFollower corev1.Pod

	return promotedFollower, wait.PollImmediate(r.Config.Times.RedisAutoFailoverCheckInterval, r.Config.Times.RedisAutoFailoverCheckTimeout, func() (bool, error) {
		for _, follower := range reachableFollowers {
			isMaster, err := r.checkIfMaster(follower.Status.PodIP)
			if err != nil {
				continue
			}
			if isMaster {
				promotedFollower = follower
				return true, nil
			}
		}
		return false, nil
	})
}

func (r *RedisClusterReconciler) isPodUpToDate(redisCluster *dbv1.RedisCluster, pod corev1.Pod) (bool, error) {
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
		if node == nil {
			continue
		}
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

func (r *RedisClusterReconciler) isClusterHealthy(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) (bool, error) {
	if len(v.Nodes) == 0 {
		r.Log.Info("[WARN] Could not find redis cluster nodes, reseting cluster...")
		redisCluster.Status.ClusterState = string(Reset)
		return false, nil
	}
	r.Log.Info("Checking for non-healthy nodes...")
	nonHealthyNodes := map[string]view.NodeState{}
	for _, n := range r.RedisClusterStateView.Nodes {
		if n.NodeState == view.NodeOK {
			node, exists := v.Nodes[n.Name]
			if exists {
				if r.checkIfNodeAligned(n, node, nonHealthyNodes) {
					continue
				}
			} else {
				r.RedisClusterStateView.SetNodeState(n.Name, n.LeaderName, view.CreateNode)
			}
		}
		nonHealthyNodes[n.Name] = n.NodeState
	}
	isComplete := r.RedisClusterStateView.ClusterState == view.ClusterOK && len(nonHealthyNodes) == 0

	r.Log.Info(fmt.Sprintf("Is cluster complete: %v", isComplete))
	if !isComplete {
		r.Log.Info(fmt.Sprintf("Missing/unhealthy nodes report: %+v", nonHealthyNodes))
	}
	return isComplete, nil
}

func (r *RedisClusterReconciler) checkIfNodeAligned(n *view.NodeStateView, node *view.NodeView, nonHealthyNodes map[string]view.NodeState) bool {
	ipsToNodesTable, err := r.ClusterNodesWaitForRedisLoadDataSetInMemory(node.Ip)
	nodesTable, exists := ipsToNodesTable[node.Ip]
	if err != nil || !exists || nodesTable == nil {
		return false
	}
	if len(*nodesTable) <= 1 {
		return false
	}
	if node.IsLeader {
		isMaster, err := r.checkIfMaster(node.Ip)
		if err != nil || !isMaster {
			return false
		}
	}
	return true
}

func (r *RedisClusterReconciler) findHealthyLeader(v *view.RedisClusterView, exclude ...map[string]bool) (name string, found bool) {
	for _, node := range v.Nodes {
		if node == nil {
			continue
		}
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
			isMaster, err := r.checkIfMaster(node.Ip)
			if err != nil {
				continue
			}
			if isMaster {
				ipsToNodesTable, err := r.ClusterNodesWaitForRedisLoadDataSetInMemory(node.Ip)
				nodesTable, exists := ipsToNodesTable[node.Ip]
				if err != nil || !exists || nodesTable == nil || len(*nodesTable) <= 1 {
					continue
				}
				return node.Name, len(node.Name) > 0
			}
		}
	}
	return "", false
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
	v, ok := r.NewRedisClusterView(redisCluster)
	if !ok {
		return nil
	}
	lostNodesDetcted := r.forgetLostNodes(redisCluster, v)
	if lostNodesDetcted {
		r.Log.Info("[Warn] Lost nodes detcted on some of cluster nodes...")
		return nil
	}

	var err error
	_, scaleType := r.isScaleRequired(redisCluster)
	switch scaleType {
	case ScaleUpLeaders:
		err = r.scaleUpLeaders(redisCluster, v)
		break
	case ScaleDownLeaders:
		err = r.scaleDownLeaders(redisCluster, v)
		break
	case ScaleUpFollowers:
		err = r.scaleUpFollowers(redisCluster, v)
		break
	case ScaleDownFollowers:
		err = r.scaleDownFollowers(redisCluster, v)
		break
	}
	return err
}

func (r *RedisClusterReconciler) scaleUpLeaders(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) error {
	r.Log.Info("Scaling up leaders")
	healthyLeaderName, found := r.findHealthyLeader(v)
	if !found {
		return errors.New("Could not find healthy reachable leader to serve scale up leaders request")
	}
	healthyLeaderIp := v.Nodes[healthyLeaderName].Ip
	leaders := r.leadersCount()
	leadersBySpec := redisCluster.Spec.LeaderCount
	newLeadersNames := []string{}
	for l := leaders; l < leadersBySpec; l++ {
		name := "redis-node-" + fmt.Sprint(l)
		newLeadersNames = append(newLeadersNames, name)
		r.RedisClusterStateView.Nodes[name] = &view.NodeStateView{
			Name:       name,
			LeaderName: name,
			NodeState:  view.NewEmptyNode,
		}
		for f := 1; f <= redisCluster.Spec.LeaderFollowersCount; f++ {
			followerName := name + "-" + fmt.Sprint(f)
			r.RedisClusterStateView.Nodes[followerName] = &view.NodeStateView{
				Name:       followerName,
				LeaderName: name,
				NodeState:  view.CreateNode,
			}
		}
	}
	if len(newLeadersNames) > 0 {
		r.RedisClusterStateView.ClusterState = view.ClusterRebalance
		e := r.addLeaderNodes(redisCluster, healthyLeaderIp, newLeadersNames, v)
		if e != nil {
			return e
		}
		r.Log.Info("Leaders added successfully to redis cluster")
		r.waitForAllNodesAgreeAboutSlotsConfiguration(v)
	} else {
		r.Log.Info("[Warn] New leader names list appeard to be empty, no leaders been added to cluster")
	}
	return nil
}

func (r *RedisClusterReconciler) scaleDownLeaders(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) error {
	r.Log.Info("Scaling down leaders")
	leaders := r.leadersCount()
	leadersBySpec := redisCluster.Spec.LeaderCount

	leadersToReshard := map[string]bool{}
	for l := leadersBySpec; l < leaders; l++ {
		leaderName := "redis-node-" + fmt.Sprint(l)
		leadersToReshard[leaderName] = true
		n, exists := r.RedisClusterStateView.Nodes[leaderName]
		if exists {
			n.NodeState = view.ReshardNode
		}
	}
	r.RedisClusterStateView.ClusterState = view.ClusterRebalance
	for leaderName, _ := range leadersToReshard {
		r.scaleDownLeader(leaderName, leaderName, leadersToReshard, v)
	}
	r.cleanMapFromNodesToRemove(redisCluster, v)
	return nil
}

func (r *RedisClusterReconciler) scaleDownLeader(name string, leaderName string, excludeList map[string]bool, v *view.RedisClusterView) {
	healthyLeaderName, found := r.findHealthyLeader(v, excludeList)
	if !found {
		return
	}
	targetLeaderName, found := r.findHealthyLeader(v, excludeList, map[string]bool{healthyLeaderName: true})
	if !found {
		return
	}
	healthyLeaderIp := v.Nodes[healthyLeaderName].Ip
	targetLeaderId := v.Nodes[targetLeaderName].Id
	r.reshardAndRemoveLeader(name, leaderName, healthyLeaderIp, targetLeaderId, v)
}

func (r *RedisClusterReconciler) scaleDownLeaderKeepInMap(name string, leaderName string, excludeList map[string]bool, v *view.RedisClusterView) {
	healthyLeaderName, found := r.findHealthyLeader(v, excludeList)
	if !found {
		return
	}
	targetLeaderName, found := r.findHealthyLeader(v, excludeList, map[string]bool{healthyLeaderName: true})
	if !found {
		return
	}
	healthyLeaderIp := v.Nodes[healthyLeaderName].Ip
	targetLeaderId := v.Nodes[targetLeaderName].Id
	r.reshardAndKeepInMap(name, leaderName, healthyLeaderIp, targetLeaderId, v)
}

func (r *RedisClusterReconciler) scaleDownSingleUnit(name string, excludeList map[string]bool, v *view.RedisClusterView) {
	healthyLeaderName, found := r.findHealthyLeader(v, excludeList)
	if !found {
		return
	}
	targetLeaderName, found := r.findHealthyLeader(v, excludeList, map[string]bool{healthyLeaderName: true})
	if !found {
		return
	}
	healthyLeaderIp := v.Nodes[healthyLeaderName].Ip
	targetLeaderId := v.Nodes[targetLeaderName].Id
	r.reshardAndRemoveSingleUnit(name, healthyLeaderIp, targetLeaderId, v)
}

func (r *RedisClusterReconciler) reshardAndRemoveLeader(name string, leaderName string, healthyLeaderIp string, targetLeaderId string, v *view.RedisClusterView) {
	if leaderToRemove, exists := v.Nodes[name]; exists && leaderToRemove != nil {
		r.Log.Info(fmt.Sprintf("Resharding node: [%s]->all slots->[%s]", leaderToRemove.Id, targetLeaderId))
		err := r.reshardLeaderCheckCoverage(healthyLeaderIp, targetLeaderId, leaderToRemove, false)
		if err != nil {
			r.RedisClusterStateView.ClusterState = view.ClusterFix
			r.Log.Error(err, fmt.Sprintf("Error during attempt to reshard node [%s]", name))
			return
		}
		r.Log.Info(fmt.Sprintf("Leader reshard successful between [%s]->[%s]", leaderToRemove.Id, targetLeaderId))
	}

	r.RedisClusterStateView.SetNodeState(name, leaderName, view.DeleteNode)
	for _, n := range r.RedisClusterStateView.Nodes {
		if n.LeaderName == leaderName {
			if n.NodeState != view.ReshardNode {
				r.RedisClusterStateView.SetNodeState(n.Name, n.LeaderName, view.DeleteNode)
			}
		}
	}
}

func (r *RedisClusterReconciler) reshardAndRemoveSingleUnit(name string, healthyLeaderIp string, targetLeaderId string, v *view.RedisClusterView) {
	if nodeToRemove, exists := v.Nodes[name]; exists && nodeToRemove != nil {
		r.Log.Info(fmt.Sprintf("Resharding node: [%s]->all slots->[%s]", nodeToRemove.Id, targetLeaderId))
		err := r.reshardLeaderCheckCoverage(healthyLeaderIp, targetLeaderId, nodeToRemove, true)
		if err != nil {
			r.RedisClusterStateView.ClusterState = view.ClusterFix
			r.Log.Error(err, fmt.Sprintf("Error during attempt to reshard node [%s]", name))
			return
		}
		r.waitForAllNodesAgreeAboutSlotsConfiguration(v)
		r.Log.Info(fmt.Sprintf("Leader reshard successful between [%s]->[%s]", nodeToRemove.Id, targetLeaderId))
		r.deletePod(nodeToRemove.Pod)
	}
}

func (r *RedisClusterReconciler) reshardAndKeepInMap(name string, leaderName, healthyLeaderIp string, targetLeaderId string, v *view.RedisClusterView) {
	if leaderToRemove, exists := v.Nodes[name]; exists && leaderToRemove != nil {
		r.Log.Info(fmt.Sprintf("Resharding node: [%s]->all slots->[%s]", leaderToRemove.Id, targetLeaderId))
		err := r.reshardLeaderCheckCoverage(healthyLeaderIp, targetLeaderId, leaderToRemove, true)
		if err != nil {
			r.RedisClusterStateView.ClusterState = view.ClusterFix
			r.Log.Error(err, fmt.Sprintf("Error during attempt to reshard node [%s]", leaderName))
			return
		}
		r.Log.Info(fmt.Sprintf("Leader reshard successful between [%s]->[%s]", leaderToRemove.Id, targetLeaderId))
	}

	r.RedisClusterStateView.SetNodeState(name, leaderName, view.DeleteNodeKeepInMap)
	for _, n := range r.RedisClusterStateView.Nodes {
		if n.LeaderName == leaderName {
			if n.NodeState != view.ReshardNodeKeepInMap {
				r.RedisClusterStateView.SetNodeState(n.Name, n.LeaderName, view.DeleteNodeKeepInMap)
			}
		}
	}
}

func (r *RedisClusterReconciler) scaleUpFollowers(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) error {
	r.Log.Info("Scaling up followers")
	leadersToFollowerCount := r.numOfFollowersPerLeader(v)
	followersBySpec := redisCluster.Spec.LeaderFollowersCount
	for leaderName, followerCount := range leadersToFollowerCount {
		for f := followerCount + 1; f <= followersBySpec; f++ {
			name := leaderName + "-" + fmt.Sprint(f)
			r.RedisClusterStateView.Nodes[name] = &view.NodeStateView{
				Name:       name,
				LeaderName: leaderName,
				NodeState:  view.CreateNode,
			}
		}
	}
	r.Log.Info("Scaling up followers: new followers will be created within the next reconcile loop")
	return nil
}

func (r *RedisClusterReconciler) scaleDownFollowers(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) error {
	r.Log.Info("Scaling down followers")
	leadersToFollowerCount := r.numOfFollowersPerLeader(v)
	followersBySpec := redisCluster.Spec.LeaderFollowersCount
	for leaderName, followerCount := range leadersToFollowerCount {
		for f := followersBySpec + 1; f <= followerCount; f++ {
			name := leaderName + "-" + fmt.Sprint(f)
			if _, exists := r.RedisClusterStateView.Nodes[name]; exists {
				r.RedisClusterStateView.Nodes[name].NodeState = view.DeleteNode
			}
		}
	}
	return nil
}

func (r *RedisClusterReconciler) checkIfMaster(nodeIP string) (bool, error) {
	info, _, err := r.RedisCLI.Info(nodeIP)
	if err != nil || info == nil {
		return false, err
	}
	if info.Replication["role"] == "master" {
		return true, nil
	}
	return false, nil
}

func (r *RedisClusterReconciler) reshardLeaderCheckCoverage(healthyLeaderIp string, targetLeaderId string, leaderToRemove *view.NodeView, keepInMap bool) error {
	isMaster, e := r.checkIfMaster(leaderToRemove.Ip)
	if e != nil {
		return e
	}
	if !isMaster {
		r.RedisClusterStateView.SetNodeState(leaderToRemove.Name, leaderToRemove.LeaderName, view.DeleteNode)
		return nil
	}
	if !keepInMap {
		r.RedisClusterStateView.SetNodeState(leaderToRemove.Name, leaderToRemove.LeaderName, view.ReshardNode)
	}

	r.Log.Info(fmt.Sprintf("Performing resharding and coverage check on leader [%s]", leaderToRemove.Name))
	maxSlotsPerLeader := 16384
	success, _, e := r.RedisCLI.ClusterReshard(healthyLeaderIp, leaderToRemove.Id, targetLeaderId, maxSlotsPerLeader)
	if e != nil || !success {
		return e
	}
	emptyLeadersIds, fullCoverage, e := r.CheckClusterAndCoverage(healthyLeaderIp)
	if !fullCoverage || e != nil {
		r.RedisClusterStateView.ClusterState = view.ClusterFix
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
		if node == nil {
			continue
		}
		if _, contained := followersPerLeader[node.LeaderName]; !contained {
			followersPerLeader[node.LeaderName] = 0
		}
		if !node.IsLeader {
			followersPerLeader[node.LeaderName]++
		}
	}
	return followersPerLeader
}

func roundFloatToPercision(num float64, percision int) float64 {
	o := math.Pow(10, float64(percision))
	return float64(round(num*o)) / o
}

func round(num float64) int {
	return int(num + math.Copysign(0.5, num))
}
