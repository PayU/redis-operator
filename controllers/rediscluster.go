package controllers

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/pkg/errors"

	dbv1 "github.com/PayU/redis-operator/api/v1"
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
	r.Log.Info("Getting cluster view...")
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
	missingPods := r.detectMissingPods(v)
	if len(missingPods) > 0 {
		r.createMissingPods(redisCluster, missingPods, v)
	}
	r.Log.Info("[OK] Redis followers initialized")
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

func (r *RedisClusterReconciler) failOverToReplica(leaderName string, v *view.RedisClusterView) (promotedReplica *view.NodeView, err error) {
	for _, n := range v.Nodes {
		if n.LeaderName == leaderName && n.Name != leaderName {
			err := r.attemptToFailOver(n.Ip)
			if err != nil {
				continue
			}
			return n, nil
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
	if err := r.waitForManualFailover(promotedNodeIp); err != nil {
		return err
	}
	return nil
}

func (r *RedisClusterReconciler) removeAndDeleteNodes(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView, healthyServerIp string) {
	toDeleteFromMap := []string{}
	// first remove all followers
	for _, n := range r.RedisClusterStateView.Nodes {
		if n.NodeState == view.RemoveNode && n.Name != n.LeaderName {
			if node, exists := v.Nodes[n.Name]; exists {
				err := r.removeNode(healthyServerIp, node)
				if err != nil {
					r.Log.Error(err, fmt.Sprintf("Could not remove node [%s:%s]", node.Name, node.Ip))
					continue
				}
				toDeleteFromMap = append(toDeleteFromMap, n.Name)
			}
		}
	}
	// second remove all leaders
	for _, n := range r.RedisClusterStateView.Nodes {
		if n.NodeState == view.RemoveNode && n.Name == n.LeaderName {
			if node, exists := v.Nodes[n.Name]; exists {
				err := r.removeNode(healthyServerIp, node)
				if err != nil {
					r.Log.Error(err, fmt.Sprintf("Could not remove node [%s:%s]", node.Name, node.Ip))
					continue
				}
				toDeleteFromMap = append(toDeleteFromMap, n.Name)
			}
		}
	}
	// third delete all removed pods
	podsToDelete := []corev1.Pod{}
	for _, n := range r.RedisClusterStateView.Nodes {
		if n.NodeState == view.DeleteNode {
			if node, exists := v.Nodes[n.Name]; exists {
				podsToDelete = append(podsToDelete, node.Pod)
			}
		}
	}
	deletedPods, err := r.deletePods(podsToDelete)
	if err != nil {
		r.Log.Error(err, "Error while attempting to delete removed pods")
		return
	}
	r.waitForPodDelete(deletedPods...)
	// four delete from map if necessary
	for _, d := range toDeleteFromMap {
		delete(r.RedisClusterStateView.Nodes, d)
	}
	// five: case of existing pods that doesnt appear in map: make sure for each one of them if has slots and if ready for deletion
}

func (r *RedisClusterReconciler) removeNode(healthyServerIp string, n *view.NodeView) error {
	r.Log.Info(fmt.Sprintf("Removing node [%s] from all tables...", n.Id))
	_, err := r.RedisCLI.DelNode(healthyServerIp, n.Id)
	if err != nil {
		return err
	}
	r.Log.Info(fmt.Sprintf("Waiting for node [%s:%s] removal to be completed...", n.Ip, n.Id))
	if pollErr := wait.PollImmediate(2*time.Second, 10*time.Second, func() (bool, error) {
		clusterNodes, _, err := r.RedisCLI.ClusterNodes(n.Ip)
		if err != nil {
			return false, err
		}
		if len(*clusterNodes) == 1 {
			return true, nil
		}
		return false, nil
	}); pollErr != nil {
		return pollErr
	}
	r.RedisClusterStateView.Nodes[n.Name].NodeState = view.DeleteNode
	return nil
}

func (r *RedisClusterReconciler) addLeaderNodes(redisCluster *dbv1.RedisCluster, healthyServerIp string, leaderNames []string) error {
	if len(leaderNames) == 0 {
		return nil
	}
	leaders, e := r.createRedisLeaderPods(redisCluster, leaderNames...)
	if e != nil || len(leaders) == 0 {
		r.deletePods(leaders)
		r.Log.Error(e, "Could not add new leaders")
		return e
	}
	var wg sync.WaitGroup
	mutex := &sync.Mutex{}
	for _, leader := range leaders {
		wg.Add(1)
		go func(leader corev1.Pod) {
			defer wg.Done()
			newLeader, e := r.waitForPodReady(leader)
			if e != nil || len(newLeader) == 0 {
				r.deletePods(newLeader)
				mutex.Lock()
				delete(r.RedisClusterStateView.Nodes, leader.Name)
				mutex.Unlock()
				r.Log.Error(e, fmt.Sprintf("Error while waiting for pod [%s] to be ready\n", leader.Name))
				return
			}
			leaderPod := newLeader[0]
			e = r.waitForRedis(leaderPod.Status.PodIP)
			if e != nil {
				r.deletePod(leaderPod)
				mutex.Lock()
				delete(r.RedisClusterStateView.Nodes, leader.Name)
				mutex.Unlock()
				r.Log.Error(e, fmt.Sprintf("Error while waiting for pod [%s] to be ready\n", leaderPod.Name))
				return
			}
			_, e = r.RedisCLI.AddLeader(leaderPod.Status.PodIP, healthyServerIp)
			if e != nil {
				r.deletePod(leaderPod)
				mutex.Lock()
				delete(r.RedisClusterStateView.Nodes, leader.Name)
				mutex.Unlock()
				r.Log.Error(e, fmt.Sprintf("Error while adding pod [%s] to redis cluster, healthy node ip: [%s]", leaderPod.Name, healthyServerIp))
				return
			}
			r.RedisClusterStateView.Nodes[leaderPod.Name].NodeState = view.NodeOK
		}(leader)
	}
	wg.Wait()
	return nil
}

func (r *RedisClusterReconciler) ensureForgetLostNodes(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) {
	for i := 0; i < 10 && r.forgetLostNodes(redisCluster, v); i++ {
	}
}

// Removes all nodes the cluster node table entries with IDs of nodes not available
// Recives the list of healthy cluster nodes (Redis is reachable and has cluster mode on)
func (r *RedisClusterReconciler) forgetLostNodes(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) bool {
	healthyIps := make([]string, 0)
	lostIds := make([]string, 0)
	visitedById := make(map[string]bool)

	// use mutexes, fix cannot run if tables not updated to remove failing rows
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
		if clusterNodes != nil {
			for _, tableNode := range *clusterNodes {
				isLost := strings.Contains(tableNode.Flags, "fail")
				if _, declaredAsLost := visitedById[tableNode.ID]; !declaredAsLost && isLost {
					lostIds = append(lostIds, tableNode.ID)
					visitedById[tableNode.ID] = true
				}
			}
		}
	}

	if len(lostIds) > 0 {
		r.Log.Info(fmt.Sprintf("List of lost nodes ids: %v", lostIds))
		for _, id := range lostIds {
			var forgetByOneWG sync.WaitGroup
			for _, ip := range healthyIps {
				forgetByOneWG.Add(1)
				go func() {
					defer forgetByOneWG.Done()
					r.RedisCLI.ClusterForget(ip, id)
				}()
			}
			forgetByOneWG.Wait()
		}
	}

	return len(lostIds) > 0
}

func (r *RedisClusterReconciler) recoverCluster(redisCluster *dbv1.RedisCluster) error {
	v, err := r.NewRedisClusterView(redisCluster)
	if err != nil {
		return err
	}

	failingPodsCleared := r.waitForNonReachablePodsTermination(redisCluster, v)
	if failingPodsCleared {
		return nil
	}

	r.ensureForgetLostNodes(redisCluster, v)

	r.Log.Info("Validating cluster state...")
	recoveryRequired, err := r.RecoverRedisCluster(redisCluster, v)
	if err != nil || recoveryRequired {
		return err
	}

	missingPods := r.detectMissingPods(v)
	if len(missingPods) > 0 {
		recoveryRequired, err = r.recreateLeaderWithoutReplicas(redisCluster, missingPods, v)
		if err != nil || recoveryRequired {
			return err
		}
		recoveryRequired = r.createMissingPods(redisCluster, missingPods, v)
	} else {
		recoveryRequired = r.recoverInterruptedFlows(redisCluster, v)
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
	return nil
}

func (r *RedisClusterReconciler) detectMissingPods(v *view.RedisClusterView) map[string]*view.MissingNodeView {
	missingNodesView := map[string]*view.MissingNodeView{}
	var wg sync.WaitGroup
	mutex := &sync.Mutex{}
	for _, n := range r.RedisClusterStateView.Nodes {
		wg.Add(1)
		go func(n *view.NodeStateView) {
			defer wg.Done()
			if _, exists := v.Nodes[n.Name]; !exists && n.NodeState != view.RemoveNode && n.NodeState != view.DeleteNode {
				for _, node := range v.Nodes {
					if n.LeaderName == node.LeaderName {
						info, _, err := r.RedisCLI.Info(node.Ip)
						if err != nil {
							continue
						}
						if info.Replication["role"] == "master" {
							mutex.Lock()
							missingNodesView[n.Name] = &view.MissingNodeView{
								Name:              n.Name,
								LeaderName:        n.LeaderName,
								CurrentMasterName: node.Name,
								CurrentMasterId:   node.Id,
								CurrentMasterIp:   node.Ip,
							}
							mutex.Unlock()
							return
						}
					}
				}
				mutex.Lock()
				missingNodesView[n.LeaderName] = &view.MissingNodeView{
					Name:              n.Name,
					LeaderName:        n.LeaderName,
					CurrentMasterName: "",
				}
				mutex.Unlock()
			}
		}(n)
	}
	wg.Wait()
	for _, cn := range v.Nodes {
		if n, exists := r.RedisClusterStateView.Nodes[cn.Name]; exists && n.NodeState == view.CreateNode {
			wg.Add(1)
			go func() {
				defer wg.Done()
				nodes, _, err := r.RedisCLI.ClusterNodes(cn.Ip)
				if err != nil || nodes == nil {
					r.deletePod(cn.Pod)
					return
				}
				if len(*nodes) > 1 {
					mutex.Lock()
					r.RedisClusterStateView.SetNodeState(n.Name, n.LeaderName, view.ReplicateNode)
					mutex.Unlock()
				} else {
					mutex.Lock()
					r.RedisClusterStateView.SetNodeState(n.Name, n.LeaderName, view.AddNode)
					mutex.Unlock()
				}
			}()
		}
	}
	wg.Wait()
	return missingNodesView
}

func (r *RedisClusterReconciler) createMissingPods(redisCluster *dbv1.RedisCluster, missingNodesView map[string]*view.MissingNodeView, v *view.RedisClusterView) bool {
	pods := r.createRedisPods(redisCluster, missingNodesView, v)
	if len(pods) == 0 {
		return false
	}
	newPods, err := r.waitForPodNetworkInterface(pods...)
	if err != nil {
		r.Log.Error(err, fmt.Sprintf("Could not re create missing pods"))
		r.deletePods(pods)
		return true
	}
	var wg sync.WaitGroup
	mutex := &sync.Mutex{}
	for _, p := range newPods {
		wg.Add(1)
		go func(p corev1.Pod) {
			defer wg.Done()
			pod, err := r.waitForRedisPod(p)
			if err != nil {
				return
			}
			m, exists := missingNodesView[pod.Name]
			if !exists {
				return
			}
			r.recoverFromAddNode(pod, m, mutex)
		}(p)
	}
	wg.Wait()
	return true
}

func (r *RedisClusterReconciler) recoverFromAddNode(p corev1.Pod, m *view.MissingNodeView, mutex *sync.Mutex) {
	masterIp := m.CurrentMasterIp
	masterId := m.CurrentMasterId
	newPodIp := p.Status.PodIP

	nodes, _, err := r.RedisCLI.ClusterNodes(newPodIp)
	if err != nil || nodes == nil {
		r.deletePod(p)
		mutex.Lock()
		r.RedisClusterStateView.SetNodeState(m.Name, m.LeaderName, view.CreateNode)
		mutex.Unlock()
		return
	}

	if len(*nodes) == 1 {
		r.Log.Info(fmt.Sprintf("Adding new redis node [%s], current master [%s]", m.Name, m.CurrentMasterName))
		mutex.Lock()
		r.RedisClusterStateView.SetNodeState(m.Name, m.LeaderName, view.AddNode)
		_, err = r.RedisCLI.AddFollower(newPodIp, masterIp, masterId)
		mutex.Unlock()
		if err != nil {
			r.Log.Error(err, fmt.Sprintf("Could not perform add node [%s] to cluster ", m.Name))
			return
		}
		r.Log.Info(fmt.Sprintf("Waiting for cluster meet [%s]-[%s]", m.CurrentMasterName, m.Name))
		mutex.Lock()
		err = r.waitForRedisMeet(newPodIp)
		mutex.Unlock()
		if err != nil {
			r.Log.Error(err, fmt.Sprintf("Error while waiting for add node [%s] to cluster ", m.Name))
			return
		}
	}
	r.recoverFromReplicateNode(p.Status.PodIP, m, mutex)
}

func (r *RedisClusterReconciler) recoverFromReplicateNode(podIp string, m *view.MissingNodeView, mutex *sync.Mutex) {
	mutex.Lock()
	r.RedisClusterStateView.SetNodeState(m.Name, m.LeaderName, view.ReplicateNode)
	mutex.Unlock()
	newPodId, err := r.RedisCLI.MyClusterID(podIp)
	if err != nil {
		return
	}
	err = r.waitForRedisReplication(m.CurrentMasterName, m.CurrentMasterIp, m.CurrentMasterId, m.Name, newPodId)
	if err != nil {
		r.Log.Error(err, fmt.Sprintf("Error while waiting for node [%s] replication ", m.Name))
		return
	}
	r.recoverFromSyncNode(podIp, m, mutex)
}

func (r *RedisClusterReconciler) recoverFromSyncNode(podIp string, m *view.MissingNodeView, mutex *sync.Mutex) {
	mutex.Lock()
	r.RedisClusterStateView.SetNodeState(m.Name, m.LeaderName, view.SyncNode)
	mutex.Unlock()
	err := r.waitForRedisSync(m.Name, podIp)
	if err != nil {
		r.Log.Error(err, fmt.Sprintf("Error while waiting for node [%s] sync process ", m.Name))
		return
	}
	r.recoverFromFailOver(podIp, m, mutex)
}

func (r *RedisClusterReconciler) recoverFromFailOver(podIp string, m *view.MissingNodeView, mutex *sync.Mutex) {
	if m.Name == m.LeaderName {
		r.Log.Info(fmt.Sprintf("Performing failover for node [%s]", m.Name))
		mutex.Lock()
		r.RedisClusterStateView.SetNodeState(m.Name, m.LeaderName, view.FailoverNode)
		err := r.doFailover(podIp, "")
		mutex.Unlock()
		if err != nil {
			r.Log.Info(fmt.Sprintf("[Warning] Attempt to failover with node [%s:%s] failed", m.Name, podIp))
			return
		}
	}
	mutex.Lock()
	r.RedisClusterStateView.SetNodeState(m.Name, m.LeaderName, view.NodeOK)
	mutex.Unlock()
}

func (r *RedisClusterReconciler) findPromotedMasterReplica(name string, v *view.RedisClusterView) (promotedReplica *view.NodeView) {
	n, exists := v.Nodes[name]
	if !exists {
		return nil
	}

	for _, clusterNode := range v.Nodes {
		if clusterNode.LeaderName == n.LeaderName {
			info, _, err := r.RedisCLI.Info(clusterNode.Ip)
			if err != nil || info == nil {
				continue
			}
			if info.Replication["role"] == "master" {
				return clusterNode
			}
		}
	}
	return nil
}

func (r *RedisClusterReconciler) recoverInterruptedFlows(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) bool {
	recoveryRequired := false
	mutex := &sync.Mutex{}
	for _, n := range r.RedisClusterStateView.Nodes {
		node, exists := v.Nodes[n.Name]
		if !exists {
			r.RedisClusterStateView.SetNodeState(n.Name, n.LeaderName, view.CreateNode)
			continue
		}
		if n.NodeState == view.ReshardNode {
			recoveryRequired = true
			healthyLeaderName := r.findHealthyLeader(v)
			healthyLeader, exists := v.Nodes[healthyLeaderName]
			if !exists {
				continue
			}
			targetLeaderName := r.findHealthyLeader(v, map[string]bool{healthyLeaderName: true})
			targetLeader, exists := v.Nodes[targetLeaderName]
			if !exists {
				continue
			}
			r.reshardAndRemoveLeader(n.Name, healthyLeader.Ip, targetLeader.Id, v)
		}

		promotedMasterReplica := r.findPromotedMasterReplica(n.Name, v)
		if promotedMasterReplica == nil {
			continue
		}

		m := &view.MissingNodeView{
			Name:              n.Name,
			LeaderName:        n.LeaderName,
			CurrentMasterName: promotedMasterReplica.Name,
			CurrentMasterId:   promotedMasterReplica.Id,
			CurrentMasterIp:   promotedMasterReplica.Ip,
		}

		switch n.NodeState {
		case view.AddNode:
			recoveryRequired = true
			r.recoverFromAddNode(node.Pod, m, mutex)
			break
		case view.ReplicateNode:
			recoveryRequired = true
			r.recoverFromReplicateNode(node.Ip, m, mutex)
			break
		case view.SyncNode:
			recoveryRequired = true
			r.recoverFromSyncNode(node.Ip, m, mutex)
			break
		case view.FailoverNode:
			recoveryRequired = true
			r.recoverFromFailOver(node.Ip, m, mutex)
			break
		}
	}
	return recoveryRequired
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
			terminatingPods = append(terminatingPods, pod)
			continue
		}
		clusterInfo, _, e := r.RedisCLI.ClusterInfo(pod.Status.PodIP)
		if e != nil || (*clusterInfo) == nil || (*clusterInfo)["cluster_state"] != "ok" {
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
	v, err := r.NewRedisClusterView(cluster)
	if err != nil {
		return err
	}
	deletedPods := []corev1.Pod{}
	missingPodsView := map[string]*view.MissingNodeView{}

	for _, n := range v.Nodes {
		if n.Name == n.LeaderName {
			podUpToDate, err := r.isPodUpToDate(redisCluster, n.Pod)
			if err != nil {
				continue
			}
			if !podUpToDate {
				promotedReplica, err := r.failOverToReplica(n.Name, v)
				if err != nil || promotedReplica == nil {
					continue
				}
				missingPodsView[n.Name] = &view.MissingNodeView{
					Name:              n.Name,
					LeaderName:        n.LeaderName,
					CurrentMasterName: promotedReplica.Name,
					CurrentMasterId:   promotedReplica.Id,
					CurrentMasterIp:   promotedReplica.Ip,
				}
				err = r.deletePod(n.Pod)
				if err != nil {
					deletedPods = append(deletedPods, n.Pod)
				}
			}
		}
	}
	if len(deletedPods) > 0 || len(missingPodsView) > 0 {
		r.waitForPodDelete(deletedPods...)
		r.createMissingPods(redisCluster, missingPodsView, v)
		return nil
	}

	for _, n := range v.Nodes {
		if n.Name != n.LeaderName {
			podUpToDate, err := r.isPodUpToDate(redisCluster, n.Pod)
			if err != nil {
				continue
			}
			if !podUpToDate {
				r.deletePod(n.Pod)
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
		if pollErr := wait.PollImmediate(3*r.Config.Times.RedisPingCheckInterval, 10*r.Config.Times.RedisPingCheckTimeout, func() (bool, error) {
			reply, err := r.RedisCLI.Ping(nodeIP)
			if err != nil {
				return false, err
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
func (r *RedisClusterReconciler) waitForRedisSync(name string, nodeIP string) error {
	r.Log.Info(fmt.Sprintf("Waiting for SYNC to start on [%s:%s]", name, nodeIP))
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
		r.Log.Info(fmt.Sprintf("[WARN] Timeout waiting for SYNC process to start on [%s:%s]", name, nodeIP))
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

		r.Log.Info(fmt.Sprintf("Node [%s:%s] is synced", name, nodeIP))
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

func (r *RedisClusterReconciler) waitForRedisReplication(leaderName string, leaderIP string, leaderID string, followerName string, followerID string) error {
	r.Log.Info(fmt.Sprintf("Waiting for CLUSTER REPLICATION [%s:%s]->[%s:%s]", leaderName, leaderID, followerName, followerID))
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

func (r *RedisClusterReconciler) waitForRedisMeet(newNodeIP string) error {
	return wait.PollImmediate(3*r.Config.Times.RedisClusterMeetCheckInterval, 10*r.Config.Times.RedisClusterMeetCheckTimeout, func() (bool, error) {
		clusterNodes, _, err := r.RedisCLI.ClusterNodes(newNodeIP)
		if err != nil {
			return false, err
		}
		if len(*clusterNodes) > 1 {
			return true, nil
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
		if _, exists := v.Nodes[n.Name]; !exists && n.NodeState != view.RemoveNode && n.NodeState != view.DeleteNode {
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
		r.removeAndDeleteNodes(redisCluster, v, h.Ip)
	}

	return isComplete, nil
}

func (r *RedisClusterReconciler) recreateLeaderWithoutReplicas(redisCluster *dbv1.RedisCluster, missingNodesView map[string]*view.MissingNodeView, v *view.RedisClusterView) (bool, error) {
	r.ensureForgetLostNodes(redisCluster, v)
	leaders := []string{}
	for _, m := range missingNodesView {
		if m.CurrentMasterName == "" {
			leaders = append(leaders, m.Name)
		}
	}
	if len(leaders) > 0 {
		healthyLeaderName := r.findHealthyLeader(v)
		if len(healthyLeaderName) == 0 {
			return true, errors.New("Could not find healthy reachable leader to serve the fix request")
		}
		healthyLeaderIp := v.Nodes[healthyLeaderName].Ip
		r.RedisClusterStateView.ClusterState = view.ClusterFix
		_, _, e := r.RedisCLI.ClusterFix(healthyLeaderIp)
		if e != nil {
			stdout, err := r.RedisCLI.ClusterCheck(healthyLeaderIp)
			if err != nil || !strings.Contains(stdout, "[OK] All 16384 slots covered") {
				return true, e
			}
		}
		r.RedisClusterStateView.ClusterState = view.ClusterRebalance
		e = r.addLeaderNodes(redisCluster, healthyLeaderIp, leaders)
		if e != nil {
			return true, e
		}
		successful, _, e := r.RedisCLI.ClusterRebalance(healthyLeaderIp, true)
		if !successful || e != nil {
			r.Log.Error(nil, fmt.Sprintf("Could not perform cluster rebalance with ip [%s]", healthyLeaderIp))
			return true, e
		}
		r.RedisClusterStateView.ClusterState = view.ClusterOK
	}
	return len(leaders) > 0, nil
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
	r.Log.Info("Scaling up leaders")
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
	if len(newLeadersNames) > 0 {
		r.RedisClusterStateView.ClusterState = view.ClusterRebalance
		e := r.addLeaderNodes(redisCluster, healthyLeaderIp, newLeadersNames)
		if e != nil {
			return e
		}
		time.Sleep(3 * time.Second)
		successful, _, e := r.RedisCLI.ClusterRebalance(healthyLeaderIp, true)
		if !successful || e != nil {
			r.Log.Error(nil, fmt.Sprintf("Could not perform cluster rebalance with ip [%s]", healthyLeaderIp))
			return e
		}
	} else {
		r.Log.Info("[Warn] New leader names list appeard to be empty, no leaders been added to cluster")
	}
	r.Log.Info("Scaling up leaders: operation succeeded")
	r.RedisClusterStateView.ClusterState = view.ClusterOK
	return nil
}

func (r *RedisClusterReconciler) scaleDownLeaders(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) error {
	r.Log.Info("Scaling down leaders")
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
		r.reshardAndRemoveLeader(leaderName, healthyLeaderIp, targetLeaderId, v)
	}
	r.removeAndDeleteNodes(redisCluster, v, healthyLeaderIp)
	r.Log.Info("Scaling down leaders: operation succeeded")
	return nil
}

func (r *RedisClusterReconciler) reshardAndRemoveLeader(leaderName string, healthyLeaderIp string, targetLeaderId string, v *view.RedisClusterView) {
	if leaderToRemove, exists := v.Nodes[leaderName]; exists {
		r.RedisClusterStateView.SetNodeState(leaderName, leaderName, view.ReshardNode)
		err := r.reshardLeaderCheckCoverage(healthyLeaderIp, targetLeaderId, leaderToRemove)
		if err != nil {
			r.RedisClusterStateView.ClusterState = view.ClusterFix
			r.Log.Error(err, fmt.Sprintf("Error during attempt to reshard node [%s]", leaderName))
			return
		}
		r.Log.Info(fmt.Sprintf("Leader reshard successful [%s]->all slots->[%s]", leaderToRemove.Id, targetLeaderId))
	} else {
		r.RedisClusterStateView.ClusterState = view.ClusterFix
		success, _, err := r.RedisCLI.ClusterFix(healthyLeaderIp)
		if err != nil || !success {
			r.Log.Error(err, "Error during attempt to fix cluster")
			return
		}
		r.RedisClusterStateView.ClusterState = view.ClusterOK
	}

	for _, node := range v.Nodes {
		if node.LeaderName == leaderName {
			r.RedisClusterStateView.SetNodeState(node.Name, node.LeaderName, view.RemoveNode)
		}
	}
}

func (r *RedisClusterReconciler) scaleUpFollowers(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) error {
	r.Log.Info("Scaling up followers")
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
	r.Log.Info("Scaling up followers: new followers will be created within the next reconcile loop")
	return nil
}

func (r *RedisClusterReconciler) scaleDownFollowers(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) error {
	r.Log.Info("Scaling down followers")
	leadersToFollowerCount := r.numOfFollowersPerLeader(v)
	followersBySpec := redisCluster.Spec.LeaderFollowersCount
	for leaderName, followerCount := range leadersToFollowerCount {
		for i := followersBySpec + 1; i <= followerCount; i++ {
			name := leaderName + "-" + fmt.Sprint(i)
			if _, exists := r.RedisClusterStateView.Nodes[name]; exists {
				r.RedisClusterStateView.Nodes[name].NodeState = view.RemoveNode
			}
		}
	}
	healthyLeaderName := r.findHealthyLeader(v)
	if len(healthyLeaderName) == 0 {
		return errors.New("Could not find healthy reachable leader to serve the fix request")
	}
	healthyLeaderIp := v.Nodes[healthyLeaderName].Ip
	r.removeAndDeleteNodes(redisCluster, v, healthyLeaderIp)
	r.Log.Info("Scaling down followers: operation succeeded completely/partially, leftover followers might be removed within next reconcile loops")
	return nil
}

func (r *RedisClusterReconciler) reshardLeaderCheckCoverage(healthyLeaderIp string, targetLeaderId string, leaderToRemove *view.NodeView) error {
	r.Log.Info(fmt.Sprintf("Performing resharding and coverage check on leader [%s]", leaderToRemove.Name))
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
