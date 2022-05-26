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

func (r *RedisClusterReconciler) initializeFollowers(redisCluster *dbv1.RedisCluster) error {
	r.Log.Info("Initializing followers...")
	v, ok := r.NewRedisClusterView(redisCluster)
	if !ok {
		return errors.New("Could not perform redis cluster initialize followers")
	}
	if r.recoverNodes(redisCluster, v) {
		r.Log.Info("[OK] Redis followers initialization attempt performed")
	}
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

func (r *RedisClusterReconciler) removeAndDeleteNodes(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) {
	healthyLeaderName, found := r.findHealthyLeader(v)
	if !found {
		r.Log.Error(errors.New(""), "Could not find healthy leader ip, aborting remove and delete operation...")
		return
	}
	healthyServerIp := v.Nodes[healthyLeaderName].Ip
	toDeleteFromMap := []string{}
	// first remove all followers
	for _, n := range r.RedisClusterStateView.Nodes {
		if n.NodeState == view.RemoveNode && n.Name != n.LeaderName {
			if node, exists := v.Nodes[n.Name]; exists {
				r.removeNode(healthyServerIp, node)
			}
			toDeleteFromMap = append(toDeleteFromMap, n.Name)
		}
	}
	// second remove all leaders
	for _, n := range r.RedisClusterStateView.Nodes {
		if n.NodeState == view.RemoveNode && n.Name == n.LeaderName {
			if node, exists := v.Nodes[n.Name]; exists {
				r.removeNode(healthyServerIp, node)
			}
			toDeleteFromMap = append(toDeleteFromMap, n.Name)
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
	// five: todo: case of existing pods that doesnt appear in map: make sure for each one of them if has slots and if ready for deletion
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
		go func(leader corev1.Pod, wg *sync.WaitGroup) {
			defer wg.Done()
			println("Wait pod " + leader.Name)
			newLeader, e := r.waitForPodReady(leader)
			if e != nil || len(newLeader) == 0 {
				message := fmt.Sprintf("Error while waiting for pod [%s] to be read", leader.Name)
				r.handleCreateErrByDeleteGracefully(leader.Name, leader, mutex, message, e)
				return
			}
			leaderPod := newLeader[0]
			println("Wait redis " + leader.Name)
			e = r.waitForRedis(leaderPod.Status.PodIP)
			if e != nil {
				message := fmt.Sprintf("Error while waiting for pod [%s] to be ready", leaderPod.Name)
				r.handleCreateErrByDeleteGracefully(leader.Name, leaderPod, mutex, message, e)
				return
			}

			r.RedisCLI.Flushall(leaderPod.Status.PodIP)
			r.RedisCLI.ClusterReset(leaderPod.Status.PodIP)

		}(leader, &wg)
	}
	wg.Wait()
	println("healthy server ip : " + healthyServerIp)
	for _, leader := range leaders {
		println("Add leader " + leader.Name)
		_, e = r.RedisCLI.AddLeader(leader.Status.PodIP, healthyServerIp)
		if e != nil {
			message := fmt.Sprintf("Error while adding pod [%s] to redis cluster, healthy node ip: [%s]", leader.Name, healthyServerIp)
			r.handleCreateErrByDeleteGracefully(leader.Name, leader, mutex, message, e)
			continue
		}
		println("Wait meet " + leader.Name)
		e = r.waitForRedisMeet(leader.Status.PodIP)
		if e != nil {
			message := fmt.Sprintf("Error while adding pod [%s] to redis cluster, healthy node ip: [%s]", leader.Name, healthyServerIp)
			r.handleCreateErrByDeleteGracefully(leader.Name, leader, mutex, message, e)
			continue
		}
		println("Setting ok " + leader.Name)
		r.RedisClusterStateView.SetNodeState(leader.Name, leader.Name, view.NewEmptyNode)

		nodes, _, err := r.RedisCLI.ClusterNodes(leader.Status.PodIP)
		if err != nil || nodes == nil {
			println("nodes list error for " + leader.Name)
		} else {
			for _, n := range *nodes {
				fmt.Printf("Node [%s] knowing node [%s]\n", leader.Name, n.Addr)
			}
		}
	}
	return nil
}

func (r *RedisClusterReconciler) handleCreateErrByDeleteGracefully(name string, leaderPod corev1.Pod, mutex *sync.Mutex, message string, e error) {
	r.deletePod(leaderPod)
	r.RedisClusterStateView.LockResourceAndRemoveFromMap(name, mutex)
	r.Log.Error(e, message)
}

// Removes all nodes the cluster node table entries with IDs of nodes not available
// Recives the list of healthy cluster nodes (Redis is reachable and has cluster mode on)
func (r *RedisClusterReconciler) forgetLostNodes(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) bool {
	healthyNodes := map[string]string{}
	lostIds := map[string]bool{}
	for _, node := range v.Nodes {
		clusterNodes, _, err := r.RedisCLI.ClusterNodes(node.Ip)
		if err == nil {
			mutex.Lock()
			healthyNodes[node.Id] = node.Ip
			mutex.Unlock()
		} else {
			r.Log.Info("[Warn] Forget lost nodes operation: non reachable pods detcted, re-attempting reconcile loop...")
			return true
		}
		if clusterNodes != nil {
			for _, tableNode := range *clusterNodes {
				isLost := strings.Contains(tableNode.Flags, "fail")
				mutex.Lock()
				if isLost {
					lostIds[tableNode.ID] = true
				}
				mutex.Unlock()
			}
		}
	}

	if len(lostIds) > 0 {
		r.Log.Info(fmt.Sprintf("List of healthy nodes: %v", healthyNodes))
		r.Log.Info(fmt.Sprintf("List of lost nodes ids: %v", lostIds))
		for id, _ := range lostIds {
			for _, ip := range healthyNodes {
				r.RedisCLI.ClusterForget(ip, id)
			}
		}
	}
	return len(lostIds) > 0
}

func (r *RedisClusterReconciler) recoverCluster(redisCluster *dbv1.RedisCluster) error {
	v, ok := r.NewRedisClusterView(redisCluster)
	if !ok {
		return errors.New("Could not perform redis cluster recover")
	}
	failingPodsCleared := r.waitForNonReachablePodsTermination(redisCluster, v)
	if failingPodsCleared {
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

	recoveryRequired, err = r.recoverLossOfLeaderAndFollowers(redisCluster, v)
	if err != nil || recoveryRequired {
		return err
	}
	recoveryRequired = r.recoverNodes(redisCluster, v)

	if !recoveryRequired {
		complete, err := r.isClusterHealthy(redisCluster, v)
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

func (r *RedisClusterReconciler) detectLossOfLeadersAndAllFollowers(v *view.RedisClusterView) []string {
	missing := []string{}
	reportedMissing := map[string]bool{}
	var wg sync.WaitGroup
	mutex := &sync.Mutex{}
	for _, n := range r.RedisClusterStateView.Nodes {
		wg.Add(1)
		go func(n *view.NodeStateView, wg *sync.WaitGroup) {
			defer wg.Done()
			_, exists := v.Nodes[n.Name]
			if !exists && n.NodeState != view.RemoveNode && n.NodeState != view.DeleteNode {
				_, leaderExists := v.Nodes[n.LeaderName]
				if leaderExists {
					l, leaderHasState := r.RedisClusterStateView.Nodes[n.LeaderName]
					if leaderHasState && l.NodeState == view.NewEmptyNode {
						return
					}
				}
				for _, node := range v.Nodes {
					if n.LeaderName == node.LeaderName {
						info, _, err := r.RedisCLI.Info(node.Ip)
						if err != nil {
							continue
						}
						if info.Replication["role"] == "master" {
							nodes, _, err := r.RedisCLI.ClusterNodes(node.Ip)
							if err != nil || nodes == nil {
								continue
							}
							if len(*nodes) > 1 {
								continue
							} else {
								r.RedisClusterStateView.LockResourceAndSetNodeState(node.Name, node.LeaderName, view.AddNode, mutex)
							}
							return
						}
					}
				}
				mutex.Lock()
				_, markedMissing := reportedMissing[n.LeaderName]
				if !markedMissing {
					missing = append(missing, n.LeaderName)
					if n.Name == n.LeaderName {
						r.RedisClusterStateView.SetNodeState(n.Name, n.LeaderName, view.ReshardNode)
					}
				}
				mutex.Unlock()
			}
		}(n, &wg)
	}
	wg.Wait()
	return missing
}

func (r *RedisClusterReconciler) retrievePodForProcessing(name string, leaderName string, pods map[string]corev1.Pod, v *view.RedisClusterView, mutex *sync.Mutex) (corev1.Pod, bool) {
	pod := corev1.Pod{}
	newPod, justCreated := pods[name]
	existingNode, exists := v.Nodes[name]
	if justCreated {
		pod = newPod
	} else {
		if exists {
			pod = existingNode.Pod
		} else {
			r.RedisClusterStateView.LockResourceAndSetNodeState(name, leaderName, view.CreateNode, mutex)
			return pod, false
		}
	}
	return pod, true
}

func (r *RedisClusterReconciler) recoverNodes(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) bool {
	actionRequired := false
	mutex := &sync.Mutex{}
	// must be synchrounous
	r.Log.Info("Checking if sharding process have been interrupted for some nodes...")
	for _, n := range r.RedisClusterStateView.Nodes {
		switch n.NodeState {
		case view.ReshardNode:
			actionRequired = true
			r.scaleDownLeader(n.Name, map[string]bool{}, v)
		case view.NewEmptyNode:
			actionRequired = true
			r.recoverFromNewEmptyNode(n.Name, v)
		}
	}
	if actionRequired {
		r.RedisClusterStateView.ClusterState = view.ClusterFix
		r.removeAndDeleteNodes(redisCluster, v)
		return actionRequired
	}
	r.Log.Info("[OK] Previous sharding requests ended successfully")

	r.Log.Info("Detecting missing nodes in cluster...")
	pods := r.createMissingRedisPods(redisCluster, v)
	if len(pods) == 0 {
		r.Log.Info("[OK] No missing pods detected for cluster")
	}
	var wg sync.WaitGroup
	// can be asynchrounous
	r.Log.Info("Mitigating failures and interrupted flows...")
	for _, n := range r.RedisClusterStateView.Nodes {
		if n.NodeState == view.NodeOK {
			continue
		}
		actionRequired = true
		wg.Add(1)
		go func(n *view.NodeStateView, wg *sync.WaitGroup) {
			defer wg.Done()
			pod, proceed := r.retrievePodForProcessing(n.Name, n.LeaderName, pods, v, mutex)
			if !proceed {
				return
			}
			promotedMasterReplica := r.findPromotedMasterReplica(n.LeaderName, v)
			if promotedMasterReplica == nil {
				return
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
				r.recoverFromAddNode(pod, m, mutex)
				break
			case view.ReplicateNode:
				r.recoverFromReplicateNode(pod.Status.PodIP, m, mutex)
				break
			case view.SyncNode:
				r.recoverFromSyncNode(pod.Status.PodIP, m, mutex)
				break
			case view.FailoverNode:
				r.recoverFromFailOver(pod.Status.PodIP, m, mutex)
				break
			}
		}(n, &wg)
	}
	wg.Wait()
	r.removeAndDeleteNodes(redisCluster, v)
	if !actionRequired {
		r.Log.Info("[OK] Cluster nodes are healthy")
	}
	return actionRequired
}

func (r *RedisClusterReconciler) recoverFromNewEmptyNode(name string, v *view.RedisClusterView) {
	if n, exists := v.Nodes[name]; exists {
		nodes, _, err := r.RedisCLI.ClusterNodes(n.Ip)
		if err != nil || nodes == nil {
			r.RedisClusterStateView.SetNodeState(n.Name, n.LeaderName, view.ReshardNode)
			return
		}
		for _, tableNode := range *nodes {
			if tableNode.ID == n.Id {
				fmt.Printf("Link state: [%v]\n", tableNode.LinkState)
				if len(tableNode.LinkState) > 0 {
					r.RedisClusterStateView.SetNodeState(n.Name, n.LeaderName, view.NodeOK)
				} else {
					r.RedisClusterStateView.ClusterState = view.ClusterRebalance
				}
			}
		}
	} else {
		r.RedisClusterStateView.SetNodeState(n.Name, n.LeaderName, view.DeleteNode)
	}
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

func (r *RedisClusterReconciler) findPromotedMasterReplica(leaderName string, v *view.RedisClusterView) (promotedReplica *view.NodeView) {

	for _, clusterNode := range v.Nodes {
		if clusterNode.LeaderName == leaderName {
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

func (r *RedisClusterReconciler) recoverRedisCluster(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) (bool, error) {
	s := r.RedisClusterStateView.ClusterState
	r.Log.Info(fmt.Sprintf("Cluster state: %v", s))
	switch s {
	case view.ClusterOK:
		return false, nil
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
	v, ok := r.NewRedisClusterView(redisCluster)
	if !ok {
		return errors.New("Could not perform redis cluster update")
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
		r.recoverNodes(redisCluster, v)
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

func (r *RedisClusterReconciler) isClusterHealthy(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) (bool, error) {
	if len(v.Nodes) == 0 {
		r.Log.Info("[WARN] Could not find redis cluster nodes, reseting cluster...")
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
	r.removeAndDeleteNodes(redisCluster, v)
	return isComplete, nil
}

func (r *RedisClusterReconciler) recoverLossOfLeaderAndFollowers(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) (bool, error) {
	r.forgetLostNodes(redisCluster, v)
	missing := r.detectLossOfLeadersAndAllFollowers(v)
	if len(missing) > 0 {
		r.Log.Info("[Warn] Loss of leader and all of his replica detected, Performing mitigation proces...")
		healthyLeaderName, found := r.findHealthyLeader(v)
		if !found {
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
		e = r.addLeaderNodes(redisCluster, healthyLeaderIp, missing)
		if e != nil {
			return true, e
		}
	}
	return len(missing) > 0, nil
}

func (r *RedisClusterReconciler) findHealthyLeader(v *view.RedisClusterView, exclude ...map[string]bool) (name string, found bool) {
	var wg sync.WaitGroup
	mutex := &sync.Mutex{}
	name = ""
	found = false
	for _, node := range v.Nodes {
		wg.Add(1)
		go func(node *view.NodeView, wg *sync.WaitGroup) {
			defer wg.Done()
			if !found {
				if len(exclude) > 0 {
					skipNode := false
					for _, excludeMap := range exclude {
						if _, excludeNode := excludeMap[node.Name]; excludeNode {
							skipNode = true
							break
						}
					}
					if skipNode {
						return
					}
				}
				if node.IsLeader {
					if n, exists := r.RedisClusterStateView.Nodes[node.Name]; exists && n.NodeState == view.NodeOK {
						info, _, err := r.RedisCLI.Info(node.Ip)
						if err != nil || info == nil {
							return
						}
						if info.Replication["role"] == "master" {
							mutex.Lock()
							name = n.Name
							found = true
							mutex.Unlock()
						}
					}
				}
			}
		}(node, &wg)
	}
	wg.Wait()
	return name, found
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
	fmt.Printf("Healthy leader name: [%s], healthy leader ip: [%s]\n", healthyLeaderName, healthyLeaderIp)
	leaders := r.leadersCount()
	leadersBySpec := redisCluster.Spec.LeaderCount
	newLeadersNames := []string{}
	for i := leaders; i < leadersBySpec; i++ {
		name := "redis-node-" + fmt.Sprint(i)
		newLeadersNames = append(newLeadersNames, name)
		r.RedisClusterStateView.Nodes[name] = &view.NodeStateView{
			Name:       name,
			LeaderName: name,
			NodeState:  view.ReshardNode,
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
			println("Ended with error")
			return e
		}
		r.Log.Info("Leaders added successfully to redis cluster")
	} else {
		r.Log.Info("[Warn] New leader names list appeard to be empty, no leaders been added to cluster")
	}
	return nil
}

func (r *RedisClusterReconciler) scaleDownLeaders(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView) error {
	r.Log.Info("Scaling down leaders")
	leaders := r.leadersCount()
	leadersBySpec := redisCluster.Spec.LeaderCount

	newLeaders := map[string]bool{}
	for i := leadersBySpec; i < leaders; i++ {
		leaderName := "redis-node-" + fmt.Sprint(i)
		newLeaders[leaderName] = true
		n, exists := r.RedisClusterStateView.Nodes[leaderName]
		if exists {
			n.NodeState = view.ReshardNode
		}
	}

	for leaderName, _ := range newLeaders {
		r.scaleDownLeader(leaderName, newLeaders, v)
	}
	r.RedisClusterStateView.ClusterState = view.ClusterRebalance
	r.removeAndDeleteNodes(redisCluster, v)
	return nil
}

func (r *RedisClusterReconciler) scaleDownLeader(leaderName string, excludeList map[string]bool, v *view.RedisClusterView) {
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
	r.reshardAndRemoveLeader(leaderName, healthyLeaderIp, targetLeaderId, v)
}

func (r *RedisClusterReconciler) reshardAndRemoveLeader(leaderName string, healthyLeaderIp string, targetLeaderId string, v *view.RedisClusterView) {
	if leaderToRemove, exists := v.Nodes[leaderName]; exists {
		r.Log.Info(fmt.Sprintf("Resharding leader: [%s]->all slots->[%s]", leaderToRemove.Id, targetLeaderId))
		r.RedisClusterStateView.SetNodeState(leaderName, leaderName, view.ReshardNode)
		err := r.reshardLeaderCheckCoverage(healthyLeaderIp, targetLeaderId, leaderToRemove)
		if err != nil {
			r.RedisClusterStateView.ClusterState = view.ClusterFix
			r.Log.Error(err, fmt.Sprintf("Error during attempt to reshard node [%s]", leaderName))
			return
		}
		r.Log.Info(fmt.Sprintf("Leader reshard successful between [%s]->[%s]", leaderToRemove.Id, targetLeaderId))
	} else {
		r.RedisClusterStateView.ClusterState = view.ClusterFix
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
	r.removeAndDeleteNodes(redisCluster, v)
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
