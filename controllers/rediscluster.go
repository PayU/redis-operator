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

func (r *RedisClusterReconciler) NewRedisClusterView(redisCluster *dbv1.RedisCluster) (*RedisClusterView, error) {
	var cv RedisClusterView

	pods, err := r.getRedisClusterPods(redisCluster)
	if err != nil {
		return nil, err
	}

	followerCounter := redisCluster.Spec.LeaderCount
	for i := 0; i < redisCluster.Spec.LeaderCount; i++ {
		leader := LeaderNode{Pod: nil, NodeName: "redis-node-" + strconv.Itoa(i), RedisID: "", Failed: true, Terminating: false, Followers: nil}
		for j := 0; j < redisCluster.Spec.LeaderFollowersCount; j++ {
			follower := FollowerNode{Pod: nil, NodeName: "redis-node-" + strconv.Itoa(followerCounter), LeaderName: "redis-node-" + strconv.Itoa(i), RedisID: "", Failed: true, Terminating: false}
			leader.Followers = append(leader.Followers, follower)
			followerCounter++
		}
		cv = append(cv, leader)
	}
	//todo
	for _, pod := range pods {
		nn, err := strconv.Atoi(strings.Split(pod.Labels["node-name"], "-")[2])
		if err != nil {
			return nil, errors.Errorf("Failed to parse node-name label: %s (%s)", pod.Labels["node-name"], pod.Name)
		}
		ln, err := strconv.Atoi(strings.Split(pod.Labels["leader-name"], "-")[2])
		if err != nil {
			return nil, errors.Errorf("Failed to parse leader-name label: %s (%s)", pod.Labels["leader-name"], pod.Name)
		}
		if nn == ln {
			cv[ln].Pod = &pod
			cv[ln].NodeName = pod.Labels["node-name"]
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
			cv[ln].Followers[index].Pod = &pod
			cv[ln].Followers[index].NodeName = pod.Labels["node-name"]
			cv[ln].Followers[index].LeaderName = pod.Labels["leader-name"]
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
	nodeNumber := redisCluster.Spec.LeaderCount // first node numbers are reserved for leaders
	for _, leaderPod := range leaderPods {
		for i := 0; i < redisCluster.Spec.LeaderFollowersCount; i++ {
			nodeNames = append(nodeNames, NodeNames{"redis-node-" + strconv.Itoa(nodeNumber), leaderPod.Labels["node-name"]})
			nodeNumber++
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
		return errors.Errorf("Failed to add followers - no node numbers: (%s)", nodeNames)
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

	recover := false
	for _, followerPod := range pods {
		if err := r.waitForRedis(followerPod.Status.PodIP); err != nil {
			return err
		}
		if len(nodeIPs[followerPod.Labels["leader-name"]]) > 0 {
			r.Log.Info(fmt.Sprintf("Replicating: %s %s", followerPod.Name, followerPod.Labels["leader-name"]))
			if err = r.replicateLeader(followerPod.Status.PodIP, nodeIPs[followerPod.Labels["leader-name"]]); err != nil {
				return err
			}
		} else {
			recover = true
		}
	}
	if recover {
		r.Log.Info("Triggeting recovery process, lost leader/s detected\n")
		redisCluster.Status.ClusterState = string(Recovering)
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

	// Getting new cluster view
	clusterView2, e := r.NewRedisClusterView2(redisCluster)
	if e != nil {
		println("Cloud not retrieve new cluster view")
		return e
	}

	healthyIps := make([]string, 0)
	lostIds := make([]string, 0)
	visitedById := make(map[string]bool)
	for _, node := range clusterView2.PodsViewByName {
		if !node.IsReachable {
			fmt.Printf("Node %+v tagged as not reachable\n", node.Name)
			visitedById[node.NodeId] = true
			lostIds = append(lostIds, node.NodeId)
			continue
		}
		for _, tableNode := range node.ClusterNodesTable {
			// If the *tableNode* is non existing pod or is non reachable pod it need to be forgotten
			tableNodeName, exists := clusterView2.NodeIdToPodName[tableNode.Id]
			if !exists {
				if _, definedAsLost := visitedById[tableNode.Id]; !definedAsLost {
					fmt.Printf("In table of node %+v, detected node with id %+v that not exists in view map, tagged as lost\n", node.Name, tableNode.Id)
					visitedById[tableNode.Id] = true
					lostIds = append(lostIds, tableNode.Id)
				}
				continue
			}
			// If the current node recognize tableNode that doesn't recognize back, then this *current node* (table owner) need to be forgotten
			recognizedNode := clusterView2.PodsViewByName[tableNodeName]
			if _, recognizeBack := recognizedNode.ClusterNodesTable[node.NodeId]; !recognizeBack {
				if _, definedAsLost := visitedById[node.NodeId]; !definedAsLost {
					fmt.Printf("Node %+v recognizing node %+v that doesn't recognize back, tagged as lost", node.Name, recognizedNode.Name)
					visitedById[node.NodeId] = true
					lostIds = append(lostIds, node.NodeId)
				}
				break
			}
		}
	}
	for _, node := range clusterView2.PodsViewByName {
		if _, lost := visitedById[node.NodeId]; !lost {
			healthyIps = append(healthyIps, node.Ip)
		}
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

	fmt.Printf("New cluster view: %+v\n", clusterView2.ToPrintableForm())

	fmt.Printf("Old cluster view: %+v\n", clusterView.String())

	fmt.Printf("New view list of lost nodes: %+v\n", lostIds)

	fmt.Printf("Old view list of lost nodes: %+v\n", lostNodeIDSet)

	fmt.Printf("New view list of healthy nodes: %+v\n", healthyIps)

	fmt.Printf("Old view list of healthy nodes: %+v\n", healthyNodeIPs)

	for id, _ := range lostNodeIDSet {
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

	r.Log.Info(fmt.Sprintf("Cleanning up: %v", podIPs))

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
		r.Log.Info(fmt.Sprintf("[WARN] Automatic failover failed for leader [%s]. Attempting forced failover.", leader.NodeName))
	} else {
		return promotedPodIP, nil
	}

	// Automatic failover failed. Attempt to force failover on a healthy follower.
	for _, follower := range leader.Followers {
		if follower.Pod != nil && !follower.Failed {
			if _, pingErr := r.RedisCLI.Ping(follower.Pod.Status.PodIP); pingErr == nil {
				if forcedFailoverErr := r.doFailover(follower.Pod.Status.PodIP, "force"); forcedFailoverErr != nil {
					if rediscli.IsFailoverNotOnReplica(forcedFailoverErr) {
						r.Log.Info(fmt.Sprintf("Forced failover successful on [%s](%s)", follower.NodeName, follower.Pod.Status.PodIP))
						promotedPodIP = follower.Pod.Status.PodIP
						break
					}
					r.Log.Error(forcedFailoverErr, fmt.Sprintf("[WARN] Failed forced attempt to make node [%s](%s) leader", follower.NodeName, follower.Pod.Status.PodIP))
				} else {
					r.Log.Info(fmt.Sprintf("Forced failover successful on [%s](%s)", follower.NodeName, follower.Pod.Status.PodIP))
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
						r.Log.Info(fmt.Sprintf("Takeover successful on [%s](%s)", follower.NodeName, follower.Pod.Status.PodIP))
						promotedPodIP = follower.Pod.Status.PodIP
						break
					}
					r.Log.Error(forcedFailoverErr, fmt.Sprintf("[WARN] Failed takeover attempt to make node [%s](%s) leader", follower.NodeName, follower.Pod.Status.PodIP))
				} else {
					r.Log.Info(fmt.Sprintf("Takeover successful on [%s](%s)", follower.NodeName, follower.Pod.Status.PodIP))
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

	fmt.Printf("Old cluster view: %+v\n", clusterView.String())

	clusterView2, e := r.NewRedisClusterView2(redisCluster)
	if e != nil {
		println("Cloud not retrieve new cluster view")
		return e
	}

	fmt.Printf("New cluster view: %+v\n", clusterView2.ToPrintableForm())

	r.Log.Info(clusterView.String())
	for i, leader := range *clusterView {
		if leader.Failed {
			runLeaderRecover = true

			if leader.Terminating {
				println("here1")
				if err = r.waitForPodDelete(*leader.Pod); err != nil {
					return errors.Errorf("Failed to wait for leader pod to be deleted %s: %v", leader.NodeName, err)
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
				println("here2")
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
		var missingFollowers []NodeNames
		var failedFollowerIPs []string
		var terminatingFollowerIPs []string
		var terminatingFollowerPods []corev1.Pod

		for _, follower := range leader.Followers {
			if follower.Pod == nil {
				missingFollowers = append(missingFollowers, NodeNames{follower.NodeName, follower.LeaderName})
			} else if follower.Terminating {
				terminatingFollowerPods = append(terminatingFollowerPods, *follower.Pod)
				terminatingFollowerIPs = append(terminatingFollowerIPs, follower.Pod.Status.PodIP)
				missingFollowers = append(missingFollowers, NodeNames{follower.NodeName, follower.LeaderName})
			} else if follower.Failed {
				failedFollowerIPs = append(failedFollowerIPs, follower.Pod.Status.PodIP)
				missingFollowers = append(missingFollowers, NodeNames{follower.NodeName, follower.LeaderName})
			}
		}
		deletedPods, err := r.deletePodsByIP(redisCluster.Namespace, failedFollowerIPs...)
		if err != nil {
			return err
		}
		println("here3")
		podsToWaitFor := append(terminatingFollowerPods, deletedPods...)
		if err = r.waitForPodDelete(podsToWaitFor...); err != nil {
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
		println("here4")
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
		println("here5")
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
	return wait.PollImmediate(2*r.Config.Times.RedisClusterMeetCheckInterval, 5*r.Config.Times.RedisClusterMeetCheckTimeout, func() (bool, error) {
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
func (r *RedisClusterReconciler) waitForFailover(redisCluster *dbv1.RedisCluster, leader *LeaderNode) (string, error) {
	r.Log.Info(fmt.Sprintf("Waiting for leader [%s] failover", leader.NodeName))
	failedFollowers := 0
	var promotedFollowerIP string

	for _, follower := range leader.Followers {
		if follower.Failed {
			failedFollowers++
		}
	}

	if failedFollowers == redisCluster.Spec.LeaderFollowersCount {
		return "", errors.Errorf("Failing leader [%s] lost all followers. Recovery unsupported.", leader.NodeName)
	}

	return promotedFollowerIP, wait.PollImmediate(2*r.Config.Times.RedisAutoFailoverCheckInterval, 5*r.Config.Times.RedisAutoFailoverCheckTimeout, func() (bool, error) {
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
			r.Log.Info("Found terminating leader: " + leader.NodeName)
			return false, nil
		}
		if leader.Failed {
			r.Log.Info("Found failed leader: " + leader.NodeName)
			return false, nil
		}
		for _, follower := range leader.Followers {
			if follower.Terminating {
				r.Log.Info("Found terminating follower: " + follower.NodeName)
				return false, nil
			}
			if follower.Failed {
				r.Log.Info("Found failed follower: " + follower.NodeName)
				return false, nil
			}
		}
	}
	data, _ := json.MarshalIndent(clusterView, "", "")
	clusterData.SaveRedisClusterView(data)

	return true, nil
}
