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
	view "github.com/PayU/redis-operator/controllers/view"
	clusterData "github.com/PayU/redis-operator/data"
)

// todo: remove
type RedisNodePodView struct {
	RedisNodeId string
	IP          string
	Port        string
	IsLeader    bool
}

type NodeCreationData struct {
	NodeName   string
	LeaderName string
}

type ClusterScaleView struct {
	CurrentLeaderCount             int
	CurrentFollowersPerLeaderCount int
	CurrentPodCount                int
	NewLeaderCount                 int
	NewFollowersPreLeaderCount     int
	NewPodCount                    int
	PodIndexToPodView              map[string]*RedisNodePodView
}

func (r *RedisClusterReconciler) NewRedisClusterView() (*view.ClusterView, error) {
	v := &view.ClusterView{
		ViewByPodName:    make(map[string]*view.ClusterNodeView),
		RedisIdToPodName: make(map[string]string),
	}
	podsView, e := r.getRedisPodsView()
	if e != nil {
		return nil, e
	}

	for ip, _ := range podsView.PodViewByIp {
		_, nodesById, _, e := r.RedisCLI.ClusterNodes(ip)
		if e != nil {
			continue
		}
		for id, node := range nodesById {
			pod, podExistsForNode := podsView.PodViewByIp[node.IP]
			if !podExistsForNode {
				v.ViewByPodName[id] = &view.ClusterNodeView{
					Name:            id,
					Namespace:       "",
					Status:          "Lost",
					ID:              node.ID,
					IP:              node.IP,
					Port:            node.Port,
					Leader:          node.Leader, //its leader id
					FollowersByName: make([]string, 0),
					IsLeader:        pod.IsLeader,
					IsReachable:     false,
					Pod:             corev1.Pod{},
				}
			} else {
				var isReachableNode bool
				var status string
				var followersByName []string
				if cliClusterId, e := r.RedisCLI.MyClusterID(node.IP); e != nil || cliClusterId != id || node.IsFailing() {
					isReachableNode = false
					status = "Lost"
					followersByName = make([]string, 0)
				} else {
					isReachableNode = true
					status = pod.Phase
					followersByName = pod.FollowersByName
				}

				if isReachableNode && !node.IsLeader {
					isLegitFollower := false
					leaderId := node.Leader
					leaderIp := nodesById[leaderId].IP
					for _, followerName := range podsView.PodViewByIp[leaderIp].FollowersByName {
						if podsView.PodNameToPodIp[followerName] == node.IP {
							isLegitFollower = true
							break
						}
					}
					isReachableNode = isLegitFollower
				}

				if _, nodeInMap := v.ViewByPodName[pod.Name]; !nodeInMap {
					v.ViewByPodName[pod.Name] = &view.ClusterNodeView{
						Name:            pod.Name,
						Namespace:       pod.Namespace,
						Status:          status,
						ID:              node.ID,
						IP:              node.IP,
						Port:            node.Port,
						Leader:          pod.LeaderName,
						FollowersByName: followersByName,
						IsLeader:        pod.IsLeader,
						IsReachable:     isReachableNode,
						Pod:             pod.Pod,
					}
					v.RedisIdToPodName[node.ID] = pod.Name
				}
			}
		}
	}
	return v, nil
}

func (r *RedisClusterReconciler) getLeaderIP(followerIP string) (string, error) {
	info, _, err := r.RedisCLI.Info(followerIP)
	if err != nil {
		return "", err
	}
	return info.Replication["master_host"], nil
}

// Returns the node number and leader number from a pod
func (r *RedisClusterReconciler) getNodeAndLeaderNamesFromIP(namespace string, podIP string) (string, string, error) {
	pod, err := r.getPodByIP(namespace, podIP)
	if err != nil {
		return "", "", err
	}
	return pod.Labels["node-name"], pod.Labels["leader-name"], err
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
	fmt.Println("Got leaders list")
	var nodesData []NodeCreationData
	for _, leaderPod := range leaderPods {
		for i := 1; i <= redisCluster.Spec.LeaderFollowersCount; i++ {
			nodesData = append(nodesData, NodeCreationData{
				NodeName:   leaderPod.Name + "-" + strconv.Itoa(i),
				LeaderName: leaderPod.Name,
			})
		}
	}
	fmt.Println("Nodes data: %+v\n", nodesData)
	err = r.addFollowers(redisCluster, nodesData)
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
		leaderNames = append(leaderNames, "node-"+strconv.Itoa(leaderNumber))
	}

	newLeaderPods, err := r.CreateRedisLeaderPods(redisCluster, leaderNames)
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
	nodeName, oldLeaderName, err := r.getNodeAndLeaderNamesFromIP(redisCluster.Namespace, promotedFollowerIP)
	if err != nil {
		return err
	}
	r.Log.Info(fmt.Sprintf("Recreating leader [%s] using node [%s]", oldLeaderName, nodeName))

	newLeaderPods, err := r.CreateRedisLeaderPods(redisCluster, []string{oldLeaderName})
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

func (r *RedisClusterReconciler) addFollowers(redisCluster *dbv1.RedisCluster, nodesData []NodeCreationData) error {

	v, e := r.NewRedisClusterView()
	if e != nil {
		return errors.Errorf("Add followers error: could not create cluster view: %+v", e.Error())
	}

	if len(nodesData) > 0 {
		for _, nodeData := range nodesData {
			if _, exists := v.ViewByPodName[nodeData.NodeName]; !exists {
				newFollowerPods, err := r.createRedisFollowerPods(redisCluster, nodesData)
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
					leaderName := followerPod.Labels["leader-name"]
					r.Log.Info(fmt.Sprintf("Replicating: follower %s leader %s", followerPod.Name, leaderName))
					if err = r.replicateLeader(followerPod.Status.PodIP, v.ViewByPodName[leaderName].IP); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

// Removes all nodes the cluster node table entries with IDs of nodes not available
// Recives the list of healthy cluster nodes (Redis is reachable and has cluster mode on)
func (r *RedisClusterReconciler) forgetLostNodes() error {
	v, e := r.NewRedisClusterView()
	if e != nil {
		return errors.New(fmt.Sprintf("Forget lost nodes error: could not get redis cluster view. %+v\n", e.Error()))
	}
	reachableNodesIPs := make([]string, 0)
	for _, node := range v.ViewByPodName {
		if node.IsReachable {
			_, nodeTable, _, e := r.RedisCLI.ClusterNodes(node.IP)
			if e != nil {
				return errors.New(fmt.Sprintf("Forget lost nodes error: could not get node table for ip %+v. %+v\n", node.IP, e.Error()))
			}
			// Node will be marked as lost (unreachable) if it is recognizing cluster member that doesn't recognize it back
			for _, member := range nodeTable {
				_, memberTable, _, e := r.RedisCLI.ClusterNodes(member.IP)
				if e != nil {
					return errors.New(fmt.Sprintf("Forget lost nodes errors: could not reach cluster member node to determine if its recognizing node should be forgotten, target member ip %+v. %+v\n", member.IP, e.Error()))
				}
				if _, recognized := memberTable[node.ID]; !recognized {
					node.IsReachable = false
					break
				}
			}
			if node.IsReachable {
				reachableNodesIPs = append(reachableNodesIPs, node.IP)
			}
		}
	}

	for _, node := range v.ViewByPodName {
		e = r.forgetNode(reachableNodesIPs, node.ID)
	}

	data, _ := json.MarshalIndent(v.ToPrintableFormat(), "", "")
	clusterData.SaveRedisClusterView(data)
	return e
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
			clusterNodes, _, _, err := r.RedisCLI.ClusterNodes(ip)
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
func (r *RedisClusterReconciler) handleFailover(failingLeaderName string, v *view.ClusterView) (string, error) {
	var promotedPodIP string = ""
	promotedPodIP, e := r.waitForFailover(failingLeaderName, v)
	if e != nil || promotedPodIP == "" {
		r.Log.Info(fmt.Sprintf("[WARN] Automatic failover failed for leader [%+v]. Attempting forced failover.", failingLeaderName))
	} else {
		return promotedPodIP, nil
	}
	for _, followerName := range v.ViewByPodName[failingLeaderName].FollowersByName {
		follower := v.ViewByPodName[followerName]
		if follower.IsReachable && follower.Status == "Running" {
			if _, e := r.RedisCLI.Ping(follower.IP); e == nil {
				if forcedFailoverErr := r.doFailover(follower.IP, "force"); forcedFailoverErr != nil {
					if rediscli.IsFailoverNotOnReplica(forcedFailoverErr) {
						r.Log.Info(fmt.Sprintf("Forced failover successful on [%+v](%+v)", follower.Name, follower.IP))
						promotedPodIP = follower.IP
						break
					}
				} else {
					r.Log.Info(fmt.Sprintf("Forced failover successful on [%+v](%+v)", follower.Name, follower.IP))
					promotedPodIP = follower.IP
					break
				}
			}
		}
	}
	if promotedPodIP != "" {
		return promotedPodIP, nil
	}
	for _, followerName := range v.ViewByPodName[failingLeaderName].FollowersByName {
		follower := v.ViewByPodName[followerName]
		if follower.IsReachable && follower.Status == "Running" {
			if _, e := r.RedisCLI.Ping(follower.IP); e == nil {
				if forcedFailoverErr := r.doFailover(follower.IP, "takeover"); forcedFailoverErr != nil {
					if rediscli.IsFailoverNotOnReplica(forcedFailoverErr) {
						r.Log.Info(fmt.Sprintf("Takeover successful on [%+v](%+v)", follower.Name, follower.IP))
						promotedPodIP = follower.Pod.Status.PodIP
						break
					}
					r.Log.Error(forcedFailoverErr, fmt.Sprintf("[WARN] Failed takeover attempt to make node [%+v](%+v) leader", follower.Name, follower.IP))
				} else {
					r.Log.Info(fmt.Sprintf("Takeover successful on [%s](%s)", follower.Name, follower.IP))
					promotedPodIP = follower.IP
					break
				}
			}
		}
	}
	return promotedPodIP, nil
}

func (r *RedisClusterReconciler) recoverLeaders(redisCluster *dbv1.RedisCluster, v *view.ClusterView) error {
	var runLeaderRecover bool = false
	for _, node := range v.ViewByPodName {
		if node.IsLeader && (!node.IsReachable || node.Status != "Running") {
			runLeaderRecover = true
			if node.Status == "Terminating" {
				if e := r.waitForPodDelete(node.Pod); e != nil {
					return errors.Errorf("Failed to wait for leader pod to be deleted %+v: %+v", node.Name, e.Error())
				}
			}
			_, e := r.handleFailover(node.Name, v)
			if e != nil {
				return errors.Errorf("Recover leaders error: failed to handle failover, %+v", e.Error())
			}
			if node.Status != "Terminating" {
				_, e := r.deletePodsByIP(node.Namespace, node.IP)
				if e != nil || node.Status != "Lost" {
					return errors.Errorf("Recover leaders error: failed to delete pod by ip %+v. %+v", node.IP, e.Error())
				}
				if e = r.waitForPodDelete(node.Pod); e != nil {
					return errors.Errorf("Recover leaders error: failed to wait for pod delete, ip %+v, %+v", node.IP, e.Error())
				}
			}
		}
	}

	if runLeaderRecover {
		v, e := r.NewRedisClusterView()
		if e != nil {
			return errors.Errorf("Recover leaders error: failed to retrieve cluster view after failover attempt. %+v", e.Error())
		}
		r.Log.Info("Recovering cluster leaders. cluster view: %+v\n", v.ToPrintableFormat())
	}
	return nil
}

func (r *RedisClusterReconciler) recoverFollowers(redisCluster *dbv1.RedisCluster, v *view.ClusterView) error {
	for _, node := range v.ViewByPodName {
		if node.IsLeader {
			var lostFollowers []NodeCreationData
			var failingFollowersIps []string
			var terminatingFollowersPods []corev1.Pod

			for _, followerName := range v.ViewByPodName[node.Name].FollowersByName {
				follower := v.ViewByPodName[followerName]
				if follower.Status == "Terminating" {
					terminatingFollowersPods = append(terminatingFollowersPods, follower.Pod)
				} else if follower.Status == "Failing" {
					failingFollowersIps = append(failingFollowersIps, follower.IP)
				} else if !follower.IsReachable {
					lostFollowers = append(lostFollowers, NodeCreationData{
						NodeName:   followerName,
						LeaderName: node.Name,
					})
				}
			}

			deletedPods, e := r.deletePodsByIP(node.Namespace, failingFollowersIps...)
			if e != nil {
				return errors.Errorf("Recover followers error: could not delete failing pods, ips: %+v, e: %+v\n", failingFollowersIps, e.Error())
			}
			if e := r.waitForPodDelete(append(terminatingFollowersPods, deletedPods...)...); e != nil {
				return errors.Errorf("Recover followers error: could not wait for pod delete, %+v\n", e.Error())
			}
			if len(lostFollowers) > 0 {
				if err := r.forgetLostNodes(); err != nil {
					return err
				}
				if err := r.addFollowers(redisCluster, lostFollowers); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (r *RedisClusterReconciler) recoverCluster(redisCluster *dbv1.RedisCluster) error {
	v, e := r.NewRedisClusterView()
	if e != nil {
		return errors.New(fmt.Sprintf("Recover cluster error: Could not retreive cluster view. %+v", e.Error()))
	}
	r.Log.Info(fmt.Sprintf("Recovering cluster, view: %+v\n", v.ToPrintableFormat()))
	if e = r.recoverLeaders(redisCluster, v); e != nil {
		return e
	}
	if e = r.recoverFollowers(redisCluster, v); e != nil {
		return e
	}
	complete, err := r.isClusterComplete(redisCluster)
	if err != nil || !complete {
		return errors.Errorf("Cluster recovery not complete")
	}
	data, _ := json.MarshalIndent(v.ToPrintableFormat(), "", "")
	clusterData.SaveRedisClusterView(data)
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

	if err := r.forgetLostNodes(); err != nil {
		return err
	}

	if err := r.recreateLeader(redisCluster, promotedFollowerIP); err != nil {
		return err
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

	if err := r.forgetLostNodes(); err != nil {
		return err
	}

	r.Log.Info(fmt.Sprintf("Starting to add follower: (%s-%s)", pod.Labels["leader-name"], pod.Labels["node-name"]))
	nodeData := []NodeCreationData{NodeCreationData{
		NodeName:   pod.Labels["node-name"],
		LeaderName: pod.Labels["leader-name"],
	}}
	if err := r.addFollowers(redisCluster, nodeData); err != nil {
		return err
	}

	return nil
}

func (r *RedisClusterReconciler) updateCluster(redisCluster *dbv1.RedisCluster) error {
	v, e := r.NewRedisClusterView()
	if e != nil {
		return errors.New(fmt.Sprintf("Update cluster error: Could not get cluster view, %+v\n", e.Error()))
	}
	r.Log.Info(fmt.Sprintf("Cluster view: %+v\n", v.ToPrintableFormat()))
	r.Log.Info("Updating...")
	for _, node := range v.ViewByPodName {
		if node.IsReachable && node.IsLeader {
			for _, followerName := range node.FollowersByName {
				if follower, exists := v.ViewByPodName[followerName]; exists && follower.IsReachable {
					podUpToDate, e := r.isPodUpToDate(redisCluster, &follower.Pod)
					if e != nil {
						return errors.New(fmt.Sprintf("Update cluster error: Could not determine if node up to date, pod ip: %+v error: %+v\n", follower.IP, e.Error()))
					}
					if !podUpToDate {
						if e = r.updateFollower(redisCluster, follower.IP); e != nil {
							return errors.New(fmt.Sprintf("Update cluster error: Could not update pod, pod ip: %+v error: %+v\n", follower.IP, e.Error()))
						}
					} else {
						if _, e := r.waitForPodReady(follower.Pod); e != nil {
							return errors.New(fmt.Sprintf("Update cluster error: Could not wait for pod ready, pod ip: %+v error: %+v\n", follower.IP, e.Error()))
						}
						if e := r.waitForRedis(follower.IP); e != nil {
							return errors.New(fmt.Sprintf("Update cluster error: Could not wait for redis node, pod ip: %+v error: %+v\n", follower.IP, e.Error()))
						}
					}
				}
			}
			podUpToDate, e := r.isPodUpToDate(redisCluster, &node.Pod)
			if e != nil {
				return errors.New(fmt.Sprintf("Update cluster error: Could not determine if node up to date, pod ip: %+v error: %+v\n", node.IP, e.Error()))
			}
			if !podUpToDate {
				if e = r.updateLeader(redisCluster, node.IP); e != nil {
					// >>> TODO the logic of checking if a leader pod (first N pods) is indeed a Redis leader must be handled separately
					// Comment on new implementation: the logic of checking if a leader pod is now with O(1) - each node has a flag that indicates it
					if rediscli.IsNodeIsNotMaster(e) {
						if _, e := r.deletePodsByIP(redisCluster.Namespace, node.IP); e != nil {
							return errors.New(fmt.Sprintf("Update cluster error: Could not delete pod by ip, pod ip: %+v error: %+v\n", node.IP, e.Error()))
						}
					}
					return errors.New(fmt.Sprintf("Update cluster error: Could not update pod, pod ip: %+v error: %+v\n", node.IP, e.Error()))
				}
			}
		}
	}
	if e = r.cleanupNodeList(v.HealthyNodesIPs()); e != nil {
		return errors.New(fmt.Sprintf("Update cluster error: Could not get healthy nodes ips, error: %+v\n", e.Error()))
	}
	data, _ := json.MarshalIndent(v.ToPrintableFormat(), "", "")
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
		if pollErr := wait.PollImmediate(3*r.Config.Times.RedisPingCheckInterval, 5*r.Config.Times.RedisPingCheckTimeout, func() (bool, error) {
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
	return wait.Poll(3*r.Config.Times.ClusterCreateInterval, 5*r.Config.Times.ClusterCreateTimeout, func() (bool, error) {
		for _, leaderIP := range leaderIPs {
			clusterInfo, _, err := r.RedisCLI.ClusterInfo(leaderIP)
			if err != nil {
				return false, err
			}
			if clusterInfo.IsClusterFail() {
				return false, nil
			}
			clusterNodes, _, _, err := r.RedisCLI.ClusterNodes(leaderIP)
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
	if err := wait.PollImmediate(3*r.Config.Times.SyncStartCheckInterval, 5*r.Config.Times.SyncStartCheckTimeout, func() (bool, error) {
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

	return wait.PollImmediate(3*r.Config.Times.SyncCheckInterval, 5*r.Config.Times.SyncCheckTimeout, func() (bool, error) {
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
	if err := wait.PollImmediate(3*r.Config.Times.LoadStartCheckInterval, 5*r.Config.Times.LoadStartCheckTimeout, func() (bool, error) {
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
	return wait.PollImmediate(3*r.Config.Times.LoadCheckInterval, 5*r.Config.Times.LoadCheckTimeout, func() (bool, error) {
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
	return wait.PollImmediate(3*r.Config.Times.RedisClusterReplicationCheckInterval, 5*r.Config.Times.RedisClusterReplicationCheckTimeout, func() (bool, error) {
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
	return wait.PollImmediate(3*r.Config.Times.RedisClusterMeetCheckInterval, 5*r.Config.Times.RedisClusterMeetCheckTimeout, func() (bool, error) {
		clusterNodes, _, _, err := r.RedisCLI.ClusterNodes(nodeIP)
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
	return wait.PollImmediate(3*r.Config.Times.RedisManualFailoverCheckInterval, 5*r.Config.Times.RedisManualFailoverCheckTimeout, func() (bool, error) {
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
func (r *RedisClusterReconciler) waitForFailover(failingLeaderName string, v *view.ClusterView) (string, error) {
	r.Log.Info(fmt.Sprintf("Waiting for leader [%+v] failover", failingLeaderName))
	failingLeader, exists := v.ViewByPodName[failingLeaderName]
	if !exists {
		return "", errors.New(fmt.Sprintf("Wait for failover error: Could not find failing leader in cluster view map. failing leader view: %+v\n", failingLeader))
	}
	foundHealthyFollower := false
	for _, followerName := range failingLeader.FollowersByName {
		follower := v.ViewByPodName[followerName]
		if follower.IsReachable {
			foundHealthyFollower = true
		}
	}
	if !foundHealthyFollower {
		return "", errors.Errorf("Failing leader [%+v] lost all followers. Recovery unsupported.", failingLeaderName)
	}
	var promotedFollowerIP string
	return promotedFollowerIP, wait.PollImmediate(3*r.Config.Times.RedisAutoFailoverCheckInterval, 5*r.Config.Times.RedisAutoFailoverCheckTimeout, func() (bool, error) {
		for _, followerName := range v.ViewByPodName[failingLeaderName].FollowersByName {
			follower := v.ViewByPodName[followerName]
			if follower.Status != "Running" {
				continue
			}
			info, _, e := r.RedisCLI.Info(follower.IP)
			if e != nil {
				continue
			}
			if info.Replication["role"] == "master" {
				promotedFollowerIP = follower.IP
				return true, nil
			}
		}
		return false, nil
	})

	return "", nil
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
	v, e := r.NewRedisClusterView()
	if e != nil {
		return false, errors.New(fmt.Sprintf("Is cluster complete error: Could not get cluster view, %+v\n", e.Error()))
	}
	for _, node := range v.ViewByPodName {
		if node.Status != "Running" || !node.IsReachable {
			r.Log.Info(fmt.Sprintf("Is cluster complete info: found node in non-running phase or not reachable state, node data: %+v\n", node.ToPrintableFormat()))
			return false, nil
		}
	}

	data, _ := json.MarshalIndent(v.ToPrintableFormat(), "", "")
	clusterData.SaveRedisClusterView(data)

	return true, nil
}

// func (r *RedisClusterReconciler) PrintForTests(redisCluster *dbv1.RedisCluster) {
// 	v, e := r.ExtractClusterInfo(redisCluster)
// 	println("LEADER COUNT: ", v.CurrentLeaderCount)
// 	println("FOLLOWER COUNT: ", v.CurrentFollowersPerLeaderCount)
// 	println("POD COUNT: ", v.CurrentPodCount)
// 	fmt.Printf("\nPODS VIEW: %+v\n", v.PodIndexToPodView)
// 	println("NEW LEADER COUNT ", v.NewLeaderCount)
// 	println("NEW FOLLOWER PER LEADER COUNT ", v.NewFollowersPreLeaderCount)
// 	println("NEW POD COUNT ", v.NewPodCount)
// 	println("New change")
// 	println("Err: ", e.Error())
// }

// func (r *RedisClusterReconciler) CreateNodeForTest(redisCluster *dbv1.RedisCluster) error {
// 	println("Getting cluster view")
// 	v, e := r.ExtractClusterInfo(redisCluster)
// 	if e != nil {
// 		println("Error: ", e.Error())
// 		return e
// 	}
// 	println("Got cluster view: ", v)
// 	println("Creating leader pod")
// 	newLeaders, e := r.CreateRedisLeaderPods(redisCluster, "6", "7", "8", "9", "10")
// 	if e != nil || len(newLeaders) == 0 {
// 		println("Error: ", e.Error())
// 		println("len(newLeaders): ", len(newLeaders))
// 		return e
// 	}
// 	println("Leader pod created")
// 	newLeader := newLeaders[0]
// 	newLeaderIp := newLeader.Status.PodIP
// 	servingLeadereIp := v.PodIndexToPodView["2"].IP
// 	println("Performing cluster meet, serving leader ip: ", servingLeadereIp)
// 	_, e = r.RedisCLI.ClusterMeet(servingLeadereIp, newLeaderIp, r.RedisCLI.Port)
// 	if e != nil {
// 		println("Error:", e.Error())
// 		return e
// 	}
// 	println("Cluster met the new leader pod :) !!")
// 	return nil
// }

// func (r *RedisClusterReconciler) ScaleHorizontally(redisCluster *dbv1.RedisCluster) error {
// 	clusterScaleView, e := r.ExtractClusterInfo(redisCluster)
// 	if e != nil {
// 		r.Log.Error(e, "Cluster scale-view creation failure. Could not extract cluster data.\n", e)
// 		return e
// 	}
// 	if r.isScaleRequired(clusterScaleView) {
// 		r.Log.Info("Horizontal scale: Attempting to scale leaders...")
// 		if e = r.scaleLeaders(clusterScaleView, redisCluster); e != nil {
// 			r.Log.Error(e, "Horizontal scale: Failed. Could not manage leaders scale re-arrangement.\n", e)
// 			return e
// 		}
// 		r.Log.Info("Horizontal scale: 1 out of 3 steps succeeded. Attempting to rebalance cluster...")
// 		e = r.requestRebalance(clusterScaleView)
// 		if e != nil {
// 			r.Log.Error(e, "Horizontal scale: Failed. Could not perform cluster rebalance.\n", e)
// 			return e
// 		}
// 		r.Log.Info("Horizontal scale: 2 out of 3 steps succeeded. Attempting to align followers to the new state...")
// 		if e = r.scaleFollowers(clusterScaleView, redisCluster); e != nil {
// 			r.Log.Error(e, "Horizontal scale: Completed improperly. Attempt to align followers to the new state was interrupted.")
// 			return e
// 		}
// 		r.Log.Info("Horizontal scale: 3 out of 3 steps succeeded.")
// 	}
// 	return nil
// }

// func (r *RedisClusterReconciler) ExtractClusterInfo(redisCluster *dbv1.RedisCluster) (*ClusterScaleView, error) {
// 	clusterView, e := r.NewRedisClusterView()
// 	if e != nil {
// 		return nil, e
// 	}

// 	clusterScaleView := &ClusterScaleView{
// 		CurrentLeaderCount:         len(clusterView.ViewByPodName), // bug, need to actually count leaders
// 		CurrentPodCount:            len(clusterView.ViewByPodName),
// 		NewLeaderCount:             redisCluster.Spec.LeaderCount,
// 		NewFollowersPreLeaderCount: redisCluster.Spec.LeaderFollowersCount,
// 		PodIndexToPodView:          make(map[string]*RedisNodePodView),
// 	}

// 	clusterScaleView.CurrentFollowersPerLeaderCount = (clusterScaleView.CurrentPodCount / clusterScaleView.CurrentLeaderCount) - 1
// 	clusterScaleView.NewPodCount = clusterScaleView.NewLeaderCount * (clusterScaleView.NewFollowersPreLeaderCount + 1)

// 	for _, node := range clusterView.ViewByPodName {
// 		nodeClusterId, e := r.RedisCLI.MyClusterID(node.IP)
// 		if e != nil {
// 			return nil, e
// 		}
// 		clusterScaleView.PodIndexToPodView[node.ID] = &RedisNodePodView{
// 			RedisNodeId: nodeClusterId,
// 			IP:          node.IP,
// 			Port:        r.RedisCLI.Port,
// 			IsLeader:    true,
// 		}
// 		if node.FollowersByName != nil && len(node.FollowersByName) > 0 {
// 			for _, followerName := range node.FollowersByName {
// 				followerClusterId, e := r.RedisCLI.MyClusterID(clusterView.ViewByPodName[followerName].ID)
// 				if e != nil {
// 					return nil, e
// 				}
// 				clusterScaleView.PodIndexToPodView[clusterView.ViewByPodName[followerName].ID] = &RedisNodePodView{
// 					RedisNodeId: followerClusterId,
// 					IP:          clusterView.ViewByPodName[followerName].IP,
// 					Port:        r.RedisCLI.Port,
// 					IsLeader:    false,
// 				}
// 			}
// 		}
// 	}
// 	return clusterScaleView, nil
// }

// func (r *RedisClusterReconciler) isScaleRequired(v *ClusterScaleView) bool {
// 	const MINIMUM_LEADERS_COUNT = 3
// 	if v.NewLeaderCount < MINIMUM_LEADERS_COUNT {
// 		r.Log.Info("[WARN] Horizontal scale: New leaders count was set to an unsupported value: %v, minimum number of leaders is %v, Attempt to perform scale horizontally won't be triggered", v.NewLeaderCount, MINIMUM_LEADERS_COUNT)
// 		return false
// 	}
// 	return v.CurrentLeaderCount != v.NewLeaderCount || v.CurrentFollowersPerLeaderCount != v.NewFollowersPreLeaderCount
// }

// func (r *RedisClusterReconciler) requestRebalance(v *ClusterScaleView) error {
// 	scaleUp := v.CurrentLeaderCount < v.NewLeaderCount
// 	scaleDown := v.CurrentLeaderCount > v.NewLeaderCount
// 	var e error
// 	if scaleUp {
// 		_, e = r.RedisCLI.ClusterRebalance(v.PodIndexToPodView["0"].IP, "--cluster-use-empty-masters")
// 	} else if scaleDown {
// 		_, e = r.RedisCLI.ClusterRebalance(v.PodIndexToPodView["0"].IP)
// 	}
// 	// Rebalance is needed only if number of leaders changed.
// 	return e
// }

// func (r *RedisClusterReconciler) scaleLeaders(v *ClusterScaleView, redisCluster *dbv1.RedisCluster) error {
// 	scaleUp := v.CurrentLeaderCount < v.NewLeaderCount
// 	scaleDown := v.CurrentLeaderCount > v.NewLeaderCount
// 	if scaleUp {
// 		if e := r.scaleUpLeaders(v, redisCluster); e != nil {
// 			return e
// 		}
// 	} else if scaleDown {
// 		if e := r.scaleDownLeaders(v); e != nil {
// 			return e
// 		}
// 	}
// 	return nil
// }

// func (r *RedisClusterReconciler) scaleUpLeaders(v *ClusterScaleView, redisCluster *dbv1.RedisCluster) error {
// 	for i := v.CurrentLeaderCount; i < v.NewLeaderCount; i++ {
// 		removedNode := v.PodIndexToPodView[string(i)]
// 		r.forgetRedisNodeAndDeletePod(removedNode, v)
// 		newLeaders, e := r.CreateRedisLeaderPods(redisCluster, string(i))
// 		if e != nil || len(newLeaders) == 0 {
// 			// re try loop
// 		}
// 		newLeader := newLeaders[0]
// 		newLeaderIp := newLeader.Status.PodIP
// 		servingLeadereIp := v.PodIndexToPodView["0"].IP
// 		_, e = r.RedisCLI.ClusterMeet(servingLeadereIp, newLeaderIp, r.RedisCLI.Port)
// 		if e != nil {
// 			// re try loop
// 		}
// 	}
// 	return nil
// }

// func (r *RedisClusterReconciler) scaleDownLeaders(v *ClusterScaleView) error {
// 	const NODES_AGREE = "[OK] All nodes agree about slots configuration."
// 	const SLOTS_COVERED = "[OK] All 16384 slots covered."
// 	LOWER_BOUND_FOR_TARGET_NODE := 0
// 	UPPER_BOUND_FOR_TARGET_NODE := v.NewLeaderCount - 1
// 	targetNode := LOWER_BOUND_FOR_TARGET_NODE
// 	for i := v.NewLeaderCount; i < v.CurrentLeaderCount; i++ {
// 		fromNodeId := v.PodIndexToPodView[string(i)].RedisNodeId
// 		toNodeId := v.PodIndexToPodView[string(targetNode)].RedisNodeId
// 		defaultNumOfSlots := ""
// 		servingLeadereIp := v.PodIndexToPodView["0"].IP
// 		r.RedisCLI.ClusterReshard(fromNodeId, toNodeId, defaultNumOfSlots, servingLeadereIp)
// 		if stdout, e := r.RedisCLI.ClusterCheck(servingLeadereIp); e == nil && strings.Contains(stdout, NODES_AGREE) && strings.Contains(stdout, SLOTS_COVERED) {
// 			slotOwnersMap, _, e := r.RedisCLI.ClusterSlots(servingLeadereIp)
// 			if e != nil {
// 				// re-try loop
// 			}
// 			if _, found := slotOwnersMap[fromNodeId]; found {
// 				return errors.New("Could not perform leaders scale down, reshard request was interrupted")
// 			}
// 			r.forgetRedisNodeAndDeletePod(v.PodIndexToPodView[string(i)], v)
// 			if targetNode == UPPER_BOUND_FOR_TARGET_NODE {
// 				targetNode = LOWER_BOUND_FOR_TARGET_NODE
// 			} else {
// 				targetNode++
// 			}
// 		} else {
// 			return errors.New("Could not perform leaders scale down, reshard request was interrupted")
// 		}
// 	}
// 	return nil
// }

// func (r *RedisClusterReconciler) scaleFollowers(v *ClusterScaleView, redisCluster *dbv1.RedisCluster) error {
// 	scaledUp := v.NewLeaderCount > v.CurrentLeaderCount
// 	var firstFollower int
// 	if scaledUp {
// 		firstFollower = v.NewLeaderCount
// 	} else {
// 		firstFollower = v.CurrentLeaderCount
// 	}
// 	if v.NewFollowersPreLeaderCount > 0 {
// 		for i := firstFollower; i < v.CurrentPodCount; i++ {
// 			newLeaderIdx := v.NewLeaderCount % i
// 			oldLeaderIdx := v.CurrentLeaderCount % i
// 			newNodeCount := v.NewLeaderCount*(v.NewFollowersPreLeaderCount+1) - 1
// 			if newLeaderIdx != oldLeaderIdx || i > newNodeCount {
// 				podView := v.PodIndexToPodView[string(i)]
// 				r.forgetRedisNodeAndDeletePod(podView, v)
// 			}
// 		}
// 	} else {
// 		for i := firstFollower; i < v.CurrentPodCount; i++ {
// 			podView := v.PodIndexToPodView[string(i)]
// 			r.forgetRedisNodeAndDeletePod(podView, v)
// 		}
// 	}
// 	return r.recoverCluster(redisCluster)
// }

// func (r *RedisClusterReconciler) forgetRedisNodeAndDeletePod(p *RedisNodePodView, v *ClusterScaleView) {
// 	for _, clusterPodView := range v.PodIndexToPodView {
// 		if clusterPodView != p {
// 			_, e := r.RedisCLI.ClusterForget(clusterPodView.IP, p.RedisNodeId)
// 			if e != nil {
// 				// re try loop
// 			}
// 		}
// 	}
// 	_, e := r.deletePodsByIP("", p.IP)
// 	if e != nil {
// 		// re try loop
// 	}
// }
