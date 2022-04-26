package controllers

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/pkg/errors"

	dbv1 "github.com/PayU/redis-operator/api/v1"
	rediscli "github.com/PayU/redis-operator/controllers/rediscli"
	view "github.com/PayU/redis-operator/controllers/view"
)

func (r *RedisClusterReconciler) NewRedisClusterView(redisCluster *dbv1.RedisCluster) (*view.RedisClusterView, error) {
	v := &view.RedisClusterView{}
	pods, e := r.getRedisClusterPods(redisCluster)
	if e != nil {
		fmt.Printf("Error fetching pods viw %+v\n", e.Error())
		return v, e
	}
	v.CreateView(pods, r.RedisCLI)
	return v, nil
}

// Returns the node name and leader name from a pod
func (r *RedisClusterReconciler) getRedisNodeNamesFromIP(namespace string, podIP string) (nodeName string, leaderName string, err error) {
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
	v, err := r.NewRedisClusterView(redisCluster)
	if err != nil {
		return err
	}
	followers := make(map[string]*view.NodeView)
	for _, leader := range v.Pods {
		for i := 1; i <= redisCluster.Spec.LeaderFollowersCount; i++ {
			followers[leader.Name+"-"+strconv.Itoa(i)] = leader
		}
	}

	err = r.addFollowers(redisCluster, followers)
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

	return r.waitForRedisSync(followerIP)
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

	if err = r.replicateLeader(newLeaderIP, promotedFollowerIP); err != nil {
		return err
	}

	r.Log.Info("Leader replication successful")

	if _, err = r.doLeaderFailover(promotedFollowerIP, "", []string{newLeaderIP}); err != nil {
		return err
	}

	r.Log.Info(fmt.Sprintf("[OK] Leader [%s] recreated successfully; new IP: [%s]", oldLeaderName, newLeaderIP))
	return nil
}

func (r *RedisClusterReconciler) deleteLeaders(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView, leaderNames map[string]bool) error {
	var healthyServerIp string
	for _, node := range v.Pods {
		if node.IsLeader && node.IsReachable {
			if _, toBeDeleted := leaderNames[node.Name]; !toBeDeleted {
				healthyServerIp = node.Ip
				break
			}
		}
	}
	if len(healthyServerIp) == 0 {
		return errors.New("Could not find healthy redis server ip to perform leader deletion operation")
	}
	var wg sync.WaitGroup
	wg.Add(len(leaderNames))
	for leaderName, _ := range leaderNames {
		go r.delLeaderNode(redisCluster, leaderName, healthyServerIp, &wg)
	}
	wg.Wait()
	return nil
}

func (r *RedisClusterReconciler) delLeaderNode(redisCluster *dbv1.RedisCluster, leaderName string, healthyServerIp string, wg *sync.WaitGroup) {
	defer wg.Done()
	pods, e := r.getRedisClusterPodsByLabel(redisCluster, "node-name", leaderName)
	if e != nil || len(pods) == 0 {
		r.Log.Error(e, "Could not get leader by name: "+leaderName)
		return
	}
	leader := pods[0]
	id, e := r.RedisCLI.MyClusterID(leader.Status.PodIP)
	if e != nil {
		r.Log.Error(e, "Could not get leader cluster id: "+leader.Name)
		return
	}
	_, e = r.RedisCLI.DelNode(healthyServerIp, id)
	if e != nil {
		r.Log.Error(e, "Could not perform cluster delete operation: "+leader.Name)
		return
	}
	r.deletePodsByIP(leader.Namespace, leader.Status.PodIP)
}

func (r *RedisClusterReconciler) addLeaders(redisCluster *dbv1.RedisCluster, v *view.RedisClusterView, leaderNames []string) error {
	leaders, e := r.createRedisLeaderPods(redisCluster, leaderNames...)
	if e != nil || len(leaders) == 0 {
		r.Log.Error(e, "Could not add new leaders")
		return e
	}
	var healthyServerIp string
	for _, node := range v.Pods {
		if node.IsLeader && node.IsReachable {
			healthyServerIp = node.Ip
			break
		}
	}
	if len(healthyServerIp) == 0 {
		return errors.New("Could not find healthy redis server ip to perform leader addition operation")
	}
	var wg sync.WaitGroup
	wg.Add(len(leaders))
	for _, leader := range leaders {
		go r.addLeader(redisCluster, leader, healthyServerIp, &wg)
	}
	wg.Wait()
	return nil
}

func (r *RedisClusterReconciler) addLeader(redisCluster *dbv1.RedisCluster, leaderPod corev1.Pod, healthyServerIp string, wg *sync.WaitGroup) {
	defer wg.Done()
	leaderPods, e := r.waitForPodReady(leaderPod)
	if e != nil || len(leaderPods) == 0 {
		r.Log.Error(e, fmt.Sprintf("Error while waiting for pod [%s] to be ready\n", leaderPod.Name))
		return
	}
	e = r.waitForRedis(leaderPods[0].Status.PodIP)
	if e != nil {
		r.Log.Error(e, fmt.Sprintf("Error while waiting for pod [%s] to be ready\n", leaderPod.Name))
		return
	}
	_, e = r.RedisCLI.AddLeader(leaderPods[0].Status.PodIP, healthyServerIp)
	if e != nil {
		r.Log.Error(e, fmt.Sprintf("Error while adding pod [%s] to redis cluster, healthy node ip: [%s]", leaderPod.Name, healthyServerIp))
		return
	}
}

// Adds one or more follower pods to the cluster
func (r *RedisClusterReconciler) addFollowers(redisCluster *dbv1.RedisCluster, missingFollowers map[string]*view.NodeView) error {
	var followerPods []corev1.Pod
	createOpts := []client.CreateOption{client.FieldOwner("redis-operator-controller")}
	for followerName, leader := range missingFollowers {
		pod, e := r.makeFollowerPod(redisCluster, followerName, leader.Name)
		if e != nil {
			return e
		}
		e = r.Create(context.Background(), &pod, createOpts...)
		if e != nil && !apierrors.IsAlreadyExists(e) {
			if strings.Contains(e.Error(), "already exists") {
				continue
			}
			return e
		}
		r.Log.Info(fmt.Sprintf("New follower pod [%s] created", followerName))
		if e != nil {
			if strings.Contains(e.Error(), "already exists") {
				continue
			}
			r.Log.Error(e, fmt.Sprintf("Could not create redis follower pod [%s] for leader [%s]\n", followerName, leader.Name))
			return e
		}
		followerPods = append(followerPods, pod)
	}
	followers, e := r.waitForPodNetworkInterface(followerPods...)
	if e != nil {
		return e
	}
	var wg sync.WaitGroup
	wg.Add(len(followers))
	for _, follower := range followers {
		leader := missingFollowers[follower.Name]
		go r.addFollower(redisCluster, follower, leader.Name, leader.Ip, &wg)
	}
	wg.Wait()
	return nil
}

func (r *RedisClusterReconciler) addFollower(redisCluster *dbv1.RedisCluster, followerPod corev1.Pod, leaderName string, leaderIp string, wg *sync.WaitGroup) {
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

	r.Log.Info(fmt.Sprintf("Replicating: %s %s", follower.Name, leaderName))

	if e = r.replicateLeader(follower.Status.PodIP, leaderIp); e != nil {
		r.Log.Error(e, "Error while waiting for pods to be ready")
		return
	}
}

// Removes all nodes the cluster node table entries with IDs of nodes not available
// Recives the list of healthy cluster nodes (Redis is reachable and has cluster mode on)
func (r *RedisClusterReconciler) forgetLostNodes(redisCluster *dbv1.RedisCluster) error {
	healthyIps := make([]string, 0)
	lostIds := make([]string, 0)
	visitedById := make(map[string]bool)

	pods, err := r.getRedisClusterPods(redisCluster)
	if err != nil {
		println("Cloud not retrieve cluster nodes")
		return err
	}
	for _, pod := range pods {
		podIp := pod.Status.PodIP
		nodeId, err := r.RedisCLI.MyClusterID(podIp)
		if err != nil {
			continue
		}
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

	r.Log.Info(fmt.Sprintf("List of lost nodes ids: %v", lostIds))

	var forgetByAllWG sync.WaitGroup
	forgetByAllWG.Add(len(lostIds))
	for _, id := range lostIds {
		r.forgetNodeByAllHealthyIps(healthyIps, id, &forgetByAllWG)
	}
	forgetByAllWG.Wait()
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
func (r *RedisClusterReconciler) handleFailover(redisCluster *dbv1.RedisCluster, leaderName string, reachableFollowers []corev1.Pod) (string, error) {
	promotedPodIP, err := r.waitForFailover(redisCluster, leaderName, reachableFollowers)
	if err == nil && promotedPodIP != "" {
		return promotedPodIP, nil
	}

	r.Log.Info(fmt.Sprintf("[WARN] Automatic failover failed for leader [%s]. Attempting forced failover.", leaderName))
	// Automatic failover failed. Attempt to force failover on a healthy follower.
	for _, follower := range reachableFollowers {
		followerIP := follower.Status.PodIP
		if forcedFailoverErr := r.doFailover(followerIP, "force"); forcedFailoverErr != nil {
			if rediscli.IsFailoverNotOnReplica(forcedFailoverErr) {
				r.Log.Info(fmt.Sprintf("Forced failover successful on [%s](%s)", follower.Name, followerIP))
				return followerIP, nil
			}
			r.Log.Error(forcedFailoverErr, fmt.Sprintf("[WARN] Failed forced attempt to make node [%s](%s) leader", follower.Name, followerIP))
		} else {
			r.Log.Info(fmt.Sprintf("Forced failover successful on [%s](%s)", follower.Name, followerIP))
			return followerIP, nil
		}
	}

	var forcedFailoverErr error
	// Forced failover failed. Attempt to takeover on a healthy follower.
	for _, follower := range reachableFollowers {
		followerIP := follower.Status.PodIP
		if forcedFailoverErr = r.doFailover(follower.Status.PodIP, "takeover"); forcedFailoverErr != nil {
			if rediscli.IsFailoverNotOnReplica(forcedFailoverErr) {
				r.Log.Info(fmt.Sprintf("Takeover successful on [%s](%s)", follower.Name, followerIP))
				return followerIP, nil
			}
			r.Log.Error(forcedFailoverErr, fmt.Sprintf("[WARN] Failed takeover attempt to make node [%s](%s) leader", follower.Name, followerIP))
		} else {
			r.Log.Info(fmt.Sprintf("Takeover successful on [%s](%s)", follower.Name, follower.Status.PodIP))
			return followerIP, nil
		}
	}
	return "", forcedFailoverErr
}

func (r *RedisClusterReconciler) recoverCluster(redisCluster *dbv1.RedisCluster) error {
	defer r.forgetLostNodes(redisCluster)
	r.Log.Info("Getting expected cluster view...")
	expectedView, err := r.GetExpectedView(cluster)
	if err != nil {
		return err
	}
	r.Log.Info("Getting actual cluster view...")
	pods, e := r.getRedisClusterPods(redisCluster)
	if e != nil {
		fmt.Printf("Error fetching pods viw %+v\n", e.Error())
		return e
	}
	terminatingPods := make([]corev1.Pod, 0)
	nonReachablePods := make([]corev1.Pod, 0)

	for _, pod := range pods {
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
	r.Log.Info(fmt.Sprintf("Removing non reachable pods...number of non reachable pods: %d", len(nonReachablePods)))
	for _, nonReachablePod := range nonReachablePods {
		deletedPod, _ := r.deletePodsByIP(nonReachablePod.Namespace, nonReachablePod.Status.PodIP)
		if len(deletedPod) > 0 {
			terminatingPods = append(terminatingPods, deletedPod...)
		}
	}
	r.Log.Info(fmt.Sprintf("Waiting for terminating pods...number of terminating pods: %d", len(terminatingPods)))
	for _, terminatingPod := range terminatingPods {
		r.waitForPodDelete(terminatingPod)
	}

	r.Log.Info(fmt.Sprintf("Validating leaders state..."))
	leadersRecovered, err := r.RecoverLeaders(redisCluster, expectedView)
	if err != nil {
		return err
	}

	if !leadersRecovered {
		r.Log.Info(fmt.Sprintf("Validating followers state..."))
		r.RecoverFollowers(redisCluster, expectedView)
	}

	complete, err := r.isClusterComplete(redisCluster, expectedView)
	if err != nil || !complete || leadersRecovered {
		r.Log.Info("Cluster recovery not complete")
	}

	return nil
}

func (r *RedisClusterReconciler) RecoverFollowers(redisCluster *dbv1.RedisCluster, expectedView map[string]string) error {
	v, err := r.NewRedisClusterView(redisCluster)
	if err != nil {
		return err
	}
	missingFollowers := make(map[string]*view.NodeView)
	for nodeName, leaderName := range expectedView {
		if nodeName != leaderName {
			_, exists := v.Pods[nodeName]
			if !exists {
				leaderNode, leaderExists := v.Pods[leaderName]
				if leaderExists {
					missingFollowers[nodeName] = leaderNode
				}
			}
		}
	}
	if err := r.addFollowers(redisCluster, missingFollowers); err != nil {
		r.Log.Error(err, fmt.Sprintf("Could not re create missing followers"))
	}
	return nil
}

func (r *RedisClusterReconciler) updateFollower(redisCluster *dbv1.RedisCluster, followerPod corev1.Pod) error {

	deletedPods, err := r.deletePodsByIP(redisCluster.Namespace, followerPod.Status.PodIP)
	if err != nil {
		return err
	} else {
		if err := r.waitForPodDelete(deletedPods...); err != nil {
			return err
		}
	}

	r.forgetLostNodes(redisCluster)
	followerName := followerPod.Name
	leaderName := followerPod.Labels["leader-name"]
	r.Log.Info(fmt.Sprintf("Starting to add follower. Follower name: (%s) LeaderName: (%s)", followerName, leaderName))

	leaderPod, e := r.getRedisClusterPodsByLabel(redisCluster, "node-name", leaderName)
	if e != nil || len(leaderPod) == 0 {
		r.Log.Error(e, fmt.Sprintf("Error while retrieving leader pod [%s]\n", leaderName))
		return e
	}

	var wg sync.WaitGroup
	wg.Add(1)
	r.addFollower(redisCluster, followerPod, leaderName, leaderPod[0].Status.PodIP, &wg)
	if err != nil {
		return err
	}
	wg.Wait()
	return nil
}

func (r *RedisClusterReconciler) updateLeader(redisCluster *dbv1.RedisCluster, leaderIP string) error {
	// TODO handle the case where a leader has no followers
	promotedFollowerIP, err := r.doLeaderFailover(leaderIP, "", []string{})
	if err != nil {
		return err
	}

	_, leaderName, err := r.getRedisNodeNamesFromIP(redisCluster.Namespace, promotedFollowerIP)
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

	if err := r.recreateLeader(redisCluster, promotedFollowerIP, leaderName); err != nil {
		return err
	}
	return nil
}

func (r *RedisClusterReconciler) updateCluster(redisCluster *dbv1.RedisCluster) error {
	r.Log.Info("Updating Cluster Pods...")
	pods, err := r.getRedisClusterPods(redisCluster)
	if err != nil {
		return err
	}
	for _, pod := range pods {
		podName := pod.Name
		podLeaderName := pod.Labels["leader-name"]
		podIp := pod.Status.PodIP
		isLeader := podName == podLeaderName
		podUpToDate, err := r.isPodUpToDate(redisCluster, &pod)
		if err != nil {
			return err
		}
		if !podUpToDate {
			if isLeader {
				if err = r.updateLeader(redisCluster, podIp); err != nil {
					// >>> TODO the logic of checking if a leader pod (first N pods) is indeed a Redis leader must be handled separately
					if rediscli.IsNodeIsNotMaster(err) {
						if _, errDel := r.deletePodsByIP(redisCluster.Namespace, podIp); errDel != nil {
							return errDel
						}
					}
					return err
				}
			} else {
				if err = r.updateFollower(redisCluster, pod); err != nil {
					return err
				}
			}
		} else {
			if _, pollErr := r.waitForPodReady(pod); pollErr != nil {
				return pollErr
			}
			if pollErr := r.waitForRedis(pod.Status.PodIP); pollErr != nil {
				return pollErr
			}
		}
	}
	r.forgetLostNodes(redisCluster)
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
	if err := wait.PollImmediate(3*r.Config.Times.SyncStartCheckInterval, 10*r.Config.Times.SyncStartCheckTimeout, func() (bool, error) {
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

	return wait.PollImmediate(3*r.Config.Times.SyncCheckInterval, 10*r.Config.Times.SyncCheckTimeout, func() (bool, error) {
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
func (r *RedisClusterReconciler) waitForFailover(redisCluster *dbv1.RedisCluster, leaderName string, reachableFollowers []corev1.Pod) (string, error) {
	r.Log.Info(fmt.Sprintf("Waiting for leader [%s] failover", leaderName))
	var promotedFollowerIP string

	return promotedFollowerIP, wait.PollImmediate(3*r.Config.Times.RedisAutoFailoverCheckInterval, 10*r.Config.Times.RedisAutoFailoverCheckTimeout, func() (bool, error) {
		for _, follower := range reachableFollowers {
			info, _, err := r.RedisCLI.Info(follower.Status.PodIP)
			if err != nil {
				continue
			}

			if info.Replication["role"] == "master" {
				promotedFollowerIP = follower.Status.PodIP
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

func (r *RedisClusterReconciler) isClusterComplete(redisCluster *dbv1.RedisCluster, expectedView map[string]string) (bool, error) {
	defer r.forgetLostNodes(redisCluster)
	view, err := r.NewRedisClusterView(redisCluster)
	if err != nil {
		return false, err
	}
	missingLeaders := []string{}
	missingFollowers := []string{}
	for nodeName, leaderName := range expectedView {
		if _, exists := view.Pods[nodeName]; !exists {
			if nodeName == leaderName {
				missingLeaders = append(missingLeaders, nodeName)
			} else {
				missingFollowers = append(missingFollowers, nodeName)
			}
		}
	}
	isComplete := len(missingLeaders) == 0 && len(missingFollowers) == 0

	r.Log.Info(fmt.Sprintf("Is cluster complete: %v", isComplete))

	return isComplete, nil
}

func (r *RedisClusterReconciler) RecoverLeaders(redisCluster *dbv1.RedisCluster, expectedView map[string]string) (bool, error) {
	missingLeaders, e := r.GetMissingLeadersMap(redisCluster, expectedView)
	if e != nil {
		r.Log.Error(e, fmt.Sprintf("Could not retrieve missing leaders, failover process failed\n"))
		return false, nil
	}
	leadersWithResponsiveFollowers, leadersWithoutFollowers, e := r.GetResponsiveFollowers(redisCluster, missingLeaders)
	if e != nil {
		r.Log.Error(e, fmt.Sprintf("Could not retrieve followers list for missing leaders, failover process failed\n"))
		return false, nil
	}
	successfulFailovers := r.FailOverMissingLeaders(redisCluster, leadersWithResponsiveFollowers)
	if len(successfulFailovers) > 0 {
		r.RecreateLeaders(redisCluster, successfulFailovers)
	}
	r.Log.Info(fmt.Sprintf("Leaders without followers: %v", leadersWithoutFollowers))
	recoveryInitiated := len(leadersWithResponsiveFollowers)+len(leadersWithoutFollowers) > 0
	return recoveryInitiated, nil
}

func (r *RedisClusterReconciler) GetMissingLeadersMap(redisCluster *dbv1.RedisCluster, expectedView map[string]string) (map[string]bool, error) {
	view, e := r.NewRedisClusterView(redisCluster)
	if e != nil {
		return nil, e
	}
	missingLeaders := make(map[string]bool)
	for _, leaderName := range expectedView {
		_, exists := view.Pods[leaderName]
		_, declaredMissing := missingLeaders[leaderName]
		if !exists && !declaredMissing {
			missingLeaders[leaderName] = true
		}
	}
	return missingLeaders, nil
}

func (r *RedisClusterReconciler) GetResponsiveFollowers(redisCluster *dbv1.RedisCluster, missingLeaders map[string]bool) (leadersWithResponsiveFollowers map[string][]corev1.Pod, leadersWithoutFollowers []string, e error) {
	leadersWithResponsiveFollowers = make(map[string][]corev1.Pod)
	leadersWithoutFollowers = make([]string, 0)
	for missingLeader, _ := range missingLeaders {
		leadersWithResponsiveFollowers[missingLeader] = make([]corev1.Pod, 0)
		responsiveFollowers, e := r.getRedisClusterPodsByLabel(redisCluster, "leader-name", missingLeader)
		if e != nil {
			return nil, nil, e
		}
		for _, responsiveFollower := range responsiveFollowers {
			if responsiveFollower.Status.Phase == "Running" {
				if responsiveFollower.Name == missingLeader {
					delete(leadersWithResponsiveFollowers, missingLeader)
					break
				}
				leadersWithResponsiveFollowers[missingLeader] = append(leadersWithResponsiveFollowers[missingLeader], responsiveFollower)
			}
		}
		if len(leadersWithResponsiveFollowers[missingLeader]) == 0 {
			leadersWithoutFollowers = append(leadersWithoutFollowers, missingLeader)
			delete(leadersWithResponsiveFollowers, missingLeader)
		}
	}
	return leadersWithResponsiveFollowers, leadersWithoutFollowers, nil
}

func (r *RedisClusterReconciler) FailOverMissingLeaders(redisCluster *dbv1.RedisCluster, followersMap map[string][]corev1.Pod) (successfulFailovers map[string]string) {
	successfulFailovers = make(map[string]string)
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
					successfulFailovers[leader] = ip
					hasPromotedFollower = true
					break
				}
			}
			if !hasPromotedFollower {
				promotedIp, e := r.handleFailover(redisCluster, leader, followers)
				if e == nil && len(promotedIp) > 0 {
					successfulFailovers[leader] = promotedIp
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
