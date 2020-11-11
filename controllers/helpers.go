package controllers

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	dbv1 "github.com/PayU/Redis-Operator/api/v1"
	"github.com/PayU/Redis-Operator/controllers/rediscli"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// RedisClusterState describes the current
// reconcile state of the redis cluster
type RedisClusterState string

// RedisGroups is a map of leaders and their corresponding followrs
// the map key is the leader number (0, 1, 2 and so on)
// and the map value is the group data
type RedisGroups map[string]RedisGroup

// RedisGroup describes a leader and its followers
type RedisGroup struct {
	LeaderNumber string // number given at pod creation time
	LeaderID     string
	LeaderPod    corev1.Pod
	FollowerPods []corev1.Pod
}

var (
	patchOpts = []client.PatchOption{client.FieldOwner("redis-operator-controller")}
)

const (
	// NotExists means there is no redis pods in the k8s cluster
	NotExists RedisClusterState = "NotExists"

	// DeployingLeaders marks the beginning of leader nodes deployment
	DeployingLeaders RedisClusterState = "DeployingLeaders"

	// InitializingLeaders is the initialize state for all cluster resources (pods, service, etc..),
	// leaders are deployed and ready to group together as cluster
	InitializingLeaders RedisClusterState = "InitializingLeaders"

	// ClusteringLeaders is the state were all leader nodes of the redis cluster
	// are grouped together but not all of the follower nodes joined yet.
	ClusteringLeaders RedisClusterState = "ClusteringLeaders"

	// DeployingFollowers state marks that the follower nodes are being deployed
	DeployingFollowers RedisClusterState = "DeployingFollowers"

	// InitializingFollowers is the initialize state for follower pods
	InitializingFollowers RedisClusterState = "InitializingFollowers"

	// ClusteringFollowers is the state were all Follower nodes of the redis cluster
	// are associated with a leader.
	ClusteringFollowers RedisClusterState = "ClusteringFollowers"

	// Ready means cluster is up & running as expected
	Ready RedisClusterState = "Ready"

	// RecoverFollowersNodes means some followers pod/s failed/crashed and has been replaced
	// with a new k8s pods but still didn't join the cluster
	RecoverFollowersNodes RedisClusterState = "RecoverFollowersNodes"

	// RecoverLeaderNodes means leader pods/s failed/crashed and master failover process
	// has finished but no update and/or new pods deployment occurred
	RecoverLeaderNodes RedisClusterState = "RecoverLeaderNodes"

	// Unknown means that we are not able to identify the current state
	Unknown RedisClusterState = "Unknown"
)

var currentRedisClusterState RedisClusterState

func getCurrentClusterState(logger logr.Logger, redisOperator *dbv1.RedisCluster) RedisClusterState {
	if len(redisOperator.Status.ClusterState) == 0 {
		return NotExists
	}

	return RedisClusterState(redisOperator.Status.ClusterState)
}

// Flatten returns Redis groups as an array
func (g *RedisGroups) Flatten() []RedisGroup {
	groupArray := make([]RedisGroup, len(*g))
	i := 0
	for _, group := range *g {
		groupArray[i] = group
		i++
	}
	return groupArray
}

func (g *RedisGroup) String() string {
	strResult := fmt.Sprintf("%s (id:%s) =>", g.LeaderPod.Status.PodIP, g.LeaderNumber)
	for _, follower := range g.FollowerPods {
		strResult += (" " + follower.Status.PodIP)
	}
	return strResult
}

// NewRedisGroups is the constructor of RedisGroups struct
func NewRedisGroups(redisPods *corev1.PodList, redisclusterNodes *rediscli.RedisClusterNodes) *RedisGroups {
	groups := make(RedisGroups)
	for _, redisPod := range redisPods.Items {
		leaderNumber := redisPod.ObjectMeta.Labels["leader-number"]

		group, found := groups[leaderNumber]
		if !found {
			groups[leaderNumber] = RedisGroup{
				LeaderNumber: leaderNumber,
				LeaderPod:    corev1.Pod{},
				FollowerPods: nil,
			}
		}

		group.LeaderNumber = leaderNumber

		if redisPod.ObjectMeta.Labels["redis-node-role"] == "leader" {
			group.LeaderPod = redisPod
		} else {
			group.FollowerPods = append(group.FollowerPods, redisPod)
		}

		if redisclusterNodes != nil {
			for _, cliClusterNode := range *redisclusterNodes {
				if !cliClusterNode.IsFailing && cliClusterNode.Leader == "-" && strings.HasPrefix(cliClusterNode.Addr, redisPod.Status.PodIP) {
					group.LeaderID = cliClusterNode.ID
				}
			}
		}

		groups[leaderNumber] = group
	}

	return &groups
}

func (r *RedisClusterReconciler) getRedisClusterPods(ctx context.Context, redisOperator *dbv1.RedisCluster, podType string) (*corev1.PodList, error) {
	pods := &corev1.PodList{}
	matchingLabels := make(map[string]string)
	matchingLabels["app"] = redisOperator.Spec.PodLabelSelector.App

	if podType != "any" {
		matchingLabels["redis-node-role"] = podType
	}

	err := r.List(ctx, pods, client.InNamespace(redisOperator.ObjectMeta.Namespace), client.MatchingLabels(matchingLabels))
	if err != nil {
		return nil, err
	}

	return pods, nil
}

// createFollowersForLeader create all followers pods for a specifc leader pod
// the function return the number of created followers for the specific leader
func (r *RedisClusterReconciler) createFollowersForLeader(ctx context.Context, redisOperator *dbv1.RedisCluster, leaderNumber int, startingSequentialNumber int) (int, error) {
	followersCount := int(redisOperator.Spec.LeaderFollowersCount)

	for i := 0; i < followersCount; i++ {
		followerPod, err := r.followerPod(redisOperator, leaderNumber, startingSequentialNumber+i)
		if err != nil {
			return 0, err
		}

		r.Log.Info(fmt.Sprintf("deploying follower-%d-%d", leaderNumber, i))

		err = r.Patch(ctx, &followerPod, client.Apply, patchOpts...)
		if err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				return 0, err
			}
			r.Log.Info(fmt.Sprintf("follower-%d-%d already exists", leaderNumber, i))
		}
	}

	return followersCount, nil
}

func (r *RedisClusterReconciler) createLeaders(ctx context.Context, redisOperator *dbv1.RedisCluster, nodeCount int) error {
	for i := 0; i < nodeCount; i++ {
		leaderPod, err := r.leaderPod(redisOperator, i, i)
		if err != nil {
			return err
		}

		r.Log.Info(fmt.Sprintf("deploying leader-%d", i))

		err = r.Patch(ctx, &leaderPod, client.Apply, patchOpts...)
		if err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				return err
			}

			r.Log.Info(fmt.Sprintf("leader-%d already exists", i))
		}
	}
	return nil
}

func (r *RedisClusterReconciler) createNewCluster(ctx context.Context, redisOperator *dbv1.RedisCluster) error {
	r.Log.Info("creating new cluster")
	desiredLeaders := int(redisOperator.Spec.LeaderReplicas)

	// create config map
	configMap, err := r.createSettingsConfigMap(redisOperator)
	err = r.Create(ctx, &configMap)
	if err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return err
		}

		r.Log.Info("config map already exists")
	}

	// create service
	service, err := r.serviceResource(redisOperator)
	err = r.Create(ctx, &service)
	if err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return err
		}

		r.Log.Info("service already exists")
	}

	// create headless service
	headlessService, err := r.headlessServiceResource(redisOperator)
	err = r.Create(ctx, &headlessService)
	if err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return err
		}

		r.Log.Info("headless service already exists")
	}

	// deploy the leader redis nodes
	err = r.createLeaders(ctx, redisOperator, desiredLeaders)
	if err != nil {
		return err
	}

	redisOperator.Status.ClusterState = string(DeployingLeaders)

	return nil
}

func (r *RedisClusterReconciler) checkRedisNodes(ctx context.Context, redisOperator *dbv1.RedisCluster, nodeRole string) (bool, error) {
	podsReady := true
	pods, err := r.getRedisClusterPods(ctx, redisOperator, nodeRole)
	if err != nil {
		return false, err
	}

	for _, pod := range pods.Items {
		for _, podCondition := range pod.Status.Conditions {
			podsReady = podsReady && podCondition.Status == corev1.ConditionTrue
		}
	}

	r.Log.Info(fmt.Sprintf("%ss ready:%t", nodeRole, podsReady))

	return podsReady, nil
}

func (r *RedisClusterReconciler) handleDeployingLeaders(ctx context.Context, redisOperator *dbv1.RedisCluster) error {
	r.Log.Info("handling deploying leaders")

	leadersReady, err := r.checkRedisNodes(ctx, redisOperator, "leader")
	if err != nil {
		return err
	}

	if leadersReady {
		redisOperator.Status.ClusterState = string(InitializingLeaders)
	}

	return nil
}

func (r *RedisClusterReconciler) handleDeployingFollowers(ctx context.Context, redisOperator *dbv1.RedisCluster) error {
	r.Log.Info("handling deploying followers")

	followersReady, err := r.checkRedisNodes(ctx, redisOperator, "follower")
	if err != nil {
		return err
	}

	if followersReady {
		redisOperator.Status.ClusterState = string(InitializingFollowers)
	}

	return nil
}

func (r *RedisClusterReconciler) handleInitializingLeaders(ctx context.Context, redisOperator *dbv1.RedisCluster) error {
	r.Log.Info("handling initializing leaders")
	leaderPods, err := r.getRedisClusterPods(ctx, redisOperator, "leader")
	if err != nil {
		return err
	}

	leaderPodIPAddresses := make([]string, 0)
	for _, leaderPod := range leaderPods.Items {
		leaderPodIPAddresses = append(leaderPodIPAddresses, fmt.Sprintf("%s:6379", leaderPod.Status.PodIP))
	}

	if err = r.RedisCLI.ClusterCreate(leaderPodIPAddresses); err != nil {
		return err
	}

	redisOperator.Status.ClusterState = string(ClusteringLeaders)

	return nil
}

func (r *RedisClusterReconciler) handleInitializingFollowers(ctx context.Context, redisOperator *dbv1.RedisCluster) error {
	r.Log.Info("handling initializing followers")

	redisPods, err := r.getRedisClusterPods(ctx, redisOperator, "any")
	if err != nil {
		return err
	}

	// wait for network interfaces to be configured
	for _, pod := range redisPods.Items {
		if pod.Status.PodIP == "" {
			return fmt.Errorf("Pod %s not ready - waiting for IP", pod.Name)
		}
	}

	redisGroups := NewRedisGroups(redisPods, nil).Flatten()

	if err != nil {
		return err
	}

	for _, group := range redisGroups {
		for _, follower := range group.FollowerPods {
			redisLeaderID, err := r.RedisCLI.GetMyClusterID(group.LeaderPod.Status.PodIP)
			if err != nil {
				return err
			}
			r.RedisCLI.AddFollower(follower.Status.PodIP, group.LeaderPod.Status.PodIP, redisLeaderID)
		}
	}

	redisOperator.Status.ClusterState = string(ClusteringFollowers)
	return nil
}

func (r *RedisClusterReconciler) handleClusteringLeaders(ctx context.Context, redisOperator *dbv1.RedisCluster) error {
	r.Log.Info("handling clustering leaders")
	leaderPods, err := r.getRedisClusterPods(ctx, redisOperator, "leader")
	if err != nil {
		return err
	}

	startingfollowerSequentialNumber := len(leaderPods.Items)

	for _, leaderPod := range leaderPods.Items {
		leaderNumber, err := strconv.Atoi(leaderPod.ObjectMeta.Labels["leader-number"])
		if err != nil {
			return err
		}

		createdFollowers, err := r.createFollowersForLeader(ctx, redisOperator, leaderNumber, startingfollowerSequentialNumber)
		if err != nil {
			return err
		}

		startingfollowerSequentialNumber += createdFollowers
	}

	redisOperator.Status.ClusterState = string(DeployingFollowers)

	return nil
}

func (r *RedisClusterReconciler) handleClusteringFollowers(ctx context.Context, redisOperator *dbv1.RedisCluster) error {
	r.Log.Info("handling clustering followers")
	redisPods, err := r.getRedisClusterPods(ctx, redisOperator, "any")
	if err != nil {
		return err
	}

	redisGroups := NewRedisGroups(redisPods, nil).Flatten()
	clusterNodes, err := r.RedisCLI.GetClusterNodesInfo(redisGroups[0].LeaderPod.Status.PodIP)
	if err != nil {
		return err
	}

	if len(*clusterNodes) != len(redisPods.Items) {
		return fmt.Errorf("Follower clustering incomplete - not all pods are inside the cluster")
		// TODO: do a more thorough check, see if all followers are linked to the right leader
	}

	r.Log.Info("follower clustering complete")
	redisOperator.Status.TotalExpectedPods = redisOperator.Spec.LeaderReplicas * (redisOperator.Spec.LeaderFollowersCount + 1)
	redisOperator.Status.ClusterState = string(Ready)

	return nil
}

func (r *RedisClusterReconciler) handleLeaderNodesRecoverState(ctx context.Context, redisCluster *dbv1.RedisCluster, namespace string) error {
	r.Log.Info("handling leader nodes recover state")
	var clusterInfo *rediscli.RedisClusterNodes
	var k8sRedisPods *corev1.PodList
	var k8sInfoRedisPod corev1.Pod
	var missingPodNumbers []int
	var err error

	if k8sRedisPods, err = r.getRedisClusterPods(ctx, redisCluster, "any"); err != nil {
		return err
	}

	for _, k8sRedisPod := range k8sRedisPods.Items {
		clusterInfo, err = r.RedisCLI.GetClusterNodesInfo(k8sRedisPod.Status.PodIP)
		if err == nil {
			k8sInfoRedisPod = k8sRedisPod
			break
		}
	}

	if err != nil {
		return err
	}

	for _, redisNode := range *clusterInfo {
		if redisNode.Leader != "-" {
			continue
		}

		// after master failover, the new leader node still has the old follower pod configuration
		// of 'follower' label and follower affinity rules. so we need to update those values without restart the pod
		for _, k8sRedisPod := range k8sRedisPods.Items {
			if strings.HasPrefix(redisNode.Addr, k8sRedisPod.Status.PodIP) {
				if k8sRedisPod.Labels["redis-node-role"] == "follower" {
					r.Log.Info(fmt.Sprintf("update k8s pod [%s, %s] with value of 'leader' to the tag 'redis-node-role'", k8sRedisPod.Name, k8sRedisPod.Status.PodIP))

					var curretPodDeployed corev1.Pod

					if err = r.Client.Get(ctx, types.NamespacedName{
						Namespace: namespace,
						Name:      k8sRedisPod.Name,
					}, &curretPodDeployed); err != nil {
						return err
					}

					patch := []byte(`{"metadata":{"labels":{"redis-node-role": "leader"}}}`)
					err = r.Client.Patch(ctx, &curretPodDeployed, client.RawPatch(types.StrategicMergePatchType, patch))

					if err != nil {
						return err
					}

					// waiting for patch to apply
					patchApply := false

					for patchApply != true {
						r.Log.Info(fmt.Sprintf("waiting for patch to apply on pod [%s %s]", k8sRedisPod.Name, k8sRedisPod.Status.PodIP))
						wait(r.Log, 500)

						if err = r.Client.Get(ctx, types.NamespacedName{
							Namespace: namespace,
							Name:      k8sRedisPod.Name,
						}, &curretPodDeployed); err != nil {
							return err
						}

						patchApply = curretPodDeployed.Labels["redis-node-role"] == "leader"
					}

					break
				}
			}
		}
	}

	// get the updated k8s pod list and create the redis group
	if k8sRedisPods, err = r.getRedisClusterPods(ctx, redisCluster, "any"); err != nil {
		return err
	}

	redisGroups := NewRedisGroups(k8sRedisPods, clusterInfo)

	// after we updated all new leaders with the 'leader' tag, we now need to deploy all of the missing pods as followers.
	// first, we will find all missing pod suffix number
	if missingPodNumbers, err = getMissingPodsNumber(k8sRedisPods, redisCluster.Status.TotalExpectedPods, r.Log); err != nil {
		return err
	}

	// second, we will deploy all the missing followers for leaders
	for _, redisNode := range *clusterInfo {
		if redisNode.Leader == "-" {
			leaderReplicas, err := r.RedisCLI.GetLeaderReplicas(k8sInfoRedisPod.Status.PodIP, redisNode.ID)
			if err != nil {
				return err
			}

			leaderNumber, err := getLeaderNumberByLeaderID(redisGroups, redisNode.ID, r.Log)
			if err != nil {
				return err
			}

			r.Log.Info(fmt.Sprintf("found %d replicas for leader id [%s]. expected replicas %d", leaderReplicas.Count, redisNode.ID, redisCluster.Spec.LeaderFollowersCount))

			for leaderReplicas.Count < redisCluster.Spec.LeaderFollowersCount {
				if missingPodNumbers, err = r.deployFollowerAfterFailure(ctx, redisCluster, leaderNumber, &missingPodNumbers); err != nil {
					return err
				}

				leaderReplicas.Count = leaderReplicas.Count + 1
			}
		}
	}

	redisCluster.Status.ClusterState = string(RecoverFollowersNodes)

	return nil
}

func (r *RedisClusterReconciler) handleFollowerNodesRecoverState(ctx context.Context, redisCluster *dbv1.RedisCluster) error {
	r.Log.Info("handling followers nodes recover state")

	redisPods, err := r.getRedisClusterPods(ctx, redisCluster, "any")
	if err != nil {
		return err
	}

	// wait for network interfaces to be configured
	for _, pod := range redisPods.Items {
		if pod.Status.PodIP == "" {
			return fmt.Errorf("Pods %s not ready - waiting for IP", pod.Name)
		}
	}

	redisGroups := NewRedisGroups(redisPods, nil).Flatten()
	clusterNodes, err := r.RedisCLI.GetClusterNodesInfo(redisGroups[0].LeaderPod.Status.PodIP)
	if err != nil {
		return err
	}

	if err != nil {
		return err
	}

	for _, group := range redisGroups {
		for _, followerPod := range group.FollowerPods {
			// check if this follower pod ip exists in the redis-cli cluster nodes list. if not,
			// that means we need to join this specific follower to the cluster
			shouldJoin := true
			for _, cliClusterNode := range *clusterNodes {
				if strings.HasPrefix(cliClusterNode.Addr, followerPod.Status.PodIP) {
					shouldJoin = false
				}
			}

			if shouldJoin {
				redisLeaderID, err := r.RedisCLI.GetMyClusterID(group.LeaderPod.Status.PodIP)
				if err != nil {
					return err
				}
				r.RedisCLI.AddFollower(followerPod.Status.PodIP, group.LeaderPod.Status.PodIP, redisLeaderID)
			}
		}
	}

	redisCluster.Status.ClusterState = string(ClusteringFollowers)
	return nil
}

/*
* when handleClusterReadyState is called, it means that the Reconcile function was triggered after the redis cluster
* was already function as expected and was working properly. this trigger can occur from various reasons like losing
* one (or many) of the redis cluster pods, a new deployment was initiated and so on..
* at first, we will identify the exact change of the state and only after that we will response to it.
* in addition, this function will trigger when we just finished to deploy the cluster for the very first time.
 */
func (r *RedisClusterReconciler) handleClusterReadyState(ctx context.Context, redisCluster *dbv1.RedisCluster) error {
	var clusterInfo *rediscli.RedisClusterNodes = nil
	var redisGroups *RedisGroups = nil
	var anyActionApplied bool = false
	var k8sRedisPods *corev1.PodList
	var err error = nil

	r.Log.Info("reconcile function was triggered while the last known cluster state was ready. checking cluster status..")

	// waiting for [1.1 * (cluster-node-timeout + repl-ping-replica-period)] amount in case 1 or more nodes
	// are unreachable so the cluster will considered them in failure state and master fail over will occuer
	// in case the failing node(s) were leader nodes
	wait(r.Log, 1.1*(5000+5000))

	k8sRedisPods, err = r.getRedisClusterPods(ctx, redisCluster, "any")
	if err != nil {
		return err
	}

	for _, k8sRedisPod := range k8sRedisPods.Items {
		clusterInfo, err = r.RedisCLI.GetClusterNodesInfo(k8sRedisPod.Status.PodIP)
		if err == nil {
			break
		}
	}

	if err != nil {
		return err
	}

	redisGroups = NewRedisGroups(k8sRedisPods, clusterInfo)

	// clusterNodesList, redisGroups and redisPods has all the current data
	// of the redis cluster as well as the redis pods status. this should be used now
	// do to all the checkes and fix any issues if found.
	// first step is checking for failing nodes. if we found any we will
	// clean them by using CLUSTER FORGET redis command and deploy new pods to replace them
	anyActionApplied, err = r.forgetFailingNodes(ctx, redisCluster, k8sRedisPods, clusterInfo, redisGroups)
	if err != nil {
		return err
	}

	if !anyActionApplied {
		r.Log.Info("redis cluster is fully operational")
	}

	return err
}

func (r *RedisClusterReconciler) forgetFailingNodes(ctx context.Context, redisCluster *dbv1.RedisCluster,
	k8sRedisPods *corev1.PodList, clusterInfo *rediscli.RedisClusterNodes, redisGroups *RedisGroups) (bool, error) {
	r.Log.Info("looking for failing cluster nodes..")

	var err error
	failingNodes := make([]rediscli.RedisClusterNode, 0)

	// search for failing nodes in the cluster
	for _, redisCliNode := range *clusterInfo {
		// add all nodes that mark as failed in redis-cli
		// to the failing nodes list
		if redisCliNode.IsFailing {
			failingNodes = append(failingNodes, redisCliNode)
		}
	}

	if len(failingNodes) == 0 {
		r.Log.Info("no failing nodes has found in the cluster")
		return false, nil
	}

	r.Log.Info(fmt.Sprintf("found %d failing redis nodes. starting cluster forget process..", len(failingNodes)))

	// iterate over all cluster nodes, for every healthy node do
	// CLUSTER FORGET command on all other failing nodes
	for _, redisCliNode := range *clusterInfo {
		if !redisCliNode.IsFailing {
			for _, failingNode := range failingNodes {
				nodeIP := strings.Split(redisCliNode.Addr, ":")[0]
				if err = r.RedisCLI.ForgetNode(nodeIP, failingNode.ID); err != nil {
					return true, err
				}

			}
		}
	}

	// failing redis nodes can occuer from a crashed pod or from internal error.
	// in case of internal error the failing redis node pod is still exists (but may not be healthy).
	// in this case we need to make sure to remove the failing pod so we can deploy a new one insted
	podsToDelete := make([]corev1.Pod, 0)
	for _, k8sPod := range k8sRedisPods.Items {
		for _, failingNode := range failingNodes {
			if strings.HasPrefix(failingNode.Addr, k8sPod.Status.PodIP) {
				podsToDelete = append(podsToDelete, k8sPod)
			}
		}
	}

	if len(podsToDelete) > 0 {
		r.Log.Info(fmt.Sprintf("found %d pods to be deleted", len(podsToDelete)))
		for _, podToDelete := range podsToDelete {
			r.Log.Info(fmt.Sprintf("deleting pod [%s] in namespace [%s]", podToDelete.Name, podToDelete.Namespace))
			r.Delete(ctx, &podToDelete)
		}

		// wait until all failed pods are deleted
		for len(k8sRedisPods.Items) > int(redisCluster.Status.TotalExpectedPods)-len(podsToDelete) {
			r.Log.Info(fmt.Sprintf("waiting for failing pods to be deleted. current number of k8s nodes is: %d", len(k8sRedisPods.Items)))
			wait(r.Log, 2500)

			k8sRedisPods, err = r.getRedisClusterPods(ctx, redisCluster, "any")
			if err != nil {
				return true, err
			}
		}

		r.Log.Info(fmt.Sprintf("number of k8s nodes after deletion is %d", len(k8sRedisPods.Items)))

		// after we deleted the failed redis k8s pods using k8sRedisPods list
		// we need to featch the update k8s pods list again
		k8sRedisPods, err = r.getRedisClusterPods(ctx, redisCluster, "any")
		if err != nil {
			return true, err
		}
	}

	// find all missing pod suffix number
	missingPodNumbers, err := getMissingPodsNumber(k8sRedisPods, redisCluster.Status.TotalExpectedPods, r.Log)
	if err != nil {
		return true, err
	}

	err = r.replaceK8sFailedNodes(ctx, redisCluster, k8sRedisPods, failingNodes, redisGroups, missingPodNumbers)
	if err != nil {
		return true, err
	}

	return true, nil
}

// after we forgot the failing nodes, we need to deploy new pod(s) to replace the failing once.
// there are 2 cases here - a failing leader or a failing follower.
// in both cases all the new nodes should be a slave nodes since in a leader failing scenario
// a "master failover" will occur and one of he's followers will be promoted to a leader (and needs a new follower to replace him).
// in a follower failing scenario we are just replacing the failing follower with a new one.
func (r *RedisClusterReconciler) replaceK8sFailedNodes(ctx context.Context, redisCluster *dbv1.RedisCluster,
	k8sRedisPods *corev1.PodList, failingNodes []rediscli.RedisClusterNode, redisGroups *RedisGroups, missingPodNumbers []int) error {

	var err error
	masterFailoverOccur := false

	for _, failingNode := range failingNodes {
		if failingNode.Leader == "-" {
			// the failing node is a leader. that means redis is now
			// swap the current failing master with one of its replicas (master failover)
			// we will wait until all failover processes to finish and then we will deploy
			// the relevant new followers for the new leaders
			masterFailoverOccur = true
			continue
		}

		// if here, the failing node is a follower
		leaderNumber, err := getLeaderNumberByLeaderID(redisGroups, failingNode.Leader, r.Log)
		if err != nil {
			return err
		}

		missingPodNumbers, err = r.deployFollowerAfterFailure(ctx, redisCluster, leaderNumber, &missingPodNumbers)
	}

	if !masterFailoverOccur {
		redisCluster.Status.ClusterState = string(RecoverFollowersNodes)
		return nil
	}

	// 1 or more of leader nodes has failed
	var clusterInfo *rediscli.RedisClusterNodes
	var k8sRedisPod corev1.Pod

	for _, k8sRedisPod = range k8sRedisPods.Items {
		clusterInfo, err = r.RedisCLI.GetClusterNodesInfo(k8sRedisPod.Status.PodIP)
		if err == nil {
			break
		}
	}

	if err != nil {
		return err
	}

	// verify all master failover processes has finished by
	// checking we are having all the requested leaders amount
	var currentLeaderNumber int32 = 0

	r.Log.Info("waiting for master failover process to finish")
	for currentLeaderNumber < redisCluster.Spec.LeaderReplicas {
		currentLeaderNumber = 0

		wait(r.Log, 1000)

		clusterInfo, err = r.RedisCLI.GetClusterNodesInfo(k8sRedisPod.Status.PodIP)
		if err != nil {
			return err
		}

		for _, redisNode := range *clusterInfo {
			if redisNode.Leader == "-" && !redisNode.IsFailing {
				currentLeaderNumber++
			}
		}
	}

	r.Log.Info("master failover process has finished successfully")
	redisCluster.Status.ClusterState = string(RecoverLeaderNodes)

	return nil
}

func (r *RedisClusterReconciler) deployFollowerAfterFailure(ctx context.Context, redisCluster *dbv1.RedisCluster, leaderNumber int, missingPodNumbers *[]int) ([]int, error) {
	// Pop Front/Shift from slice
	sliceCopy := *missingPodNumbers
	podSequentialNumber, sliceCopy := sliceCopy[0], sliceCopy[1:]
	followerPod, err := r.followerPod(redisCluster, leaderNumber, podSequentialNumber)
	applyOpts := []client.CreateOption{client.FieldOwner("redis-operator-controller")}

	if err != nil {
		return nil, err
	}

	r.Log.Info(fmt.Sprintf("deploying follower for leader %d", leaderNumber))
	err = r.Create(ctx, &followerPod, applyOpts...)
	if err != nil {
		return nil, err
	}

	return sliceCopy, nil
}

func getLeaderNumberByLeaderID(redisGroups *RedisGroups, leaderID string, log logr.Logger) (int, error) {
	var leaderNumber int = -1
	var err error

	log.Info(fmt.Sprintf("searching for leader number for leader id [%s]", leaderID))

	for _, redisGroup := range *redisGroups {
		if redisGroup.LeaderID == leaderID {
			leaderNumber, err = strconv.Atoi(redisGroup.LeaderNumber)
			if err != nil {
				return -1, err
			}

			break
		}
	}

	log.Info(fmt.Sprintf("found leader number %d for leader id [%s", leaderNumber, leaderID))

	return leaderNumber, nil
}

// getMissingPodsNumber gets a list of k8s redis pods, the expected pod number and return the missing suffixes
// for example the pod list is [redis-node-0 redis-node-1 redis-nod-2 redis-node 4] and the expected is 5
// then it will return a slice with length of 1 with the value 3: [3]
func getMissingPodsNumber(redisPods *corev1.PodList, totalExpectedPods int32, logger logr.Logger) ([]int, error) {
	logger.Info(fmt.Sprintf("compute missing k8s pods sequential number. total known pod count: %d", len(redisPods.Items)))

	missingPodNumbers := make([]int, 0)
	podNumbersArray := make([]int, totalExpectedPods)
	for _, redisPod := range redisPods.Items {
		podNumber, err := strconv.Atoi(strings.Split(redisPod.Name, "-")[2]) // redis pod name format is 'redis-node-<number>'
		if err != nil {
			return nil, err
		}

		podNumbersArray[podNumber] = 1
	}

	for podIndex, value := range podNumbersArray {
		if value == 0 {
			logger.Info(fmt.Sprintf("missing pod number found: %d", podIndex))
			missingPodNumbers = append(missingPodNumbers, podIndex)
		}
	}

	logger.Info(fmt.Sprintf("missing pods sequential numbers are: %v", missingPodNumbers))

	return missingPodNumbers, nil
}

// wait pauses the current goroutine for at least the duration milliseconds.
func wait(logger logr.Logger, milliseconds int32) {
	logger.Info(fmt.Sprintf("sleeping for %d milliseconds", milliseconds))
	time.Sleep(time.Duration(milliseconds) * time.Millisecond)
}
