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

	// Unknown means that we are not able to identify the current state
	Unknown RedisClusterState = "Unknown"
)

var currentRedisClusterState RedisClusterState

func getCurrentClusterState(logger logr.Logger, redisOperator *dbv1.RedisCluster) RedisClusterState {
	clusterState := Unknown

	if len(redisOperator.Status.ClusterState) == 0 {
		return NotExists
	}

	switch redisOperator.Status.ClusterState {
	case string(DeployingLeaders):
		clusterState = DeployingLeaders
		break
	case string(InitializingLeaders):
		clusterState = InitializingLeaders
		break
	case string(ClusteringLeaders):
		clusterState = ClusteringLeaders
		break
	case string(DeployingFollowers):
		clusterState = DeployingFollowers
		break
	case string(InitializingFollowers):
		clusterState = InitializingFollowers
		break
	case string(ClusteringFollowers):
		clusterState = ClusteringFollowers
		break
	case string(Ready):
		clusterState = Ready
		break
	}

	return clusterState
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
func NewRedisGroups(redisPods *corev1.PodList) *RedisGroups {
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
		if redisPod.ObjectMeta.Labels["redis-node-role"] == "leader" {
			group.LeaderPod = redisPod
		} else {
			group.FollowerPods = append(group.FollowerPods, redisPod)
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
func (r *RedisClusterReconciler) createFollowersForLeader(ctx context.Context, applyOpts []client.CreateOption, redisOperator *dbv1.RedisCluster, leaderNumber int, startingSequentialNumber int) (int, error) {
	followersCount := int(redisOperator.Spec.LeaderFollowersCount)

	for i := 0; i < followersCount; i++ {
		followerPod, err := r.followerPod(redisOperator, leaderNumber, startingSequentialNumber+i)
		if err != nil {
			return 0, err
		}

		r.Log.Info(fmt.Sprintf("deploying follower-%d-%d", leaderNumber, i))

		err = r.Create(ctx, &followerPod, applyOpts...)
		if err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				return 0, err
			}
			r.Log.Info(fmt.Sprintf("follower-%d-%d already exists", leaderNumber, i))
		}
	}

	return followersCount, nil
}

func (r *RedisClusterReconciler) createLeaders(ctx context.Context, applyOpts []client.CreateOption, redisOperator *dbv1.RedisCluster, nodeCount int) error {
	for i := 0; i < nodeCount; i++ {
		leaderPod, err := r.leaderPod(redisOperator, i, i)
		if err != nil {
			return err
		}

		r.Log.Info(fmt.Sprintf("deploying leader-%d", i))

		err = r.Create(ctx, &leaderPod, applyOpts...)
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
	applyOpts := []client.CreateOption{client.FieldOwner("redis-operator-controller")}

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
	err = r.createLeaders(ctx, applyOpts, redisOperator, desiredLeaders)
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
			return fmt.Errorf("Pods not ready - waiting for IP")
		}
	}

	redisGroups := NewRedisGroups(redisPods).Flatten()

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
	applyOpts := []client.CreateOption{client.FieldOwner("redis-operator-controller")}
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

		createdFollowers, err := r.createFollowersForLeader(ctx, applyOpts, redisOperator, leaderNumber, startingfollowerSequentialNumber)
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

	redisGroups := NewRedisGroups(redisPods).Flatten()
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

/*
* when handleClusterReadyState is called, it means that the Reconcile function was triggered after the redis cluster
* was already function as expected and was working properly. this trigger can occur from various reasons like losing
* one (or many) of the redis cluster pods, a new deployment was initiated and so on..
* at first, we will identify the exact change of the state and only after that we will response to it.
* in addition, this function will trigger when we just finished to deploy the cluster for the very first time.
 */
func (r *RedisClusterReconciler) handleClusterReadyState(ctx context.Context, redisCluster *dbv1.RedisCluster) error {
	r.Log.Info("reconcile function was triggered while the last known cluster state was ready. checking cluster status..")
	applyOpts := []client.CreateOption{client.FieldOwner("redis-operator-controller")}
	var actionApply bool = false
	var err error = nil

	// first step is checking for failing nodes. if we found any we will
	// clean them by using CLUSTER FORGET redis command
	actionApply, err = r.forgetFailingNodesIfExists(ctx, redisCluster, applyOpts)
	if err != nil {
		return err
	}

	if !actionApply {
		r.Log.Info("redis cluster is fully operational")
	}

	return err
}

func (r *RedisClusterReconciler) forgetFailingNodesIfExists(ctx context.Context, redisCluster *dbv1.RedisCluster, applyOpts []client.CreateOption) (bool, error) {
	result := false
	failingNodes := make([]rediscli.RedisClusterNode, 0)

	// waiting for [1.2 * cluster-node-timeout] amount in case 1 or more nodes
	// are unreachable so the cluster will considered them in failure state.
	wait(r.Log, 5000*1.2)

	r.Log.Info("looking for failing cluster nodes..")

	redisPods, err := r.getRedisClusterPods(ctx, redisCluster, "any")
	if err != nil {
		return result, err
	}

	redisGroups := NewRedisGroups(redisPods)
	flattenRedisGroups := redisGroups.Flatten()

	clusterNodesList, err := r.RedisCLI.GetClusterNodesInfo(flattenRedisGroups[0].LeaderPod.Status.PodIP)
	if err != nil {
		return result, err
	}

	// search for failing nodes in the cluster
	for _, clusterNode := range *clusterNodesList {
		if clusterNode.IsFailing {
			failingNodes = append(failingNodes, clusterNode)
		}
	}

	if len(failingNodes) > 0 {
		r.Log.Info(fmt.Sprintf("found %d failing redis nodes. starting cluster forget process..", len(failingNodes)))

		// iterate over all cluster nodes, for every healthy node do
		// CLUSTER FORGET command on all other failing nodes
		for _, clusterNode := range *clusterNodesList {
			if !clusterNode.IsFailing {
				if isLeader {
					clusterNode.Addr
				}
				for _, failingNode := range failingNodes {
					nodeIP := strings.Split(clusterNode.Addr, ":")[0]
					r.RedisCLI.ForgetNode(nodeIP, failingNode.ID)
				}
			}
		}

		// test
		missingPodNumbers := make([]int, 0)
		podNumbersArray := make([]int, redisCluster.Status.TotalExpectedPods)
		for _, redisPod := range redisPods.Items {
			redisPod.Status.PodIP
			podNumber, err := strconv.Atoi(strings.Split(redisPod.Name, "-")[2]) // redis pod name format is 'redis-node-<number>'
			if err != nil {
				return result, err
			}

			podNumbersArray[podNumber] = 1
		}

		for podIndex, value := range podNumbersArray {
			if value == 0 {
				r.Log.Info(fmt.Sprintf("missing pod number found: %d", podIndex))
				missingPodNumbers = append(missingPodNumbers, podIndex)
			}
		}

		err = r.replaceK8sFailedNodes(ctx, redisCluster, applyOpts, failingNodes, redisGroups, missingPodNumbers)
		if err != nil {
			return result, err
		}

		result = true
	} else {
		r.Log.Info("no failing nodes has found in the cluster")
	}

	return result, nil
}

// after we forgot the failing nodes, we need to deploy new pod(s) to replace the failing once.
// there are 2 cases here - a failing leader or a failing follower.
// in both cases all the new nodes should be a slave nodes since in a leader failing scenario
// a "master failover" will occur and one of he's followers will be promoted to a leader (and needs a new follower to replace him).
// in a follower failing scenario we are just replacing the failing follower with a new one.
func (r *RedisClusterReconciler) replaceK8sFailedNodes(ctx context.Context, redisCluster *dbv1.RedisCluster, applyOpts []client.CreateOption,
	failingNodes []rediscli.RedisClusterNode, redisGroups *RedisGroups, missingPodNumbers []int) error {
	var err error = nil
	var podSequentialNumber int

	for _, failingNode := range failingNodes {

		if failingNode.Leader != "0" { // if true, then the failing node was a follower
			var leaderNumber int = -1

			// test
			for _, redisGroup := range *redisGroups {
				r.Log.Info(fmt.Sprintf("for testing. group leader-id: [%s]. fail node leader id: [%s]", redisGroup.LeaderID, failingNode.Leader))
				if redisGroup.LeaderID == failingNode.Leader {
					leaderNumber, err = strconv.Atoi(redisGroup.LeaderNumber)
					if err != nil {
						return err
					}

					break
				}
			}

			// Pop Front/Shift from slice
			podSequentialNumber, missingPodNumbers = missingPodNumbers[0], missingPodNumbers[1:]
			followerPod, err := r.followerPod(redisCluster, leaderNumber, podSequentialNumber)
			if err != nil {
				return err
			}

			r.Log.Info(fmt.Sprintf("deploying follower for leader %d", leaderNumber))
			err = r.Create(ctx, &followerPod, applyOpts...)
			if err != nil {
				if !strings.Contains(err.Error(), "already exists") {
					return err
				}

				r.Log.Info(fmt.Sprintf("follower for leader %d already exists", leaderNumber))
			}

		} else {
			// the failing node was a leader
		}
	}

	return nil
}

// wait pauses the current goroutine for at least the duration milliseconds.
func wait(logger logr.Logger, milliseconds int32) {
	logger.Info(fmt.Sprintf("sleeping for %d milliseconds", milliseconds))
	time.Sleep(time.Duration(milliseconds) * time.Millisecond)
}
