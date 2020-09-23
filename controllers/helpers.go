package controllers

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	dbv1 "github.com/PayU/Redis-Operator/api/v1"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// RedisClusterState describes the current
// reconcile state of the redis cluster
type RedisClusterState string

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

func getCurrentClusterState(logger logr.Logger, redisOperator *dbv1.RedisOperator) RedisClusterState {
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

// Return Redis groups as an array
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

func (r *RedisOperatorReconciler) getRedisClusterPods(ctx context.Context, redisOperator *dbv1.RedisOperator, podType string) (*corev1.PodList, error) {
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

func (r *RedisOperatorReconciler) createFollowersForLeader(ctx context.Context, applyOpts []client.CreateOption, redisOperator *dbv1.RedisOperator, leaderNumber int) error {
	followersCount := int(redisOperator.Spec.LeaderFollowersCount)

	for i := 0; i < followersCount; i++ {
		followerPod, err := r.followerPod(redisOperator, i, leaderNumber)
		if err != nil {
			return err
		}
		r.Log.Info(fmt.Sprintf("deploying follower-%d-%d", leaderNumber, i))

		err = r.Create(ctx, &followerPod, applyOpts...)
		if err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				return err
			}
			r.Log.Info(fmt.Sprintf("follower-%d-%d already exists", leaderNumber, i))
		}
	}

	return nil
}

func (r *RedisOperatorReconciler) createLeaders(ctx context.Context, applyOpts []client.CreateOption, redisOperator *dbv1.RedisOperator, nodeCount int) error {
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

func (r *RedisOperatorReconciler) createNewCluster(ctx context.Context, redisOperator *dbv1.RedisOperator) error {
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

func (r *RedisOperatorReconciler) checkRedisNodes(ctx context.Context, redisOperator *dbv1.RedisOperator, nodeRole string) (bool, error) {
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

func (r *RedisOperatorReconciler) handleDeployingLeaders(ctx context.Context, redisOperator *dbv1.RedisOperator) error {
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

func (r *RedisOperatorReconciler) handleDeployingFollowers(ctx context.Context, redisOperator *dbv1.RedisOperator) error {
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

func (r *RedisOperatorReconciler) handleInitializingLeaders(ctx context.Context, redisOperator *dbv1.RedisOperator) error {
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

func (r *RedisOperatorReconciler) handleInitializingFollowers(ctx context.Context, redisOperator *dbv1.RedisOperator) error {
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

func (r *RedisOperatorReconciler) handleClusteringLeaders(ctx context.Context, redisOperator *dbv1.RedisOperator) error {
	r.Log.Info("handling clustering leaders")
	applyOpts := []client.CreateOption{client.FieldOwner("redis-operator-controller")}
	leaderPods, err := r.getRedisClusterPods(ctx, redisOperator, "leader")
	if err != nil {
		return err
	}

	for _, leaderPod := range leaderPods.Items {
		leaderNumber, err := strconv.Atoi(leaderPod.ObjectMeta.Labels["leader-number"])
		if err != nil {
			return err
		}
		err = r.createFollowersForLeader(ctx, applyOpts, redisOperator, leaderNumber)
		if err != nil {
			return err
		}
	}

	redisOperator.Status.ClusterState = string(DeployingFollowers)

	return nil
}

func (r *RedisOperatorReconciler) handleClusteringFollowers(ctx context.Context, redisOperator *dbv1.RedisOperator) error {
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
	} else {
		r.Log.Info("follower clustering complete")
	}

	redisOperator.Status.ClusterState = string(Ready)
	return nil
}
