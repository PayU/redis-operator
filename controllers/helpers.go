package controllers

import (
	"context"
	"errors"
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

func (r *RedisOperatorReconciler) getClusterPods(ctx context.Context, redisOperator *dbv1.RedisOperator, getLeaderPods bool) (*corev1.PodList, error) {
	pods := &corev1.PodList{}
	matchingLabels := make(map[string]string)
	matchingLabels["app"] = redisOperator.Spec.PodLabelSelector.App
	matchingLabels["redis-node-role"] = "follower"

	if getLeaderPods {
		matchingLabels["redis-node-role"] = "leader"
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
	pods, err := r.getClusterPods(ctx, redisOperator, nodeRole == "leader")
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

func getPodIPs(pods *corev1.PodList) ([]string, error) {
	podIPAddr := make([]string, 0)
	for _, pod := range pods.Items {
		if pod.Status.PodIP != "" {
			podIPAddr = append(podIPAddr, fmt.Sprintf("%s:6379", pod.Status.PodIP))
		} else {
			return nil, errors.New("Leader pod network is not ready")
		}
	}
	return podIPAddr, nil
}

func (r *RedisOperatorReconciler) handleInitializingLeaders(ctx context.Context, redisOperator *dbv1.RedisOperator) error {
	r.Log.Info("handling initializing leaders")
	leaderPods, err := r.getClusterPods(ctx, redisOperator, true)
	if err != nil {
		return err
	}

	leaderPodIPAddresses, err := getPodIPs(leaderPods)
	if err != nil {
		return err
	}

	if err = r.redisCliClusterCreate(leaderPodIPAddresses); err != nil {
		return err
	}

	redisOperator.Status.ClusterState = string(ClusteringLeaders)

	return nil
}

func (r *RedisOperatorReconciler) handleInitializingFollowers(ctx context.Context, redisOperator *dbv1.RedisOperator) error {
	r.Log.Info("handling initializing leaders")
	followerPods, err := r.getClusterPods(ctx, redisOperator, false)
	if err != nil {
		return err
	}

	_, err = getPodIPs(followerPods)
	if err != nil {
		return err
	}

	redisOperator.Status.ClusterState = string(ClusteringFollowers)
	return nil
}

func (r *RedisOperatorReconciler) handleClusteringLeaders(ctx context.Context, redisOperator *dbv1.RedisOperator) error {
	r.Log.Info("handling clustering leaders")
	applyOpts := []client.CreateOption{client.FieldOwner("redis-operator-controller")}
	leaderPods, err := r.getClusterPods(ctx, redisOperator, true)
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
	redisOperator.Status.ClusterState = string(Ready)
	return nil
}
