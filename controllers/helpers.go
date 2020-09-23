package controllers

import (
	"context"
	"fmt"
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

	// Deploying means the cluster is during he's first startup
	Deploying RedisClusterState = "Deploying"

	// Initializing all cluster resources (pods, service, etc..)
	// are deployed and ready to group together as cluster
	Initializing RedisClusterState = "Initializing"

	// MasterCohesive is the state were all master nodes of the redis cluster
	// are grouped together but not all of the follower nodes joined yet.
	MasterCohesive RedisClusterState = "MasterCohesive"

	// Ready means cluster is up & running as expected
	Ready RedisClusterState = "Ready"

	// Unknown means that we are not able to identify the current state
	Unknown RedisClusterState = "Unknown"
)

var currentRedisClusterState RedisClusterState

func computeCurrentClusterState(logger logr.Logger, redisOperator *dbv1.RedisOperator) RedisClusterState {
	clusterState := Unknown

	if len(redisOperator.Status.ClusterState) == 0 {
		return NotExists
	}

	switch redisOperator.Status.ClusterState {
	case string(Deploying):
		clusterState = Deploying
		break
	case string(Initializing):
		clusterState = Initializing
		break
	case string(MasterCohesive):
		clusterState = MasterCohesive
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

func (r *RedisOperatorReconciler) createFollowers(ctx context.Context,
	applyOpts []client.CreateOption, redisOperator *dbv1.RedisOperator, followerCount int, leaderCount int) error {
	currentFollower := 0
	nodeID := leaderCount
	for leaderID := 0; leaderID < leaderCount; leaderID++ {
		for i := 0; i < followerCount; i++ {
			followerPod, err := r.followerPod(redisOperator, currentFollower, fmt.Sprintf("%d", nodeID), fmt.Sprintf("%d", leaderID))
			currentFollower++
			nodeID++
			if err != nil {
				return err
			}

			r.Log.Info(fmt.Sprintf("deploying follower-%s-%d", fmt.Sprintf("%d", leaderID), i))

			err = r.Create(ctx, &followerPod, applyOpts...)
			if err != nil {
				if !strings.Contains(err.Error(), "already exists") {
					return err
				}

				r.Log.Info(fmt.Sprintf("follower-%s-%d already exists", fmt.Sprintf("%d", leaderID), i))
			}
		}
	}
	return nil
}

func (r *RedisOperatorReconciler) createLeaders(ctx context.Context, applyOpts []client.CreateOption, redisOperator *dbv1.RedisOperator, nodeCount int) error {
	var leaderIDs []string
	for i := 0; i < nodeCount; i++ {
		leaderPod, err := r.leaderPod(redisOperator, i, fmt.Sprintf("%d", i))
		leaderIDs = append(leaderIDs, fmt.Sprintf("%d", i))
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
	desiredFollowers := int(redisOperator.Spec.LeaderFollowersCount)
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

	err = r.createLeaders(ctx, applyOpts, redisOperator, desiredLeaders)
	err = r.createFollowers(ctx, applyOpts, redisOperator, desiredFollowers, desiredLeaders)

	redisOperator.Status.ClusterState = string(Deploying)

	return nil
}

func (r *RedisOperatorReconciler) handleDeployingCluster(ctx context.Context, redisOperator *dbv1.RedisOperator) error {
	r.Log.Info("handling deploying cluster")

	leadersAreReady := true
	followersAreReady := true
	leaderPods, err := r.getClusterPods(ctx, redisOperator, true)
	if err != nil {
		return err
	}

	for _, leaderPod := range leaderPods.Items {
		for _, podCondition := range leaderPod.Status.Conditions {
			leadersAreReady = leadersAreReady && podCondition.Status == corev1.ConditionTrue
		}
	}

	r.Log.Info(fmt.Sprintf("leaders ready:%t", leadersAreReady))

	if leadersAreReady && followersAreReady {
		redisOperator.Status.ClusterState = string(Initializing)
	}

	return nil
}

func (r *RedisOperatorReconciler) handleInitializingCluster(ctx context.Context, redisOperator *dbv1.RedisOperator) error {
	r.Log.Info("handling initializing cluster")
	leaderPods, err := r.getClusterPods(ctx, redisOperator, true)
	if err != nil {
		return err
	}

	leaderPodIPAddresses := make([]string, 0)
	for _, leaderPod := range leaderPods.Items {
		leaderPodIPAddresses = append(leaderPodIPAddresses, fmt.Sprintf("%s:6379", leaderPod.Status.PodIP))
	}

	err = r.redisCliClusterCreate(leaderPodIPAddresses)
	if err != nil {
		return err
	}

	redisOperator.Status.ClusterState = string(MasterCohesive)

	return nil
}

func (r *RedisOperatorReconciler) handleMasterCohesiveCluster(ctx context.Context, redisOperator *dbv1.RedisOperator) error {
	r.Log.Info("handling master cohesive cluster")
	return nil
}
