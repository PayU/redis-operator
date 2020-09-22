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

	// Initializing means the cluster is during he's first startup
	Initializing RedisClusterState = "Initializing"

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
	case string(Initializing):
		clusterState = Initializing
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

	// deploy all cluster leaders
	for i := 0; i < desiredLeaders; i++ {
		leaderPod, err := r.leaderPod(redisOperator, i)
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

	redisOperator.Status.ClusterState = string(Initializing)

	return nil
}

func (r *RedisOperatorReconciler) handleInitializingCluster() error {
	r.Log.Info("handling initializing cluster")

	return nil
}
