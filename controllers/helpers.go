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

// CurrentClusterState describes the current
// reconcile state of the redis cluster
type CurrentClusterState string

const (
	// NotExists means there is no redis pods in the k8s cluster
	NotExists CurrentClusterState = "NotExists"

	// Unknown means that we are not able to identify the current state
	Unknown CurrentClusterState = "Unknown"
)

func computeCurrentClusterState(logger logr.Logger, leaderPods *corev1.PodList, followerPods *corev1.PodList) CurrentClusterState {
	clusterState := Unknown
	logger.Info(fmt.Sprintf("Current number of leader nodes:%d", len(leaderPods.Items)))

	if len(leaderPods.Items) == 0 {
		clusterState = NotExists
	}

	logger.Info(fmt.Sprintf("Current cluster state is:%s", clusterState))
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
	desiredLeaders := int(redisOperator.Spec.LeaderReplicas)

	// create config map
	configMap, err := r.createSettingsConfigMap(redisOperator)
	err = r.Create(ctx, &configMap)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return err
	}

	// create service
	service, err := r.serviceResource(redisOperator)
	err = r.Create(ctx, &service)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return err
	}

	// create headless service
	headlessService, err := r.headlessServiceResource(redisOperator)
	err = r.Create(ctx, &headlessService)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return err
	}

	// deploy all cluster leaders
	for i := 0; i < desiredLeaders; i++ {
		leaderPod, err := r.leaderPod(redisOperator, i)

		if err != nil {
			return err
		}

		r.Log.Info(fmt.Sprintf("deploying leader-%d", i))

		err = r.Create(ctx, &leaderPod)
		if err != nil {
			return err
		}
	}

	// deploy all cluster leaders followers

	return nil
}
