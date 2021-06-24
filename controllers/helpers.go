package controllers

import (
	"time"

	dbv1 "github.com/PayU/redis-operator/api/v1"
)

// RedisClusterState describes the current
// reconcile state of the redis cluster
type RedisClusterState string

const (
	syncCheckInterval     = 500 * time.Millisecond
	syncCheckTimeout      = 10 * time.Second
	loadCheckInterval     = 500 * time.Millisecond
	loadCheckTimeout      = 10 * time.Second
	genericCheckInterval  = 2 * time.Second
	genericCheckTimeout   = 50 * time.Second
	clusterCreateInterval = 5 * time.Second
	clusterCreateTimeout  = 30 * time.Second
)

const (
	// NotExists: the RedisCluster custom resource has just been created
	NotExists RedisClusterState = "NotExists"

	// InitializingCluster: ConfigMap, Service resources are created; the leader
	// pods are created and clusterized
	InitializingCluster RedisClusterState = "InitializingCluster"

	// InitializingFollowers: followers are added to the cluster
	InitializingFollowers RedisClusterState = "InitializingFollowers"

	// Ready: cluster is up & running as expected
	Ready RedisClusterState = "Ready"

	// Recovering: one ore note nodes are in fail state and are being recreated
	Recovering RedisClusterState = "Recovering"

	// Updating: the cluster is in the middle of a rolling update
	Updating RedisClusterState = "Updating"
)

func getCurrentClusterState(redisCluster *dbv1.RedisCluster) RedisClusterState {
	if len(redisCluster.Status.ClusterState) == 0 {
		return NotExists
	}
	return RedisClusterState(redisCluster.Status.ClusterState)
}

func (r *RedisClusterReconciler) handleInitializingCluster(redisCluster *dbv1.RedisCluster) error {
	r.Log.Info("Handling initializing cluster...")
	if err := r.createNewRedisCluster(redisCluster); err != nil {
		return err
	}
	redisCluster.Status.ClusterState = string(InitializingFollowers)
	return nil
}

func (r *RedisClusterReconciler) handleInitializingFollowers(redisCluster *dbv1.RedisCluster) error {
	r.Log.Info("Handling initializing followers...")
	if err := r.initializeFollowers(redisCluster); err != nil {
		return err
	}
	redisCluster.Status.ClusterState = string(Ready)
	return nil
}

func (r *RedisClusterReconciler) handleReadyState(redisCluster *dbv1.RedisCluster) error {
	complete, err := r.isClusterComplete(redisCluster)
	if err != nil {
		r.Log.Info("Could not check if cluster is complete")
		return err
	}
	if !complete {
		redisCluster.Status.ClusterState = string(Recovering)
		return nil
	}

	uptodate, err := r.isClusterUpToDate(redisCluster)
	if err != nil {
		r.Log.Info("Could not check if cluster is updated")
		redisCluster.Status.ClusterState = string(Recovering)
		return err
	}
	if !uptodate {
		redisCluster.Status.ClusterState = string(Updating)
		return nil
	}
	r.Log.Info("Cluster is healthy")
	return nil
}

func (r *RedisClusterReconciler) handleRecoveringState(redisCluster *dbv1.RedisCluster) error {
	r.Log.Info("Handling cluster recovery...")
	if err := r.recoverCluster(redisCluster); err != nil {
		r.Log.Info("Cluster recovery failed")
		return err
	}
	redisCluster.Status.ClusterState = string(Ready)
	return nil
}

func (r *RedisClusterReconciler) handleUpdatingState(redisCluster *dbv1.RedisCluster) error {
	r.Log.Info("Handling rolling update...")
	if err := r.updateCluster(redisCluster); err != nil {
		r.Log.Info("Rolling update failed")
		redisCluster.Status.ClusterState = string(Recovering)
		return err
	}
	redisCluster.Status.ClusterState = string(Ready)
	return nil
}
