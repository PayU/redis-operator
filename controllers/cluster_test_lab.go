package controllers

import (
	"testing"

	dbv1 "github.com/PayU/redis-operator/api/v1"
	view "github.com/PayU/redis-operator/controllers/view"
)

func cluster_test_e2e(t *testing.T) {
	waitForHealthyCluster(reconciler, cluster)
}

func waitForHealthyCluster(r *RedisClusterReconciler, c *dbv1.RedisCluster) bool {
	if r == nil || c == nil {
		return false
	}
	return c.Status.ClusterState == string(Ready) && r.RedisClusterStateView.ClusterState == view.ClusterOK

}
