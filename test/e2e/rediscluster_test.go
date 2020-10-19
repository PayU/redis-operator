// +build e2e_redis_op

package e2e

import (
	"fmt"
	"testing"

	"github.com/PayU/Redis-Operator/test/framework"
)

func TestRedisCreateDeleteCluster(t *testing.T) {

	fmt.Printf("Running test %s\n", t.Name())
	fmt.Println("Server version: ", tfw.K8sServerVersion)

	ctx := framework.NewTestCtx(t, t.Name())
	defer ctx.Cleanup(t)

	// create namespace, roles, rolebindings and CRD
	if err := tfw.InitializeDefaultResources(&ctx, defaultConfig.KustomizePath,
		defaultConfig.OperatorImage, defaultConfig.TestTimeout); err != nil {
		t.Fatalf("Failed to initialize resources %v", err)
	}

	redisCluster, err := tfw.CreateRedisCluster(&ctx, defaultConfig.TestTimeout)
	if err != nil {
		t.Fatalf("Failed to create Redis cluster resource %v", err)
	}

	ctx.PopFinalizer() // removing the Redis finalizer from the context since it is deleted bellow
	if err = tfw.DeleteRedisCluster(redisCluster); err != nil {
		t.Fatalf("Failed to delete Redis cluster %v", err)
	}
	// TODO wait until all resources are out of 'terminating'
}

func TestRedisClusterAvailabilityZoneDistribution(t *testing.T) {}
