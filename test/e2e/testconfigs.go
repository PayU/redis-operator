// +build e2e_redis_op

package e2e

import "time"

type TestConfig struct {
	KustomizePath            string
	OperatorImage            string
	RedisClusterYAMLPath     string
	UpdateImage              string
	ContainerResourcePatch   []byte
	K8sResourceSetupTimeout  time.Duration // used for general resources such as roles and deployments
	RedisClusterSetupTimeout time.Duration // used for the Redis cluster creations
}

var (
	defaultConfig = TestConfig{
		KustomizePath:            "../../config/default",
		OperatorImage:            "redis-operator-docker:local",
		RedisClusterYAMLPath:     "../../config/samples/local_cluster.yaml",
		UpdateImage:              "redis:update",
		ContainerResourcePatch:   []byte(`{"spec":{"redisContainerResources":{"limits":{"cpu":"500m","memory":"500Mi"},"requests":{"cpu":"55m","memory":"55Mi"}}}}`),
		K8sResourceSetupTimeout:  20 * time.Second,
		RedisClusterSetupTimeout: 120 * time.Second,
	}
)
