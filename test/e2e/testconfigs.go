// +build e2e_redis_op

package e2e

import "time"

type TestConfig struct {
	KustomizePath               string
	OperatorImage               string
	RedisClusterYAMLPath        string
	RedisClusterUpdatedYAMLPath string
	UpdateImage                 string
	Namespace                   string
	K8sResourceSetupTimeout     time.Duration // used for general resources such as roles and deployments
	RedisClusterSetupTimeout    time.Duration // used for the Redis cluster creations
}

var (
	defaultConfig = TestConfig{
		KustomizePath:               "../../config/default",
		OperatorImage:               "redis-operator-docker:local",
		RedisClusterYAMLPath:        "../../config/samples/local_cluster.yaml",
		RedisClusterUpdatedYAMLPath: "../../config/samples/updated_cluster.yaml",
		UpdateImage:                 "redis:update",
		Namespace:                   "default",
		K8sResourceSetupTimeout:     20 * time.Second,
		RedisClusterSetupTimeout:    180 * time.Second,
	}
)
