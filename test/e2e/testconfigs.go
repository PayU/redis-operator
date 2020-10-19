// +build e2e_redis_op

package e2e

import "time"

type TestConfig struct {
	KustomizePath string
	OperatorImage string
	TestTimeout   time.Duration
}

var (
	defaultConfig = TestConfig{
		KustomizePath: "../../config/default",
		OperatorImage: "redis-operator-docker:local",
		TestTimeout:   20 * time.Second,
	}
)
