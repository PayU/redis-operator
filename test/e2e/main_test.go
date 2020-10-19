package e2e

import (
	"log"
	"os"
	"testing"

	operatorFramework "github.com/PayU/Redis-Operator/test/framework"
)

func TestMain(m *testing.M) {
	framework, err := operatorFramework.New()
	if err != nil {
		log.Printf("E2e test framework could not be initalized: %v\n", err)
		os.Exit(1)
	}
	_ = framework.CreateRedisOperator()
}
