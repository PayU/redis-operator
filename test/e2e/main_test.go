// +build e2e_redis_op

package e2e

import (
	"flag"
	"log"
	"os"
	"testing"

	"github.com/PayU/Redis-Operator/test/framework"
)

var (
	tfw     *framework.Framework
	opImage *string
)

func TestMain(m *testing.M) {
	kubeconfig := flag.String(
		"kubeconf",
		"../../hack/kubeconfig.yaml",
		"kube config path, e.g. $HOME/.kube/config",
	)
	opImage = flag.String(
		"operator-image",
		"redis-operator-docker:local",
		"operator image, e.g. redis:latest",
	)
	flag.Parse()

	if *kubeconfig == "" {
		*kubeconfig = "../../hack/kubeconfig.yaml"
		log.Println("No kubeconfig file provided, using hack/kubeconfig.yaml")
	}

	if *opImage == "" {
		*opImage = "redis-operator-docker:local"
		log.Printf("No docker image provided, using %s\n", *opImage)
	}

	var err error

	if tfw, err = framework.NewFramework(*kubeconfig, *opImage); err != nil {
		log.Printf("failed to setup framework: %v\n", err)
		os.Exit(1)
	}

	exitCode := m.Run()

	os.Exit(exitCode)
}
