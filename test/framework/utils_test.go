// +build e2e_redis_op

package framework

import (
	"fmt"
	"testing"
)

// TestKustomizeGeneration checks if the kustomize configuration can be generated,
// and parsed,
func TestKustomizeBuild(t *testing.T) {
	var sout string
	var err error

	fmt.Printf("Running the %s test\n", t.Name())
	table := []struct {
		configPath string
		imageName  string
	}{
		{"../../config/default", "redis-operator-docker:local"},
		{"../../config/development", "redis-operator-docker:local"},
	}
	f := Framework{}
	for _, tData := range table {
		fmt.Printf("[%s] Checking %v\n", t.Name(), tData)
		sout, err = f.BuildKustomizeConfig(tData.configPath, tData.imageName)
		if err != nil {
			t.Fatalf("Could not generate kustomize configuration: %v\n", err)
		}
		_, _, err := f.ParseKustomizeConfig(sout) // TODO the result should also be validated
		if err != nil {
			t.Fatalf("Could not parse kustomize YAML configuration: %v\n", err)
		}
	}
}

func TestKubectl(t *testing.T) {
	table := []struct {
		configPath string
		imageName  string
	}{
		{"../../config/default", "redis-operator-docker:local"},
		{"../../config/development", "redis-operator-docker:local"},
	}
	f := Framework{}

	for _, tData := range table {
		fmt.Printf("[%s] Checking %v\n", t.Name(), tData)
		_, yamlMap, err := f.BuildAndParseKustomizeConfig(tData.configPath, tData.imageName)
		if err != nil {
			t.Fatalf("Could not get kustomize config %v", err)
		}
		for _, res := range []string{yamlMap["crd"]} {
			if _, _, err = f.kubectlApply(res, true); err != nil {
				t.Errorf("kubectl apply returned error %v", err)
			}
		}
	}
}
