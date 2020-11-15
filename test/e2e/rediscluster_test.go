// +build e2e_redis_op

package e2e

import (
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	// "k8s.io/apimachinery/pkg/version"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/PayU/Redis-Operator/test/framework"
)

func testCreateDeleteRedisCluster(t *testing.T) {

	fmt.Printf("Running test %s\n", t.Name())

	ctx := framework.NewTestCtx(t, t.Name())
	defer ctx.Cleanup()

	// create namespace, roles, rolebindings and CRD
	err := tfw.InitializeDefaultResources(&ctx, defaultConfig.KustomizePath,
		defaultConfig.OperatorImage,
		defaultConfig.RedisClusterSetupTimeout)
	Check(err, t.Fatalf, "Failed to initialize resources")

	fmt.Println("[E2E] Finished creating default resources")

	redisCluster, err := tfw.MakeRedisCluster(defaultConfig.RedisClusterYAMLPath)
	Check(err, t.Fatalf, "Failed to make Redis cluster resource")

	err = tfw.CreateRedisClusterAndWaitUntilReady(&ctx, redisCluster, defaultConfig.RedisClusterSetupTimeout)
	Check(err, t.Fatalf, "Failed to create Redis cluster")

	fmt.Println("[E2E] Finished creating redis cluster")

	ctx.PopFinalizer() // removing the Redis finalizer from the context since it is deleted bellow
	err = tfw.DeleteRedisCluster(redisCluster, defaultConfig.RedisClusterSetupTimeout)
	Check(err, t.Fatalf, "Failed to delete Redis cluster")

	fmt.Println("[E2E] Finished deleting redis cluster")
}

func TestRedisClusterAvailabilityZoneDistribution(t *testing.T) {

	fmt.Printf("---\n[E2E] Running test: %s\n", t.Name())

	ctx := framework.NewTestCtx(t, t.Name())
	defer ctx.Cleanup()

	// the availability zone label differs between K8s 1.17+ and older
	// https://kubernetes.io/docs/reference/kubernetes-api/labels-annotations-taints/#failure-domainbetakubernetesiozone
	var zoneLabel string

	if tfw.K8sServerVersion.String() < "v1.17" {
		zoneLabel = "failure-domain.beta.kubernetes.io/zone"
	} else {
		zoneLabel = "topology.kubernetes.io/zone"
	}

	err := tfw.InitializeDefaultResources(&ctx, defaultConfig.KustomizePath,
		defaultConfig.OperatorImage, defaultConfig.RedisClusterSetupTimeout)
	Check(err, t.Fatalf, "Failed to initialize resources")

	ns := tfw.KustomizeConfig.Namespace

	redisCluster, err := tfw.MakeRedisCluster(defaultConfig.RedisClusterYAMLPath)
	Check(err, t.Fatalf, "Failed to make Redis cluster resource")

	fmt.Println("[E2E] Creating redis cluster...")

	err = tfw.CreateRedisClusterAndWaitUntilReady(&ctx, redisCluster, defaultConfig.RedisClusterSetupTimeout)
	Check(err, t.Fatalf, "Failed to create Redis cluster")

	fmt.Println("[E2E] Finished creating redis cluster")

	nodes, err := tfw.GetNodes()
	Check(err, t.Fatalf, "Failed to get Kubernetes nodes")

	// AZ map with structure Map[AZ][leaderId] -> pod list
	azMap := make(map[string]map[string]([]*corev1.Pod))

	for _, node := range nodes.Items {
		opts := []client.ListOption{
			client.InNamespace(ns.Name),
			client.MatchingFields{"spec.nodeName": node.Name},
		}

		leaders, err := tfw.GetRedisPods("leader", opts...)
		Check(err, t.Fatalf, "Failed to get leader nodes")

		followers, err := tfw.GetRedisPods("follower", opts...)
		Check(err, t.Fatalf, "Failed to get follower nodes")

		if len(leaders.Items) == 0 && len(followers.Items) == 0 {
			continue
		}

		if len(leaders.Items)+len(followers.Items) > 1 {
			t.Error("Pod distribution error - found more then one Redis pod on the same node")
		}

		az, foundAZ := azMap[node.Labels[zoneLabel]]
		if !foundAZ {
			az = make(map[string]([]*corev1.Pod))
		}

		for _, pod := range leaders.Items {
			group, found := az[pod.Labels["leader-number"]]
			if !found {
				group = []*corev1.Pod{&pod}
			} else {
				group = append(group, &pod)
			}
			az[pod.Labels["leader-number"]] = group
		}

		for _, pod := range followers.Items {
			group, found := az[pod.Labels["leader-number"]]
			if !found {
				group = []*corev1.Pod{&pod}
			} else {
				group = append(group, &pod)
			}
			az[pod.Labels["leader-number"]] = group
		}

		azMap[node.Labels[zoneLabel]] = az
	}

	// check if any AZ has two leaders or two nodes from the same Redis group
	for az, groups := range azMap {
		leaderCount := 0
		for leaderID, pods := range groups {
			for _, pod := range pods {
				if pod.Labels["redis-node-role"] == "leader" {
					leaderCount++
				}
			}
			if len(pods) > 1 {
				t.Errorf("The group with leader ID %s has %d nodes in %s\n", leaderID, len(pods), az)
			}
		}
		if leaderCount > 1 {
			t.Errorf("Found more then one leader in %s\n", az)
		}
	}
	printAZMap(azMap)
}

func printAZMap(azMap map[string]map[string][]*corev1.Pod) {
	fmt.Println("*** Redis cluster AZ map")
	for az, groups := range azMap {
		fmt.Printf("%s\n", az)
		for leaderID, pods := range groups {
			fmt.Printf("Group [%s]:", leaderID)
			for _, pod := range pods {
				fmt.Printf(" %s", pod.Name)
			}
			fmt.Println()
		}
		fmt.Println()
	}
}
