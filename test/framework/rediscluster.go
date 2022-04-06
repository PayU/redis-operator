//go:build e2e_redis_op
// +build e2e_redis_op

package framework

import (
	"context"
	"fmt"
	"io/ioutil"
	"strconv"
	"sync"
	"time"

	dbv1 "github.com/PayU/redis-operator/api/v1"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

// GetRedisPods gets the Redis pods of a given type. The type of the pod requested can be
// one of "follower", "leader" or "any".
func (f *Framework) GetRedisPods(podType string, opts ...client.ListOption) (*corev1.PodList, error) {
	matchingLabels := client.MatchingLabels{
		"app": "redis-cluster-pod",
	}
	if podType != "any" {
		if podType != "leader" && podType != "follower" {
			fmt.Printf("[E2E][WARN] Using custom Redis role: %s\n", podType)
		}
		matchingLabels["redis-node-role"] = podType
	}
	opts = append(opts, matchingLabels)
	return f.GetPods(opts...)
}

// MakeRedisCluster returns the object for a RedisCluster
func (f *Framework) MakeRedisCluster(filePath string) (*dbv1.RedisCluster, error) {
	redisCluster := &dbv1.RedisCluster{}
	yamlRes, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, errors.Wrap(err, "Could not read the Redis cluster YAML resource")
	}
	if err = yaml.Unmarshal(yamlRes, &redisCluster); err != nil {
		return nil, errors.Wrap(err, "Could not unmarshal the Redis cluster YAML resource")
	}
	return redisCluster, nil
}

// CreateRedisCluster creates the Redis cluster inside a K8s cluster
func (f *Framework) CreateRedisCluster(ctx *TestCtx, redisCluster *dbv1.RedisCluster, timeout time.Duration) error {
	if err := f.CreateResources(ctx, timeout, redisCluster); err != nil {
		return errors.Wrap(err, "Could not create the Redis cluster resource")
	}
	return nil
}

func (f *Framework) CreateRedisClusterAndWaitUntilReady(ctx *TestCtx, redisCluster *dbv1.RedisCluster, timeout time.Duration) error {
	if err := f.CreateRedisCluster(ctx, redisCluster, timeout); err != nil {
		return err
	}

	if timeout == 0 {
		return nil
	}

	if err := f.WaitForState(redisCluster, "Ready", timeout); err != nil {
		return errors.Wrap(err, "Creation of Redis cluster timed out")
	}

	return nil
}

func (f *Framework) DeleteRedisCluster(obj runtime.Object, timeout time.Duration) error {
	return f.DeleteResource(obj, timeout)
}

func (f *Framework) WaitForState(redisCluster *dbv1.RedisCluster, state string, timeout ...time.Duration) error {
	t := 10 * time.Second
	if len(timeout) > 0 {
		t = timeout[0]
	}
	return wait.PollImmediate(2*time.Second, 5*t, func() (bool, error) {
		key, err := client.ObjectKeyFromObject(redisCluster)
		if err != nil {
			return false, err
		}
		if err = f.RuntimeClient.Get(context.Background(), key, redisCluster); err != nil {
			return false, err
		}
		if redisCluster.Status.ClusterState == state {
			return true, nil
		}
		return false, nil
	})
}

func (f *Framework) UpdateRedisImage(redisCluster *dbv1.RedisCluster, image string) error {
	currentRdc := dbv1.RedisCluster{}
	key, err := client.ObjectKeyFromObject(redisCluster)
	if err != nil {
		return err
	}
	if err = f.RuntimeClient.Get(context.Background(), key, &currentRdc); err != nil {
		return err
	}
	for i, container := range currentRdc.Spec.RedisPodSpec.Containers {
		if container.Name == "redis-container" {
			currentRdc.Spec.RedisPodSpec.Containers[i].Image = image
			break
		}
	}
	return f.RuntimeClient.Update(context.Background(), &currentRdc)
}

func (f *Framework) PopulateDatabase(keyCount int, keyName string, keySize int) error {
	var wg sync.WaitGroup

	leaderPods, err := f.GetRedisPods("any")
	if err != nil {
		return err
	}

	errs := make(chan error, len(leaderPods.Items))

	for i, leaderPod := range leaderPods.Items {
		if leaderPod.Status.PodIP != "" {
			wg.Add(1)
			go func(keyCount string, keyName string, keySize string, pod corev1.Pod, wg *sync.WaitGroup) {
				defer wg.Done()
				stdout, stderr, err := f.kubectlContainerShell(pod, "redis-container", "redis-cli", "debug", "populate", keyCount, keyName, keySize)
				if err != nil {
					fmt.Printf("Failed to run container shell: %s | %s\n", stdout, stderr)
					errs <- err
				}
			}(strconv.Itoa(keyCount), keyName, strconv.Itoa(keySize), leaderPods.Items[i], &wg)
		} else {
			fmt.Printf("Node %s had no IP\n", leaderPod.Labels["node-name"])
		}
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
