// +build e2e_redis_op

package framework

import (
	"context"
	"fmt"
	"io/ioutil"
	"time"

	dbv1 "github.com/PayU/Redis-Operator/api/v1"
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
		"app": "redis",
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
	if err := f.CreateResource(ctx, redisCluster, timeout); err != nil {
		return errors.Wrap(err, "Could not create the Redis cluster resource")
	}
	return nil
}

func (f *Framework) CreateRedisClusterAndWaitUntilReady(ctx *TestCtx, redisCluster *dbv1.RedisCluster, timeout time.Duration) error {
	buf := dbv1.RedisCluster{}
	if err := f.CreateRedisCluster(ctx, redisCluster, timeout); err != nil {
		return err
	}

	if timeout == 0 {
		return nil
	}

	key, err := client.ObjectKeyFromObject(redisCluster)
	if err != nil {
		return errors.Wrap(err, "Could not create resource - object key error")
	}

	err = wait.PollImmediate(2*time.Second, timeout, func() (bool, error) {
		if err = f.RuntimeClient.Get(context.TODO(), key, &buf); err != nil {
			return false, err
		}
		if string(buf.Status.ClusterState) != "Ready" {
			return false, nil
		}
		return true, nil
	})

	if err != nil {
		return errors.Wrap(err, "Creation of Redis cluster timed out")
	}

	return nil
}

func (f *Framework) DeleteRedisCluster(obj runtime.Object, timeout time.Duration) error {
	return f.DeleteResource(obj, timeout)
}
