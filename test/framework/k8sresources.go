// +build e2e_redis_op

package framework

import (
	"context"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/pkg/errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	dbv1 "github.com/PayU/Redis-Operator/api/v1"
)

func (f *Framework) CreateResources(ctx *TestCtx, objs []runtime.Object, timeout time.Duration) error {
	for _, obj := range objs {
		if err := f.CreateResource(ctx, obj, timeout); err != nil {
			return err
		}
	}
	return nil
}

func waitForResource() {

}

func (f *Framework) CreateResource(ctx *TestCtx, obj runtime.Object, timeout time.Duration) error {
	var err error

	key, err := client.ObjectKeyFromObject(obj)
	if err != nil {
		return errors.Wrap(err, "Could not create resource - object key error")
	}

	existingResource := obj.DeepCopyObject()
	err = f.RuntimeClient.Get(context.TODO(), key, existingResource)
	switch {
	case apierrors.IsNotFound(err):
		if err = f.RuntimeClient.Create(context.TODO(), obj, &client.CreateOptions{}); err != nil {
			return err
		}
		ctx.AddFinalizerFn(func() error {
			return f.DeleteResource(obj)
		})
	case err != nil:
		return err
	default:
		fmt.Printf("Object already exists (%v). Updating.\n", err)
		// TODO the resource version should also be updated
		if err = f.RuntimeClient.Update(context.TODO(), obj); err != nil {
			return err
		}
	}

	err = wait.PollImmediate(2*time.Second, timeout, func() (bool, error) {
		if err = f.RuntimeClient.Get(context.TODO(), key, existingResource); err != nil {
			return false, err
		}
		// TODO the object should be checked to be the same
		// if !reflect.DeepEqual(obj, existingResource) {
		// 	return false, err
		// }
		return true, nil
	})

	if err != nil {
		return errors.Wrapf(err, "Creation of resource failed during wait for %v", key)
	}

	return nil
}

func (f *Framework) CreateYAMLResources(ctx *TestCtx, yamlResources []string, timeout time.Duration) error {
	for _, res := range yamlResources {
		if err := f.CreateYAMLResource(ctx, res, timeout); err != nil {
			return err
		}
	}
	return nil
}

// CreateYAMLResource is intended to be used for resources that cannot be made by the
// Kubernetes libraries, usually because of version differences between the library
// and the Kubernetes server.
func (f *Framework) CreateYAMLResource(ctx *TestCtx, yamlResource string, timeout time.Duration) error {
	if _, _, err := f.kubectlApply(yamlResource, timeout, false); err != nil {
		return err
	}
	ctx.AddFinalizerFn(func() error {
		return f.DeleteYAMLResource(yamlResource, timeout)
	})
	return nil
}

func (f *Framework) DeleteResource(obj runtime.Object) error {
	return f.RuntimeClient.Delete(context.TODO(), obj)
}

func (f *Framework) DeleteYAMLResource(yamlResource string, timeout time.Duration) error {
	_, _, err := f.kubectlDelete(yamlResource, timeout)
	return err
}

func (f *Framework) InitializeDefaultResources(ctx *TestCtx, kustPath string, opImage string, timeout time.Duration) error {
	kustomizeConfig, yamlMap, err := f.BuildAndParseKustomizeConfig(kustPath, opImage)
	if err != nil {
		return errors.Wrap(err, "Could not get kustomize config")
	}

	resources := []runtime.Object{
		&kustomizeConfig.Namespace,
		&kustomizeConfig.ClusterRole,
		&kustomizeConfig.ClusterRoleBinding,
		&kustomizeConfig.Role,
		&kustomizeConfig.RoleBinding,
		&kustomizeConfig.Deployment,
	}

	// the CRD is installed from YAML via kubectl because of library compatibility issues
	// it will be changed to a runtime object after upgrading to Kubernetes 1.16
	yamlResources := []string{
		yamlMap["crd"],
	}

	err = f.CreateResources(ctx, resources, timeout)
	if err != nil {
		return errors.Wrap(err, "Could not create all resources")
	}

	err = f.CreateYAMLResources(ctx, yamlResources, timeout)
	if err != nil {
		return errors.Wrap(err, "Could not create all YAML resources")
	}

	return nil
}

func (f *Framework) CreateRedisCluster(ctx *TestCtx, timeout time.Duration) (*dbv1.RedisCluster, error) {
	filePath := "../../config/samples/local_cluster.yaml"
	redisCluster := dbv1.RedisCluster{}
	yamlRes, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, errors.Wrap(err, "Could not read the Redis cluster YAML resource")
	}
	if err = yaml.Unmarshal(yamlRes, &redisCluster); err != nil {
		return nil, errors.Wrap(err, "Could not unmarshal the Redis cluster YAML resource")
	}
	if err := f.CreateResource(ctx, &redisCluster, timeout); err != nil {
		return nil, errors.Wrap(err, "Could not create the Redis cluster resource")
	}
	return &redisCluster, nil
}

func (f *Framework) DeleteRedisCluster(obj runtime.Object) error {
	return f.DeleteResource(obj)
}
