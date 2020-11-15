// +build e2e_redis_op

package framework

// TODO polling done for waiting resources should be use lists of resources
// example:
// https://github.com/kubernetes-sigs/controller-runtime/blob/master/pkg/envtest/crd.go#L185

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (f *Framework) CreateResources(ctx *TestCtx, objs []runtime.Object, timeout time.Duration) error {
	for _, obj := range objs {
		if err := f.CreateResource(ctx, obj, timeout); err != nil {
			return err
		}
	}
	return nil
}

// TODO should also add support for CreateResourceAndWaitUntilReady
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
			return f.DeleteResource(obj, timeout)
		})
	case err != nil:
		return err
	default:
		fmt.Printf("Object already exists (%s). Updating.\n", obj.GetObjectKind().GroupVersionKind().Kind)
		// TODO the resource version should also be updated
		if err = f.RuntimeClient.Update(context.TODO(), obj); err != nil {
			return err
		}
	}

	if timeout == 0 {
		return nil
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

func (f *Framework) DeleteResource(obj runtime.Object, timeout time.Duration) error {
	if err := f.RuntimeClient.Delete(context.TODO(), obj); err != nil {
		return err
	}

	if timeout == 0 {
		return nil
	}

	key, err := client.ObjectKeyFromObject(obj)
	if err != nil {
		return errors.Wrap(err, "Could not check delete resource - object key error")
	}

	err = wait.PollImmediate(2*time.Second, timeout, func() (bool, error) {
		err = f.RuntimeClient.Get(context.TODO(), key, obj)
		switch {
		case apierrors.IsNotFound(err):
			return true, nil
		case err != nil:
			return false, errors.Wrap(err, "Could not get object for deletion")
		default:
			return false, nil
		}
	})
	return nil
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

func (f *Framework) GetNodes() (*corev1.NodeList, error) {
	return f.KubeClient.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
}

func (f *Framework) GetPods(opts ...client.ListOption) (*corev1.PodList, error) {
	podList := corev1.PodList{}
	err := f.RuntimeClient.List(context.TODO(), &podList, opts...)
	if err != nil {
		return nil, err
	}
	return &podList, nil
}
