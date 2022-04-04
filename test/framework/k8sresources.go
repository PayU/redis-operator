//go:build e2e_redis_op
// +build e2e_redis_op

package framework

// TODO polling done for waiting resources should be use lists of resources
// example:
// https://github.com/kubernetes-sigs/controller-runtime/blob/master/pkg/envtest/crd.go#L185

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubectl/pkg/drain"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (f *Framework) CreateResources(ctx *TestCtx, timeout time.Duration, objs ...runtime.Object) error {
	for _, obj := range objs {
		key, err := client.ObjectKeyFromObject(obj)
		if err != nil {
			return errors.Wrap(err, "Could not create resource - object key error")
		}

		existingResource := obj.DeepCopyObject()
		fmt.Printf("Check existing resource %v\n", key)
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
		default: // objects that should not be recreated
			if obj.GetObjectKind().GroupVersionKind().Kind == "Namespace" {
				fmt.Printf("Object already exists (%s). Skipping.\n", obj.GetObjectKind().GroupVersionKind().Kind)
				break
			}
			fmt.Printf("Object already exists (%s). Recreating.\n", obj.GetObjectKind().GroupVersionKind().Kind)
			err = f.DeleteResource(obj, 20*time.Second)
			if err != nil {
				return err
			}
			if err = f.RuntimeClient.Update(context.TODO(), obj); err != nil {
				return err
			}
		}

		if timeout == 0 {
			return nil
		}

		fmt.Printf("Waiting on resource %v...\n", key)
		err = wait.PollImmediate(5*time.Second, 5*timeout, func() (bool, error) {
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
	}
	return nil
}

// CreateYAMLResources uses kubectl apply to create resources using a YAML string.
func (f *Framework) CreateYAMLResources(ctx *TestCtx, timeout time.Duration, yamlResources ...string) error {
	for _, res := range yamlResources {
		resKind := strings.Split(strings.Split(res, "kind: ")[1], "\n")[0]
		fmt.Printf("[E2E] Creating resource %v...\n", resKind)
		if _, _, err := f.kubectlApply(res, timeout, false); err != nil {
			fmt.Printf("Failed to create resource %v\n", resKind)
			return err
		}
		ctx.AddFinalizerFn(func() error {
			return f.DeleteYAMLResource(res, timeout)
		})
	}
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

	if pollErr := wait.PollImmediate(5*time.Second, 5*timeout, func() (bool, error) {
		err = f.RuntimeClient.Get(context.TODO(), key, obj)
		switch {
		case apierrors.IsNotFound(err):
			return true, nil
		case err != nil:
			return false, errors.Wrap(err, "Could not get object for deletion")
		default:
			return false, nil
		}
	}); pollErr != nil {
		return pollErr
	}
	return nil
}

func (f *Framework) DeleteYAMLResource(yamlResource string, timeout time.Duration) error {
	_, _, err := f.kubectlDelete(yamlResource, timeout)
	return err
}

func (f *Framework) InitializeDefaultResources(ctx *TestCtx, kustPath string, opImage string, namespace string, timeout time.Duration) error {
	_, kustomizeConfigYAML, err := f.BuildKustomizeConfig(kustPath, opImage, namespace)
	if err != nil {
		return errors.Wrap(err, "Could not get kustomize config")
	}

	if crd, ok := kustomizeConfigYAML["CustomResourceDefinition"]; ok {
		err = f.CreateYAMLResources(ctx, timeout, crd[0])
		if err != nil {
			return errors.Wrap(err, "Could not create all resources")
		}
	}

	for kind, res := range kustomizeConfigYAML {
		if kind == "CustomResourceDefinition" {
			continue
		}
		for i := range res {
			err = f.CreateYAMLResources(ctx, timeout, res[i])
			if err != nil {
				return errors.Wrap(err, "Could not create all resources")
			}
		}
	}
	return nil
}

func (f *Framework) GetNodes() (*corev1.NodeList, error) {
	return f.KubeClient.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
}

// GetAvailabilityZoneNodes returns all nodes that are in a given AZ.
func (f *Framework) GetAvailabilityZoneNodes(AZName string) (*corev1.NodeList, error) {
	stable, err := f.KubeClient.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("topology.kubernetes.io/zone=%s", AZName),
	})
	if err != nil {
		return nil, err
	}

	if len(stable.Items) > 0 {
		return stable, nil
	}

	beta, err := f.KubeClient.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("failure-domain.beta.kubernetes.io/zone=%s", AZName),
	})
	if err != nil {
		return nil, err
	}

	return beta, nil
}

func (f *Framework) GetPods(opts ...client.ListOption) (*corev1.PodList, error) {
	podList := corev1.PodList{}
	err := f.RuntimeClient.List(context.TODO(), &podList, opts...)
	if err != nil {
		return nil, err
	}
	return &podList, nil
}

func (f *Framework) PatchResource(obj runtime.Object, patch []byte) error {
	err := f.RuntimeClient.Patch(context.TODO(), obj, client.RawPatch(types.MergePatchType, patch))
	if err != nil {
		return err
	}
	return nil
}

func (f *Framework) CordonNode(nodeName string, unschedule bool, timeout time.Duration) error {
	// TODO the method should also accept lists of nodes and apply them in parallel
	node, err := f.KubeClient.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	drainer := drain.Helper{
		Ctx:    context.TODO(),
		Client: f.KubeClient,
		Force:  false,
	}
	if err = drain.RunCordonOrUncordon(&drainer, node, unschedule); err != nil {
		fmt.Printf("Failed to cordon/uncordon: %v\n", err)
		return err
	}
	if pollErr := wait.PollImmediate(5*time.Second, 5*timeout, func() (bool, error) {
		node, err := f.KubeClient.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if node.Spec.Unschedulable == unschedule {
			return true, nil
		}
		return false, nil
	}); pollErr != nil {
		return pollErr
	}
	return nil
}

// Iterates on all nodes and runs the uncordon command. Intended as a cleanup
// method.
func (f *Framework) UncordonAll(nodeTimeout time.Duration) error {
	nodes, err := f.GetNodes()
	if err != nil {
		return err
	}
	for _, node := range nodes.Items {
		if err = f.CordonNode(node.Name, false, nodeTimeout); err != nil {
			return err
		}
	}
	return nil
}

func (f *Framework) DrainNode(nodeName string, timeout time.Duration) error {
	drainer := drain.Helper{
		Ctx:                 context.TODO(),
		Client:              f.KubeClient,
		Force:               false,
		DeleteLocalData:     true,
		IgnoreAllDaemonSets: true,
		Timeout:             timeout,
		Out:                 os.Stdout,
		ErrOut:              os.Stderr,
	}

	if err := drain.RunNodeDrain(&drainer, nodeName); err != nil {
		return err
	}
	return nil
}
