package controllers

import (
	"context"
	"crypto/sha256"
	"fmt"
	"reflect"
	"sync"
	"time"

	rediscli "github.com/PayU/redis-operator/controllers/rediscli"
	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	dbv1 "github.com/PayU/redis-operator/api/v1"
)

/*
	The Redis config controller is responsible for monitoring configuration files
	of Redis and loading them on the nodes when changed.
	More features can be added easily here since the config controller is
	separated from the main controller to keep the logic more clean.

	Currently used configuration files:

	- redis.conf: ConfigMap, holds the Redis node main configuration, currently
	it is not actively managed by the controller so any change will have to be
	propagated with a manual rolling restart of the cluster
	https://raw.githubusercontent.com/antirez/redis/6.2.4/redis.conf

	- aclfile: ConfigMap, holds the Redis account information, any change is
	automatically propagated to all cluster nodes.
	https://redis.io/topics/acl
*/

type RedisConfigReconciler struct {
	client.Client
	Log      logr.Logger
	Scheme   *runtime.Scheme
	RedisCLI *rediscli.RedisCLI
}

//+kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=configmaps/status,verbs=get;update;patch

// Defines how long it takes for the ACL configmap to be reloaded by the kubelet
// and visible in the volume mount
const ACLFilePropagationDuration time.Duration = time.Millisecond * 5000

// Defines the time it takes for Redis to load the new config
const ACLFileLoadDuration time.Duration = time.Millisecond * 500
const redisConfigLabelKey string = "redis-cluster"

func (r *RedisConfigReconciler) syncConfig(latestConfigHash string, redisPods ...corev1.Pod) error {

	time.Sleep(ACLFilePropagationDuration)

	for _, pod := range redisPods {
		msg, err := r.RedisCLI.ACLLoad(pod.Status.PodIP)
		if err != nil {
			r.Log.Info(fmt.Sprintf("Failed to load ACL file: %s | %+v", msg, err))
			return err
		}

		time.Sleep(ACLFileLoadDuration)

		loadedConfig, err := r.RedisCLI.ACLList(pod.Status.PodIP)
		if err != nil {
			r.Log.Error(err, fmt.Sprintf("Failed to list new ACL config from %s(%s)", pod.Name, pod.Status.PodIP))
			return err
		}

		loadedConfigHash := fmt.Sprintf("%x", sha256.Sum256([]byte(loadedConfig.String())))

		if !reflect.DeepEqual(loadedConfigHash, latestConfigHash) {
			return errors.Errorf("Failed to sync ACL config for node %s(%s) | configs: (current: %s | latest: %s)",
				pod.Name, pod.Status.PodIP, loadedConfigHash, latestConfigHash)
		} else {
			err := r.updateACLHashStatus(latestConfigHash, pod)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Updates the 'acl-config' annotation on the Redis cluster pods with the provided string value
func (r *RedisConfigReconciler) updateACLHashStatus(status string, redisPods ...corev1.Pod) error {
	patch := []byte(fmt.Sprintf(`{"metadata":{"annotations":{"acl-config": "%s"}}}`, status))
	for i, pod := range redisPods {
		if err := r.Patch(context.Background(), &redisPods[i], client.RawPatch(types.StrategicMergePatchType, patch)); err != nil {
			r.Log.Error(err, fmt.Sprintf("Failed to patch the ACL file hash on pod %s (%s), status: %s", pod.Name, pod.Status.PodIP, status))
		}
	}
	return nil
}

// Retrieves the ACL config from a Redis node and returns its SHA256 hash
func (r *RedisConfigReconciler) getACLConfigHash(pod *corev1.Pod) (string, error) {
	acl, err := r.RedisCLI.ACLList(pod.Status.PodIP)
	if err != nil {
		r.Log.Error(err, fmt.Sprintf("Failed to list previous ACL config from %s(%s) ", pod.Name, pod.Status.PodIP))
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(acl.String()))), nil
}

func (r *RedisConfigReconciler) handleACLConfig(configMap *corev1.ConfigMap) error {
	var handleFail error = nil
	var wg sync.WaitGroup
	var syncFail bool = false

	rdcName := configMap.GetObjectMeta().GetLabels()["redis-cluster"]
	ns := configMap.Namespace
	r.Log.Info(fmt.Sprintf("Reconciling ACL config for Redis cluster [%s/%s]", ns, rdcName))

	rdc := dbv1.RedisCluster{}
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: configMap.Namespace, Name: rdcName}, &rdc); err != nil {
		return err
	}

	rdcPods := corev1.PodList{}
	err := r.List(context.Background(), &rdcPods,
		client.InNamespace(configMap.Namespace),
		client.MatchingLabels{"redis-cluster": rdc.Name})
	if err != nil {
		r.Log.Error(err, "Failed to get pods of the Redis cluster")
	}

	acl, err := rediscli.NewRedisACL(configMap.Data["users.acl"])
	if err != nil {
		r.Log.Error(err, fmt.Sprintf("Failed to parse the Redis ACL config from %s", configMap.Name))
		return err
	}

	configMapACLHash := fmt.Sprintf("%x", sha256.Sum256([]byte(acl.String())))
	r.Log.Info(fmt.Sprintf("Computed hash: %s", configMapACLHash))

	for i := range rdcPods.Items {
		wg.Add(1)
		go func(failSignal *bool, pod *corev1.Pod, wg *sync.WaitGroup) error {
			defer wg.Done()
			redisNodeConfigHash, err := r.getACLConfigHash(pod)
			if err != nil {
				r.Log.Error(err, "Failed to get the config for %s(%s)", pod.Name, pod.Status.PodIP)
				*failSignal = true
				return err
			}
			annotationHash, ok := pod.Annotations["acl-config"]
			if !ok {
				if redisNodeConfigHash == configMapACLHash {
					if err := r.updateACLHashStatus(configMapACLHash, *pod); err != nil {
						*failSignal = true
						return err
					}
				} else {
					if err := r.updateACLHashStatus("update", *pod); err != nil {
						*failSignal = true
						return err
					}
					if err := r.syncConfig(configMapACLHash, *pod); err != nil {
						*failSignal = true
						return err
					}
					r.Log.Info(fmt.Sprintf("Successfully synced ACL config of %s(%s)", pod.Name, pod.Status.PodIP))
				}
			} else {
				if configMapACLHash != redisNodeConfigHash {
					if err := r.updateACLHashStatus("update", *pod); err != nil {
						*failSignal = true
						return err
					}
					if err := r.syncConfig(configMapACLHash, *pod); err != nil {
						*failSignal = true
						return err
					}
					r.Log.Info(fmt.Sprintf("Successfully synced ACL config of %s(%s)", pod.Name, pod.Status.PodIP))
				} else if annotationHash != configMapACLHash {
					if err := r.updateACLHashStatus(configMapACLHash, *pod); err != nil {
						*failSignal = true
						return err
					}
				}
			}
			return nil
		}(&syncFail, &rdcPods.Items[i], &wg)
	}

	wg.Wait()

	if syncFail {
		handleFail = errors.Errorf("Failed to sync all ACL configurations")
	}

	return handleFail
}

func (r *RedisClusterReconciler) handleRedisConfig(configMap *corev1.ConfigMap) error {
	r.Log.Info("Detected change on the redis.conf configmap")
	return nil
}

func (r *RedisConfigReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	var configMap corev1.ConfigMap

	if err := r.Get(context.Background(), req.NamespacedName, &configMap); err != nil {
		r.Log.Error(err, "Failed to fetch configmap")
	}
	labels := configMap.GetObjectMeta().GetLabels()
	for label := range labels {
		if label == redisConfigLabelKey {
			if _, ok := configMap.Data["users.acl"]; ok {
				if err := r.handleACLConfig(&configMap); err != nil {
					r.Log.Error(err, "Failed to reconcile ACL config")
					return ctrl.Result{}, err
				}
				return ctrl.Result{}, nil
			}
		}
	}

	return ctrl.Result{}, nil
}

func (r *RedisConfigReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.ConfigMap{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Complete(r)
}
