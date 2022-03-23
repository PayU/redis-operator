/*


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	dbv1 "github.com/PayU/redis-operator/api/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/PayU/redis-operator/controllers/rediscli"
	clusterData "github.com/PayU/redis-operator/data"
)

const (
	// NotExists: the RedisCluster custom resource has just been created
	NotExists RedisClusterState = "NotExists"

	// InitializingCluster: ConfigMap, Service resources are created; the leader
	// pods are created and clusterized
	InitializingCluster RedisClusterState = "InitializingCluster"

	// InitializingFollowers: followers are added to the cluster
	InitializingFollowers RedisClusterState = "InitializingFollowers"

	// Ready: cluster is up & running as expected
	Ready RedisClusterState = "Ready"

	// Recovering: one ore note nodes are in fail state and are being recreated
	Recovering RedisClusterState = "Recovering"

	// Updating: the cluster is in the middle of a rolling update
	Updating RedisClusterState = "Updating"
)

type RedisClusterState string

type RedisClusterReconciler struct {
	client.Client
	Log      logr.Logger
	Scheme   *runtime.Scheme
	RedisCLI *rediscli.RedisCLI
	Config   *OperatorConfig
	State    RedisClusterState
}

// +kubebuilder:rbac:groups=db.payu.com,resources=redisclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=db.payu.com,resources=redisclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=*,resources=pods;services;configmaps,verbs=create;update;patch;get;list;watch;delete

func getCurrentClusterState(redisCluster *dbv1.RedisCluster) RedisClusterState {
	if len(redisCluster.Status.ClusterState) == 0 {
		return NotExists
	}
	return RedisClusterState(redisCluster.Status.ClusterState)
}

func (r *RedisClusterReconciler) handleInitializingCluster(redisCluster *dbv1.RedisCluster) error {
	r.Log.Info("Handling initializing cluster...")
	if err := r.createNewRedisCluster(redisCluster); err != nil {
		return err
	}
	redisCluster.Status.ClusterState = string(InitializingFollowers)
	return nil
}

func (r *RedisClusterReconciler) handleInitializingFollowers(redisCluster *dbv1.RedisCluster) error {
	r.Log.Info("Handling initializing followers...")
	if err := r.initializeFollowers(redisCluster); err != nil {
		return err
	}
	redisCluster.Status.ClusterState = string(Ready)
	return nil
}

func (r *RedisClusterReconciler) handleReadyState(redisCluster *dbv1.RedisCluster) error {
	complete, err := r.isClusterComplete(redisCluster)
	if err != nil {
		r.Log.Info("Could not check if cluster is complete")
		return err
	}
	if !complete {
		redisCluster.Status.ClusterState = string(Recovering)
		return nil
	}

	uptodate, err := r.isClusterUpToDate(redisCluster)
	if err != nil {
		r.Log.Info("Could not check if cluster is updated")
		redisCluster.Status.ClusterState = string(Recovering)
		return err
	}
	if !uptodate {
		redisCluster.Status.ClusterState = string(Updating)
		return nil
	}
	r.Log.Info("Cluster is healthy")
	return nil
}

func (r *RedisClusterReconciler) handleRecoveringState(redisCluster *dbv1.RedisCluster) error {
	r.Log.Info("Handling cluster recovery...")
	if err := r.recoverCluster(redisCluster); err != nil {
		r.Log.Info("Cluster recovery failed")
		return err
	}
	redisCluster.Status.ClusterState = string(Ready)
	return nil
}

func (r *RedisClusterReconciler) handleUpdatingState(redisCluster *dbv1.RedisCluster) error {
	r.Log.Info("Handling rolling update...")
	if err := r.updateCluster(redisCluster); err != nil {
		r.Log.Info("Rolling update failed")
		redisCluster.Status.ClusterState = string(Recovering)
		return err
	}
	redisCluster.Status.ClusterState = string(Ready)
	return nil
}

func (r *RedisClusterReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	r.Status()

	var redisCluster dbv1.RedisCluster
	var err error

	if err = r.Get(context.Background(), req.NamespacedName, &redisCluster); err != nil {
		r.Log.Info("Unable to fetch RedisCluster resource")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	r.State = getCurrentClusterState(&redisCluster)
	fmt.Printf("State: %+v\n", r.State)

	switch r.State {
	case NotExists:
		redisCluster.Status.ClusterState = string(InitializingCluster)
		err = r.handleInitializingCluster(&redisCluster)
		break
	case InitializingCluster:
		err = r.handleInitializingCluster(&redisCluster)
		break
	case InitializingFollowers:
		err = r.handleInitializingFollowers(&redisCluster)
		break
	case Ready:
		err = r.handleReadyState(&redisCluster)
		break
	case Recovering:
		err = r.handleRecoveringState(&redisCluster)
		break
	case Updating:
		err = r.handleUpdatingState(&redisCluster)
		break
	}

	clusterData.SaveRedisClusterState(string(r.State))

	if err != nil {
		r.Log.Error(err, "Handling error")
	}

	clusterState := getCurrentClusterState(&redisCluster)
	if clusterState != r.State {
		err := r.Status().Update(context.Background(), &redisCluster)
		if err != nil && !apierrors.IsConflict(err) {
			r.Log.Info("Failed to update state to " + string(clusterState))
			return ctrl.Result{}, err
		}
		if apierrors.IsConflict(err) {
			r.Log.Info("Conflict when updating state to " + string(clusterState))
		}
		r.Client.Status()
		r.State = clusterState
		r.Log.Info(fmt.Sprintf("Updated state to: [%s]", clusterState))
	}

	v, e := r.getRedisPodsView()
	if e != nil {
		println("Error: ", e)
	} else {
		for key, val := range v {
			fmt.Printf("Name: %+v\n", key)
			fmt.Printf("Pods: %+v\n", val)
		}
	}

	return ctrl.Result{}, nil
}

func (r *RedisClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &corev1.Pod{}, "status.podIP", func(rawObj runtime.Object) []string {
		pod := rawObj.(*corev1.Pod)
		return []string{pod.Status.PodIP}
	}); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&dbv1.RedisCluster{}).
		Owns(&corev1.Pod{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Complete(r)
}
