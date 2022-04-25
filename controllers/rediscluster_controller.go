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
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/go-logr/logr"
	"github.com/labstack/echo/v4"
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

	UpdateView RedisClusterState = "UpdateView"

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
	Log                  logr.Logger
	Scheme               *runtime.Scheme
	RedisCLI             *rediscli.RedisCLI
	Config               *OperatorConfig
	State                RedisClusterState
	ClusterStatusMapName string
}

var reconciler *RedisClusterReconciler
var cluster *dbv1.RedisCluster
var clusterCreated bool = false

// +kubebuilder:rbac:groups=db.payu.com,resources=redisclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=db.payu.com,resources=redisclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=*,resources=pods;services;configmaps,verbs=create;update;patch;get;list;watch;delete

func (r *RedisClusterReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	reconciler = r
	r.Status()

	var redisCluster dbv1.RedisCluster
	var err error

	if err = r.Get(context.Background(), req.NamespacedName, &redisCluster); err != nil {
		r.Log.Info("Unable to fetch RedisCluster resource")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	r.State = RedisClusterState(redisCluster.Status.ClusterState)
	if len(redisCluster.Status.ClusterState) == 0 {
		r.State = NotExists
	}

	cluster = &redisCluster

	switch r.State {
	case NotExists:
		r.deleteAllRedisClusterPods()
		err = r.handleInitializingCluster(&redisCluster)
		break
	case UpdateView:
		r.UpdateView(&redisCluster)
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

	r.updateClusterView(&redisCluster)

	if err != nil {
		r.Log.Error(err, "Handling error")
	}

	err = r.Status().Update(context.Background(), &redisCluster)
	clusterState := redisCluster.Status.ClusterState
	if err != nil && !apierrors.IsConflict(err) {
		r.Log.Info("Failed to update state to " + string(clusterState))
		return ctrl.Result{}, err
	}
	if apierrors.IsConflict(err) {
		r.Log.Info("Conflict when updating state to " + string(clusterState))
	}
	r.Client.Status()
	r.Log.Info(fmt.Sprintf("Updated state to: [%s]", clusterState))

	return ctrl.Result{}, nil
}

func (r *RedisClusterReconciler) updateClusterView(redisCluster *dbv1.RedisCluster) {
	view, err := r.NewRedisClusterView(redisCluster)
	if err != nil {
		r.Log.Info("[Warn] Could not get view for api view update, Error: %v", err.Error())
		return
	}
	data, _ := json.MarshalIndent(view, "", "")
	clusterData.SaveRedisClusterView(data)
	clusterData.SaveRedisClusterState(redisCluster.Status.ClusterState)
}

func (r *RedisClusterReconciler) handleInitializingCluster(redisCluster *dbv1.RedisCluster) error {
	r.Log.Info("Handling initializing leaders...")
	if err := r.createNewRedisCluster(redisCluster); err != nil {
		redisCluster.Status.ClusterState = string(InitializingCluster)
		return err
	}
	r.Log.Info("Handling initializing followers...")
	if err := r.initializeFollowers(redisCluster); err != nil {
		redisCluster.Status.ClusterState = string(InitializingCluster)
		return err
	}
	redisCluster.Status.ClusterState = string(UpdateView)
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
	expectedView, err := r.GetExpectedView(cluster)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go r.recoverCluster(redisCluster, &wg)
	detectUrgentEvents := make(chan struct{})
	go func() {
		for {
			r.mitigateUrgentEvents(redisCluster, expectedView)
		}
	}()
	wg.Wait()
	close(detectUrgentEvents)
	redisCluster.Status.ClusterState = string(Ready)
	return nil
}

func (r *RedisClusterReconciler) handleUpdatingState(redisCluster *dbv1.RedisCluster) error {
	var err error = nil
	r.Log.Info("Handling rolling update...")
	if err = r.updateCluster(redisCluster); err != nil {
		r.Log.Info("Rolling update failed")
	}
	redisCluster.Status.ClusterState = string(Recovering)
	return err
}

func (r *RedisClusterReconciler) mitigateUrgentEvents(redisCluster *dbv1.RedisCluster, expectedView map[string]string) {
	r.GetMissingLeadersAndFailover(redisCluster, expectedView, map[string]bool{})
}

func (r *RedisClusterReconciler) validateStateUpdated(redisCluster *dbv1.RedisCluster) (ctrl.Result, error) {
	clusterState := RedisClusterState(redisCluster.Status.ClusterState)
	if len(redisCluster.Status.ClusterState) == 0 {
		clusterState = NotExists
	}
	if clusterState != r.State {
		err := r.Status().Update(context.Background(), redisCluster)
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
	return ctrl.Result{}, nil
}

func (r *RedisClusterReconciler) UpdateView(redisCluster *dbv1.RedisCluster) error {
	e := r.UpdateExpectedView(redisCluster)
	if e != nil {
		return e
	}
	redisCluster.Status.ClusterState = string(Ready)
	return nil
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

func DoResetCluster(c echo.Context) error {
	reconciler.deleteAllRedisClusterPods()
	cluster.Status.ClusterState = string(InitializingCluster)
	return c.String(http.StatusOK, "All pods deleted")
}

func UpdateExpectedView(c echo.Context) error {
	e := reconciler.UpdateExpectedView(cluster)
	if e != nil {
		return e
	}
	return c.String(http.StatusOK, "Config map updated")
}

func GetExpectedView(c echo.Context) error {
	expectedView, err := reconciler.GetExpectedView(cluster)
	if err != nil {
		return err
	}
	return c.String(http.StatusOK, fmt.Sprintf("%v", expectedView))
}

func DeleteAllPods(c echo.Context) error {
	reconciler.deleteAllRedisClusterPods()
	return c.String(http.StatusOK, "All pods deleted")
}
