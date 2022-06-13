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
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/PayU/redis-operator/controllers/redisclient"
	"github.com/PayU/redis-operator/controllers/testlab"
	"github.com/PayU/redis-operator/controllers/view"

	"github.com/go-logr/logr"
	"github.com/labstack/echo/v4"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
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

	Reset RedisClusterState = "Reset"

	// Ready: cluster is up & running as expected
	Ready RedisClusterState = "Ready"

	// Recovering: one ore note nodes are in fail state and are being recreated
	Recovering RedisClusterState = "Recovering"

	// Updating: the cluster is in the middle of a rolling update
	Updating RedisClusterState = "Updating"

	Scale RedisClusterState = "Scale"
)

type RedisClusterState string

type RedisClusterReconciler struct {
	client.Client
	Cache                 cache.Cache
	Log                   logr.Logger
	Scheme                *runtime.Scheme
	RedisCLI              *rediscli.RedisCLI
	Config                *OperatorConfig
	State                 RedisClusterState
	RedisClusterStateView *view.RedisClusterStateView
}

var reconciler *RedisClusterReconciler
var cluster *dbv1.RedisCluster

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
		return ctrl.Result{Requeue: true, RequeueAfter: 15 * time.Second}, client.IgnoreNotFound(err)
	}

	r.State = RedisClusterState(redisCluster.Status.ClusterState)
	if len(redisCluster.Status.ClusterState) == 0 {
		r.State = NotExists
	}

	cluster = &redisCluster

	if r.State != NotExists && r.State != Reset {
		err = r.getClusterStateView(&redisCluster)
		if err != nil {
			r.Log.Error(err, "Could not perform reconcile loop")
			return ctrl.Result{Requeue: true, RequeueAfter: 20 * time.Second}, nil
		}
	}
	// todo: ? scenario where cluster exists and for some reason map is missing -> trigger flow of build state view map out of existing cluster (with entry point by router), alert if it happens?

	r.saveClusterStateOnSigTerm(&redisCluster)
	switch r.State {
	case NotExists:
		err = r.handleInitializingCluster(&redisCluster)
		break
	case Reset:
		err = r.handleInitializingCluster(&redisCluster)
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
	case Scale:
		err = r.handleScaleState(&redisCluster)
	}
	if err != nil {
		r.Log.Error(err, "Handling error")
	}

	r.saveClusterView(&redisCluster)
	return ctrl.Result{Requeue: true, RequeueAfter: 15 * time.Second}, err
}

func (r *RedisClusterReconciler) saveClusterStateOnSigTerm(redisCluster *dbv1.RedisCluster) {
	if r.RedisClusterStateView != nil {
		mutex := &sync.Mutex{}
		saveStatusOnQuit := make(chan os.Signal, 1)
		signal.Notify(saveStatusOnQuit, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGKILL)
		go func() {
			<-saveStatusOnQuit
			close(saveStatusOnQuit)
			r.Log.Info("[WARN] reconcile loop interrupted by os signal, saving cluster state view...")
			mutex.Lock()
			r.saveClusterStateView(redisCluster)
			mutex.Unlock()
		}()
	}
}

func (r *RedisClusterReconciler) saveOperatorState(redisCluster *dbv1.RedisCluster) {
	r.Status().Update(context.Background(), redisCluster)
	operatorState := redisCluster.Status.ClusterState
	r.Client.Status()
	r.Log.Info(fmt.Sprintf("Operator state: [%s], Cluster state: [%s]", operatorState, r.RedisClusterStateView.ClusterState))
}

func (r *RedisClusterReconciler) saveClusterView(redisCluster *dbv1.RedisCluster) {
	if redisCluster.Status.ClusterState == string(Ready) && r.RedisClusterStateView.ClusterState == view.ClusterOK {
		r.RedisClusterStateView.NumOfReconcileLoopsSinceHealthyCluster = 0
	} else {
		r.RedisClusterStateView.NumOfReconcileLoopsSinceHealthyCluster++
	}
	r.saveClusterStateView(redisCluster)
	v, ok := r.NewRedisClusterView(redisCluster)
	if !ok {
		return
	}
	for _, n := range v.Nodes {
		if n != nil {
			n.Pod = corev1.Pod{}
		}
	}
	data, _ := json.MarshalIndent(v, "", "")
	clusterData.SaveRedisClusterView(data)
	clusterData.SaveRedisClusterState(redisCluster.Status.ClusterState)
	r.saveOperatorState(redisCluster)
}

func (r *RedisClusterReconciler) handleInitializingCluster(redisCluster *dbv1.RedisCluster) error {
	r.Log.Info("Clear all cluster pods...")
	e := r.deleteAllRedisClusterPods()
	if e != nil {
		return e
	}
	r.Log.Info("Clear cluster state map...")
	r.deleteClusterStateView(redisCluster)
	r.RedisClusterStateView.CreateStateView(redisCluster.Spec.LeaderCount, redisCluster.Spec.LeaderFollowersCount)
	r.Log.Info("Handling initializing cluster...")
	if err := r.createNewRedisCluster(redisCluster); err != nil {
		redisCluster.Status.ClusterState = string(Reset)
		return err
	}
	redisCluster.Status.ClusterState = string(Ready)
	r.postNewClusterStateView(redisCluster)
	r.saveClusterView(redisCluster)
	return nil
}

func (r *RedisClusterReconciler) handleReadyState(redisCluster *dbv1.RedisCluster) error {
	r.Log.Info("Handling ready state...")
	v, ok := r.NewRedisClusterView(redisCluster)
	if !ok {
		r.RedisClusterStateView.NumOfReconcileLoopsSinceHealthyCluster++
		r.RedisClusterStateView.NumOfHealthyReconcileLoopsInRow = 0
		redisCluster.Status.ClusterState = string(Recovering)
		return nil
	}
	lostNodesDetected := r.forgetLostNodes(redisCluster, v)
	if lostNodesDetected {
		r.RedisClusterStateView.NumOfHealthyReconcileLoopsInRow = 0
		r.Log.Info("[Warn] Lost nodes detcted on some of the nodes tables...")
		return nil
	}
	healthy, err := r.isClusterHealthy(redisCluster, v)
	if err != nil {
		r.RedisClusterStateView.NumOfHealthyReconcileLoopsInRow = 0
		r.Log.Info("Could not check if cluster is healthy")
		return err
	}
	if !healthy {
		r.RedisClusterStateView.NumOfHealthyReconcileLoopsInRow = 0
		redisCluster.Status.ClusterState = string(Recovering)
		return nil
	}
	uptodate, err := r.isClusterUpToDate(redisCluster, v)
	if err != nil {
		r.RedisClusterStateView.NumOfHealthyReconcileLoopsInRow = 0
		r.Log.Info("Could not check if cluster is updated")
		redisCluster.Status.ClusterState = string(Recovering)
		return err
	}
	if !uptodate {
		r.RedisClusterStateView.NumOfHealthyReconcileLoopsInRow = 0
		redisCluster.Status.ClusterState = string(Updating)
		return nil
	}
	scale, scaleType := r.isScaleRequired(redisCluster)
	if scale {
		r.RedisClusterStateView.NumOfHealthyReconcileLoopsInRow = 0
		r.Log.Info(fmt.Sprintf("Scale is required, scale type: [%v]", scaleType.String()))
		redisCluster.Status.ClusterState = string(Scale)
	}
	r.Log.Info("Cluster is healthy")
	if r.RedisClusterStateView.NumOfHealthyReconcileLoopsInRow < 10 {
		r.RedisClusterStateView.NumOfHealthyReconcileLoopsInRow++
	} else {
		r.Log.Info("[OK] Cluster is in finalized state")
	}
	return nil
}

func (r *RedisClusterReconciler) handleScaleState(redisCluster *dbv1.RedisCluster) error {
	r.Log.Info("Handling cluster scale...")
	e := r.scaleCluster(redisCluster)
	if e != nil {
		r.Log.Error(e, "Could not perform cluster scale")
	}
	redisCluster.Status.ClusterState = string(Ready)
	return nil
}

func (r *RedisClusterReconciler) handleRecoveringState(redisCluster *dbv1.RedisCluster) error {
	r.Log.Info("Handling cluster recovery...")
	v, ok := r.NewRedisClusterView(redisCluster)
	if !ok {
		return nil
	}
	e := r.recoverCluster(redisCluster, v)
	r.cleanMapFromNodesToRemove(redisCluster, v)
	if e != nil {
		return e
	}
	return nil
}

func (r *RedisClusterReconciler) handleUpdatingState(redisCluster *dbv1.RedisCluster) error {
	var err error = nil
	r.Log.Info("Handling rolling update...")
	r.updateCluster(redisCluster)
	redisCluster.Status.ClusterState = string(Recovering)
	reconciler.saveOperatorState(cluster)
	return err
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
	if reconciler == nil || cluster == nil {
		return c.String(http.StatusOK, "Could not perform cluster reset action")
	}
	cluster.Status.ClusterState = string(Reset)
	reconciler.saveOperatorState(cluster)
	return c.String(http.StatusOK, "Set cluster state to reset mode")
}

func ClusterRebalance(c echo.Context) error {
	if reconciler == nil || cluster == nil {
		return c.String(http.StatusOK, "Could not perform cluster rebalance action")
	}
	reconciler.saveClusterStateView(cluster)
	v, ok := reconciler.NewRedisClusterView(cluster)
	if !ok {
		return c.String(http.StatusOK, "Could not retrieve redis cluster view to")
	}
	reconciler.removeSoloLeaders(v)
	healthyServerName, found := reconciler.findHealthyLeader(v)
	if !found {
		return c.String(http.StatusOK, "Could not find healthy server to serve the rebalance request")
	}
	mutex := &sync.Mutex{}
	mutex.Lock()
	reconciler.RedisClusterStateView.ClusterState = view.ClusterRebalance
	healthyServerIp := v.Nodes[healthyServerName].Ip
	reconciler.waitForAllNodesAgreeAboutSlotsConfiguration(v)
	_, _, err := reconciler.RedisCLI.ClusterRebalance(healthyServerIp, true)
	if err != nil {
		reconciler.Log.Error(err, "Could not perform cluster rebalance")
	}
	reconciler.RedisClusterStateView.ClusterState = view.ClusterOK
	mutex.Unlock()
	reconciler.saveClusterStateView(cluster)
	return c.String(http.StatusOK, "Cluster rebalance attempt executed")
}

func ClusterFix(c echo.Context) error {
	if reconciler == nil || cluster == nil {
		return c.String(http.StatusOK, "Could not perform cluster fix action")
	}
	reconciler.saveClusterStateView(cluster)
	v, ok := reconciler.NewRedisClusterView(cluster)
	if !ok {
		return c.String(http.StatusOK, "Could not retrieve redis cluster view to")
	}
	healthyServerName, found := reconciler.findHealthyLeader(v)
	if !found {
		return c.String(http.StatusOK, "Could not find healthy server to serve the fix request")
	}
	healthyServerIp := v.Nodes[healthyServerName].Ip
	mutex := &sync.Mutex{}
	mutex.Lock()
	reconciler.RedisClusterStateView.ClusterState = view.ClusterFix
	_, _, err := reconciler.RedisCLI.ClusterFix(healthyServerIp)
	if err != nil {
		reconciler.Log.Error(err, "Could not perform cluster fix")
	}
	reconciler.RedisClusterStateView.ClusterState = view.ClusterOK
	mutex.Unlock()
	reconciler.saveClusterStateView(cluster)
	return c.String(http.StatusOK, "Cluster fix attempt executed")
}

func ForceReconcile(c echo.Context) error {
	if reconciler == nil || cluster == nil {
		return c.String(http.StatusOK, "Could not perform cluster reconcile action")
	}
	reconciler.saveClusterStateView(cluster)
	_, err := reconciler.Reconcile(ctrl.Request{types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}})
	if err != nil {
		reconciler.Log.Error(err, "Could not perform reconcile trigger")
	}
	return c.String(http.StatusOK, "Force Reconcile request triggered, direct reconcile trigger might run the loop without enqueue it again causing the operator to not scheduling another run within requested time.\nIn case of need run eforced reconcile manually several times until recovery is complete, and restart manager when cluster is stable")
}

func ClusterTest(c echo.Context) error {
	if reconciler == nil || cluster == nil {
		return c.String(http.StatusOK, "Could not perform cluster test")
	}
	t := &testlab.TestLab{
		Client:             reconciler.Client,
		RedisCLI:           reconciler.RedisCLI,
		Cluster:            cluster,
		RedisClusterClient: nil,
		Log:                reconciler.Log,
		Report:             "",
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go t.RunTest(&wg, &reconciler.RedisClusterStateView.Nodes)
	wg.Wait()
	return c.String(http.StatusOK, t.Report)
}

func PopulateClusterWithData(c echo.Context) error {
	if reconciler == nil || cluster == nil {
		return c.String(http.StatusOK, "Could not perform cluster popluate data")
	}
	var cl *redisclient.RedisClusterClient = nil
	v, ok := reconciler.NewRedisClusterView(cluster)
	if !ok || v == nil {
		return c.String(http.StatusOK, "Could not perform cluster populate data")
	}
	cl = redisclient.GetRedisClusterClient(v, reconciler.RedisCLI)

	for _, n := range v.Nodes {
		info, _, err := reconciler.RedisCLI.Info(n.Ip)
		if err != nil || info == nil {
			continue
		}
		println(n.Name + ": " + info.Memory["used_memory_human"])
	}

	total := 50000000
	init := 30000000
	sw := 0

	println("populating: ")
	println(total)

	for i := init; i < init+total; i++ {
		key := "key" + fmt.Sprintf("%v", i)
		val := "val" + fmt.Sprintf("%v", i)
		err := cl.Set(key, val, 3)
		if err == nil {
			sw++
		}
	}

	for _, n := range v.Nodes {
		info, _, err := reconciler.RedisCLI.Info(n.Ip)
		if err != nil || info == nil {
			continue
		}
		println(n.Name + ": " + info.Memory["used_memory_human"])
	}
	return c.String(http.StatusOK, "Cluster populated with data")
}

func FlushClusterData(c echo.Context) error {
	if reconciler == nil || cluster == nil {
		return c.String(http.StatusOK, "Could not perform cluster popluate data")
	}
	var cl *redisclient.RedisClusterClient = nil
	v, ok := reconciler.NewRedisClusterView(cluster)
	if !ok || v == nil {
		return c.String(http.StatusOK, "Could not perform cluster flush data")
	}
	cl = redisclient.GetRedisClusterClient(v, reconciler.RedisCLI)
	println("flushing")
	cl.FlushAllData()
	time.Sleep(10 * time.Second)
	for _, n := range v.Nodes {
		info, _, err := reconciler.RedisCLI.Info(n.Ip)
		if err != nil || info == nil {
			continue
		}
		println(n.Name + ": " + info.Memory["used_memory_human"])
	}
	return c.String(http.StatusOK, "Cluster data flushed")
}
