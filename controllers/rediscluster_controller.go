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
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/PayU/redis-operator/controllers/view"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"

	"github.com/go-logr/logr"
	"github.com/labstack/echo/v4"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
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
var mutex *sync.Mutex = &sync.Mutex{}

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
		return ctrl.Result{RequeueAfter: 15 * time.Second}, client.IgnoreNotFound(err)
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
			return ctrl.Result{RequeueAfter: 20 * time.Second}, nil
		}
	}
	// todo: scenario where cluster exists and for some reason map is missing -> trigger flow of build state view map out of existing cluster (with entry point by router), related metric that will alert us if it happens?

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
	return ctrl.Result{Requeue: true, RequeueAfter: 15 * time.Second}, nil
}

func (r *RedisClusterReconciler) saveClusterStateOnSigTerm(redisCluster *dbv1.RedisCluster) {
	if r.RedisClusterStateView != nil {
		saveStatusOnQuit := make(chan os.Signal, 1)
		signal.Notify(saveStatusOnQuit, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGSEGV, syscall.SIGKILL)
		go func() {
			<-saveStatusOnQuit
			close(saveStatusOnQuit)
			r.Log.Info("[WARN] reconcile loop interrupted by os signal, saving cluster state view...")
			r.saveClusterStateView(redisCluster)
		}()
	}
}

func (r *RedisClusterReconciler) saveClusterState(redisCluster *dbv1.RedisCluster) {
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
		n.Pod = corev1.Pod{}
	}
	data, _ := json.MarshalIndent(v, "", "")
	clusterData.SaveRedisClusterView(data)
	clusterData.SaveRedisClusterState(redisCluster.Status.ClusterState)
	r.saveClusterState(redisCluster)
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
		redisCluster.Status.ClusterState = string(Recovering)
		return nil
	}
	println("s1")
	lostNodesDetected := r.forgetLostNodes(redisCluster, v)
	if lostNodesDetected {
		r.Log.Info("[Warn] Lost nodes detcted on some of the nodes tables...")
		return nil
	}
	println("s2")
	healthy, err := r.isClusterHealthy(redisCluster, v)
	if err != nil {
		r.Log.Info("Could not check if cluster is healthy")
		return err
	}
	println("s3")
	if !healthy {
		redisCluster.Status.ClusterState = string(Recovering)
		return nil
	}
	println("s4")
	uptodate, err := r.isClusterUpToDate(redisCluster, v)
	if err != nil {
		r.Log.Info("Could not check if cluster is updated")
		redisCluster.Status.ClusterState = string(Recovering)
		return err
	}
	println("s5")
	if !uptodate {
		redisCluster.Status.ClusterState = string(Updating)
		return nil
	}
	println("s6")
	scale, scaleType := r.isScaleRequired(redisCluster)
	if scale {
		r.Log.Info(fmt.Sprintf("Scale is required, scale type: [%v]", scaleType.String()))
		redisCluster.Status.ClusterState = string(Scale)
	}
	r.Log.Info("Cluster is healthy")
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
		return errors.New("Could not perform redis cluster recovery, error while fetching cluster view")
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
	if err = r.updateCluster(redisCluster); err != nil {
		r.Log.Info("Rolling update failed")
	}
	redisCluster.Status.ClusterState = string(Recovering)
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
	reconciler.saveClusterState(cluster)
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

func DoReconcile(c echo.Context) error {
	if reconciler == nil || cluster == nil {
		return c.String(http.StatusOK, "Could not perform cluster reconcile action")
	}
	reconciler.saveClusterStateView(cluster)
	_, err := reconciler.Reconcile(ctrl.Request{types.NamespacedName{Name: "dev-rdc", Namespace: "default"}})
	if err != nil {
		reconciler.Log.Error(err, "Could not perform reconcile trigger")
	}
	reconciler.saveClusterStateView(cluster)
	return c.String(http.StatusOK, "Reconcile request triggered")
}

func RunE2ETest(c echo.Context) error {
	if reconciler == nil || cluster == nil {
		return c.String(http.StatusOK, "Could not perform cluster e2e test")
	}
	ClusterTest()
	return c.String(http.StatusOK, "E2E test executed")
}

// Test lab

func InsertData(c echo.Context) error {
	var ctx = context.Background()
	node0 := redis.NewClient(&redis.Options{
		Addr:     "10.244.5.55:6379",
		Username: "admin",
		Password: "adminpass",
	})
	key := ":myKey"
	val := "myVal"
	err := node0.Set(ctx, key, val, 0).Err()
	if err != nil {
		println("Set node0 error: " + err.Error())
	}
	v0, err := node0.Get(ctx, key).Result()
	println("Get node0 result: " + string(v0))
	if err != nil {
		println("Get node0 error: " + err.Error())
	}
	node1 := redis.NewClient(&redis.Options{
		Addr:     "10.244.6.10:6379",
		Username: "admin",
		Password: "adminpass",
	})
	v1, err := node1.Get(ctx, key).Result()
	println("Get node1 result: " + string(v1))
	if err != nil {
		println("Get node1 error: " + err.Error())
	}
	return c.String(http.StatusOK, "OK")
}

var clusterHealthCheckInterval = 30 * time.Second
var clusterHealthCheckTimeOutLimit = 5 * time.Minute
var report string = "\n[TEST LAB] Cluster test report:\n"

func ClusterTest() {
	var wg sync.WaitGroup
	wg.Add(1)
	go test_cluster_e2e(&wg)
	wg.Wait()
}

func test_cluster_e2e(wg *sync.WaitGroup) {
	defer wg.Done()
	isReady := waitForHealthyCluster(reconciler, cluster)
	if !isReady {
		println(report)
		return
	}
	report += "\n"
	test_1 := test_delete_follower(reconciler, cluster)
	report += fmt.Sprintf("\n[TEST LAB] Test 1: delete follower result [%v]\n", test_1)
	if !test_1 {
		println(report)
		return
	}
	time.Sleep(5 * time.Second)
	test_2 := test_delete_leader(reconciler, cluster)
	report += fmt.Sprintf("\n[TEST LAB] Test 2: delete leader result [%v]\n", test_2)
	if !test_2 {
		println(report)
		return
	}
	time.Sleep(5 * time.Second)
	test_3 := test_delete_leader_and_follower(reconciler, cluster)
	report += fmt.Sprintf("\n[TEST LAB] Test 3: delete leader and follower result [%v]\n", test_3)
	if !test_3 {
		println(report)
		return
	}
	time.Sleep(5 * time.Second)
	test_4 := test_delete_all_followers(reconciler, cluster)
	report += fmt.Sprintf("\n[TEST LAB] Test 4: delete all followers result [%v]\n", test_4)
	if !test_4 {
		println(report)
		return
	}
	time.Sleep(5 * time.Second)
	test_5 := test_delete_all_azs_beside_one(reconciler, cluster)
	report += fmt.Sprintf("\n[TEST LAB] Test 5: delete all pods beside one replica foreach set result [%v]\n", test_5)
	if !test_5 {
		println(report)
		return
	}
	time.Sleep(5 * time.Second)
	test_6 := test_delete_leader_and_all_its_followers(reconciler, cluster)
	report += fmt.Sprintf("\n[TEST LAB] Test 6: delete leader and all its followers result [%v]\n", test_6)
	if !test_6 {
		println(report)
		return
	}
	println(report)
}

func waitForHealthyCluster(r *RedisClusterReconciler, c *dbv1.RedisCluster) bool {
	if r == nil || c == nil {
		return false
	}
	report += fmt.Sprintf("\n[TEST LAB] Waiting for cluster to be declared healthy...")
	isHealthyCluster := false
	if pollErr := wait.PollImmediate(clusterHealthCheckInterval, clusterHealthCheckTimeOutLimit, func() (bool, error) {
		isHealthyCluster = isClusterAligned(r, c)
		return isHealthyCluster, nil
	}); pollErr != nil {
		report += fmt.Sprintf("\n[TEST LAB] Error while waiting for cluster to heal, probe intervals: [%v * sec], probe timeout: [%v * min]\n", clusterHealthCheckInterval, clusterHealthCheckTimeOutLimit)
		return false
	}
	return isHealthyCluster
}

func isClusterAligned(r *RedisClusterReconciler, c *dbv1.RedisCluster) bool {
	report += fmt.Sprintf("\n[TEST LAB] Checking if cluster aligned...")
	v, viewIsReady := r.NewRedisClusterView(c)
	if !viewIsReady {
		report += fmt.Sprintf("\n[TEST LAB] Alignment check result : %v", false)
		return false
	}
	var wg sync.WaitGroup
	mutex := &sync.Mutex{}
	aligned := true
	for _, n := range r.RedisClusterStateView.Nodes {
		wg.Add(1)
		go func(n *view.NodeStateView, wg *sync.WaitGroup) {
			time.Sleep(3 * time.Second)
			mutex.Lock()
			defer wg.Done()
			mutex.Unlock()
			if !aligned {
				return
			}
			node, exists := v.Nodes[n.Name]
			if !exists || node == nil {
				mutex.Lock()
				aligned = false
				mutex.Unlock()
				return
			}
			if node.IsLeader {
				isMaster, e := r.checkIfMaster(node.Ip)
				if e != nil || !isMaster {
					mutex.Lock()
					aligned = false
					mutex.Unlock()
					return
				}
				nodes, _, e := r.RedisCLI.ClusterNodes(node.Ip)
				if e != nil || nodes == nil || len(*nodes) <= 1 {
					mutex.Lock()
					aligned = false
					mutex.Unlock()
					return
				}
			} else {
				_, leaderExists := r.RedisClusterStateView.Nodes[n.LeaderName]
				if !leaderExists {
					mutex.Lock()
					aligned = false
					mutex.Unlock()
					return
				}
				isMaster, e := r.checkIfMaster(node.Ip)
				if e != nil || isMaster {
					mutex.Lock()
					aligned = false
					mutex.Unlock()
					return
				}
			}
			aligned = (n.NodeState == view.NodeOK)
		}(n, &wg)
	}
	wg.Wait()
	totalExpectedNodes := c.Spec.LeaderCount * (c.Spec.LeaderFollowersCount + 1)
	clusterOK := aligned && len(v.Nodes) == totalExpectedNodes && len(r.RedisClusterStateView.Nodes) == totalExpectedNodes
	if clusterOK {
		r.waitForAllNodesAgreeAboutSlotsConfiguration(v)
		report += fmt.Sprintf("\n[TEST LAB] Alignment check result : %v", clusterOK)
	}
	return clusterOK
}

func test_delete_pods(r *RedisClusterReconciler, c *dbv1.RedisCluster, podsToDelete []string) bool {
	v, viewIsReady := r.NewRedisClusterView(c)
	if !viewIsReady {
		return false
	}
	for _, toDelete := range podsToDelete {
		node, exists := v.Nodes[toDelete]
		if !exists || node == nil {
			continue
		}
		time.Sleep(3 * time.Second)
		r.deletePod(node.Pod)
	}
	return waitForHealthyCluster(r, c)
}

func pickRandomeFollower(exclude map[string]bool, r *RedisClusterReconciler, c *dbv1.RedisCluster, retry int) string {
	k := rand.Intn(c.Spec.LeaderFollowersCount*c.Spec.LeaderCount - len(exclude))
	i := 0
	for _, n := range r.RedisClusterStateView.Nodes {
		if _, ex := exclude[n.Name]; ex || n.Name == n.LeaderName {
			continue
		}
		if i == k {
			return n.Name
		}
		i++
	}
	if retry == 0 {
		return ""
	}
	return pickRandomeFollower(exclude, r, c, retry-1)
}

func pickRandomeLeader(exclude map[string]bool, r *RedisClusterReconciler, c *dbv1.RedisCluster, retry int) string {
	k := rand.Intn(c.Spec.LeaderCount - len(exclude))
	i := 0
	for _, n := range r.RedisClusterStateView.Nodes {
		if _, ex := exclude[n.Name]; ex || n.Name != n.LeaderName {
			continue
		}
		if i == k {
			return n.Name
		}
		i++
	}
	if retry == 0 {
		return ""
	}
	return pickRandomeLeader(exclude, r, c, retry-1)
}

func test_delete_follower(r *RedisClusterReconciler, c *dbv1.RedisCluster) bool {
	report += fmt.Sprintf("\n[TEST LAB] Test delete follower...")
	randomFollower := pickRandomeFollower(map[string]bool{}, r, c, 5)
	return test_delete_pods(r, c, []string{randomFollower})
}

func test_delete_leader(r *RedisClusterReconciler, c *dbv1.RedisCluster) bool {
	report += fmt.Sprintf("\n[TEST LAB] Test delete leader...")
	randomLeader := pickRandomeLeader(map[string]bool{}, r, c, 5)
	return test_delete_pods(r, c, []string{randomLeader})
}

func test_delete_leader_and_follower(r *RedisClusterReconciler, c *dbv1.RedisCluster) bool {
	report += fmt.Sprintf("\n[TEST LAB] Test delete leader and follower...")
	randomFollower := pickRandomeFollower(map[string]bool{}, r, c, 5)
	f, exists := r.RedisClusterStateView.Nodes[randomFollower]
	if !exists {
		return false
	}
	randomLeader := pickRandomeLeader(map[string]bool{f.LeaderName: true}, r, c, 5)
	return test_delete_pods(r, c, []string{randomFollower, randomLeader})
}

func test_delete_all_followers(r *RedisClusterReconciler, c *dbv1.RedisCluster) bool {
	report += fmt.Sprintf("\n[TEST LAB] Test delete all followers...")
	followers := []string{}
	for _, n := range r.RedisClusterStateView.Nodes {
		if n.Name != n.LeaderName {
			followers = append(followers, n.Name)
		}
	}
	return test_delete_pods(r, c, followers)
}

func test_delete_leader_and_all_its_followers(r *RedisClusterReconciler, c *dbv1.RedisCluster) bool {
	report += fmt.Sprintf("\n[TEST LAB] Test delete leader and all his followers...")
	toDelete := []string{}
	randomFollower := pickRandomeFollower(map[string]bool{}, r, c, 5)
	f, exists := r.RedisClusterStateView.Nodes[randomFollower]
	if !exists {
		return false
	}
	for _, n := range r.RedisClusterStateView.Nodes {
		if n.LeaderName == f.LeaderName {
			toDelete = append(toDelete, n.Name)
		}
	}
	return test_delete_pods(r, c, toDelete)
}

func test_delete_all_azs_beside_one(r *RedisClusterReconciler, c *dbv1.RedisCluster) bool {
	report += fmt.Sprintf("\n[TEST LAB] Test delete all pods beside one replica foreach set...")
	del := map[string]bool{}
	keep := map[string]bool{}
	d := false
	for _, n := range r.RedisClusterStateView.Nodes {
		if n.Name == n.LeaderName {
			del[n.Name] = d
			d = !d
		}
	}
	for _, n := range r.RedisClusterStateView.Nodes {
		if n.Name != n.LeaderName {
			delLeader, leaderInMap := del[n.LeaderName]
			if !leaderInMap {
				del[n.Name] = false
				keep[n.LeaderName] = true
			} else {
				if delLeader {
					_, hasReplicaToKeep := keep[n.LeaderName]
					if hasReplicaToKeep {
						del[n.Name] = true
					} else {
						del[n.Name] = false
						keep[n.LeaderName] = true
					}
				} else {
					del[n.Name] = true
				}
			}
		}
	}
	toDelete := []string{}
	for n, d := range del {
		if d {
			toDelete = append(toDelete, n)
		}
	}
	return test_delete_pods(r, c, toDelete)
}
