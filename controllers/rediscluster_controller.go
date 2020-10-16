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
	"strings"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dbv1 "github.com/PayU/Redis-Operator/api/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/PayU/Redis-Operator/controllers/rediscli"
)

// RedisClusterReconciler reconciles a RedisCluster object
type RedisClusterReconciler struct {
	client.Client
	Log      logr.Logger
	Scheme   *runtime.Scheme
	RedisCLI *rediscli.RedisCLI
	State    RedisClusterState
}

// +kubebuilder:rbac:groups=db.payu.com,resources=redisclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=db.payu.com,resources=redisclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=*,resources=pods;services;configmaps,verbs=create;update;patch;get;list;watch;delete

func (r *RedisClusterReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx := context.Background()
	log := r.Log.WithValues("rediscluster", req.NamespacedName)
	log.Info("Starting redis-cluster reconciling")

	/*
		### 1: Load the redis operator by name
		We'll fetch the RO using our client. All client methods take a
		context (to allow for cancellation) as their first argument, and the object
		in question as their last.  Get is a bit special, in that it takes a
		[`NamespacedName`](https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/client?tab=doc#ObjectKey)
		as the middle argument (most don't have a middle argument, as we'll see below).
	*/
	var redisCluster dbv1.RedisCluster
	var err error

	if err := r.Get(ctx, req.NamespacedName, &redisCluster); err != nil {
		log.Error(err, "unable to fetch Redis Operator")
		// we'll ignore not-found errors, since they can't be fixed by an immediate
		// requeue (we'll need to wait for a new notification), and we can get them
		// on deleted requests.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	/*
		### 2: act according to the current state
	*/
	clusterState := getCurrentClusterState(r.Log, &redisCluster)

	switch clusterState {
	case NotExists:
		err = r.createNewCluster(ctx, &redisCluster)
		break
	case DeployingLeaders:
		err = r.handleDeployingLeaders(ctx, &redisCluster)
		break
	case InitializingLeaders:
		err = r.handleInitializingLeaders(ctx, &redisCluster)
		break
	case ClusteringLeaders:
		err = r.handleClusteringLeaders(ctx, &redisCluster)
		break
	case DeployingFollowers:
		err = r.handleDeployingFollowers(ctx, &redisCluster)
		break
	case InitializingFollowers:
		err = r.handleInitializingFollowers(ctx, &redisCluster)
		break
	case ClusteringFollowers:
		err = r.handleClusteringFollowers(ctx, &redisCluster)
		break
	}

	if err != nil {
		return ctrl.Result{}, err
	}

	/*
		### 3: Update the current status
	*/
	clusterState = getCurrentClusterState(r.Log, &redisCluster)
	if clusterState != r.State {
		log.Info(fmt.Sprintf("update cluster state to: %s", redisCluster.Status.ClusterState))
		if err = r.Status().Update(ctx, &redisCluster); err != nil {
			if !strings.Contains(err.Error(), "please apply your changes to the latest version") {
				return ctrl.Result{}, err
			}
		}
		r.State = clusterState
	}

	return ctrl.Result{}, nil
}

func (r *RedisClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&dbv1.RedisCluster{}).
		Owns(&corev1.Pod{}).
		Complete(r)
}
