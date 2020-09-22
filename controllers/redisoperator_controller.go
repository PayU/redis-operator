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
)

// RedisOperatorReconciler reconciles a RedisOperator object
type RedisOperatorReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=db.payu.com,resources=redisoperators,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=db.payu.com,resources=redisoperators/status,verbs=get;update;patch

func (r *RedisOperatorReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx := context.Background()
	log := r.Log.WithValues("redisoperator", req.NamespacedName)
	log.Info("Starting redis-operator reconciling")

	/*
		### 1: Load the redis operator by name
		We'll fetch the RO using our client. All client methods take a
		context (to allow for cancellation) as their first argument, and the object
		in question as their last.  Get is a bit special, in that it takes a
		[`NamespacedName`](https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/client?tab=doc#ObjectKey)
		as the middle argument (most don't have a middle argument, as we'll see below).
	*/
	var redisOperator dbv1.RedisOperator
	var err error

	if err := r.Get(ctx, req.NamespacedName, &redisOperator); err != nil {
		log.Error(err, "unable to fetch Redis Operator")
		// we'll ignore not-found errors, since they can't be fixed by an immediate
		// requeue (we'll need to wait for a new notification), and we can get them
		// on deleted requests.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	/*
		### 2: act according to the current state
	*/
	clusterState := computeCurrentClusterState(r.Log, &redisOperator)

	switch clusterState {
	case NotExists:
		err = r.createNewCluster(ctx, &redisOperator)
		break
	case Deploying:
		err = r.handleDeployingCluster(ctx, &redisOperator)
		break
	}

	if err != nil {
		return ctrl.Result{}, err
	}

	/*
		### 3: Update the current status
	*/
	log.Info(fmt.Sprintf("update cluster state to:%s", redisOperator.Status.ClusterState))
	if err = r.Status().Update(ctx, &redisOperator); err != nil {
		if !strings.Contains(err.Error(), "please apply your changes to the latest version") {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *RedisOperatorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&dbv1.RedisOperator{}).
		Owns(&corev1.Pod{}).
		Complete(r)
}
