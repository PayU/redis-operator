// +build e2e_redis_op

package framework

import (
	"fmt"

	"github.com/pkg/errors"

	apiext "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/runtime"
	apimachineryversion "k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dbv1 "github.com/PayU/Redis-Operator/api/v1"
	"github.com/PayU/Redis-Operator/controllers/rediscli"
)

// Testing framework for integration and e2e tests.
type Framework struct {
	K8sServerVersion *apimachineryversion.Info
	RuntimeClient    client.Client
	KubeClient       kubernetes.Interface
	ExtentionClient  apiextclient.Interface
	RedisCLI         *rediscli.RedisCLI
}

// A new framework instance is initialised with a cluster configuration that
// can be provided as a flag.
func NewFramework(kubeconfig, opImage string) (*Framework, error) {

	scheme := runtime.NewScheme()
	clientgoscheme.AddToScheme(scheme)
	dbv1.AddToScheme(scheme)
	apiext.AddToScheme(scheme)
	apiextv1.AddToScheme(scheme)
	apiextv1beta1.AddToScheme(scheme)
	scheme.SetVersionPriority(apiextv1beta1.SchemeGroupVersion, apiextv1.SchemeGroupVersion)

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, errors.Wrap(err, "build config from flags failed")
	}

	cli, err := client.New(config, client.Options{
		Scheme: scheme,
	})
	if err != nil {
		return nil, errors.Wrap(err, "creating new runtime client failed")
	}

	goCli, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "creating new kube client failed")
	}

	extCli, err := apiextclient.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "creating new apiextensions client failed")
	}

	redisCLI := rediscli.NewRedisCLI(ctrl.Log.WithName("redis-op-testing"))

	serverVersion, err := GetServerVersion(goCli.RESTClient())
	if err != nil {
		return nil, errors.Wrap(err, "could not get Kubernetes server version")
	}

	fmt.Printf("[E2E] Kubernetes server version: %s\n", serverVersion)

	return &Framework{
		RuntimeClient:    cli,
		KubeClient:       goCli,
		ExtentionClient:  extCli,
		RedisCLI:         redisCLI,
		K8sServerVersion: serverVersion,
	}, nil
}

func GetServerVersion(discoveryClient rest.Interface) (*apimachineryversion.Info, error) {
	discoverer := discovery.NewDiscoveryClient(discoveryClient)
	return discoverer.ServerVersion()
}
