package main

import (
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	dbv1 "github.com/PayU/Redis-Operator/api/v1"
	"github.com/PayU/Redis-Operator/controllers"
	"github.com/PayU/Redis-Operator/controllers/rediscli"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	_ = clientgoscheme.AddToScheme(scheme)

	_ = dbv1.AddToScheme(scheme)
	// +kubebuilder:scaffold:scheme
}

// used in zap logger in order to configure settings
func loggerOptions(*zap.Options) {}

func main() {
	var metricsAddr, namespace, enableLeaderElection string
	flag.StringVar(&metricsAddr, "metrics-addr", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&namespace, "namespace", "default", "The namespace the operator will manage.")
	flag.StringVar(&enableLeaderElection, "enable-leader-election", "true",
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.Parse()

	ctrl.SetLogger(zap.New(loggerOptions))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:             scheme,
		Namespace:          namespace,
		MetricsBindAddress: metricsAddr,
		Port:               9443,
		LeaderElection:     enableLeaderElection == "true",
		LeaderElectionID:   "1747e98e.payu.com",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	log := ctrl.Log.WithName("controllers").WithName("RedisCluster")

	if err = (&controllers.RedisClusterReconciler{
		Client:   mgr.GetClient(),
		Log:      log,
		Scheme:   mgr.GetScheme(),
		RedisCLI: rediscli.NewRedisCLI(log),
		State:    controllers.NotExists,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "RedisCluster")
		os.Exit(1)
	}
	// +kubebuilder:scaffold:builder

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
