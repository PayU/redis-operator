package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	dbv1 "github.com/PayU/redis-operator/api/v1"
	"github.com/PayU/redis-operator/controllers"
	"github.com/PayU/redis-operator/controllers/rediscli"
	"github.com/PayU/redis-operator/server"
	"github.com/go-logr/logr"
	// +kubebuilder:scaffold:imports
)

var scheme = runtime.NewScheme()

func init() {
	_ = clientgoscheme.AddToScheme(scheme)
	_ = dbv1.AddToScheme(scheme)
	// +kubebuilder:scaffold:scheme
}

func getRedisCLI(log *logr.Logger) *rediscli.RedisCLI {
	cli := rediscli.NewRedisCLI(log)
	user := os.Getenv("REDIS_USERNAME")
	if user != "" {
		cli.Auth = &rediscli.RedisAuth{
			User: user,
		}
	}
	return cli
}

func main() {
	go server.StartServer()

	startManager()
}

func startManager() {
	var metricsAddr, namespace, enableLeaderElection, devmode string

	flag.StringVar(&metricsAddr, "metrics-addr", "0.0.0.0:9808", "The address the metric endpoint binds to.")
	flag.StringVar(&namespace, "namespace", "default", "The namespace the operator will manage.")
	flag.StringVar(&devmode, "devmode", "false", "Development mode toggle.")
	flag.StringVar(&enableLeaderElection, "enable-leader-election", "true",
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.Parse()

	setupLogger := zap.New(zap.UseDevMode(devmode == "true")).WithName("setup")

	retryLockDuration := 4 * time.Second

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:             scheme,
		Namespace:          namespace,
		MetricsBindAddress: metricsAddr,
		Port:               9443,
		LeaderElection:     enableLeaderElection == "true",
		LeaderElectionID:   "1747e98e.payu.com",
		RetryPeriod:        &retryLockDuration,
	})
	if err != nil {
		setupLogger.Error(err, "failed to create new manager")
		os.Exit(1)
	}

	rdcLogger := zap.New(zap.UseDevMode(devmode == "true")).
		WithName("controllers").
		WithName("RedisCluster")
	configLogger := zap.New(zap.UseDevMode(devmode == "true")).
		WithName("controllers").
		WithName("RedisConfig")

	operatorConfig, err := controllers.NewRedisOperatorConfig("/usr/local/etc/operator.conf", setupLogger)
	if err != nil {
		setupLogger.Error(err, fmt.Sprintf("Failed to get operator config file, falling back to default config"))
		operatorConfig = controllers.DefaultRedisOperatorConfig(setupLogger)
		setupLogger.Info(fmt.Sprintf("Loaded config: %+v", operatorConfig.Config))
	}

	k8sManager := controllers.K8sManager{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Log:    configLogger}

	if err = (&controllers.RedisClusterReconciler{
		Client:               mgr.GetClient(),
		Log:                  rdcLogger,
		Scheme:               mgr.GetScheme(),
		RedisCLI:             getRedisCLI(&rdcLogger),
		Config:               &operatorConfig.Config,
		State:                controllers.NotExists,
		ClusterStatusMapName: "cluster-status-map",
	}).SetupWithManager(mgr); err != nil {
		setupLogger.Error(err, "unable to create controller", "controller", "RedisCluster")
		os.Exit(1)
	}

	if err = (&controllers.RedisConfigReconciler{
		Client:     mgr.GetClient(),
		Log:        configLogger,
		K8sManager: &k8sManager,
		Scheme:     mgr.GetScheme(),
		Config:     &operatorConfig.Config,
		RedisCLI:   getRedisCLI(&configLogger),
	}).SetupWithManager(mgr); err != nil {
		setupLogger.Error(err, "unable to create controller", "controller", "RedisConfig")
		os.Exit(1)
	}

	operatorConfig.Log = configLogger

	// +kubebuilder:scaffold:builder

	setupLogger.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLogger.Error(err, "failed to start the manager")
		os.Exit(1)
	}
}
