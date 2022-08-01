package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (

	NonHealthyReconcileLoopsMetric = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "non_healthy_reconcile_loops",
		Help: "Exposes number of non healthy reconcile loops in row since last healthy one",
	})

)

func init(){
	//prometheus.MustRegister(NonHealthyReconcileLoopsMetric)
	metrics.Registry.MustRegister(NonHealthyReconcileLoopsMetric)
}