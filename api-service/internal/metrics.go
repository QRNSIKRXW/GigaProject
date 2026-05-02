package internal

import "github.com/prometheus/client_golang/prometheus"

var (
	apiRunRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "api_run_requests_total",
			Help: "Total number of run requests by language and outcome",
		},
		[]string{"lang", "outcome"},
	)

	apiRunCodeSizeBytes = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "api_run_code_size_bytes",
			Help:    "Code size in bytes for run requests",
			Buckets: []float64{128, 512, 1024, 4096, 16384, 65536, 131072, 262144},
		},
		[]string{"lang"},
	)

	apiTaskEnqueueErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "api_task_enqueue_errors_total",
			Help: "Total number of task enqueue errors by language",
		},
		[]string{"lang"},
	)
)

func init() {
	prometheus.MustRegister(
		apiRunRequestsTotal,
		apiRunCodeSizeBytes,
		apiTaskEnqueueErrorsTotal,
	)
}
