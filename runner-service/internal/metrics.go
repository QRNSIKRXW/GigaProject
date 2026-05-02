package internal

import "github.com/prometheus/client_golang/prometheus"

var (
	runnerRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "runner_requests_total",
			Help: "Total number of runner requests by language and outcome",
		},
		[]string{"lang", "outcome"},
	)

	runnerRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "runner_request_duration_seconds",
			Help:    "Runner request duration by language and outcome",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"lang", "outcome"},
	)

	runnerValidationErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "runner_validation_errors_total",
			Help: "Total number of validation errors in runner requests",
		},
		[]string{"reason"},
	)
)

func init() {
	prometheus.MustRegister(
		runnerRequestsTotal,
		runnerRequestDuration,
		runnerValidationErrorsTotal,
	)
}
