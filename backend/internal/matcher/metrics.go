package matcher

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
)

// Metrics are the contract's pandora_matcher_* series.
type Metrics struct {
	Commands        *prometheus.CounterVec   // commands_total{type}
	WALProduce      prometheus.Histogram     // wal_produce_seconds
	Apply           prometheus.Histogram     // apply_seconds (per batch)
	Request         *prometheus.HistogramVec // request_seconds{rpc}
	LastSeq         *prometheus.GaugeVec     // last_seq{partition}
	BatchSize       prometheus.Histogram     // batch_size
	PublishFailures prometheus.Counter       // events_publish_failures_total
}

var latencyBuckets = []float64{.0001, .00025, .0005, .001, .0025, .005, .01, .02, .05, .1, .25, .5, 1, 2.5}

// NewMetrics registers the matcher series on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	opts := func(name, help string) prometheus.HistogramOpts {
		return prometheus.HistogramOpts{Namespace: "pandora", Subsystem: "matcher", Name: name, Help: help, Buckets: latencyBuckets}
	}
	return &Metrics{
		Commands:   f.NewCounterVec(prometheus.CounterOpts{Namespace: "pandora", Subsystem: "matcher", Name: "commands_total", Help: "Commands applied by type."}, []string{"type"}),
		WALProduce: f.NewHistogram(opts("wal_produce_seconds", "Time to produce one WAL batch and receive the ack.")),
		Apply:      f.NewHistogram(opts("apply_seconds", "Time to apply one batch to the engine.")),
		Request:    f.NewHistogramVec(opts("request_seconds", "RPC handler latency."), []string{"rpc"}),
		LastSeq:    f.NewGaugeVec(prometheus.GaugeOpts{Namespace: "pandora", Subsystem: "matcher", Name: "last_seq", Help: "Last applied seq."}, []string{"partition"}),
		BatchSize: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: "pandora", Subsystem: "matcher", Name: "batch_size", Help: "Requests per WAL batch.",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256},
		}),
		PublishFailures: f.NewCounter(prometheus.CounterOpts{
			Namespace: "pandora", Subsystem: "matcher", Name: "events_publish_failures_total",
			Help: "Failed engine.events publishes; each one is retried until it succeeds.",
		}),
	}
}

// commandType is the commands_total{type} label.
func commandType(cmd *enginev1.Command) string {
	switch cmd.Body.(type) {
	case *enginev1.Command_SubmitOrder:
		return "submit_order"
	case *enginev1.Command_CancelOrder:
		return "cancel_order"
	case *enginev1.Command_CreateMarket:
		return "create_market"
	case *enginev1.Command_LockMarket:
		return "lock_market"
	case *enginev1.Command_ResolveMarket:
		return "resolve_market"
	case *enginev1.Command_VoidMarket:
		return "void_market"
	}
	return "unknown"
}
