package gateway

import (
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// DefaultAllowedOrigins matches the default of config.Gateway.CORSOrigins
// (env CORS_ORIGINS): local pnpm dev and the kind web NodePort.
var DefaultAllowedOrigins = []string{"http://localhost:3000", "http://localhost:30300"}

var requestSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "pandora", Subsystem: "gateway", Name: "request_seconds", Help: "Gateway request latency.",
	Buckets: []float64{0.001, 0.0025, 0.005, 0.01, 0.02, 0.04, 0.08, 0.16, 0.32, 0.64, 1.28, 2.56},
}, []string{"route", "status"})

// withCORS echoes allowlisted origins and answers preflights with 204.
func withCORS(origins []string, next http.Handler) http.Handler {
	allowed := make(map[string]bool, len(origins))
	for _, o := range origins {
		allowed[o] = true
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); allowed[origin] {
			h := w.Header()
			h.Set("Access-Control-Allow-Origin", origin)
			h.Add("Vary", "Origin")
			h.Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
			h.Set("Access-Control-Allow-Headers", "Content-Type, X-User-Id, traceparent, tracestate")
			h.Set("Access-Control-Max-Age", "600")
		}
		if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// statusRecorder captures the response status for logging and metrics.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

// WriteHeader records code and forwards it.
func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// observe must wrap the ServeMux directly: the mux sets r.Pattern on the same *http.Request.
func observe(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		d := time.Since(start)
		requestSeconds.WithLabelValues(route, strconv.Itoa(rec.status)).Observe(d.Seconds())
		log.Info("http", "method", r.Method, "path", r.URL.Path, "route", route, "status", rec.status,
			"dur_ms", float64(d.Microseconds())/1000, "user", r.Header.Get("X-User-Id"))
	})
}
