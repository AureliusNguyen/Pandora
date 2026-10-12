// Package httpx provides the per-service metrics/health server and a
// helper that runs several http.Servers with graceful shutdown.
package httpx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ShutdownTimeout bounds graceful shutdown in Run.
var ShutdownTimeout = 10 * time.Second

// Readiness is an atomic ready flag. The zero value is "not ready".
type Readiness struct{ ready atomic.Bool }

// SetReady sets the flag.
func (r *Readiness) SetReady(v bool) { r.ready.Store(v) }

// Ready reports the flag.
func (r *Readiness) Ready() bool { return r.ready.Load() }

// NewMetricsServer serves /metrics from g, /healthz (always 200) and
// /readyz (200 when ready, else 503) on addr.
func NewMetricsServer(addr string, g prometheus.Gatherer, ready *Readiness) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(g, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeText(w, http.StatusOK, "ok")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if ready.Ready() {
			writeText(w, http.StatusOK, "ready")
			return
		}
		writeText(w, http.StatusServiceUnavailable, "not ready")
	})
	return &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
}

func writeText(w http.ResponseWriter, code int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(code)
	_, _ = io.WriteString(w, body+"\n")
}

// Run starts every server and blocks until ctx is done or any server fails
// (for example the port is in use). Then it shuts all servers down within
// ShutdownTimeout. A clean ctx-driven stop returns nil.
func Run(ctx context.Context, servers ...*http.Server) error {
	errCh := make(chan error, len(servers))
	for _, s := range servers {
		go func() {
			if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- fmt.Errorf("httpx: serve %s: %w", s.Addr, err)
			}
		}()
	}
	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-errCh:
	}
	shutCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ShutdownTimeout)
	defer cancel()
	errs := []error{runErr}
	for _, s := range servers {
		if err := s.Shutdown(shutCtx); err != nil {
			errs = append(errs, fmt.Errorf("httpx: shutdown %s: %w", s.Addr, err))
		}
	}
	return errors.Join(errs...)
}
