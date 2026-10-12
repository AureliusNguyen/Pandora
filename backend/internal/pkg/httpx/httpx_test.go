package httpx_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/aureliusnguyen/pandora/backend/internal/pkg/httpx"
)

func get(t *testing.T, srv *http.Server, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, http.NoBody))
	return rec
}

func TestReadyzToggles(t *testing.T) {
	var ready httpx.Readiness
	srv := httpx.NewMetricsServer(":0", prometheus.NewRegistry(), &ready)
	if c := get(t, srv, "/readyz").Code; c != http.StatusServiceUnavailable {
		t.Fatalf("initial /readyz = %d, want 503", c)
	}
	ready.SetReady(true)
	if c := get(t, srv, "/readyz").Code; c != http.StatusOK {
		t.Fatalf("/readyz after SetReady(true) = %d, want 200", c)
	}
	ready.SetReady(false)
	if c := get(t, srv, "/readyz").Code; c != http.StatusServiceUnavailable {
		t.Fatalf("/readyz after SetReady(false) = %d, want 503", c)
	}
	if c := get(t, srv, "/healthz").Code; c != http.StatusOK {
		t.Fatalf("/healthz = %d, want 200", c)
	}
}

func TestMetricsServesRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := prometheus.NewCounter(prometheus.CounterOpts{Namespace: "pandora", Name: "httpx_test_total", Help: "test"})
	reg.MustRegister(c)
	c.Inc()
	rec := get(t, httpx.NewMetricsServer(":0", reg, &httpx.Readiness{}), "/metrics")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "pandora_httpx_test_total 1") {
		t.Fatalf("/metrics = %d %q", rec.Code, rec.Body.String())
	}
}

func runAsync(ctx context.Context, servers ...*http.Server) <-chan error {
	done := make(chan error, 1)
	go func() { done <- httpx.Run(ctx, servers...) }()
	return done
}

func TestRunStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	srv := &http.Server{Addr: "127.0.0.1:0", Handler: http.NotFoundHandler(), ReadHeaderTimeout: time.Second}
	done := runAsync(ctx, srv)
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run after cancel = %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestRunReturnsBindErrorAndStopsOthers(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	busy := &http.Server{Addr: ln.Addr().String(), Handler: http.NotFoundHandler(), ReadHeaderTimeout: time.Second}
	ok := &http.Server{Addr: "127.0.0.1:0", Handler: http.NotFoundHandler(), ReadHeaderTimeout: time.Second}
	select {
	case err := <-runAsync(context.Background(), ok, busy):
		if err == nil || !strings.Contains(err.Error(), ln.Addr().String()) {
			t.Fatalf("Run = %v, want bind error naming %s", err, ln.Addr())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run blocked despite bind failure")
	}
}
