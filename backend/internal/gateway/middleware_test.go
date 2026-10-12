package gateway

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func preflight(h http.Handler, origin string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("OPTIONS", "/v1/orders", nil)
	req.Header.Set("Origin", origin)
	req.Header.Set("Access-Control-Request-Method", "POST")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestCORSPreflight(t *testing.T) {
	h := newTestGateway(t, &fakeMatcher{})
	// Defaults: local pnpm dev (:3000) and the kind web NodePort (:30300).
	for _, origin := range []string{"http://localhost:3000", "http://localhost:30300"} {
		rec := preflight(h, origin)
		if rec.Code != 204 || rec.Header().Get("Access-Control-Allow-Origin") != origin ||
			!strings.Contains(rec.Header().Get("Access-Control-Allow-Headers"), "X-User-Id") {
			t.Fatalf("preflight %s: %d %v", origin, rec.Code, rec.Header())
		}
	}
	req := httptest.NewRequest("GET", "/v1/markets", nil)
	req.Header.Set("Origin", "http://evil.example")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("foreign origin must not be allowed")
	}
}

func TestCORSOriginsOverride(t *testing.T) {
	// Preflights never reach the matcher, so a nil client is fine here.
	h := NewServer(nil, slog.New(slog.NewTextHandler(io.Discard, nil))).WithAllowedOrigins([]string{"http://x.test"}).Handler()
	if got := preflight(h, "http://x.test").Header().Get("Access-Control-Allow-Origin"); got != "http://x.test" {
		t.Fatalf("override origin not allowed: %q", got)
	}
	if got := preflight(h, "http://localhost:3000").Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("override must replace the defaults, got %q", got)
	}
}

func TestRequestMetricLabelsByPattern(t *testing.T) {
	h := newTestGateway(t, &fakeMatcher{})
	do(h, "GET", "/v1/markets/mkt_demo/book", "", "")
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "pandora_gateway_request_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if labels["route"] == "GET /v1/markets/{market_id}/book" && labels["status"] == "200" && m.GetHistogram().GetSampleCount() > 0 {
				return
			}
		}
	}
	t.Fatal("no pandora_gateway_request_seconds sample for the book route")
}
