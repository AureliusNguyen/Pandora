// Command gateway is the public REST edge: it validates orders and forwards
// them to the matcher over connect-go.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/aureliusnguyen/pandora/backend/internal/gateway"
	"github.com/aureliusnguyen/pandora/backend/internal/pkg/config"
	"github.com/aureliusnguyen/pandora/backend/internal/pkg/httpx"
	"github.com/aureliusnguyen/pandora/backend/internal/pkg/logx"
	"github.com/aureliusnguyen/pandora/backend/internal/pkg/otelx"
)

// version is overridden at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() { os.Exit(run()) }

func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg, err := config.Load[config.Gateway]()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	log, err := logx.Setup("gateway", cfg.LogLevel)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	log = log.With("version", version)
	tel, err := otelx.Setup(ctx, otelx.Options{ServiceName: "gateway", OTLPEndpoint: cfg.OTLPEndpoint})
	if err != nil {
		log.Error("otel", "err", err)
		return 2
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tel.Shutdown(sctx)
	}()
	client, err := gateway.NewMatcherClient(cfg.MatcherAddr)
	if err != nil {
		log.Error("matcher client", "err", err)
		return 2
	}

	// internal/gateway registers pandora_gateway_* with promauto on the default
	// registry; serve it next to the otelx registry minus the duplicate Go and
	// process collectors.
	prometheus.Unregister(collectors.NewGoCollector())
	prometheus.Unregister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	gatherer := prometheus.Gatherers{tel.Registry, prometheus.DefaultGatherer}

	// Stateless edge: ready as soon as it serves; matcher outages map to 503 per request.
	var ready httpx.Readiness
	ready.SetReady(true)
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           gateway.NewServer(client, log).WithAllowedOrigins(cfg.CORSOrigins).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Info("gateway listening", "addr", cfg.HTTPAddr, "matcher", cfg.MatcherAddr, "cors_origins", cfg.CORSOrigins)
	if err := httpx.Run(ctx, srv, httpx.NewMetricsServer(cfg.MetricsAddr, gatherer, &ready)); err != nil {
		log.Error("server", "err", err)
		return 1
	}
	return 0
}
