// Command fanout streams book snapshots and deltas over WebSocket. At start
// it records engine.events end offsets, loads matcher GetBook snapshots, then
// consumes engine.events (no consumer group) from those offsets.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/aureliusnguyen/pandora/backend/gen/go/pandora/matcher/v1/matcherv1connect"
	"github.com/aureliusnguyen/pandora/backend/internal/fanout"
	"github.com/aureliusnguyen/pandora/backend/internal/pkg/config"
	"github.com/aureliusnguyen/pandora/backend/internal/pkg/httpx"
	"github.com/aureliusnguyen/pandora/backend/internal/pkg/kafkax"
	"github.com/aureliusnguyen/pandora/backend/internal/pkg/logx"
	"github.com/aureliusnguyen/pandora/backend/internal/pkg/otelx"
)

// version is overridden at build time with -ldflags "-X main.version=...".
var version = "dev"

// M1 bootstraps the single demo market; markets created later arrive as MarketCreated events.
var bootstrapMarkets = []string{"mkt_demo"}

func main() { os.Exit(run()) }

func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg, err := config.Load[config.Fanout]()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	log, err := logx.Setup("fanout", cfg.LogLevel)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	log = log.With("version", version)
	tel, err := otelx.Setup(ctx, otelx.Options{ServiceName: "fanout", OTLPEndpoint: cfg.OTLPEndpoint})
	if err != nil {
		log.Error("otel", "err", err)
		return 2
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tel.Shutdown(sctx)
	}()
	brokers, err := kafkax.CleanBrokers(cfg.Brokers)
	if err != nil {
		log.Error("kafka brokers", "err", err)
		return 2
	}

	// internal/fanout registers pandora_fanout_* with promauto on the default
	// registry; serve it next to the otelx registry minus the duplicate Go and
	// process collectors.
	prometheus.Unregister(collectors.NewGoCollector())
	prometheus.Unregister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	gatherer := prometheus.Gatherers{tel.Registry, prometheus.DefaultGatherer}

	// /healthz answers during bootstrap; /readyz flips once the book view is
	// loaded. A metrics bind error stops the whole process (exit 1).
	var ready httpx.Readiness
	opsErr := make(chan error, 1)
	go func() {
		err := httpx.Run(ctx, httpx.NewMetricsServer(cfg.MetricsAddr, gatherer, &ready))
		if err != nil {
			log.Error("metrics server", "err", err)
			stop()
		}
		opsErr <- err
	}()

	hub := fanout.NewHub()
	matcher := matcherv1connect.NewMatcherServiceClient(&http.Client{Timeout: 2 * time.Second}, cfg.MatcherAddr)
	cl, err := fanout.Bootstrap(ctx, brokers, matcher, hub, bootstrapMarkets, log)
	if err != nil {
		interrupted := ctx.Err() != nil
		stop()
		if opsErr := <-opsErr; opsErr != nil {
			return 1
		}
		if interrupted { // SIGTERM while waiting for Kafka or the matcher
			log.Info("stopped during bootstrap")
			return 0
		}
		log.Error("bootstrap", "err", err)
		return 1
	}
	defer cl.Close()

	readerErr := make(chan error, 1)
	go func() {
		err := fanout.NewReader(cl, hub, log).Run(ctx)
		if ctx.Err() != nil {
			err = nil
		}
		if err != nil {
			log.Error("reader stopped", "err", err)
			stop()
		}
		readerErr <- err
	}()

	// No Read/WriteTimeout: they would cut long-lived WebSocket connections,
	// whose frames have their own write, ping and read deadlines.
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           fanout.NewWSServer(hub, log, cfg.WSOrigins).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	ready.SetReady(true)
	log.Info("fanout listening", "addr", cfg.HTTPAddr, "matcher", cfg.MatcherAddr)
	runErr := httpx.Run(ctx, srv)
	ready.SetReady(false)
	stop() // also stops the metrics server and reader if Run returned on a bind error
	if err := errors.Join(runErr, <-opsErr, <-readerErr); err != nil {
		log.Error("server", "err", err)
		return 1
	}
	return 0
}
