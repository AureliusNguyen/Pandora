// Command ledger consumes engine.events into the double-entry Postgres
// ledger. With --invariants it only runs the SQL invariant checks against
// PG_DSN and exits (used by `make invariants`). It never runs migrations:
// those run via `make migrate` locally and the goose init container in Helm.
//
// HTTP_ADDR (config.Ledger.HTTPAddr, default :9002) is unused in M1: the
// ledger serves only the metrics and health listener on METRICS_ADDR (:9090),
// which is what the Helm Service targets.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/aureliusnguyen/pandora/backend/internal/ledger"
	"github.com/aureliusnguyen/pandora/backend/internal/pkg/config"
	"github.com/aureliusnguyen/pandora/backend/internal/pkg/httpx"
	"github.com/aureliusnguyen/pandora/backend/internal/pkg/kafkax"
	"github.com/aureliusnguyen/pandora/backend/internal/pkg/logx"
	"github.com/aureliusnguyen/pandora/backend/internal/pkg/otelx"
	pgxutil "github.com/aureliusnguyen/pandora/backend/internal/pkg/pgx"
)

// version is overridden at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	invariants := flag.Bool("invariants", false, "run SQL invariant checks against PG_DSN and exit non-zero on violation")
	flag.Parse()
	os.Exit(run(*invariants))
}

func run(invariantsOnly bool) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg, err := config.Load[config.Ledger]()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	log, err := logx.Setup("ledger", cfg.LogLevel)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	log = log.With("version", version)
	pool, err := pgxutil.NewPool(ctx, cfg.PGDSN)
	if err != nil {
		log.Error("postgres", "err", err)
		return 2
	}
	defer pool.Close()

	if invariantsOnly {
		vs, err := ledger.CheckInvariants(ctx, pool)
		if err != nil {
			log.Error("invariants query failed", "err", err)
			return 2
		}
		for _, v := range vs {
			fmt.Printf("VIOLATION %s: %s\n", v.Check, v.Detail)
		}
		if len(vs) > 0 {
			return 1
		}
		fmt.Println("invariants OK")
		return 0
	}

	tel, err := otelx.Setup(ctx, otelx.Options{ServiceName: "ledger", OTLPEndpoint: cfg.OTLPEndpoint})
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
	consumer, err := ledger.NewConsumer(brokers, ledger.NewApplier(ledger.NewPGRepo(pool), log), log)
	if err != nil {
		log.Error("kafka", "err", err)
		return 2
	}
	defer consumer.Close()

	// internal/ledger registers pandora_ledger_* with promauto on the default
	// registry; serve it next to the otelx registry. otelx already has the Go
	// and process collectors, so drop the default registry's copies.
	prometheus.Unregister(collectors.NewGoCollector())
	prometheus.Unregister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	gatherer := prometheus.Gatherers{tel.Registry, prometheus.DefaultGatherer}

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
	ready.SetReady(true)
	log.Info("ledger consuming", "topic", "engine.events", "group", ledger.ConsumerName)
	runErr := consumer.Run(ctx)
	interrupted := ctx.Err() != nil
	ready.SetReady(false)
	stop()
	if err := <-opsErr; err != nil {
		return 1
	}
	if runErr != nil && !interrupted {
		log.Error("consumer stopped", "err", runErr)
		return 1
	}
	return 0
}
