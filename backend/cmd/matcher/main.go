// Command matcher runs the sequencer + matching engine for partition 0.
//
// Configuration comes from the environment (internal/pkg/config.Matcher):
// HTTP_ADDR (:9001, MatcherService over h2c), METRICS_ADDR (:9090,
// /metrics, /healthz, /readyz), KAFKA_BROKERS (localhost:19092),
// OTEL_EXPORTER_OTLP_ENDPOINT (empty disables trace export), LOG_LEVEL
// (info), ENGINE_PARTITIONS (must be 1 in M1), DEMO_MARKET (true) and
// SHUTDOWN_GRACE (config.Matcher.ShutdownGrace: a positive Go duration,
// default 10s) which bounds how long unpublished events keep being retried
// after SIGTERM, as one budget for the whole shutdown. RPC request bodies
// are capped at 1 MiB (connect.WithReadMaxBytes).
//
// Shutdown worst case (set terminationGracePeriodSeconds above it): the
// RPC server drains for up to httpx.ShutdownTimeout (10 s), the runner keeps
// retrying unpublished events for SHUTDOWN_GRACE (10 s), KafkaWAL.Close
// flushes for up to 10 s and the otel shutdown takes up to 5 s: about 35 s
// with the defaults.
//
// Startup: ensure the topics (15 s bound), connect the WAL, serve the
// metrics port (/healthz answers the startup probe during replay), replay
// engine.wal, then mark /readyz ready and serve RPCs.
//
// Retention: engine.wal keeps 7 days and M1 has no snapshots, so replay
// needs every command since seq 1. A matcher restarted after its oldest
// journaled command has aged out finds a seq gap and exits 3 (ErrGap) on
// every start until the topics are recreated; M3 snapshots close this.
//
// Security: matcher rpc is unauthenticated in M1 (internal network only;
// AdminCommand can create, lock, resolve and void markets); never expose
// this port outside the cluster; auth lands in M4. The same sentence is
// logged as a WARN at startup.
//
// Exit codes: 0 clean stop (including SIGTERM during replay), 3 WAL seq gap
// or a WAL produce that failed after retries (the runner exits 3 itself),
// 1 anything else (bad config, broker unreachable, port in use).
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

	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/aureliusnguyen/pandora/backend/gen/go/pandora/matcher/v1/matcherv1connect"
	"github.com/aureliusnguyen/pandora/backend/internal/matcher"
	"github.com/aureliusnguyen/pandora/backend/internal/pkg/config"
	"github.com/aureliusnguyen/pandora/backend/internal/pkg/httpx"
	"github.com/aureliusnguyen/pandora/backend/internal/pkg/ids"
	"github.com/aureliusnguyen/pandora/backend/internal/pkg/kafkax"
	"github.com/aureliusnguyen/pandora/backend/internal/pkg/logx"
	"github.com/aureliusnguyen/pandora/backend/internal/pkg/otelx"
)

// version is overridden at build time with -ldflags "-X main.version=...".
var version = "dev"

const (
	// brokerTimeout bounds each broker round trip at startup (topic
	// creation, the WAL ping) so a dead broker fails fast instead of hanging.
	brokerTimeout = 15 * time.Second
	// maxRequestBytes caps every RPC request body.
	maxRequestBytes = 1 << 20
)

// unauthenticatedWarning is logged at startup and mirrored in the package doc.
const unauthenticatedWarning = "matcher rpc is unauthenticated in M1 (internal network only; " +
	"AdminCommand can create, lock, resolve and void markets); never expose this port outside the cluster; auth lands in M4"

// Process exit codes.
const (
	exitOK    = 0
	exitError = 1
	exitWAL   = 3
)

func main() { os.Exit(run()) }

// run wires and runs the matcher and returns the process exit code. All
// cleanup happens in defers here so main can call os.Exit.
func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load[config.Matcher]()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitError
	}
	log, err := logx.Setup("matcher", cfg.LogLevel)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitError
	}
	log = log.With("version", version)
	if cfg.EnginePartitions != 1 {
		log.Error("ENGINE_PARTITIONS must be 1 in M1", "value", cfg.EnginePartitions)
		return exitError
	}
	grace := cfg.ShutdownGrace
	if grace <= 0 {
		log.Error("SHUTDOWN_GRACE must be a positive duration", "value", grace)
		return exitError
	}
	brokers, err := kafkax.CleanBrokers(cfg.Brokers)
	if err != nil {
		log.Error("kafka brokers", "err", err)
		return exitError
	}

	tel, err := otelx.Setup(ctx, otelx.Options{ServiceName: "matcher", OTLPEndpoint: cfg.OTLPEndpoint})
	if err != nil {
		log.Error("otel setup", "err", err)
		return exitError
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tel.Shutdown(sctx); err != nil {
			log.Warn("otel shutdown", "err", err)
		}
	}()

	tctx, tcancel := context.WithTimeout(ctx, brokerTimeout)
	err = kafkax.EnsureTopics(tctx, brokers, kafkax.M1Topics())
	tcancel()
	if err != nil {
		if ctx.Err() != nil {
			log.Info("stopped before startup completed")
			return exitOK
		}
		log.Error("ensure topics", "err", err)
		return exitError
	}
	wctx, wcancel := context.WithTimeout(ctx, brokerTimeout)
	wal, err := matcher.NewKafkaWAL(wctx, brokers, log)
	wcancel()
	if err != nil {
		if ctx.Err() != nil {
			log.Info("stopped before startup completed")
			return exitOK
		}
		log.Error("kafka wal", "err", err)
		return exitError
	}
	defer wal.Close()

	svc := matcher.NewService(matcher.Options{
		Partition: 0, WAL: wal, Metrics: matcher.NewMetrics(tel.Registry), Log: log,
		NewID: ids.New, DemoMarket: cfg.DemoMarket, ShutdownGrace: grace,
	})

	// The metrics server runs during replay so /healthz answers the startup
	// probe; /readyz stays 503 until Start returns. A bind failure on it
	// stops the whole process (via stop) and is reported as exit 1.
	var ready httpx.Readiness
	opsErr := make(chan error, 1)
	go func() {
		err := httpx.Run(ctx, httpx.NewMetricsServer(cfg.MetricsAddr, tel.Registry, &ready))
		if err != nil {
			stop()
		}
		opsErr <- err
	}()
	// stopOps stops the metrics server and returns its error (nil when it
	// stopped cleanly).
	stopOps := func() error {
		stop()
		return <-opsErr
	}

	if err := svc.Start(ctx); err != nil {
		// Capture whether a signal interrupted Start BEFORE svc.Stop and
		// stopOps: stopOps calls stop(), which cancels ctx, so checking
		// ctx.Err() afterwards would report every failure as an interrupt.
		interrupted := ctx.Err() != nil
		svc.Stop()
		if opErr := stopOps(); opErr != nil {
			log.Error("metrics server", "err", opErr)
			return exitError
		}
		switch {
		case errors.Is(err, matcher.ErrGap):
			log.Error("start: wal seq gap", "err", err)
			return exitWAL
		case interrupted:
			log.Info("stopped during replay", "err", err)
			return exitOK
		default:
			log.Error("start", "err", err)
			return exitError
		}
	}

	otelInterceptor, err := otelconnect.NewInterceptor()
	if err != nil {
		log.Error("otelconnect", "err", err)
		svc.Stop()
		_ = stopOps()
		return exitError
	}
	mux := http.NewServeMux()
	mux.Handle(matcherv1connect.NewMatcherServiceHandler(svc, connect.WithInterceptors(otelInterceptor), connect.WithReadMaxBytes(maxRequestBytes)))
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           h2c.NewHandler(mux, &http2.Server{}),
		ReadHeaderTimeout: 5 * time.Second,
	}
	ready.SetReady(svc.Ready())

	log.Warn(unauthenticatedWarning, "addr", cfg.HTTPAddr)
	log.Info("matcher listening", "addr", cfg.HTTPAddr, "metrics", cfg.MetricsAddr, "shutdown_grace", grace)
	// Blocks until SIGTERM/SIGINT or a bind failure, then shuts srv down
	// gracefully (in-flight RPCs finish before the runner stops).
	runErr := httpx.Run(ctx, srv)
	ready.SetReady(false)
	svc.Stop()
	runErr = errors.Join(runErr, stopOps())
	if runErr != nil {
		log.Error("server", "err", runErr)
		return exitError
	}
	log.Info("matcher stopped")
	return exitOK
}
