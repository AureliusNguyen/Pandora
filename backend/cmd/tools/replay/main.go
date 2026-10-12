// Command replay rebuilds a partition's engine from engine.wal and prints
// one market's book as JSON. With --compare it diffs the rebuilt book
// against the live matcher's GetBook and exits 1 on any difference (used by
// the pod-kill chaos test to prove replay fidelity).
//
// Flags: --brokers (default $KAFKA_BROKERS or localhost:19092),
// --partition (0), --market (mkt_demo), --compare MATCHER_URL (e.g.
// http://localhost:9001), --timeout (5m, bounds the whole run).
//
// Exit codes: 0 book printed (and matches live with --compare), 1 replay
// and live differ, 2 usage, connect, replay or GetBook error, 3 WAL seq gap.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"

	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
	matcherv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/matcher/v1"
	"github.com/aureliusnguyen/pandora/backend/gen/go/pandora/matcher/v1/matcherv1connect"
	"github.com/aureliusnguyen/pandora/backend/internal/engine"
	"github.com/aureliusnguyen/pandora/backend/internal/matcher"
	"github.com/aureliusnguyen/pandora/backend/internal/pkg/kafkax"
)

// Process exit codes.
const (
	exitOK    = 0
	exitDiff  = 1
	exitError = 2
	exitGap   = 3
)

// connectTimeout bounds the initial broker ping so a dead broker fails fast.
const connectTimeout = 15 * time.Second

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

// envOr returns the environment variable key, or def when it is unset or empty.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// run parses args, replays the WAL and prints (and optionally diffs) the
// book; it returns the process exit code.
func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	fs.SetOutput(stderr)
	brokers := fs.String("brokers", envOr("KAFKA_BROKERS", "localhost:19092"), "comma-separated Kafka brokers")
	partition := fs.Uint("partition", 0, "engine.wal partition to replay")
	market := fs.String("market", matcher.DemoMarketID, "market to print")
	compare := fs.String("compare", "", "matcher base URL; diff against live GetBook and exit 1 on difference")
	timeout := fs.Duration("timeout", 5*time.Minute, "deadline for the whole run (connect, replay, GetBook)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitError
	}
	log := slog.New(slog.NewTextHandler(stderr, nil))
	if fs.NArg() != 0 {
		log.Error("unexpected arguments", "args", fs.Args())
		return exitError
	}
	if *partition > 0xFFFFFFFF || *timeout <= 0 {
		log.Error("invalid flags", "partition", *partition, "timeout", *timeout)
		return exitError
	}
	bs, err := kafkax.CleanBrokers(strings.Split(*brokers, ","))
	if err != nil {
		log.Error("brokers", "err", err)
		return exitError
	}
	part := uint32(*partition)

	sctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(sctx, *timeout)
	defer cancel()

	cctx, ccancel := context.WithTimeout(ctx, connectTimeout)
	wal, err := matcher.NewKafkaWAL(cctx, bs, log)
	ccancel()
	if err != nil {
		log.Error("connect", "err", err)
		return exitError
	}
	defer wal.Close()
	eng := engine.New(part)
	n, err := matcher.ReplayInto(ctx, wal, part, eng, log)
	if err != nil {
		log.Error("replay", "err", err, "applied", n)
		if errors.Is(err, matcher.ErrGap) {
			return exitGap
		}
		return exitError
	}
	levels, seq, status, ok := eng.Book(*market)
	if !ok {
		log.Error("market not present in WAL", "market", *market, "applied", n, "last_seq", eng.LastSeq())
		return exitError
	}
	local := &matcherv1.GetBookResponse{MarketId: *market, MarketSeq: seq, Status: status, Levels: levels}
	out, err := protojson.MarshalOptions{Multiline: true, UseProtoNames: true}.Marshal(local)
	if err != nil {
		log.Error("marshal", "err", err)
		return exitError
	}
	_, _ = fmt.Fprintln(stdout, string(out))
	log.Info("replayed", "commands", n, "last_seq", eng.LastSeq())
	if *compare == "" {
		return exitOK
	}
	client := matcherv1connect.NewMatcherServiceClient(http.DefaultClient, *compare)
	live, err := client.GetBook(ctx, connect.NewRequest(&matcherv1.GetBookRequest{MarketId: *market}))
	if err != nil {
		log.Error("live GetBook", "err", err)
		return exitError
	}
	diffs := diffBooks(local, live.Msg)
	if len(diffs) > 0 {
		_, _ = fmt.Fprintln(stderr, "DIFF (replay vs live):")
		for _, d := range diffs {
			_, _ = fmt.Fprintln(stderr, "  "+d)
		}
		return exitDiff
	}
	_, _ = fmt.Fprintf(stderr, "OK: replay matches live book at market_seq %d (%d levels)\n", seq, len(levels))
	return exitOK
}

// levelKey identifies a level as "<outcome> <BUY|SELL> <price>".
func levelKey(l *commonv1.Level) string {
	return fmt.Sprintf("%d %s %d", l.Outcome, strings.TrimPrefix(l.Side.String(), "SIDE_"), l.Price)
}

// diffBooks compares market_seq, status and the level set (order-insensitive)
// and returns one human-readable line per difference, levels sorted by key.
func diffBooks(replay, live *matcherv1.GetBookResponse) []string {
	var diffs []string
	if replay.MarketSeq != live.MarketSeq {
		diffs = append(diffs, fmt.Sprintf("market_seq %d != %d", replay.MarketSeq, live.MarketSeq))
	}
	if replay.Status != live.Status {
		diffs = append(diffs, fmt.Sprintf("status %s != %s", replay.Status, live.Status))
	}
	r := make(map[string]uint64, len(replay.Levels))
	for _, l := range replay.Levels {
		r[levelKey(l)] = l.Qty
	}
	l := make(map[string]uint64, len(live.Levels))
	for _, lv := range live.Levels {
		l[levelKey(lv)] = lv.Qty
	}
	keys := make([]string, 0, len(r)+len(l))
	for k := range r {
		keys = append(keys, k)
	}
	for k := range l {
		if _, dup := r[k]; !dup {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		rq, inR := r[k]
		lq, inL := l[k]
		switch {
		case inR && !inL:
			diffs = append(diffs, fmt.Sprintf("%s: only in replay (qty %d)", k, rq))
		case !inR && inL:
			diffs = append(diffs, fmt.Sprintf("%s: only in live (qty %d)", k, lq))
		case rq != lq:
			diffs = append(diffs, fmt.Sprintf("%s: %d != %d", k, rq, lq))
		}
	}
	return diffs
}
