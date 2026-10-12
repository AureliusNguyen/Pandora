package matcher

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
	matcherv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/matcher/v1"
	"github.com/aureliusnguyen/pandora/backend/internal/pkg/ids"
)

func newServiceT(t *testing.T, w *FakeWAL, demo bool) *Service {
	t.Helper()
	var n atomic.Int64 // NewID is called from concurrent handler goroutines
	svc := NewService(Options{
		Partition: 0, WAL: w, Metrics: NewMetrics(prometheus.NewRegistry()), Log: slog.Default(),
		NewID: func() string { return "01J" + strconv.FormatInt(n.Add(1), 10) }, DemoMarket: demo, Now: func() int64 { return 7 },
		Fatal: func(string, error) {},
	})
	return svc
}

func TestServiceStartCreatesDemoMarketOnceAndBecomesReady(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := NewFakeWAL()
	svc := newServiceT(t, w, true)
	if svc.Ready() {
		t.Fatal("must not be ready before Start")
	}
	if err := svc.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if !svc.Ready() || !svc.Runner().HasMarket(DemoMarketID) || len(w.Records[0]) != 1 {
		t.Fatalf("ready=%v wal=%d", svc.Ready(), len(w.Records[0]))
	}
	if w.EventCount() != 1 {
		t.Fatalf("demo creation must publish MarketCreated once, got %d events", w.EventCount())
	}
	svc.Stop()
	if svc.Ready() {
		t.Fatal("must not be ready after Stop")
	}
	// Restart on the same WAL: replay recreates the market, no new WAL record, no republished events.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	svc2 := newServiceT(t, w, true)
	if err := svc2.Start(ctx2); err != nil {
		t.Fatal(err)
	}
	if len(w.Records[0]) != 1 || w.EventCount() != 1 || svc2.Runner().eng.LastSeq() != 1 {
		t.Fatalf("second start: wal=%d events=%d last=%d", len(w.Records[0]), w.EventCount(), svc2.Runner().eng.LastSeq())
	}
	if got := testutil.ToFloat64(svc2.Runner().m.LastSeq.WithLabelValues("0")); got != 1 {
		t.Fatalf("last_seq after replay = %v, want 1", got)
	}
	if svc2.Runner().shutdownGrace != closeFlushTimeout {
		t.Fatalf("default shutdown grace %v, want %v", svc2.Runner().shutdownGrace, closeFlushTimeout)
	}
	svc2.Stop()
}

func TestServiceStartWithoutDemoAndGap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := NewFakeWAL()
	svc := newServiceT(t, w, false)
	if err := svc.Start(ctx); err != nil || svc.Runner().HasMarket(DemoMarketID) || len(w.Records[0]) != 0 {
		t.Fatalf("no demo expected: err=%v", err)
	}
	svc.Stop()
	w.Records[0] = [][]byte{mustMarshal(t, createCmd(1)), mustMarshal(t, submitCmd(3, "a", 40))}
	svc3 := newServiceT(t, w, true)
	if err := svc3.Start(ctx); !errors.Is(err, ErrGap) || svc3.Ready() {
		t.Fatalf("gap must fail start and stay not-ready: err=%v ready=%v", err, svc3.Ready())
	}
	svc3.Stop() // must be safe after a failed Start
}

// The runner must outlive the Start context: only Stop ends it.
func TestServiceRunnerOutlivesStartContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	w := NewFakeWAL()
	svc := newServiceT(t, w, true)
	if err := svc.Start(ctx); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-svc.Runner().Done():
		t.Fatal("runner stopped when the Start context was canceled")
	default:
	}
	if _, err := svc.Runner().Submit(context.Background(), createCmd(0)); err != nil {
		t.Fatalf("submit after Start context canceled: %v", err)
	}
	svc.Stop()
	<-svc.Runner().Done()
}

// Without NewID the service assigns ULIDs from internal/pkg/ids.
func TestServiceDefaultsNewIDToULID(t *testing.T) {
	w := NewFakeWAL()
	svc := NewService(Options{WAL: w, Metrics: NewMetrics(prometheus.NewRegistry()), Log: slog.Default(), DemoMarket: true})
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(svc.Stop)
	resp, err := svc.SubmitOrder(context.Background(), connect.NewRequest(&matcherv1.SubmitOrderRequest{Order: order("u_alice", commonv1.Side_SIDE_BUY, 40, 1)}))
	if err != nil || !ids.Valid(resp.Msg.OrderId) {
		t.Fatalf("order_id %q err=%v", resp.Msg.GetOrderId(), err)
	}
}

func TestServiceShutdownGraceOption(t *testing.T) {
	svc := NewService(Options{WAL: NewFakeWAL(), Metrics: NewMetrics(prometheus.NewRegistry()), Log: slog.Default(), ShutdownGrace: time.Second})
	if svc.Runner().shutdownGrace != time.Second {
		t.Fatalf("shutdown grace %v, want 1s", svc.Runner().shutdownGrace)
	}
}
