package fanout

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
	matcherv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/matcher/v1"
	"github.com/aureliusnguyen/pandora/backend/gen/go/pandora/matcher/v1/matcherv1connect"
)

func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newCluster(t *testing.T) (*kfake.Cluster, *kgo.Client) {
	t.Helper()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, eventsTopic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	prod, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(prod.Close)
	return cluster, prod
}

func produce(ctx context.Context, t *testing.T, prod *kgo.Client, evs ...*enginev1.Event) {
	t.Helper()
	recs := make([]*kgo.Record, 0, len(evs))
	for _, ev := range evs {
		v, err := proto.Marshal(ev)
		if err != nil {
			t.Fatal(err)
		}
		recs = append(recs, &kgo.Record{Topic: eventsTopic, Key: []byte(ev.GetMarketId()), Value: v})
	}
	if err := prod.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// next waits for the next queued message for c.
func next(ctx context.Context, t *testing.T, c *Client) map[string]any {
	t.Helper()
	select {
	case b := <-c.Send():
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		return m
	case <-ctx.Done():
		t.Fatal("timed out waiting for a message")
		return nil
	}
}

func startReader(ctx context.Context, t *testing.T, cl *kgo.Client, hub *Hub) {
	t.Helper()
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- NewReader(cl, hub, discardLog()).Run(runCtx) }()
	t.Cleanup(func() {
		stop()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("Run = %v, want context.Canceled", err)
		}
	})
}

// fakeMatcher serves GetBook from a fixed snapshot; onGetBook runs first.
type fakeMatcher struct {
	matcherv1connect.MatcherServiceClient
	resp      *matcherv1.GetBookResponse
	onGetBook func()
}

func (f *fakeMatcher) GetBook(_ context.Context, req *connect.Request[matcherv1.GetBookRequest]) (*connect.Response[matcherv1.GetBookResponse], error) {
	if req.Msg.GetMarketId() != f.resp.GetMarketId() {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("unknown market"))
	}
	if f.onGetBook != nil {
		f.onGetBook()
	}
	return connect.NewResponse(f.resp), nil
}

// Bootstrap must record end offsets before GetBook: events already in the
// topic are skipped (the snapshot covers them) and an event published while
// GetBook runs is still consumed.
func TestBootstrapOffsetsThenSnapshotThenConsume(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cluster, prod := newCluster(t)
	produce(ctx, t, prod, lvl(90, 41, 0, 0, 55, 777)) // already in the snapshot below
	hub := NewHub()
	m := &fakeMatcher{resp: snap(), onGetBook: func() { produce(ctx, t, prod, lvl(100, 43, 0, 0, 55, 3)) }}
	cl, err := Bootstrap(ctx, cluster.ListenAddrs(), m, hub, []string{"mkt_demo"}, discardLog())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close) // LIFO: runs after startReader stops the reader
	c, _ := newTestClient()
	hub.Register(c)
	hub.Subscribe(c, []string{"mkt_demo"})
	if s := next(ctx, t, c); s["market_seq"].(float64) != 42 {
		t.Fatalf("snapshot = %v", s)
	}
	startReader(ctx, t, cl, hub)
	d := next(ctx, t, c)
	if d["type"] != "delta" || d["market_seq"].(float64) != 43 {
		t.Fatalf("first delta = %v (the pre-bootstrap event must not be replayed past the snapshot)", d)
	}
	levels := d["levels"].([]any)
	if len(levels) != 1 || levels[0].(map[string]any)["qty"].(float64) != 3 {
		t.Fatalf("delta levels = %v", levels)
	}
}

func TestBootstrapStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m := &fakeMatcher{resp: snap()}
	if _, err := Bootstrap(ctx, []string{"127.0.0.1:1"}, m, NewHub(), []string{"mkt_demo"}, discardLog()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Bootstrap = %v, want context.Canceled", err)
	}
}

// The idle flush may release a command before its tail arrives. The tail must
// still reach the book view and is sent as a second delta with the same
// market_seq; the split is counted.
func TestReaderIdleFlushSplitsCommand(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cluster, prod := newCluster(t)
	hub := NewHub()
	hub.LoadSnapshot(snap())
	cl, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{eventsTopic: {0: kgo.NewOffset().AtStart()}}),
		kgo.FetchMaxWait(50*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close) // LIFO: runs after startReader stops the reader
	c, _ := newTestClient()
	hub.Register(c)
	hub.Subscribe(c, []string{"mkt_demo"})
	next(ctx, t, c) // snapshot
	before := testutil.ToFloat64(splitTails)
	startReader(ctx, t, cl, hub)

	// The head is one record, so nothing else of command 100 is in its fetch.
	produce(ctx, t, prod, mintFill(100, 43, 0))
	head := next(ctx, t, c) // released by the idle flush: no next command yet
	if head["market_seq"].(float64) != 43 || len(head["trades"].([]any)) != 1 || len(head["levels"].([]any)) != 0 {
		t.Fatalf("head delta = %v", head)
	}
	produce(ctx, t, prod, lvl(100, 43, 1, 1, 40, 0), lvl(100, 43, 2, 0, 55, 5), lvl(101, 44, 0, 0, 54, 2))
	// The tail may itself arrive in more than one fetch; every piece is a
	// delta with market_seq 43 and together they carry both levels.
	var tailLevels []any
	for {
		d := next(ctx, t, c)
		if d["type"] != "delta" {
			t.Fatalf("unexpected message %v", d)
		}
		if d["market_seq"].(float64) == 44 {
			break
		}
		if d["market_seq"].(float64) != 43 || len(d["trades"].([]any)) != 0 {
			t.Fatalf("tail delta = %v", d)
		}
		tailLevels = append(tailLevels, d["levels"].([]any)...)
	}
	if len(tailLevels) != 2 {
		t.Fatalf("tail levels = %v, want the two level updates of command 100", tailLevels)
	}
	if got := testutil.ToFloat64(splitTails) - before; got < 1 {
		t.Fatalf("split tails metric diff = %v, want >= 1", got)
	}
	hub.Subscribe(c, []string{"mkt_demo"})
	s := next(ctx, t, c)
	want := map[float64]float64{54: 2, 55: 5} // outcome 0 levels; outcome 1 @40 removed by the head
	ls := s["levels"].([]any)
	if s["market_seq"].(float64) != 44 || len(ls) != len(want) {
		t.Fatalf("final snapshot = %v", s)
	}
	for _, l := range ls {
		lm := l.(map[string]any)
		if want[lm["price"].(float64)] != lm["qty"].(float64) {
			t.Fatalf("final snapshot level %v, want %v", lm, want)
		}
	}
}
