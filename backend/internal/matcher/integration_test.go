//go:build integration

package matcher

import (
	"context"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
	matcherv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/matcher/v1"
	"github.com/aureliusnguyen/pandora/backend/internal/engine"
	"github.com/aureliusnguyen/pandora/backend/internal/pkg/kafkax"
)

// TestKafkaWALRoundTripAndReplay runs two services against one Redpanda:
// the first journals orders, the second rebuilds the same book from the WAL
// without re-creating the demo market or republishing events.
func TestKafkaWALRoundTripAndReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	rp, err := redpanda.Run(ctx, "docker.redpanda.com/redpandadata/redpanda:v24.2.7")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rp.Terminate(context.Background()) })
	broker, err := rp.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatal(err)
	}
	brokers := []string{broker}
	if err := kafkax.EnsureTopics(ctx, brokers, []kafkax.TopicSpec{
		{Name: TopicWAL, Partitions: 1, RetentionMs: -1}, {Name: TopicEvents, Partitions: 1, RetentionMs: -1},
	}); err != nil {
		t.Fatal(err)
	}
	log := slog.Default()

	newSvc := func() (*Service, *KafkaWAL) {
		wal, err := NewKafkaWAL(ctx, brokers, log)
		if err != nil {
			t.Fatal(err)
		}
		var n int
		svc := NewService(Options{
			Partition: 0, WAL: wal, Metrics: NewMetrics(prometheus.NewRegistry()), Log: log,
			NewID: func() string { n++; return "it" + strconv.Itoa(n) }, DemoMarket: true,
			// Never os.Exit inside go test: a WAL failure fails the test instead.
			Fatal: func(m string, e error) { t.Errorf("fatal: %s: %v", m, e) },
		})
		if err := svc.Start(ctx); err != nil {
			t.Fatal(err)
		}
		return svc, wal
	}

	svc1, wal1 := newSvc()
	users := []string{"u_alice", "u_bob", "u_carol"}
	for i := 0; i < 200; i++ {
		o := &enginev1.SubmitOrder{
			MarketId: DemoMarketID, UserId: users[i%3], ClientOrderId: "c" + strconv.Itoa(i),
			Outcome: uint32(i % 2), Side: commonv1.Side_SIDE_BUY, Price: uint32(30 + i%45), Qty: uint64(1 + i%7),
			Tif: commonv1.TimeInForce_TIME_IN_FORCE_GTC,
		}
		if _, err := svc1.SubmitOrder(ctx, connect.NewRequest(&matcherv1.SubmitOrderRequest{Order: o})); err != nil {
			t.Fatalf("order %d: %v", i, err)
		}
	}
	levels1, seq1, status1, _ := svc1.Runner().Book(DemoMarketID)
	last1 := svc1.Runner().eng.LastSeq()
	if last1 != 201 {
		t.Fatalf("expected seq 201 after demo create + 200 orders, got %d", last1)
	}
	svc1.Stop()
	wal1.Close()

	// Offline rebuild with the shared routine (what the replay tool does).
	rwal, err := NewKafkaWAL(ctx, brokers, log)
	if err != nil {
		t.Fatal(err)
	}
	eng := engine.New(0)
	if n, err := ReplayInto(ctx, rwal, 0, eng, log); err != nil || n != 201 {
		t.Fatalf("replay: n=%d err=%v", n, err)
	}
	rwal.Close()

	svc2, wal2 := newSvc()
	defer wal2.Close()
	defer svc2.Stop()
	levels2, seq2, status2, _ := svc2.Runner().Book(DemoMarketID)
	if svc2.Runner().eng.LastSeq() != last1 || seq1 != seq2 || status1 != status2 || len(levels1) != len(levels2) {
		t.Fatalf("replayed state differs: last=%d seq=%d/%d levels=%d/%d", svc2.Runner().eng.LastSeq(), seq1, seq2, len(levels1), len(levels2))
	}
	for i := range levels1 {
		if !proto.Equal(levels1[i], levels2[i]) {
			t.Fatalf("level %d differs: %v vs %v", i, levels1[i], levels2[i])
		}
	}
	// The second service must keep sequencing from 202, proving no demo re-create.
	res, err := svc2.SubmitOrder(ctx, connect.NewRequest(&matcherv1.SubmitOrderRequest{Order: &enginev1.SubmitOrder{
		MarketId: DemoMarketID, UserId: "u_alice", ClientOrderId: "after-replay", Outcome: 0, Side: commonv1.Side_SIDE_BUY, Price: 5, Qty: 1,
		Tif: commonv1.TimeInForce_TIME_IN_FORCE_GTC,
	}}))
	if err != nil || res.Msg.Seq != 202 {
		t.Fatalf("post-replay seq: %v err=%v", res, err)
	}
}
