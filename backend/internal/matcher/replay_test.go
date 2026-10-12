package matcher

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"testing"

	"google.golang.org/protobuf/proto"

	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
	"github.com/aureliusnguyen/pandora/backend/internal/engine"
)

func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func protoUnmarshal(b []byte, m proto.Message) error { return proto.Unmarshal(b, m) }

func itoa(i int) string { return strconv.Itoa(i) }

func createCmd(seq uint64) *enginev1.Command {
	return &enginev1.Command{
		Seq: seq, Partition: 0, Epoch: Epoch, EngineTimeUnixNanos: int64(seq),
		Body: &enginev1.Command_CreateMarket{CreateMarket: &enginev1.CreateMarket{MarketId: "mkt_demo", Title: "d", OutcomeLabels: []string{"YES", "NO"}}},
	}
}

func submitCmd(seq uint64, id string, price uint32) *enginev1.Command {
	return &enginev1.Command{
		Seq: seq, Partition: 0, Epoch: Epoch, EngineTimeUnixNanos: int64(seq),
		Body: &enginev1.Command_SubmitOrder{SubmitOrder: &enginev1.SubmitOrder{
			OrderId: id, MarketId: "mkt_demo", UserId: "u_alice", ClientOrderId: "c" + id,
			Outcome: 0, Side: 1, Price: price, Qty: 5, Tif: 1,
		}},
	}
}

func TestReplayIntoAppliesSkipsDuplicatesAndDetectsGaps(t *testing.T) {
	ctx := context.Background()
	log := slog.Default()
	w := NewFakeWAL()
	w.Records[0] = [][]byte{
		mustMarshal(t, createCmd(1)),
		mustMarshal(t, submitCmd(2, "a", 40)),
		mustMarshal(t, submitCmd(2, "a", 40)), // duplicate: skipped
		mustMarshal(t, submitCmd(3, "b", 41)),
	}
	eng := engine.New(0)
	n, err := ReplayInto(ctx, w, 0, eng, log)
	if err != nil || n != 3 || eng.LastSeq() != 3 {
		t.Fatalf("n=%d err=%v last=%d", n, err, eng.LastSeq())
	}
	levels, _, _, _ := eng.Book("mkt_demo")
	if len(levels) != 2 {
		t.Fatalf("replayed book has %d levels", len(levels))
	}
	// gap
	w.Records[0] = append(w.Records[0], mustMarshal(t, submitCmd(5, "c", 42)))
	eng2 := engine.New(0)
	if _, err := ReplayInto(ctx, w, 0, eng2, log); !errors.Is(err, ErrGap) {
		t.Fatalf("expected ErrGap, got %v", err)
	}
	// corrupt record
	w.Records[0] = [][]byte{{0xff, 0xff, 0xff}}
	if _, err := ReplayInto(ctx, w, 0, engine.New(0), log); err == nil || errors.Is(err, ErrGap) {
		t.Fatalf("corrupt record must fail without ErrGap, got %v", err)
	}
}
