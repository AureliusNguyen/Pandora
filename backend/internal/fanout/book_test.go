package fanout

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
	matcherv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/matcher/v1"
)

func snap() *matcherv1.GetBookResponse {
	return &matcherv1.GetBookResponse{
		MarketId: "mkt_demo", MarketSeq: 42, Status: commonv1.MarketStatus_MARKET_STATUS_OPEN,
		Levels: []*commonv1.Level{
			{Outcome: 1, Side: commonv1.Side_SIDE_BUY, Price: 40, Qty: 5},
			{Outcome: 0, Side: commonv1.Side_SIDE_BUY, Price: 55, Qty: 10},
		},
	}
}

func lvl(cmdSeq, mseq uint64, idx uint32, outcome, price uint32, qty uint64) *enginev1.Event {
	return &enginev1.Event{Seq: cmdSeq, Idx: idx, MarketId: "mkt_demo", MarketSeq: mseq, Body: &enginev1.Event_LevelUpdate{
		LevelUpdate: &enginev1.LevelUpdate{Level: &commonv1.Level{Outcome: outcome, Side: commonv1.Side_SIDE_BUY, Price: price, Qty: qty}},
	}}
}

func mintFill(cmdSeq, mseq uint64, idx uint32) *enginev1.Event {
	return &enginev1.Event{
		Seq: cmdSeq, Idx: idx, MarketId: "mkt_demo", MarketSeq: mseq, EngineTimeUnixNanos: 1_700_000_000_000_000_000,
		Body: &enginev1.Event_Fill{Fill: &commonv1.Fill{FillId: "f1", Kind: commonv1.FillKind_FILL_KIND_MINT, Qty: 5, Legs: []*commonv1.FillLeg{
			{Outcome: 0, Side: commonv1.Side_SIDE_BUY, Price: 55, IsTaker: false},
			{Outcome: 1, Side: commonv1.Side_SIDE_BUY, Price: 45, IsTaker: true},
		}}},
	}
}

func TestSnapshotSortedAndComplete(t *testing.T) {
	s := NewBookView(snap()).Snapshot()
	if s.Type != "snapshot" || s.MarketSeq != 42 || s.Status != "open" || len(s.Levels) != 2 || s.Levels[0].Outcome != 0 {
		t.Fatalf("snapshot = %+v", s)
	}
}

func TestApplyDeltaThenDropStale(t *testing.T) {
	b := NewBookView(snap())
	d, mm, tail, ok := b.Apply([]*enginev1.Event{mintFill(100, 43, 0), lvl(100, 43, 1, 1, 40, 0), lvl(100, 43, 2, 0, 55, 5)})
	if !ok || tail || mm != nil || d.MarketSeq != 43 || len(d.Levels) != 2 || len(d.Trades) != 1 {
		t.Fatalf("delta = %+v ok=%v", d, ok)
	}
	if tr := d.Trades[0]; tr.Price != 45 || tr.Outcome != 1 || tr.TakerSide != "BUY" || tr.Kind != "MINT" || tr.TS != "2023-11-14T22:13:20Z" {
		t.Fatalf("trade = %+v", tr)
	}
	s := b.Snapshot()
	if len(s.Levels) != 1 || s.Levels[0].Qty != 5 || s.MarketSeq != 43 {
		t.Fatalf("after delta snapshot = %+v", s)
	}
	if _, _, _, ok := b.Apply([]*enginev1.Event{lvl(99, 43, 0, 0, 55, 999)}); ok {
		t.Fatal("market_seq == current must be dropped")
	}
	if _, _, _, ok := b.Apply([]*enginev1.Event{lvl(98, 41, 0, 0, 55, 999)}); ok || b.Snapshot().Levels[0].Qty != 5 {
		t.Fatal("older market_seq must be dropped without mutation")
	}
}

// A command split by the reader's idle flush arrives as two Apply calls with
// the same (partition, seq) and market_seq. The tail must be applied (not
// dropped as stale) and is flagged so the hub sends a snapshot instead.
func TestApplySplitCommandTail(t *testing.T) {
	b := NewBookView(snap())
	before := testutil.ToFloat64(splitTails)
	if _, _, tail, ok := b.Apply([]*enginev1.Event{mintFill(100, 43, 0), lvl(100, 43, 1, 1, 40, 0)}); !ok || tail {
		t.Fatalf("head of the command must apply as a non-tail: ok=%v tail=%v", ok, tail)
	}
	if _, _, tail, ok := b.Apply([]*enginev1.Event{lvl(100, 43, 2, 0, 55, 5)}); !ok || !tail {
		t.Fatalf("tail must apply and be flagged: ok=%v tail=%v", ok, tail)
	}
	if s := b.Snapshot(); len(s.Levels) != 1 || s.Levels[0].Qty != 5 || s.MarketSeq != 43 {
		t.Fatalf("after tail snapshot = %+v", s)
	}
	if got := testutil.ToFloat64(splitTails) - before; got != 1 {
		t.Fatalf("split tails metric diff = %v, want 1", got)
	}
	// Same market_seq from another partition is not the same command.
	other := lvl(100, 43, 3, 0, 55, 999)
	other.Partition = 1
	if _, _, _, ok := b.Apply([]*enginev1.Event{other}); ok {
		t.Fatal("equal market_seq from a different command must be dropped")
	}
}

// The snapshot alone does not name the command behind its market_seq, so an
// equal-seq event replayed after bootstrap is already in the snapshot.
func TestApplyEqualSeqAfterSnapshotDropped(t *testing.T) {
	b := NewBookView(snap())
	if _, _, _, ok := b.Apply([]*enginev1.Event{lvl(90, 42, 0, 0, 55, 999)}); ok || b.Snapshot().Levels[0].Qty != 10 {
		t.Fatal("event with the snapshot's market_seq must be dropped")
	}
}

func TestApplyResolvedEmitsMarketMsg(t *testing.T) {
	b := NewBookView(snap())
	_, mm, _, ok := b.Apply([]*enginev1.Event{{
		Seq: 101, MarketId: "mkt_demo", MarketSeq: 43,
		Body: &enginev1.Event_MarketResolved{MarketResolved: &enginev1.MarketResolved{WinningOutcome: 1}},
	}})
	if !ok || mm == nil || mm.Status != "resolved" || mm.WinningOutcome == nil || *mm.WinningOutcome != 1 {
		t.Fatalf("market msg = %+v", mm)
	}
}

func TestGrouperHoldsUntilNextCommand(t *testing.T) {
	var g CommandGrouper
	if out := g.Add(lvl(100, 43, 0, 0, 55, 1)); out != nil {
		t.Fatal("first event must be held")
	}
	if out := g.Add(lvl(100, 43, 1, 0, 54, 1)); out != nil {
		t.Fatal("same command must be held")
	}
	out := g.Add(lvl(101, 44, 0, 0, 53, 1))
	if len(out) != 2 || !g.Pending() {
		t.Fatalf("flush on next command: %d pending=%v", len(out), g.Pending())
	}
	if rest := g.Flush(); len(rest) != 1 || g.Pending() {
		t.Fatal("Flush must drain the held command")
	}
}
