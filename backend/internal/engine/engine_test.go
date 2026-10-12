package engine

import (
	"testing"

	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
)

func TestApplyPanicsOnNonContiguousSeq(t *testing.T) {
	e := New(0)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on seq 2 after 0")
		}
	}()
	e.Apply(cmdAt(2, &enginev1.CreateMarket{MarketId: "m", OutcomeLabels: []string{"a", "b"}}))
}

func TestCreateMarketEmitsAndIsIdempotent(t *testing.T) {
	e := New(3)
	evs := e.Apply(cmdAt(1, &enginev1.CreateMarket{MarketId: "mkt_demo", Title: "Demo", OutcomeLabels: []string{"YES", "NO"}}))
	wantKinds(t, evs, "created")
	ev := evs[0]
	if ev.Seq != 1 || ev.Idx != 0 || ev.Partition != 3 || ev.Epoch != 1 || ev.EngineTimeUnixNanos != 1000 || ev.MarketId != "mkt_demo" || ev.MarketSeq != 1 {
		t.Fatalf("event header wrong: %+v", ev)
	}
	if ev.GetMarketCreated().GetMarket().GetTitle() != "Demo" {
		t.Fatal("MarketCreated must carry the CreateMarket body")
	}
	if e.LastSeq() != 1 {
		t.Fatalf("LastSeq=%d", e.LastSeq())
	}
	levels, seq, status, ok := e.Book("mkt_demo")
	if !ok || len(levels) != 0 || seq != 1 || status != commonv1.MarketStatus_MARKET_STATUS_OPEN {
		t.Fatalf("Book after create: ok=%v levels=%d seq=%d status=%v", ok, len(levels), seq, status)
	}
	// Second create of the same id is a no-op: no events, market_seq unchanged.
	evs = e.Apply(cmdAt(2, &enginev1.CreateMarket{MarketId: "mkt_demo", OutcomeLabels: []string{"A", "B"}}))
	if len(evs) != 0 {
		t.Fatalf("duplicate create emitted %s", kinds(evs))
	}
	if _, seq, _, _ = e.Book("mkt_demo"); seq != 1 {
		t.Fatalf("market_seq moved on no-op create: %d", seq)
	}
}

func TestCreateMarketRejectsBadInput(t *testing.T) {
	e := New(0)
	cases := []*enginev1.CreateMarket{
		{MarketId: "", OutcomeLabels: []string{"a", "b"}},
		{MarketId: "one", OutcomeLabels: []string{"a"}},
		{MarketId: "many", OutcomeLabels: make([]string, MaxOutcomes+1)},
	}
	for i, c := range cases {
		if evs := e.Apply(cmdAt(uint64(i+1), c)); len(evs) != 0 {
			t.Fatalf("case %d: expected no events, got %s", i, kinds(evs))
		}
		if _, _, _, ok := e.Book(c.MarketId); ok {
			t.Fatalf("case %d: market must not exist", i)
		}
	}
	if e.LastSeq() != 3 {
		t.Fatal("ignored commands still advance LastSeq")
	}
}

func TestBookUnknownMarketAndPositionDefault(t *testing.T) {
	e := New(0)
	if _, _, _, ok := e.Book("nope"); ok {
		t.Fatal("unknown market must report ok=false")
	}
	if e.Position("nope", "u", 0) != 0 {
		t.Fatal("unknown position must be 0")
	}
}

func TestOutcomes(t *testing.T) {
	te := newT(t)
	if n, ok := te.e.Outcomes("mkt_demo"); !ok || n != 2 {
		t.Fatalf("Outcomes(mkt_demo) = %d, %v", n, ok)
	}
	if _, ok := te.e.Outcomes("ghost"); ok {
		t.Fatal("unknown market reported outcomes")
	}
}
