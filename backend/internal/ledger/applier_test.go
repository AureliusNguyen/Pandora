package ledger

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
)

const mkt = "mkt_t"

var (
	buy  = commonv1.Side_SIDE_BUY
	sell = commonv1.Side_SIDE_SELL
)

func at(seq uint64, idx uint32, e *enginev1.Event) *enginev1.Event {
	e.Seq, e.Idx, e.Partition, e.Epoch = seq, idx, 0, 1
	if e.MarketId == "" {
		e.MarketId = mkt
	}
	e.MarketSeq = seq
	e.EngineTimeUnixNanos = 1_700_000_000_000_000_000
	return e
}

func created() *enginev1.Event {
	return &enginev1.Event{Body: &enginev1.Event_MarketCreated{MarketCreated: &enginev1.MarketCreated{
		Market: &enginev1.CreateMarket{MarketId: mkt, Title: "T", OutcomeLabels: []string{"YES", "NO"}},
	}}}
}

func order(id, user, coid string, outcome uint32, side commonv1.Side, price uint32, qty uint64) *enginev1.SubmitOrder {
	return &enginev1.SubmitOrder{
		OrderId: id, MarketId: mkt, UserId: user, ClientOrderId: coid, Outcome: outcome,
		Side: side, Price: price, Qty: qty, Tif: commonv1.TimeInForce_TIME_IN_FORCE_GTC,
	}
}

func accepted(o *enginev1.SubmitOrder, resting uint64) *enginev1.Event {
	return &enginev1.Event{Body: &enginev1.Event_OrderAccepted{OrderAccepted: &enginev1.OrderAccepted{Order: o, RestingQty: resting}}}
}

func rejected(o *enginev1.SubmitOrder) *enginev1.Event {
	return &enginev1.Event{Body: &enginev1.Event_OrderRejected{OrderRejected: &enginev1.OrderRejected{
		Order: o, Reason: commonv1.RejectReason_REJECT_REASON_DUPLICATE_CLIENT_ORDER_ID,
	}}}
}

func canceled(id, user string, qty uint64) *enginev1.Event {
	return &enginev1.Event{Body: &enginev1.Event_OrderCanceled{OrderCanceled: &enginev1.OrderCanceled{
		OrderId: id, UserId: user, CanceledQty: qty, Reason: "ioc",
	}}}
}

func leg(orderID, user string, outcome uint32, side commonv1.Side, price uint32, taker bool) *commonv1.FillLeg {
	return &commonv1.FillLeg{OrderId: orderID, UserId: user, Outcome: outcome, Side: side, Price: price, IsTaker: taker}
}

func fill(id string, kind commonv1.FillKind, qty uint64, legs ...*commonv1.FillLeg) *enginev1.Event {
	return &enginev1.Event{Body: &enginev1.Event_Fill{Fill: &commonv1.Fill{FillId: id, MarketId: mkt, Kind: kind, Qty: qty, Legs: legs}}}
}

func batch(evs ...*enginev1.Event) []Envelope {
	out := make([]Envelope, len(evs))
	for i, e := range evs {
		out[i] = Envelope{KafkaPartition: 0, Offset: int64(i), Event: e}
	}
	return out
}

// mintEvents: alice rests BUY YES@60 x10, bob takes BUY NO@40 x10 -> MINT.
func mintEvents() []*enginev1.Event {
	return []*enginev1.Event{
		at(1, 0, created()),
		at(2, 0, accepted(order("o_a", "u_alice", "c1", 0, buy, 60, 10), 10)),
		at(3, 0, accepted(order("o_b", "u_bob", "c1", 1, buy, 40, 10), 0)),
		at(3, 1, fill("f1", commonv1.FillKind_FILL_KIND_MINT, 10,
			leg("o_a", "u_alice", 0, buy, 60, false), leg("o_b", "u_bob", 1, buy, 40, true))),
	}
}

func apply(t *testing.T, r *fakeRepo, evs ...*enginev1.Event) {
	t.Helper()
	a := NewApplier(r, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := a.ApplyBatch(context.Background(), batch(evs...)); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
}

func TestMintFillBalancedThreeEntries(t *testing.T) {
	r := newFakeRepo()
	apply(t, r, mintEvents()...)
	if len(r.txns) != 1 || r.txns[0].Kind != "mint" || len(r.txns[0].Entries) != 3 {
		t.Fatalf("want one 3-entry mint txn, got %+v", r.txns)
	}
	if got := r.balance(EscrowAccount(mkt)); got != 1000 {
		t.Fatalf("escrow = %d, want 1000", got)
	}
	if r.wallets["u_alice"] != -600 || r.wallets["u_bob"] != -400 {
		t.Fatalf("wallets = %v", r.wallets)
	}
	pa := r.positions[PositionKey{"u_alice", mkt, 0}]
	pb := r.positions[PositionKey{"u_bob", mkt, 1}]
	if pa.Qty != 10 || pa.Cost != 600 || pb.Qty != 10 || pb.Cost != 400 {
		t.Fatalf("positions alice=%+v bob=%+v", pa, pb)
	}
	if o := r.orders["o_a"]; o.Remaining != 0 || o.Status != "filled" {
		t.Fatalf("maker order = %+v", o)
	}
	if o := r.orders["o_b"]; o.Remaining != 0 || o.Status != "filled" {
		t.Fatalf("taker order = %+v", o)
	}
	if r.offsets["pandora-ledger/0"] != 4 {
		t.Fatalf("offsets = %v", r.offsets)
	}
}

func TestTransferMovesCashAndShares(t *testing.T) {
	r := newFakeRepo()
	apply(t, r, mintEvents()...)
	apply(t, r,
		at(4, 0, accepted(order("o_s", "u_alice", "c2", 0, sell, 70, 4), 4)),
		at(5, 0, accepted(order("o_c", "u_carol", "c1", 0, buy, 70, 4), 0)),
		at(5, 1, fill("f2", commonv1.FillKind_FILL_KIND_TRANSFER, 4,
			leg("o_c", "u_carol", 0, buy, 70, true), leg("o_s", "u_alice", 0, sell, 70, false))),
	)
	if r.wallets["u_alice"] != -600+280 || r.wallets["u_carol"] != -280 {
		t.Fatalf("wallets = %v", r.wallets)
	}
	if r.positions[PositionKey{"u_alice", mkt, 0}].Qty != 6 || r.positions[PositionKey{"u_carol", mkt, 0}].Qty != 4 {
		t.Fatal("shares did not move")
	}
	if r.balance(EscrowAccount(mkt)) != 1000 {
		t.Fatal("transfer must not touch escrow")
	}
}

func TestReplaySameBatchIsNoop(t *testing.T) {
	r := newFakeRepo()
	apply(t, r, mintEvents()...)
	apply(t, r, mintEvents()...)
	if len(r.txns) != 1 || r.wallets["u_alice"] != -600 || r.positions[PositionKey{"u_alice", mkt, 0}].Qty != 10 {
		t.Fatalf("replay changed state: txns=%d wallets=%v", len(r.txns), r.wallets)
	}
}

func TestResolvePaysWinnersEscrowZero(t *testing.T) {
	r := newFakeRepo()
	evs := append(mintEvents(), at(6, 0, &enginev1.Event{Body: &enginev1.Event_MarketResolved{
		MarketResolved: &enginev1.MarketResolved{WinningOutcome: 0},
	}}))
	apply(t, r, evs...)
	if r.balance(EscrowAccount(mkt)) != 0 {
		t.Fatalf("escrow = %d", r.balance(EscrowAccount(mkt)))
	}
	if r.wallets["u_alice"] != 400 || r.wallets["u_bob"] != -400 {
		t.Fatalf("wallets = %v", r.wallets)
	}
	if m := r.markets[mkt]; m.Status != "resolved" || m.Winning == nil || *m.Winning != 0 {
		t.Fatalf("market = %+v", m)
	}
	if len(r.settlements) != 1 || r.settlements[0].PayoutCents != 1000 || r.settlements[0].PositionCount != 1 {
		t.Fatalf("settlements = %+v", r.settlements)
	}
	if !r.positions[PositionKey{"u_bob", mkt, 1}].Settled {
		t.Fatal("losing position not marked settled")
	}
}

func TestVoidRefundsCostBasis(t *testing.T) {
	r := newFakeRepo()
	evs := append(mintEvents(), at(6, 0, &enginev1.Event{Body: &enginev1.Event_MarketVoided{
		MarketVoided: &enginev1.MarketVoided{Reason: "feeds stale"},
	}}))
	apply(t, r, evs...)
	if r.wallets["u_alice"] != 0 || r.wallets["u_bob"] != 0 || r.balance(EscrowAccount(mkt)) != 0 {
		t.Fatalf("wallets=%v escrow=%d", r.wallets, r.balance(EscrowAccount(mkt)))
	}
	if r.markets[mkt].Status != "voided" {
		t.Fatal("market not voided")
	}
}

func TestMintWithoutMarketCreated(t *testing.T) {
	r := newFakeRepo()
	apply(t, r, mintEvents()[1:]...)
	if r.balance(EscrowAccount(mkt)) != 1000 {
		t.Fatal("escrow account not ensured on demand")
	}
}

func TestDuplicateCOIDRejectionDoesNotFail(t *testing.T) {
	r := newFakeRepo()
	apply(t, r,
		at(2, 0, accepted(order("o_a", "u_alice", "c1", 0, buy, 60, 10), 10)),
		at(3, 0, rejected(order("o_dup", "u_alice", "c1", 0, buy, 60, 10))),
	)
	if r.orders["o_a"].Status != "accepted" || r.orders["o_dup"] != nil {
		t.Fatalf("orders = %+v", r.orders)
	}
}

func TestIOCCancelClampsRemaining(t *testing.T) {
	r := newFakeRepo()
	apply(t, r,
		at(2, 0, accepted(order("o_i", "u_bob", "c9", 0, buy, 50, 5), 0)),
		at(2, 1, canceled("o_i", "u_bob", 5)),
	)
	if o := r.orders["o_i"]; o.Remaining != 0 || o.Status != "canceled" {
		t.Fatalf("order = %+v", o)
	}
}

func TestMalformedMintSkippedNotWedged(t *testing.T) {
	r := newFakeRepo()
	before := testutil.ToFloat64(malformedEvents)
	apply(t, r, at(3, 1, fill("fx", commonv1.FillKind_FILL_KIND_MINT, 10,
		leg("o_a", "u_alice", 0, buy, 60, false), leg("o_b", "u_bob", 1, buy, 30, true))))
	if len(r.txns) != 0 || !r.applied[EventKey{0, 3, 1}] {
		t.Fatalf("malformed fill must be skipped and marked applied: txns=%d", len(r.txns))
	}
	if got := testutil.ToFloat64(malformedEvents) - before; got != 1 {
		t.Fatalf("malformed counter grew by %v, want 1", got)
	}
}

// The engine reserves a client_order_id only on accept, so a rejected order
// must not block a later accepted order with the same coid.
func TestRejectThenAcceptSameCOID(t *testing.T) {
	r := newFakeRepo()
	apply(t, r,
		at(1, 0, created()),
		at(2, 0, rejected(order("o_rej", "u_alice", "c1", 0, buy, 60, 10))),
		at(3, 0, accepted(order("o_a", "u_alice", "c1", 0, buy, 60, 10), 10)),
		at(4, 0, accepted(order("o_b", "u_bob", "c1", 1, buy, 40, 4), 0)),
		at(4, 1, fill("f1", commonv1.FillKind_FILL_KIND_MINT, 4,
			leg("o_a", "u_alice", 0, buy, 60, false), leg("o_b", "u_bob", 1, buy, 40, true))),
	)
	if r.orders["o_rej"] != nil {
		t.Fatalf("rejected row not replaced: %+v", r.orders["o_rej"])
	}
	if o := r.orders["o_a"]; o == nil || o.Remaining != 6 || o.Status != "partially_filled" {
		t.Fatalf("accepted order = %+v", o)
	}
}

// An undecodable record arrives as an Envelope with a nil Event: it is not
// applied but still advances the stored consumer offset.
func TestNilEventAdvancesOffset(t *testing.T) {
	r := newFakeRepo()
	a := NewApplier(r, slog.New(slog.NewTextHandler(io.Discard, nil)))
	envs := append(batch(mintEvents()...), Envelope{KafkaPartition: 0, Offset: 9})
	if err := a.ApplyBatch(context.Background(), envs); err != nil {
		t.Fatal(err)
	}
	if r.offsets["pandora-ledger/0"] != 10 || len(r.txns) != 1 {
		t.Fatalf("offsets = %v txns = %d", r.offsets, len(r.txns))
	}
}

// Dedup is claim-first: the first write of every batch claims the event keys,
// and only claimed events are applied, so a concurrent transaction that
// already claimed (and committed) a key makes this one apply nothing.
func TestClaimKeysIsFirstWriteAndGatesApply(t *testing.T) {
	r := newFakeRepo()
	evs := mintEvents()
	for _, e := range evs {
		r.applied[keyOf(e)] = true // claimed by another consumer's committed txn
	}
	apply(t, r, evs...)
	if len(r.calls) < 2 || r.calls[0] != "InTx" || r.calls[1] != "ClaimKeys" {
		t.Fatalf("first Tx call must be ClaimKeys, got %v", r.calls)
	}
	if len(r.txns) != 0 || len(r.wallets) != 0 || len(r.positions) != 0 || len(r.orders) != 0 {
		t.Fatalf("pre-claimed batch applied: txns=%d wallets=%v", len(r.txns), r.wallets)
	}
	if r.offsets["pandora-ledger/0"] != 4 {
		t.Fatalf("offsets = %v", r.offsets)
	}
}

// uint32 leg prices 101 + (2^32-1) wrap to 100; validation must use int64
// and require each price in 1..99, skipping the fill as malformed.
func TestMintPriceWrapIsMalformed(t *testing.T) {
	r := newFakeRepo()
	before := testutil.ToFloat64(malformedEvents)
	apply(t, r, at(3, 1, fill("fw", commonv1.FillKind_FILL_KIND_MINT, 1,
		leg("o_a", "u_alice", 0, buy, 101, false), leg("o_b", "u_bob", 1, buy, 4294967295, true))))
	if len(r.txns) != 0 || len(r.wallets) != 0 || !r.applied[EventKey{0, 3, 1}] {
		t.Fatalf("wrapping mint applied: txns=%d wallets=%v", len(r.txns), r.wallets)
	}
	if got := testutil.ToFloat64(malformedEvents) - before; got != 1 {
		t.Fatalf("malformed counter grew by %v, want 1", got)
	}
	apply(t, r, at(4, 1, fill("fz", commonv1.FillKind_FILL_KIND_MINT, 1,
		leg("o_a", "u_alice", 0, buy, 0, false), leg("o_b", "u_bob", 1, buy, 100, true))))
	if len(r.txns) != 0 {
		t.Fatal("zero-priced mint leg must be malformed")
	}
}

func TestSecondSettlementIsNoop(t *testing.T) {
	r := newFakeRepo()
	evs := append(mintEvents(),
		at(6, 0, &enginev1.Event{Body: &enginev1.Event_MarketResolved{MarketResolved: &enginev1.MarketResolved{WinningOutcome: 0}}}),
		at(7, 0, &enginev1.Event{Body: &enginev1.Event_MarketResolved{MarketResolved: &enginev1.MarketResolved{WinningOutcome: 1}}}),
		at(8, 0, &enginev1.Event{Body: &enginev1.Event_MarketVoided{MarketVoided: &enginev1.MarketVoided{Reason: "late"}}}),
	)
	apply(t, r, evs...)
	if len(r.settlements) != 1 || len(r.txns) != 2 {
		t.Fatalf("settlements=%d txns=%d, want 1 and 2", len(r.settlements), len(r.txns))
	}
	if m := r.markets[mkt]; m.Status != "resolved" || *m.Winning != 0 {
		t.Fatalf("market = %+v", m)
	}
	if r.wallets["u_alice"] != 400 || r.wallets["u_bob"] != -400 {
		t.Fatalf("wallets = %v", r.wallets)
	}
	for _, k := range []EventKey{{0, 7, 0}, {0, 8, 0}} {
		if !r.applied[k] {
			t.Fatalf("ignored settlement %v not marked applied", k)
		}
	}
}
