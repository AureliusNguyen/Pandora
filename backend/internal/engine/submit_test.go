package engine

import (
	"math"
	"testing"

	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
)

func TestSubmitRestsGTCAndEmitsLevel(t *testing.T) {
	te := newT(t)
	evs := te.apply(sub("b1", "u_alice", 0, BUY, 55, 10))
	wantKinds(t, evs, "accepted(10) level(0 BUY 55 10)")
	if evs[0].Idx != 0 || evs[1].Idx != 1 || evs[0].MarketSeq != 2 || evs[1].MarketSeq != 2 {
		t.Fatalf("idx/market_seq wrong: %+v %+v", evs[0], evs[1])
	}
	te.apply(sub("b2", "u_bob", 0, BUY, 55, 5))
	te.apply(sub("b3", "u_bob", 0, BUY, 60, 1))
	te.apply(sub("n1", "u_carol", 1, BUY, 30, 2))
	wantBook(t, te.e, "mkt_demo", "0 BUY 60 1", "0 BUY 55 15", "1 BUY 30 2")
	if te.e.Position("mkt_demo", "u_alice", 0) != 0 {
		t.Fatal("resting buy must not change held position")
	}
}

func TestSubmitRejections(t *testing.T) {
	cases := []struct {
		name string
		o    *enginev1.SubmitOrder
		want string
	}{
		{"unknown market", func() *enginev1.SubmitOrder { o := sub("x", "u_alice", 0, BUY, 50, 1); o.MarketId = "nope"; return o }(), "rejected(MARKET_NOT_FOUND)"},
		{"price 0", sub("x", "u_alice", 0, BUY, 0, 1), "rejected(INVALID)"},
		{"price 100", sub("x", "u_alice", 0, BUY, 100, 1), "rejected(INVALID)"},
		{"qty 0", sub("x", "u_alice", 0, BUY, 50, 0), "rejected(INVALID)"},
		{"outcome out of range", sub("x", "u_alice", 2, BUY, 50, 1), "rejected(INVALID)"},
		{"side unspecified", sub("x", "u_alice", 0, commonv1.Side_SIDE_UNSPECIFIED, 50, 1), "rejected(INVALID)"},
		{"tif unspecified", withTif(sub("x", "u_alice", 0, BUY, 50, 1), commonv1.TimeInForce_TIME_IN_FORCE_UNSPECIFIED), "rejected(INVALID)"},
		{"empty order id", func() *enginev1.SubmitOrder { o := sub("", "u_alice", 0, BUY, 50, 1); return o }(), "rejected(INVALID)"},
		{"empty user", sub("x", "", 0, BUY, 50, 1), "rejected(INVALID)"},
		{"sell without shares", sub("x", "u_alice", 0, SELL, 50, 1), "rejected(INSUFFICIENT_SHARES)"},
		{"buy over limit", sub("x", "u_alice", 0, BUY, 50, PositionLimit+1), "rejected(POSITION_LIMIT)"},
		{"buy huge qty does not wrap", sub("x", "u_alice", 0, BUY, 50, math.MaxUint64), "rejected(POSITION_LIMIT)"},
		{"sell huge qty does not wrap", sub("x", "u_alice", 0, SELL, 50, math.MaxUint64), "rejected(INSUFFICIENT_SHARES)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			te := newT(t)
			evs := te.apply(tc.o)
			wantKinds(t, evs, tc.want)
			wantBook(t, te.e, "mkt_demo")
		})
	}
}

func TestRejectedDoesNotAdvanceMarketSeqButCarriesMarketID(t *testing.T) {
	te := newT(t)
	evs := te.apply(sub("x", "u_alice", 0, BUY, 0, 1))
	if evs[0].MarketSeq != 1 || evs[0].MarketId != "mkt_demo" {
		t.Fatalf("known-market rejection: seq=%d id=%s", evs[0].MarketSeq, evs[0].MarketId)
	}
	o := sub("y", "u_alice", 0, BUY, 50, 1)
	o.MarketId = "ghost"
	evs = te.apply(o)
	if evs[0].MarketSeq != 0 || evs[0].MarketId != "ghost" || evs[0].GetOrderRejected().GetOrder().GetOrderId() != "y" {
		t.Fatalf("unknown-market rejection must carry market_id and seq 0: %+v", evs[0])
	}
}

func TestDuplicateClientOrderID(t *testing.T) {
	te := newT(t)
	wantKinds(t, te.apply(sub("a1", "u_alice", 0, BUY, 50, 1)), "accepted(1) level(0 BUY 50 1)")
	dup := sub("a2", "u_alice", 0, BUY, 50, 1)
	dup.ClientOrderId = "c_a1"
	wantKinds(t, te.apply(dup), "rejected(DUPLICATE_CLIENT_ORDER_ID)")
	other := sub("b1", "u_bob", 0, BUY, 50, 1)
	other.ClientOrderId = "c_a1" // same cid, different user: fine
	wantKinds(t, te.apply(other), "accepted(1) level(0 BUY 50 2)")
	// duplicate order_id is INVALID
	wantKinds(t, te.apply(sub("a1", "u_carol", 1, BUY, 50, 1)), "rejected(INVALID)")
	// a rejected order's cid is not consumed
	bad := sub("z", "u_carol", 0, BUY, 0, 1)
	bad.ClientOrderId = "c_z"
	te.apply(bad)
	good := sub("z2", "u_carol", 0, BUY, 10, 1)
	good.ClientOrderId = "c_z"
	wantKinds(t, te.apply(good), "accepted(1) level(0 BUY 10 1)")
}

func TestPositionLimitCountsRestingBuys(t *testing.T) {
	te := newT(t)
	wantKinds(t, te.apply(sub("a1", "u_alice", 0, BUY, 10, 6000)), "accepted(6000) level(0 BUY 10 6000)")
	wantKinds(t, te.apply(sub("a2", "u_alice", 0, BUY, 11, 4001)), "rejected(POSITION_LIMIT)")
	wantKinds(t, te.apply(sub("a3", "u_alice", 0, BUY, 11, 4000)), "accepted(4000) level(0 BUY 11 4000)")
	wantKinds(t, te.apply(sub("a4", "u_alice", 1, BUY, 11, 1)), "accepted(1) level(1 BUY 11 1)") // other outcome unaffected
}

func TestSubmitOnLockedMarketRejected(t *testing.T) {
	te := newT(t)
	te.e.markets["mkt_demo"].status = commonv1.MarketStatus_MARKET_STATUS_LOCKED
	wantKinds(t, te.apply(sub("a1", "u_alice", 0, BUY, 10, 1)), "rejected(MARKET_LOCKED)")
}
