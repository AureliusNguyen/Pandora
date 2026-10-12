package engine

import (
	"testing"

	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
)

func cancelOf(id, user string) *enginev1.CancelOrder {
	return &enginev1.CancelOrder{OrderId: id, MarketId: "mkt_demo", UserId: user}
}

func TestCancelResting(t *testing.T) {
	te := newT(t)
	te.apply(sub("b1", "u_alice", 0, BUY, 55, 10))
	te.apply(sub("b2", "u_bob", 0, BUY, 55, 5))
	wantKinds(t, te.apply(cancelOf("b1", "u_alice")), "canceled(user,10) level(0 BUY 55 5)")
	wantBook(t, te.e, "mkt_demo", "0 BUY 55 5")
	// second cancel of the same id: nothing
	if evs := te.apply(cancelOf("b1", "u_alice")); len(evs) != 0 {
		t.Fatalf("double cancel emitted %s", kinds(evs))
	}
	// openBuy released: alice can now buy the full limit again
	wantKinds(t, te.apply(sub("b3", "u_alice", 0, BUY, 10, PositionLimit)), "accepted(10000) level(0 BUY 10 10000)")
}

func TestCancelIgnoresWrongUserMarketOrUnknown(t *testing.T) {
	te := newT(t)
	te.create("mkt_other", "A", "B")
	te.apply(sub("b1", "u_alice", 0, BUY, 55, 10))
	cases := []*enginev1.CancelOrder{
		cancelOf("b1", "u_bob"),
		{OrderId: "b1", MarketId: "mkt_other", UserId: "u_alice"},
		{OrderId: "b1", MarketId: "ghost", UserId: "u_alice"},
		cancelOf("nope", "u_alice"),
	}
	for i, c := range cases {
		if evs := te.apply(c); len(evs) != 0 {
			t.Fatalf("case %d emitted %s", i, kinds(evs))
		}
		wantBook(t, te.e, "mkt_demo", "0 BUY 55 10")
	}
	// empty user_id means "no ownership check" (admin/replay use)
	wantKinds(t, te.apply(cancelOf("b1", "")), "canceled(user,10) level(0 BUY 55 0)")
}

func TestCancelSellReleasesLockedShares(t *testing.T) {
	te := newT(t)
	seedMint(t, te, 10)
	te.apply(sub("s1", "u_bob", 0, SELL, 70, 8))
	wantKinds(t, te.apply(sub("s2", "u_bob", 0, SELL, 70, 3)), "rejected(INSUFFICIENT_SHARES)")
	te.apply(cancelOf("s1", "u_bob"))
	wantKinds(t, te.apply(sub("s3", "u_bob", 0, SELL, 70, 10)), "accepted(10) level(0 SELL 70 10)")
}

func TestLockCancelsEverythingBestFirst(t *testing.T) {
	te := newT(t)
	seedMint(t, te, 10)
	te.apply(sub("b1", "u_alice", 0, BUY, 50, 1))
	te.apply(sub("b2", "u_alice", 0, BUY, 52, 1))
	te.apply(sub("b3", "u_alice", 0, BUY, 52, 2))
	te.apply(sub("s1", "u_bob", 0, SELL, 60, 3))
	te.apply(sub("n1", "u_carol", 1, BUY, 20, 4))
	evs := te.apply(&enginev1.LockMarket{MarketId: "mkt_demo"})
	wantKinds(t, evs, "locked canceled(locked,1) canceled(locked,2) canceled(locked,1) canceled(locked,3) canceled(locked,4) level(0 BUY 52 0) level(0 BUY 50 0) level(0 SELL 60 0) level(1 BUY 20 0)")
	if evs[1].GetOrderCanceled().OrderId != "b2" || evs[2].GetOrderCanceled().OrderId != "b3" || evs[3].GetOrderCanceled().OrderId != "b1" {
		t.Fatal("lock must cancel best price first, FIFO within level")
	}
	wantBook(t, te.e, "mkt_demo")
	_, _, status, _ := te.e.Book("mkt_demo")
	if status != commonv1.MarketStatus_MARKET_STATUS_LOCKED {
		t.Fatalf("status=%v", status)
	}
	wantKinds(t, te.apply(sub("late", "u_alice", 0, BUY, 50, 1)), "rejected(MARKET_LOCKED)")
	if evs := te.apply(cancelOf("b1", "u_alice")); len(evs) != 0 {
		t.Fatal("cancel after lock must be a no-op")
	}
	if evs := te.apply(&enginev1.LockMarket{MarketId: "mkt_demo"}); len(evs) != 0 {
		t.Fatal("second lock must be a no-op")
	}
	if te.e.Position("mkt_demo", "u_bob", 0) != 10 {
		t.Fatal("lock must not change held positions")
	}
}

func TestResolveImplicitlyLocksThenResolves(t *testing.T) {
	te := newT(t)
	te.apply(sub("b1", "u_alice", 0, BUY, 50, 1))
	evs := te.apply(&enginev1.ResolveMarket{MarketId: "mkt_demo", WinningOutcome: 1})
	wantKinds(t, evs, "locked canceled(locked,1) resolved(1) level(0 BUY 50 0)")
	_, _, status, _ := te.e.Book("mkt_demo")
	if status != commonv1.MarketStatus_MARKET_STATUS_RESOLVED {
		t.Fatalf("status=%v", status)
	}
	if evs := te.apply(&enginev1.ResolveMarket{MarketId: "mkt_demo", WinningOutcome: 0}); len(evs) != 0 {
		t.Fatal("resolve of a resolved market must be a no-op")
	}
	wantKinds(t, te.apply(sub("late", "u_alice", 0, BUY, 50, 1)), "rejected(MARKET_LOCKED)")
}

func TestResolveOnLockedAndInvalidOutcome(t *testing.T) {
	te := newT(t)
	te.apply(&enginev1.LockMarket{MarketId: "mkt_demo"})
	if evs := te.apply(&enginev1.ResolveMarket{MarketId: "mkt_demo", WinningOutcome: 2}); len(evs) != 0 {
		t.Fatal("winning outcome out of range must be ignored")
	}
	wantKinds(t, te.apply(&enginev1.ResolveMarket{MarketId: "mkt_demo", WinningOutcome: 0}), "resolved(0)")
}

func TestVoid(t *testing.T) {
	te := newT(t)
	te.apply(sub("b1", "u_alice", 0, BUY, 50, 1))
	wantKinds(t, te.apply(&enginev1.VoidMarket{MarketId: "mkt_demo", Reason: "feeds stale"}),
		"locked canceled(locked,1) voided(feeds stale) level(0 BUY 50 0)")
	_, _, status, _ := te.e.Book("mkt_demo")
	if status != commonv1.MarketStatus_MARKET_STATUS_VOIDED {
		t.Fatalf("status=%v", status)
	}
	if evs := te.apply(&enginev1.VoidMarket{MarketId: "ghost"}); len(evs) != 0 {
		t.Fatal("void of unknown market must be a no-op")
	}
}
