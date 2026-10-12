package engine

import (
	"testing"

	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
)

func mkOrder(id string, price uint32, qty uint64) *order {
	return &order{id: id, userID: "u_" + id, price: price, qty: qty, remaining: qty}
}

func TestLadderBidBestIsHighestAndFIFO(t *testing.T) {
	l := &ladder{side: commonv1.Side_SIDE_BUY}
	if l.bestLevel() != nil {
		t.Fatal("empty ladder must have no best level")
	}
	a, b, c := mkOrder("a", 50, 10), mkOrder("b", 55, 5), mkOrder("c", 55, 7)
	l.add(a)
	l.add(b)
	l.add(c)
	if l.best != 55 || l.bestLevel().qty != 12 || l.count != 3 {
		t.Fatalf("best=%d qty=%d count=%d", l.best, l.bestLevel().qty, l.count)
	}
	if l.bestLevel().head != b || b.next != c || c.prev != b || l.bestLevel().tail != c {
		t.Fatal("FIFO order inside level broken")
	}
	if n := l.next(55); n != 50 {
		t.Fatalf("next(55)=%d want 50", n)
	}
	if n := l.next(50); n != 0 {
		t.Fatalf("next(50)=%d want 0", n)
	}
}

func TestLadderAskBestIsLowest(t *testing.T) {
	l := &ladder{side: commonv1.Side_SIDE_SELL}
	l.add(mkOrder("a", 60, 1))
	l.add(mkOrder("b", 45, 1))
	l.add(mkOrder("c", 99, 1))
	if l.best != 45 || l.next(45) != 60 || l.next(60) != 99 || l.next(99) != 0 {
		t.Fatalf("ask ordering wrong: best=%d", l.best)
	}
}

func TestLadderFillAndRemoveMaintainQtyAndBest(t *testing.T) {
	l := &ladder{side: commonv1.Side_SIDE_SELL}
	a, b := mkOrder("a", 45, 10), mkOrder("b", 45, 4)
	c := mkOrder("c", 50, 3)
	l.add(a)
	l.add(b)
	l.add(c)
	l.fill(a, 4)
	if a.remaining != 6 || l.at(45).qty != 10 || l.at(45).head != a {
		t.Fatalf("partial fill: remaining=%d lvl=%d", a.remaining, l.at(45).qty)
	}
	l.fill(a, 6)
	if l.at(45).head != b || l.at(45).qty != 4 || l.count != 2 {
		t.Fatalf("full fill should unlink head: head=%v qty=%d count=%d", l.at(45).head, l.at(45).qty, l.count)
	}
	l.remove(b)
	if l.best != 50 || l.at(45).qty != 0 || l.at(45).head != nil || l.at(45).tail != nil {
		t.Fatalf("remove last order must empty level and advance best: best=%d", l.best)
	}
	l.remove(c)
	if l.best != 0 || l.count != 0 {
		t.Fatalf("ladder should be empty: best=%d count=%d", l.best, l.count)
	}
}

func TestLadderRemoveMiddleOfLevel(t *testing.T) {
	l := &ladder{side: commonv1.Side_SIDE_BUY}
	a, b, c := mkOrder("a", 70, 1), mkOrder("b", 70, 2), mkOrder("c", 70, 3)
	l.add(a)
	l.add(b)
	l.add(c)
	l.remove(b)
	if a.next != c || c.prev != a || l.at(70).qty != 4 || l.at(70).head != a || l.at(70).tail != c {
		t.Fatal("middle unlink broke list")
	}
}

func TestMarketPositionsAndLadders(t *testing.T) {
	m := newMarket("mkt_demo", 2)
	if m.posGet("u_alice", 0) != nil {
		t.Fatal("posGet must not create")
	}
	p := m.posMut("u_alice", 0)
	p.qty = 5
	if m.posGet("u_alice", 0).qty != 5 || m.posMut("u_alice", 0) != p {
		t.Fatal("posMut must return the same position")
	}
	if m.ladder(1, commonv1.Side_SIDE_SELL) != &m.books[1].asks || m.ladder(0, commonv1.Side_SIDE_BUY) != &m.books[0].bids {
		t.Fatal("ladder lookup wrong")
	}
	if m.status != commonv1.MarketStatus_MARKET_STATUS_OPEN || m.n != 2 || m.seq != 0 {
		t.Fatal("new market must be open with seq 0")
	}
}
