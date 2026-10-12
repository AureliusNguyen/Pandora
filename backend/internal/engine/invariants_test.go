package engine

import (
	"testing"

	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
)

// checkInvariants walks internal state. It is the oracle for the property
// test and must hold after every command:
//  1. complete-set symmetry: total shares on every outcome are equal;
//  2. per position: locked <= qty and qty + openBuy <= PositionLimit;
//  3. every level's qty == sum of remaining of its FIFO; every resting
//     order is in the index at its own price; ladder count and best agree;
//  4. sum(locked) == resting SELL qty and sum(openBuy) == resting BUY qty.
func checkInvariants(t testing.TB, e *Engine) {
	t.Helper()
	for id, m := range e.markets {
		totals := make([]uint64, m.n)
		var lockedSum, openBuySum uint64
		for k, p := range m.positions {
			totals[k.outcome] += p.qty
			if p.locked > p.qty {
				t.Fatalf("%s %v: locked %d > qty %d", id, k, p.locked, p.qty)
			}
			if p.qty+p.openBuy > PositionLimit {
				t.Fatalf("%s %v: qty %d + openBuy %d exceeds limit", id, k, p.qty, p.openBuy)
			}
			lockedSum += p.locked
			openBuySum += p.openBuy
		}
		for i := uint32(1); i < m.n; i++ {
			if totals[i] != totals[0] {
				t.Fatalf("%s: outcome totals differ: %v", id, totals)
			}
		}
		var restingSell, restingBuy, resting int
		for i := range m.books {
			for _, lad := range [2]*ladder{&m.books[i].bids, &m.books[i].asks} {
				var bestSeen uint32
				var onLadder int
				for p := MinPrice; p <= MaxPrice; p++ {
					lvl := lad.at(p)
					var sum uint64
					for o := lvl.head; o != nil; o = o.next {
						if o.price != p || o.outcome != uint32(i) || o.side != lad.side {
							t.Fatalf("%s: order %s filed on wrong level", id, o.id)
						}
						if m.orders[o.id] != o {
							t.Fatalf("%s: resting order %s missing from index", id, o.id)
						}
						if o.remaining == 0 {
							t.Fatalf("%s: zero-remaining order %s resting", id, o.id)
						}
						sum += o.remaining
						resting++
						onLadder++
						if lad.side == SELL {
							restingSell += int(o.remaining)
						} else {
							restingBuy += int(o.remaining)
						}
					}
					if sum != lvl.qty {
						t.Fatalf("%s: level %d qty %d != sum %d", id, p, lvl.qty, sum)
					}
					if lvl.qty > 0 && (bestSeen == 0 || lad.better(p, bestSeen)) {
						bestSeen = p
					}
				}
				if bestSeen != lad.best {
					t.Fatalf("%s: cached best %d != actual %d", id, lad.best, bestSeen)
				}
				if onLadder != lad.count {
					t.Fatalf("%s: ladder count %d != actual %d", id, lad.count, onLadder)
				}
			}
		}
		if resting != len(m.orders) {
			t.Fatalf("%s: %d resting orders but index has %d", id, resting, len(m.orders))
		}
		if uint64(restingSell) != lockedSum || uint64(restingBuy) != openBuySum {
			t.Fatalf("%s: reservations drift: locked %d vs sell %d, openBuy %d vs buy %d", id, lockedSum, restingSell, openBuySum, restingBuy)
		}
		if m.status != commonv1.MarketStatus_MARKET_STATUS_OPEN && len(m.orders) != 0 {
			t.Fatalf("%s: non-open market still has resting orders", id)
		}
	}
}
