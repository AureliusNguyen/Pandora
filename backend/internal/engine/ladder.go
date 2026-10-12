// Package engine is the deterministic matching engine: same Command sequence
// in, same Events and state out. No clocks, no I/O, no goroutines.
package engine

import (
	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
)

const (
	// MinPrice is the lowest valid order price in integer cents.
	MinPrice uint32 = 1
	// MaxPrice is the highest valid order price in integer cents.
	MaxPrice uint32 = 99
	// PositionLimit is the maximum shares a user may hold or have on open BUY
	// orders per outcome.
	PositionLimit uint64 = 10000
	// MaxOutcomes is the most outcomes a market may have; it sizes the
	// fixed per-fill maker arrays so matching does not allocate. The engine
	// refuses a CreateMarket with fewer than 2 or more than MaxOutcomes
	// labels: no market and no event (the journaled command is a no-op).
	MaxOutcomes = 8
)

// order is a resting order. It is allocated only when an order actually
// rests; a taker that fills completely never allocates one.
type order struct {
	id        string
	userID    string
	clientID  string
	outcome   uint32
	side      commonv1.Side
	price     uint32
	qty       uint64
	remaining uint64
	prev      *order
	next      *order
}

// level is one price level: an intrusive FIFO list of resting orders plus
// the cached sum of their remaining qty. touchedSeq dedupes LevelUpdate
// events inside one command without a map.
type level struct {
	qty        uint64
	head, tail *order
	touchedSeq uint64
}

// ladder is one side of one outcome's book. Prices are 1..99, so levels is a
// fixed array indexed by price: O(1) lookup, zero allocations, and the best
// level is a short scan from the cached best price. FIFO inside a level is
// the intrusive list (O(1) append, O(1) unlink on cancel or full fill).
type ladder struct {
	side   commonv1.Side
	levels [MaxPrice + 1]level
	best   uint32 // price of the best non-empty level, 0 when the ladder is empty
	count  int    // resting orders on this ladder
}

func (l *ladder) at(price uint32) *level { return &l.levels[price] }

func (l *ladder) bestLevel() *level {
	if l.best == 0 {
		return nil
	}
	return &l.levels[l.best]
}

// better reports whether price a is strictly better than b for this side.
func (l *ladder) better(a, b uint32) bool {
	if l.side == commonv1.Side_SIDE_BUY {
		return a > b
	}
	return a < b
}

// next returns the next worse non-empty price after price, or 0.
func (l *ladder) next(price uint32) uint32 {
	if l.side == commonv1.Side_SIDE_BUY {
		for p := int(price) - 1; p >= int(MinPrice); p-- {
			if l.levels[p].qty > 0 {
				return uint32(p)
			}
		}
		return 0
	}
	for p := price + 1; p <= MaxPrice; p++ {
		if l.levels[p].qty > 0 {
			return p
		}
	}
	return 0
}

// add appends o to the FIFO at o.price.
func (l *ladder) add(o *order) {
	lvl := &l.levels[o.price]
	o.prev, o.next = lvl.tail, nil
	if lvl.tail != nil {
		lvl.tail.next = o
	} else {
		lvl.head = o
	}
	lvl.tail = o
	lvl.qty += o.remaining
	l.count++
	if l.best == 0 || l.better(o.price, l.best) {
		l.best = o.price
	}
}

func (l *ladder) unlink(o *order) {
	lvl := &l.levels[o.price]
	if o.prev != nil {
		o.prev.next = o.next
	} else {
		lvl.head = o.next
	}
	if o.next != nil {
		o.next.prev = o.prev
	} else {
		lvl.tail = o.prev
	}
	o.prev, o.next = nil, nil
	l.count--
	if lvl.qty == 0 && o.price == l.best {
		l.best = l.next(o.price)
	}
}

// fill reduces o by q (q <= o.remaining) and unlinks it when it is done.
func (l *ladder) fill(o *order, q uint64) {
	o.remaining -= q
	l.levels[o.price].qty -= q
	if o.remaining == 0 {
		l.unlink(o)
	}
}

// remove takes a resting order off the ladder (cancel, lock).
func (l *ladder) remove(o *order) {
	l.levels[o.price].qty -= o.remaining
	l.unlink(o)
}
