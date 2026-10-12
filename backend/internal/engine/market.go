package engine

import (
	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
)

type posKey struct {
	user    string
	outcome uint32
}

// position is one user's holding on one outcome. locked is the part of qty
// committed to resting SELL orders; openBuy is the qty on resting BUY orders
// (counted against PositionLimit so resting buys cannot breach it).
type position struct {
	qty     uint64
	locked  uint64
	openBuy uint64
}

type outcomeBook struct {
	bids ladder
	asks ladder
}

type market struct {
	id        string
	status    commonv1.MarketStatus
	n         uint32 // outcome count
	seq       uint64 // market_seq
	books     []outcomeBook
	orders    map[string]*order
	positions map[posKey]*position
}

func newMarket(id string, n uint32) *market {
	m := &market{
		id:        id,
		status:    commonv1.MarketStatus_MARKET_STATUS_OPEN,
		n:         n,
		books:     make([]outcomeBook, n),
		orders:    make(map[string]*order),
		positions: make(map[posKey]*position),
	}
	for i := range m.books {
		m.books[i].bids.side = commonv1.Side_SIDE_BUY
		m.books[i].asks.side = commonv1.Side_SIDE_SELL
	}
	return m
}

func (m *market) ladder(outcome uint32, side commonv1.Side) *ladder {
	if side == commonv1.Side_SIDE_BUY {
		return &m.books[outcome].bids
	}
	return &m.books[outcome].asks
}

// posGet returns the position or nil; it never allocates.
func (m *market) posGet(user string, outcome uint32) *position {
	return m.positions[posKey{user, outcome}]
}

// posMut returns the position, creating it when absent.
func (m *market) posMut(user string, outcome uint32) *position {
	k := posKey{user, outcome}
	p := m.positions[k]
	if p == nil {
		p = &position{}
		m.positions[k] = p
	}
	return p
}
