package engine

import (
	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
)

// cancel removes a resting order on a user's request. Unknown market or
// order: no events. A non-empty user_id must match the resting order's
// user; an empty user_id skips the ownership check (replay and admin use;
// the RPC layer rejects empty user_id before it gets here).
func (e *Engine) cancel(c *enginev1.CancelOrder) {
	m, ok := e.markets[c.GetMarketId()]
	e.em.setMarket(c.GetMarketId(), m)
	if !ok {
		return
	}
	o, ok := m.orders[c.GetOrderId()]
	if !ok {
		return
	}
	if c.GetUserId() != "" && c.GetUserId() != o.userID {
		return
	}
	e.removeResting(m, o, "user")
}

// removeResting takes a resting order off the book, releases its position
// reservation and emits OrderCanceled with reason.
func (e *Engine) removeResting(m *market, o *order, reason string) {
	lad := m.ladder(o.outcome, o.side)
	lad.remove(o)
	delete(m.orders, o.id)
	p := m.posMut(o.userID, o.outcome)
	if o.side == commonv1.Side_SIDE_BUY {
		p.openBuy -= o.remaining
	} else {
		p.locked -= o.remaining
	}
	e.em.touch(o.outcome, o.side, o.price, lad.at(o.price))
	e.em.push(&enginev1.Event{Body: &enginev1.Event_OrderCanceled{OrderCanceled: &enginev1.OrderCanceled{
		OrderId: o.id, UserId: o.userID, CanceledQty: o.remaining, Reason: reason,
	}}}, true)
}

// lock handles LockMarket: only an OPEN market changes; anything else is
// a no-op with no events.
func (e *Engine) lock(id string) {
	m, ok := e.markets[id]
	e.em.setMarket(id, m)
	if !ok || m.status != commonv1.MarketStatus_MARKET_STATUS_OPEN {
		return
	}
	e.lockMarket(m)
}

// lockMarket sets LOCKED and cancels every resting order with reason
// "locked": outcome ascending, bids then asks, best price first, FIFO.
func (e *Engine) lockMarket(m *market) {
	m.status = commonv1.MarketStatus_MARKET_STATUS_LOCKED
	e.em.push(&enginev1.Event{Body: &enginev1.Event_MarketLocked{MarketLocked: &enginev1.MarketLocked{}}}, true)
	for i := range m.books {
		for _, lad := range [2]*ladder{&m.books[i].bids, &m.books[i].asks} {
			for lad.best != 0 {
				e.removeResting(m, lad.at(lad.best).head, "locked")
			}
		}
	}
}

// lockForTerminal returns the market ready for resolve/void (locking an
// OPEN market first) or nil when the command must be ignored.
func (e *Engine) lockForTerminal(id string) *market {
	m, ok := e.markets[id]
	e.em.setMarket(id, m)
	if !ok {
		return nil
	}
	switch m.status {
	case commonv1.MarketStatus_MARKET_STATUS_OPEN:
		e.lockMarket(m)
	case commonv1.MarketStatus_MARKET_STATUS_LOCKED:
	default:
		return nil
	}
	return m
}

// resolve handles ResolveMarket: an invalid winning outcome, an unknown
// market or a market already resolved/voided is ignored; an OPEN market is
// locked first (MarketLocked and the "locked" cancels precede MarketResolved).
func (e *Engine) resolve(r *enginev1.ResolveMarket) {
	m, ok := e.markets[r.GetMarketId()]
	if ok && r.GetWinningOutcome() >= m.n {
		e.em.setMarket(r.GetMarketId(), m)
		return
	}
	if m = e.lockForTerminal(r.GetMarketId()); m == nil {
		return
	}
	m.status = commonv1.MarketStatus_MARKET_STATUS_RESOLVED
	e.em.push(&enginev1.Event{Body: &enginev1.Event_MarketResolved{MarketResolved: &enginev1.MarketResolved{WinningOutcome: r.GetWinningOutcome()}}}, true)
}

// void handles VoidMarket with the same lock-first semantics as resolve.
func (e *Engine) void(v *enginev1.VoidMarket) {
	m := e.lockForTerminal(v.GetMarketId())
	if m == nil {
		return
	}
	m.status = commonv1.MarketStatus_MARKET_STATUS_VOIDED
	e.em.push(&enginev1.Event{Body: &enginev1.Event_MarketVoided{MarketVoided: &enginev1.MarketVoided{Reason: v.GetReason()}}}, true)
}
