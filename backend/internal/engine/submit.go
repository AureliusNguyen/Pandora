package engine

import (
	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
)

// submit validates o against the market and the user's position, rejecting
// with the contract's reason, and hands a valid order to execute. The
// validation here is what guarantees the ladder preconditions: price in
// 1..99, qty > 0, outcome < n.
func (e *Engine) submit(o *enginev1.SubmitOrder) {
	m, ok := e.markets[o.GetMarketId()]
	e.em.setMarket(o.GetMarketId(), m)
	if !ok {
		e.reject(o, commonv1.RejectReason_REJECT_REASON_MARKET_NOT_FOUND)
		return
	}
	if m.status != commonv1.MarketStatus_MARKET_STATUS_OPEN {
		e.reject(o, commonv1.RejectReason_REJECT_REASON_MARKET_LOCKED)
		return
	}
	if !validShape(o, m) {
		e.reject(o, commonv1.RejectReason_REJECT_REASON_INVALID)
		return
	}
	if o.ClientOrderId != "" {
		if _, dup := e.cids[cidKey{o.UserId, o.ClientOrderId}]; dup {
			e.reject(o, commonv1.RejectReason_REJECT_REASON_DUPLICATE_CLIENT_ORDER_ID)
			return
		}
	}
	if _, dup := m.orders[o.OrderId]; dup {
		e.reject(o, commonv1.RejectReason_REJECT_REASON_INVALID)
		return
	}
	p := m.posGet(o.UserId, o.Outcome)
	switch o.Side {
	case commonv1.Side_SIDE_SELL:
		var free uint64
		if p != nil {
			free = p.qty - p.locked
		}
		if o.Qty > free {
			e.reject(o, commonv1.RejectReason_REJECT_REASON_INSUFFICIENT_SHARES)
			return
		}
	default: // BUY
		var held uint64
		if p != nil {
			held = p.qty + p.openBuy
		}
		// o.Qty > PositionLimit first so held+o.Qty cannot wrap.
		if o.Qty > PositionLimit || held+o.Qty > PositionLimit {
			e.reject(o, commonv1.RejectReason_REJECT_REASON_POSITION_LIMIT)
			return
		}
	}
	e.execute(m, o)
}

func validShape(o *enginev1.SubmitOrder, m *market) bool {
	if o.OrderId == "" || o.UserId == "" {
		return false
	}
	if o.Price < MinPrice || o.Price > MaxPrice || o.Qty == 0 || o.Outcome >= m.n {
		return false
	}
	if o.Side != commonv1.Side_SIDE_BUY && o.Side != commonv1.Side_SIDE_SELL {
		return false
	}
	switch o.Tif {
	case commonv1.TimeInForce_TIME_IN_FORCE_GTC, commonv1.TimeInForce_TIME_IN_FORCE_IOC, commonv1.TimeInForce_TIME_IN_FORCE_FOK:
		return true
	}
	return false
}

func (e *Engine) reject(o *enginev1.SubmitOrder, reason commonv1.RejectReason) {
	e.em.push(&enginev1.Event{Body: &enginev1.Event_OrderRejected{OrderRejected: &enginev1.OrderRejected{Order: o, Reason: reason}}}, false)
}

// accept emits OrderAccepted and consumes the client_order_id.
func (e *Engine) accept(o *enginev1.SubmitOrder, restingQty uint64) {
	if o.ClientOrderId != "" {
		e.cids[cidKey{o.UserId, o.ClientOrderId}] = struct{}{}
	}
	e.em.push(&enginev1.Event{Body: &enginev1.Event_OrderAccepted{OrderAccepted: &enginev1.OrderAccepted{Order: o, RestingQty: restingQty}}}, true)
}

// rest places the unfilled remainder on the book and reserves it against
// the position (openBuy for BUY, locked shares for SELL).
func (e *Engine) rest(m *market, o *enginev1.SubmitOrder, remaining uint64) {
	ro := &order{
		id: o.OrderId, userID: o.UserId, clientID: o.ClientOrderId,
		outcome: o.Outcome, side: o.Side, price: o.Price, qty: o.Qty, remaining: remaining,
	}
	lad := m.ladder(o.Outcome, o.Side)
	lad.add(ro)
	m.orders[ro.id] = ro
	p := m.posMut(o.UserId, o.Outcome)
	if o.Side == commonv1.Side_SIDE_BUY {
		p.openBuy += remaining
	} else {
		p.locked += remaining
	}
	e.em.touch(o.Outcome, o.Side, o.Price, lad.at(o.Price))
}
