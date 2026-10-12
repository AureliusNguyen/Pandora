package engine

import (
	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
)

// cursor walks a ladder best-first without mutating it, so the planner can
// look ahead (FOK, post_only, STP) using the same priority as execution.
type cursor struct {
	lad   *ladder
	price uint32
	ord   *order
	taken uint64 // qty already planned from ord
}

func (c *cursor) init(lad *ladder) {
	c.lad, c.price, c.taken, c.ord = lad, lad.best, 0, nil
	if c.price != 0 {
		c.ord = lad.at(c.price).head
	}
}

func (c *cursor) done() bool { return c.ord == nil }

func (c *cursor) avail() uint64 { return c.ord.remaining - c.taken }

func (c *cursor) consume(q uint64) {
	c.taken += q
	if c.taken < c.ord.remaining {
		return
	}
	c.taken = 0
	c.ord = c.ord.next
	if c.ord == nil {
		c.price = c.lad.next(c.price)
		if c.price != 0 {
			c.ord = c.lad.at(c.price).head
		}
	}
}

type plannedFill struct {
	kind   commonv1.FillKind
	price  uint32 // taker leg price
	qty    uint64
	makers [maxOutcomes]*order // TRANSFER: makers[0]; MINT: makers[j] for every j != taker outcome
}

// planner buffers are reused across commands so planning does not allocate.
type planner struct {
	fills   []plannedFill
	cursors [maxOutcomes]cursor
	ask     cursor
	filled  uint64
	stp     bool // the walk stopped because the next maker is the taker's own order
	crosses bool // the best opposing price crosses the taker (post_only check)
}

func (pl *planner) reset() {
	pl.fills = pl.fills[:0]
	pl.filled, pl.stp, pl.crosses = 0, false, false
}

// planBuy plans a taker BUY on outcome i: per step the TRANSFER candidate
// is the best ask on i at <= o.Price and the MINT candidate is the set of
// best bids on every other outcome when o.Price + sum(bids) >= 100 (taker
// pays 100 - sum). The lower taker price wins; a tie goes to TRANSFER.
func (pl *planner) planBuy(m *market, o *enginev1.SubmitOrder) {
	pl.reset()
	i := o.Outcome
	pl.ask.init(&m.books[i].asks)
	for j := uint32(0); j < m.n; j++ {
		if j != i {
			pl.cursors[j].init(&m.books[j].bids)
		}
	}
	remaining := o.Qty
	for remaining > 0 {
		transferOK := !pl.ask.done() && pl.ask.price <= o.Price
		mintOK := true
		var sum uint32
		for j := uint32(0); j < m.n; j++ {
			if j == i {
				continue
			}
			if pl.cursors[j].done() {
				mintOK = false
				break
			}
			sum += pl.cursors[j].price
		}
		var mintPrice uint32
		if mintOK {
			// taker pays 100 - sum, which must be >= 1 and <= o.Price.
			if sum >= 100 || o.Price+sum < 100 {
				mintOK = false
			} else {
				mintPrice = 100 - sum
			}
		}
		if !transferOK && !mintOK {
			return
		}
		pl.crosses = true
		if transferOK && (!mintOK || pl.ask.price <= mintPrice) {
			if pl.ask.ord.userID == o.UserId {
				pl.stp = true
				return
			}
			q := min(remaining, pl.ask.avail())
			f := plannedFill{kind: commonv1.FillKind_FILL_KIND_TRANSFER, price: pl.ask.price, qty: q}
			f.makers[0] = pl.ask.ord
			pl.fills = append(pl.fills, f)
			pl.ask.consume(q)
			remaining -= q
			pl.filled += q
			continue
		}
		q := remaining
		for j := uint32(0); j < m.n; j++ {
			if j == i {
				continue
			}
			if pl.cursors[j].ord.userID == o.UserId {
				pl.stp = true
				return
			}
			q = min(q, pl.cursors[j].avail())
		}
		f := plannedFill{kind: commonv1.FillKind_FILL_KIND_MINT, price: mintPrice, qty: q}
		for j := uint32(0); j < m.n; j++ {
			if j != i {
				f.makers[j] = pl.cursors[j].ord
				pl.cursors[j].consume(q)
			}
		}
		pl.fills = append(pl.fills, f)
		remaining -= q
		pl.filled += q
	}
}

// planSell plans a taker SELL on its outcome: TRANSFER against resting
// bids at >= o.Price, best price first, FIFO within a level.
func (pl *planner) planSell(m *market, o *enginev1.SubmitOrder) {
	pl.reset()
	c := &pl.cursors[o.Outcome]
	c.init(&m.books[o.Outcome].bids)
	remaining := o.Qty
	for remaining > 0 && !c.done() && c.price >= o.Price {
		pl.crosses = true
		if c.ord.userID == o.UserId {
			pl.stp = true
			return
		}
		q := min(remaining, c.avail())
		f := plannedFill{kind: commonv1.FillKind_FILL_KIND_TRANSFER, price: c.price, qty: q}
		f.makers[0] = c.ord
		pl.fills = append(pl.fills, f)
		c.consume(q)
		remaining -= q
		pl.filled += q
	}
}

// execute runs a validated order: plan, decide (post_only, FOK), then emit
// OrderAccepted, the fills in order, an ioc/stp cancel, and rest the rest.
// A post_only or FOK taker that would hit its own order is rejected
// (POST_ONLY_WOULD_CROSS / FOK_UNFILLABLE); STP cancel-newest applies only
// to GTC and IOC takers.
func (e *Engine) execute(m *market, o *enginev1.SubmitOrder) {
	pl := &e.plan
	if o.Side == commonv1.Side_SIDE_BUY {
		pl.planBuy(m, o)
	} else {
		pl.planSell(m, o)
	}
	if o.PostOnly && pl.crosses {
		e.reject(o, commonv1.RejectReason_REJECT_REASON_POST_ONLY_WOULD_CROSS)
		return
	}
	if o.Tif == commonv1.TimeInForce_TIME_IN_FORCE_FOK && pl.filled < o.Qty {
		e.reject(o, commonv1.RejectReason_REJECT_REASON_FOK_UNFILLABLE)
		return
	}
	remaining := o.Qty - pl.filled
	var restQty uint64
	cancelReason := ""
	switch {
	case remaining == 0:
	case pl.stp:
		cancelReason = "stp"
	case o.Tif == commonv1.TimeInForce_TIME_IN_FORCE_IOC:
		cancelReason = "ioc"
	default:
		restQty = remaining
	}
	e.accept(o, restQty)
	for k := range pl.fills {
		e.applyFill(m, o, &pl.fills[k])
	}
	if cancelReason != "" {
		e.em.push(&enginev1.Event{Body: &enginev1.Event_OrderCanceled{OrderCanceled: &enginev1.OrderCanceled{
			OrderId: o.OrderId, UserId: o.UserId, CanceledQty: remaining, Reason: cancelReason,
		}}}, true)
	}
	if restQty > 0 {
		e.rest(m, o, restQty)
	}
}

func takerLeg(o *enginev1.SubmitOrder, price uint32) *commonv1.FillLeg {
	return &commonv1.FillLeg{OrderId: o.OrderId, UserId: o.UserId, Outcome: o.Outcome, Side: o.Side, Price: price, IsTaker: true}
}

func makerLeg(mk *order, price uint32) *commonv1.FillLeg {
	return &commonv1.FillLeg{OrderId: mk.id, UserId: mk.userID, Outcome: mk.outcome, Side: mk.side, Price: price}
}

// applyFill mutates makers, positions and levels for one planned fill and
// emits the Fill event. TRANSFER legs are [buyer, seller]; MINT legs are one
// per outcome in outcome order, all BUY, prices summing to 100.
func (e *Engine) applyFill(m *market, o *enginev1.SubmitOrder, f *plannedFill) {
	fill := &commonv1.Fill{FillId: e.em.fillID(), MarketId: m.id, Kind: f.kind, Qty: f.qty}
	taker := m.posMut(o.UserId, o.Outcome)
	switch f.kind {
	case commonv1.FillKind_FILL_KIND_TRANSFER:
		mk := f.makers[0]
		mp := m.posMut(mk.userID, mk.outcome)
		if o.Side == commonv1.Side_SIDE_BUY {
			fill.Legs = []*commonv1.FillLeg{takerLeg(o, f.price), makerLeg(mk, f.price)}
			taker.qty += f.qty
			mp.qty -= f.qty
			mp.locked -= f.qty
		} else {
			fill.Legs = []*commonv1.FillLeg{makerLeg(mk, f.price), takerLeg(o, f.price)}
			taker.qty -= f.qty
			mp.qty += f.qty
			mp.openBuy -= f.qty
		}
		e.consume(m, mk, f.qty)
	default: // MINT
		fill.Legs = make([]*commonv1.FillLeg, 0, m.n)
		for j := uint32(0); j < m.n; j++ {
			if j == o.Outcome {
				fill.Legs = append(fill.Legs, takerLeg(o, f.price))
				taker.qty += f.qty
				continue
			}
			mk := f.makers[j]
			fill.Legs = append(fill.Legs, makerLeg(mk, mk.price))
			mp := m.posMut(mk.userID, j)
			mp.qty += f.qty
			mp.openBuy -= f.qty
			e.consume(m, mk, f.qty)
		}
	}
	e.em.push(&enginev1.Event{Body: &enginev1.Event_Fill{Fill: fill}}, true)
}

// consume reduces a maker by q on its ladder, touches the level and drops
// the order from the index when done.
func (e *Engine) consume(m *market, mk *order, q uint64) {
	lad := m.ladder(mk.outcome, mk.side)
	lad.fill(mk, q)
	e.em.touch(mk.outcome, mk.side, mk.price, lad.at(mk.price))
	if mk.remaining == 0 {
		delete(m.orders, mk.id)
	}
}
