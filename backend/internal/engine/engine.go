package engine

import (
	"fmt"

	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
)

type cidKey struct {
	user string
	cid  string
}

// Engine is deterministic: same Command sequence -> same Events and state.
// No clocks, no I/O, no goroutines. Not safe for concurrent use.
type Engine struct {
	partition uint32
	lastSeq   uint64
	markets   map[string]*market
	cids      map[cidKey]struct{} // accepted client_order_ids per user (this partition)
	em        emitter
	plan      planner
}

// New returns an empty engine for partition.
func New(partition uint32) *Engine {
	return &Engine{
		partition: partition,
		markets:   make(map[string]*market),
		cids:      make(map[cidKey]struct{}),
	}
}

// LastSeq returns the seq of the last applied command (0 initially).
func (e *Engine) LastSeq() uint64 { return e.lastSeq }

// Apply validates and applies cmd, returning events in emission order.
// cmd.Seq must be LastSeq()+1 or Apply panics (replay contiguity is checked
// by the caller). Commands that do nothing (unknown order to cancel,
// duplicate CreateMarket, lock of a locked market) return no events but
// still advance LastSeq.
func (e *Engine) Apply(cmd *enginev1.Command) []*enginev1.Event {
	if cmd.Seq != e.lastSeq+1 {
		panic(fmt.Sprintf("engine: non-contiguous seq %d, last applied %d", cmd.Seq, e.lastSeq))
	}
	e.lastSeq = cmd.Seq
	e.em.begin(e.partition, cmd)
	switch b := cmd.Body.(type) {
	case *enginev1.Command_CreateMarket:
		e.createMarket(b.CreateMarket)
	case *enginev1.Command_SubmitOrder:
		e.submit(b.SubmitOrder)
	case *enginev1.Command_CancelOrder:
		e.cancel(b.CancelOrder)
	case *enginev1.Command_LockMarket:
		e.lock(b.LockMarket.GetMarketId())
	case *enginev1.Command_ResolveMarket:
		e.resolve(b.ResolveMarket)
	case *enginev1.Command_VoidMarket:
		e.void(b.VoidMarket)
	default:
		// Unknown or unset body: no-op, LastSeq still advances.
	}
	return e.em.finish()
}

func (e *Engine) createMarket(c *enginev1.CreateMarket) {
	id := c.GetMarketId()
	n := len(c.GetOutcomeLabels())
	if id == "" || n < 2 || n > MaxOutcomes {
		return
	}
	if _, exists := e.markets[id]; exists {
		return // idempotent: replay of DEMO_MARKET creation is a no-op
	}
	m := newMarket(id, uint32(n))
	e.markets[id] = m
	e.em.setMarket(id, m)
	e.em.push(&enginev1.Event{Body: &enginev1.Event_MarketCreated{MarketCreated: &enginev1.MarketCreated{Market: c}}}, true)
}

// Book returns a snapshot of all non-empty levels and the market status.
// Order: outcome ascending; BUY levels best (highest) first, then SELL
// levels best (lowest) first. This order is canonical and is what the
// replay tool diffs.
func (e *Engine) Book(marketID string) (levels []*commonv1.Level, marketSeq uint64, status commonv1.MarketStatus, ok bool) {
	m, found := e.markets[marketID]
	if !found {
		return nil, 0, commonv1.MarketStatus_MARKET_STATUS_UNSPECIFIED, false
	}
	levels = make([]*commonv1.Level, 0, 16)
	for i := uint32(0); i < m.n; i++ {
		b := &m.books[i]
		for p := b.bids.best; p != 0; p = b.bids.next(p) {
			levels = append(levels, &commonv1.Level{Outcome: i, Side: commonv1.Side_SIDE_BUY, Price: p, Qty: b.bids.at(p).qty})
		}
		for p := b.asks.best; p != 0; p = b.asks.next(p) {
			levels = append(levels, &commonv1.Level{Outcome: i, Side: commonv1.Side_SIDE_SELL, Price: p, Qty: b.asks.at(p).qty})
		}
	}
	return levels, m.seq, m.status, true
}

// Position returns shares held by user on outcome (0 if none).
func (e *Engine) Position(marketID, userID string, outcome uint32) uint64 {
	m, found := e.markets[marketID]
	if !found {
		return 0
	}
	if p := m.posGet(userID, outcome); p != nil {
		return p.qty
	}
	return 0
}

// Outcomes returns the outcome count of marketID (ok false when unknown),
// so the RPC layer can reject an out-of-range outcome before journaling.
func (e *Engine) Outcomes(marketID string) (uint32, bool) {
	m, found := e.markets[marketID]
	if !found {
		return 0, false
	}
	return m.n, true
}
