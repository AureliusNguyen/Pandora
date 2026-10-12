package fanout

import (
	"cmp"
	"slices"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
	matcherv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/matcher/v1"
)

var splitTails = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "pandora", Subsystem: "fanout", Name: "split_command_tails_total",
	Help: "Commands whose events reached a book view in more than one batch (idle flush split); each tail is sent as an extra delta with the same market_seq.",
})

type levelKey struct {
	outcome uint32
	side    commonv1.Side
	price   uint32
}

// cmdKey identifies an engine command: seq is contiguous per partition.
type cmdKey struct {
	partition uint32
	seq       uint64
}

// BookView is one market's book as seen by this pod. Not concurrency-safe; the Hub guards it.
type BookView struct {
	marketID string
	seq      uint64
	status   commonv1.MarketStatus
	levels   map[levelKey]uint64
	// last is the command that produced seq; valid only when hasLast. A
	// GetBook snapshot does not name it, so equal-seq events after a
	// snapshot are already included and dropped.
	last    cmdKey
	hasLast bool
}

// NewEmptyBookView returns an open market with no levels at market_seq 0.
func NewEmptyBookView(marketID string) *BookView {
	return &BookView{marketID: marketID, status: commonv1.MarketStatus_MARKET_STATUS_OPEN, levels: map[levelKey]uint64{}}
}

// NewBookView builds a view from a matcher GetBook snapshot.
func NewBookView(resp *matcherv1.GetBookResponse) *BookView {
	b := NewEmptyBookView(resp.GetMarketId())
	b.seq, b.status = resp.GetMarketSeq(), resp.GetStatus()
	for _, l := range resp.GetLevels() {
		if l.GetQty() > 0 {
			b.levels[levelKey{l.GetOutcome(), l.GetSide(), l.GetPrice()}] = l.GetQty()
		}
	}
	return b
}

// Seq is the market_seq of the last applied command.
func (b *BookView) Seq() uint64 { return b.seq }

// Snapshot renders the whole book sorted by (outcome, side, price).
func (b *BookView) Snapshot() SnapshotMsg {
	out := SnapshotMsg{Type: "snapshot", MarketID: b.marketID, MarketSeq: b.seq, Status: statusName(b.status), Levels: make([]LevelJSON, 0, len(b.levels))}
	keys := make([]levelKey, 0, len(b.levels))
	for k := range b.levels {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(x, y levelKey) int {
		return cmp.Or(cmp.Compare(x.outcome, y.outcome), cmp.Compare(x.side, y.side), cmp.Compare(x.price, y.price))
	})
	for _, k := range keys {
		out.Levels = append(out.Levels, LevelJSON{Outcome: k.outcome, Side: sideName(k.side), Price: k.price, Qty: b.levels[k]})
	}
	return out
}

// accept decides per event, in order: a newer market_seq starts a new command;
// an equal market_seq is applied only when it continues the last applied
// command (the tail of a command split by the reader's idle flush); anything
// else is a replay already reflected in the view and is dropped.
func (b *BookView) accept(ev *enginev1.Event) (ok, tail bool) {
	k := cmdKey{ev.GetPartition(), ev.GetSeq()}
	switch ms := ev.GetMarketSeq(); {
	case ms > b.seq:
		b.seq, b.last, b.hasLast = ms, k, true
		return true, false
	case ms == b.seq && b.hasLast && k == b.last:
		return true, true
	default:
		return false, false
	}
}

// Apply applies a batch of events for this market, normally one whole
// command. Every event not already reflected in the view is applied in order,
// so a command split across batches never loses its tail. ok is false when
// nothing in the batch was newer than the view (stale, dropped untouched).
func (b *BookView) Apply(events []*enginev1.Event) (*DeltaMsg, *MarketMsg, bool) {
	var d *DeltaMsg
	var mm *MarketMsg
	for i, ev := range events {
		ok, tail := b.accept(ev)
		if !ok {
			continue
		}
		if d == nil {
			if tail && i == 0 {
				splitTails.Inc()
			}
			d = &DeltaMsg{Type: "delta", MarketID: b.marketID, Levels: []LevelJSON{}, Trades: []TradeJSON{}}
		}
		d.MarketSeq = b.seq
		switch body := ev.GetBody().(type) {
		case *enginev1.Event_LevelUpdate:
			l := body.LevelUpdate.GetLevel()
			k := levelKey{l.GetOutcome(), l.GetSide(), l.GetPrice()}
			if l.GetQty() == 0 {
				delete(b.levels, k)
			} else {
				b.levels[k] = l.GetQty()
			}
			d.Levels = append(d.Levels, levelJSON(l))
		case *enginev1.Event_Fill:
			if t, ok := tradeJSON(body.Fill, ev.GetEngineTimeUnixNanos()); ok {
				d.Trades = append(d.Trades, t)
			}
		case *enginev1.Event_MarketLocked:
			b.status = commonv1.MarketStatus_MARKET_STATUS_LOCKED
			mm = &MarketMsg{Type: "market", MarketID: b.marketID, Status: "locked"}
		case *enginev1.Event_MarketResolved:
			b.status = commonv1.MarketStatus_MARKET_STATUS_RESOLVED
			w := body.MarketResolved.GetWinningOutcome()
			mm = &MarketMsg{Type: "market", MarketID: b.marketID, Status: "resolved", WinningOutcome: &w}
		case *enginev1.Event_MarketVoided:
			b.status = commonv1.MarketStatus_MARKET_STATUS_VOIDED
			mm = &MarketMsg{Type: "market", MarketID: b.marketID, Status: "voided"}
		}
	}
	return d, mm, d != nil
}
