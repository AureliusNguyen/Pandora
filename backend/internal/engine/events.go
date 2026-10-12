package engine

import (
	"strconv"

	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
)

type touchRef struct {
	outcome uint32
	side    commonv1.Side
	price   uint32
	lvl     *level
}

// emitter collects the events of the command being applied. It stamps the
// shared header, numbers idx in emission order, records touched levels for
// the trailing LevelUpdate events, and decides whether market_seq advances.
type emitter struct {
	partition uint32
	seq       uint64
	epoch     uint64
	t         int64
	marketID  string
	market    *market
	changed   bool // any event other than OrderRejected was emitted
	events    []*enginev1.Event
	touched   []touchRef
	fillBuf   []byte
}

func (em *emitter) begin(partition uint32, cmd *enginev1.Command) {
	em.partition = partition
	em.seq = cmd.Seq
	em.epoch = cmd.Epoch
	em.t = cmd.EngineTimeUnixNanos
	em.marketID = ""
	em.market = nil
	em.changed = false
	em.events = make([]*enginev1.Event, 0, 4) // the caller owns the returned slice
	em.touched = em.touched[:0]
}

// setMarket records the market the command addresses; m may be nil
// (unknown market) in which case events carry market_seq 0.
func (em *emitter) setMarket(id string, m *market) {
	em.marketID = id
	em.market = m
}

// push appends ev, stamping the header. changes=false is used only for
// OrderRejected, which must not advance market_seq.
func (em *emitter) push(ev *enginev1.Event, changes bool) {
	ev.Seq = em.seq
	ev.Idx = uint32(len(em.events))
	ev.Partition = em.partition
	ev.Epoch = em.epoch
	ev.EngineTimeUnixNanos = em.t
	ev.MarketId = em.marketID
	em.events = append(em.events, ev)
	if changes {
		em.changed = true
	}
}

// touch marks a level as changed by this command (deduped per command).
func (em *emitter) touch(outcome uint32, side commonv1.Side, price uint32, lvl *level) {
	if lvl.touchedSeq == em.seq {
		return
	}
	lvl.touchedSeq = em.seq
	em.touched = append(em.touched, touchRef{outcome: outcome, side: side, price: price, lvl: lvl})
}

// fillID is deterministic: "f<partition>-<seq>-<idx>" where idx is the idx
// the Fill event will receive (the (partition, seq, idx) dedup key).
func (em *emitter) fillID() string {
	em.fillBuf = append(em.fillBuf[:0], 'f')
	em.fillBuf = strconv.AppendUint(em.fillBuf, uint64(em.partition), 10)
	em.fillBuf = append(em.fillBuf, '-')
	em.fillBuf = strconv.AppendUint(em.fillBuf, em.seq, 10)
	em.fillBuf = append(em.fillBuf, '-')
	em.fillBuf = strconv.AppendUint(em.fillBuf, uint64(len(em.events)), 10)
	return string(em.fillBuf)
}

// finish appends LevelUpdate events for touched levels (absolute qty),
// advances market_seq if the command changed the market, stamps market_seq
// on every event and returns them.
func (em *emitter) finish() []*enginev1.Event {
	for _, tr := range em.touched {
		em.push(&enginev1.Event{Body: &enginev1.Event_LevelUpdate{LevelUpdate: &enginev1.LevelUpdate{
			Level: &commonv1.Level{Outcome: tr.outcome, Side: tr.side, Price: tr.price, Qty: tr.lvl.qty},
		}}}, true)
	}
	var ms uint64
	if em.market != nil {
		if em.changed {
			em.market.seq++
		}
		ms = em.market.seq
	}
	for _, ev := range em.events {
		ev.MarketSeq = ms
	}
	return em.events
}
