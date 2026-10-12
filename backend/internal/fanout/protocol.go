// Package fanout is the WebSocket edge: it keeps a per-market book view built
// from a matcher GetBook snapshot plus engine.events, and streams snapshots
// and deltas to subscribed browser clients.
package fanout

import (
	"strings"
	"time"

	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
)

// ClientMsg is a client -> server frame: subscribe, unsubscribe or ping.
type ClientMsg struct {
	Type    string   `json:"type"`
	Markets []string `json:"markets"`
}

// LevelJSON is one price level with its absolute resting quantity.
type LevelJSON struct {
	Outcome uint32 `json:"outcome"`
	Side    string `json:"side"`
	Price   uint32 `json:"price"`
	Qty     uint64 `json:"qty"`
}

// TradeJSON is one fill as seen by the tape. For MINT fills Price and
// Outcome are the taker leg's.
type TradeJSON struct {
	FillID    string `json:"fill_id"`
	Kind      string `json:"kind"`
	Price     uint32 `json:"price"`
	Qty       uint64 `json:"qty"`
	Outcome   uint32 `json:"outcome"`
	TakerSide string `json:"taker_side"`
	TS        string `json:"ts"`
}

// SnapshotMsg is the full book, sent on subscribe and broadcast in place of
// the delta for the tail of a command split by the reader's idle flush (the
// tail carries the head's market_seq). Clients apply snapshot levels
// unconditionally, replacing their book and market_seq, and append Trades to
// the tape, deduping by fill_id. Trades holds the tail's fills and is empty
// (omitted) on subscribe-time snapshots.
type SnapshotMsg struct {
	Type      string      `json:"type"`
	MarketID  string      `json:"market_id"`
	MarketSeq uint64      `json:"market_seq"`
	Status    string      `json:"status"`
	Levels    []LevelJSON `json:"levels"`
	Trades    []TradeJSON `json:"trades,omitempty"`
}

// DeltaMsg carries the levels touched and trades made by one command. Each
// delta has a market_seq newer than the previous message for the market;
// clients drop deltas whose market_seq is <= their current one and apply
// snapshots unconditionally.
type DeltaMsg struct {
	Type      string      `json:"type"`
	MarketID  string      `json:"market_id"`
	MarketSeq uint64      `json:"market_seq"`
	Levels    []LevelJSON `json:"levels"`
	Trades    []TradeJSON `json:"trades"`
}

// MarketMsg announces a market status change. WinningOutcome is optional:
// it is present only when Status is "resolved".
type MarketMsg struct {
	Type           string  `json:"type"`
	MarketID       string  `json:"market_id"`
	Status         string  `json:"status"`
	WinningOutcome *uint32 `json:"winning_outcome,omitempty"`
}

// SimpleMsg is a pong or an error frame.
type SimpleMsg struct {
	Type    string `json:"type"`
	Message string `json:"message,omitempty"`
}

func sideName(s commonv1.Side) string { return strings.TrimPrefix(s.String(), "SIDE_") }

func statusName(s commonv1.MarketStatus) string {
	return strings.ToLower(strings.TrimPrefix(s.String(), "MARKET_STATUS_"))
}

func levelJSON(l *commonv1.Level) LevelJSON {
	return LevelJSON{Outcome: l.GetOutcome(), Side: sideName(l.GetSide()), Price: l.GetPrice(), Qty: l.GetQty()}
}

// tradeJSON reports the taker leg; for MINT that is the taker's outcome and price.
func tradeJSON(f *commonv1.Fill, engineNanos int64) (TradeJSON, bool) {
	for _, l := range f.GetLegs() {
		if l.GetIsTaker() {
			return TradeJSON{
				FillID: f.GetFillId(), Kind: strings.TrimPrefix(f.GetKind().String(), "FILL_KIND_"), Price: l.GetPrice(),
				Qty: f.GetQty(), Outcome: l.GetOutcome(), TakerSide: sideName(l.GetSide()),
				TS: time.Unix(0, engineNanos).UTC().Format(time.RFC3339Nano),
			}, true
		}
	}
	return TradeJSON{}, false
}
