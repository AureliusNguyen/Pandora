package engine

import (
	"fmt"
	"strings"
	"testing"

	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
)

// tengine wraps an Engine with an auto-incrementing seq for tests.
type tengine struct {
	t   *testing.T
	e   *Engine
	seq uint64
}

func newT(t *testing.T) *tengine {
	t.Helper()
	te := &tengine{t: t, e: New(0)}
	te.create("mkt_demo", "YES", "NO")
	return te
}

func (te *tengine) apply(body any) []*enginev1.Event {
	te.t.Helper()
	te.seq++
	return te.e.Apply(cmdAt(te.seq, body))
}

// cmdAt builds a Command at seq from a body message.
func cmdAt(seq uint64, body any) *enginev1.Command {
	c := &enginev1.Command{Seq: seq, Partition: 0, Epoch: 1, EngineTimeUnixNanos: int64(seq) * 1000}
	switch b := body.(type) {
	case *enginev1.SubmitOrder:
		c.Body = &enginev1.Command_SubmitOrder{SubmitOrder: b}
	case *enginev1.CancelOrder:
		c.Body = &enginev1.Command_CancelOrder{CancelOrder: b}
	case *enginev1.CreateMarket:
		c.Body = &enginev1.Command_CreateMarket{CreateMarket: b}
	case *enginev1.LockMarket:
		c.Body = &enginev1.Command_LockMarket{LockMarket: b}
	case *enginev1.ResolveMarket:
		c.Body = &enginev1.Command_ResolveMarket{ResolveMarket: b}
	case *enginev1.VoidMarket:
		c.Body = &enginev1.Command_VoidMarket{VoidMarket: b}
	default:
		panic(fmt.Sprintf("cmdAt: unsupported body %T", body))
	}
	return c
}

func (te *tengine) create(id string, labels ...string) []*enginev1.Event {
	return te.apply(&enginev1.CreateMarket{MarketId: id, Title: "t", OutcomeLabels: labels})
}

// sub builds a GTC limit order on mkt_demo. Variants set tif/post_only.
func sub(id, user string, outcome uint32, side commonv1.Side, price uint32, qty uint64) *enginev1.SubmitOrder {
	return &enginev1.SubmitOrder{
		OrderId: id, MarketId: "mkt_demo", UserId: user, ClientOrderId: "c_" + id,
		Outcome: outcome, Side: side, Price: price, Qty: qty, Tif: commonv1.TimeInForce_TIME_IN_FORCE_GTC,
	}
}

func withTif(o *enginev1.SubmitOrder, tif commonv1.TimeInForce) *enginev1.SubmitOrder {
	o.Tif = tif
	return o
}

func postOnly(o *enginev1.SubmitOrder) *enginev1.SubmitOrder {
	o.PostOnly = true
	return o
}

const (
	BUY  = commonv1.Side_SIDE_BUY
	SELL = commonv1.Side_SIDE_SELL
	IOC  = commonv1.TimeInForce_TIME_IN_FORCE_IOC
	FOK  = commonv1.TimeInForce_TIME_IN_FORCE_FOK
)

// kinds renders events compactly, e.g. "accepted(10) fill(T55x10) canceled(ioc,5) level(0 BUY 55 0)".
func kinds(evs []*enginev1.Event) string {
	parts := make([]string, 0, len(evs))
	for _, ev := range evs {
		switch b := ev.Body.(type) {
		case *enginev1.Event_OrderAccepted:
			parts = append(parts, fmt.Sprintf("accepted(%d)", b.OrderAccepted.RestingQty))
		case *enginev1.Event_OrderRejected:
			parts = append(parts, "rejected("+strings.TrimPrefix(b.OrderRejected.Reason.String(), "REJECT_REASON_")+")")
		case *enginev1.Event_OrderCanceled:
			parts = append(parts, fmt.Sprintf("canceled(%s,%d)", b.OrderCanceled.Reason, b.OrderCanceled.CanceledQty))
		case *enginev1.Event_Fill:
			f := b.Fill
			k := "T"
			if f.Kind == commonv1.FillKind_FILL_KIND_MINT {
				k = "M"
			}
			legs := make([]string, 0, len(f.Legs))
			for _, l := range f.Legs {
				tk := ""
				if l.IsTaker {
					tk = "*"
				}
				legs = append(legs, fmt.Sprintf("%s%s@%d", l.OrderId, tk, l.Price))
			}
			parts = append(parts, fmt.Sprintf("fill(%s%d:%s)", k, f.Qty, strings.Join(legs, ",")))
		case *enginev1.Event_LevelUpdate:
			l := b.LevelUpdate.Level
			parts = append(parts, fmt.Sprintf("level(%d %s %d %d)", l.Outcome, strings.TrimPrefix(l.Side.String(), "SIDE_"), l.Price, l.Qty))
		case *enginev1.Event_MarketCreated:
			parts = append(parts, "created")
		case *enginev1.Event_MarketLocked:
			parts = append(parts, "locked")
		case *enginev1.Event_MarketResolved:
			parts = append(parts, fmt.Sprintf("resolved(%d)", b.MarketResolved.WinningOutcome))
		case *enginev1.Event_MarketVoided:
			parts = append(parts, "voided("+b.MarketVoided.Reason+")")
		}
	}
	return strings.Join(parts, " ")
}

// bookStrings renders Book() as "outcome SIDE price qty" lines in Book order.
func bookStrings(e *Engine, market string) []string {
	levels, _, _, _ := e.Book(market)
	out := make([]string, 0, len(levels))
	for _, l := range levels {
		out = append(out, fmt.Sprintf("%d %s %d %d", l.Outcome, strings.TrimPrefix(l.Side.String(), "SIDE_"), l.Price, l.Qty))
	}
	return out
}

func wantKinds(t *testing.T, evs []*enginev1.Event, want string) {
	t.Helper()
	if got := kinds(evs); got != want {
		t.Fatalf("events:\n got %s\nwant %s", got, want)
	}
}

func wantBook(t *testing.T, e *Engine, market string, want ...string) {
	t.Helper()
	got := bookStrings(e, market)
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("book:\n got %v\nwant %v", got, want)
	}
}
