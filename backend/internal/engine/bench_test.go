package engine

import (
	"strconv"
	"testing"

	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
)

const ring = 4096

// BenchmarkApplySubmit/resting: one GTC submit that rests, followed by its
// cancel, per iteration (ns/op is the pair; target < 2 us for the submit).
// BenchmarkApplySubmit/crossing: a resting ask then a crossing buy that
// fully fills it; roles alternate so positions never drift toward the
// limit (ns/op is the pair; target < 12 us, i.e. < 10 us for the cross).
func BenchmarkApplySubmit(b *testing.B) {
	b.Run("resting", func(b *testing.B) {
		e := New(0)
		seq := uint64(1)
		e.Apply(cmdAt(seq, &enginev1.CreateMarket{MarketId: "mkt_demo", OutcomeLabels: []string{"YES", "NO"}}))
		subs := make([]*enginev1.Command, ring)
		cans := make([]*enginev1.Command, ring)
		for i := range subs {
			id := "o" + strconv.Itoa(i)
			o := sub(id, users[i%len(users)], 0, BUY, uint32(20+i%40), 10)
			o.ClientOrderId = ""
			subs[i] = cmdAt(0, o)
			cans[i] = cmdAt(0, &enginev1.CancelOrder{OrderId: id, MarketId: "mkt_demo"})
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k := i % ring
			seq++
			subs[k].Seq = seq
			evs := e.Apply(subs[k])
			if i == 0 && evs[0].GetOrderAccepted().GetRestingQty() != 10 {
				b.Fatalf("first submit did not rest 10: %v", evs[0])
			}
			seq++
			cans[k].Seq = seq
			e.Apply(cans[k])
		}
	})
	b.Run("crossing", func(b *testing.B) {
		e := New(0)
		seq := uint64(1)
		e.Apply(cmdAt(seq, &enginev1.CreateMarket{MarketId: "mkt_demo", OutcomeLabels: []string{"YES", "NO"}}))
		seq++
		e.Apply(cmdAt(seq, sub("seedN", "u_carol", 1, BUY, 40, PositionLimit)))
		seq++
		e.Apply(cmdAt(seq, sub("seedY", "u_bob", 0, BUY, 60, PositionLimit))) // bob holds 10000 YES
		asks := make([]*enginev1.Command, ring)
		buys := make([]*enginev1.Command, ring)
		for i := range asks {
			seller, buyer := "u_bob", "u_alice"
			if i%2 == 1 {
				seller, buyer = "u_alice", "u_bob"
			}
			a := sub("a"+strconv.Itoa(i), seller, 0, SELL, 55, 10)
			a.ClientOrderId = ""
			bo := sub("b"+strconv.Itoa(i), buyer, 0, BUY, 55, 10)
			bo.ClientOrderId = ""
			asks[i], buys[i] = cmdAt(0, a), cmdAt(0, bo)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k := i % ring
			seq++
			asks[k].Seq = seq
			if evs := e.Apply(asks[k]); evs[0].GetOrderRejected() != nil {
				b.Fatalf("ask rejected: %v", evs[0])
			}
			seq++
			buys[k].Seq = seq
			if evs := e.Apply(buys[k]); evs[0].GetOrderAccepted() == nil || evs[0].GetOrderAccepted().GetRestingQty() != 0 {
				b.Fatalf("buy did not fully cross: %v", evs[0])
			}
		}
	})
}
