package engine

import (
	"fmt"
	"math/rand"
	"testing"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
)

var users = []string{"u_alice", "u_bob", "u_carol", "u_dave"}

// genStream builds n random commands against mkt_demo (plus a second
// market so cross-market cancels get exercised). It never looks at engine
// state: everything the engine rejects is part of the test.
func genStream(rng *rand.Rand, n int) []*enginev1.Command {
	cmds := []*enginev1.Command{
		cmdAt(1, &enginev1.CreateMarket{MarketId: "mkt_demo", Title: "d", OutcomeLabels: []string{"YES", "NO"}}),
		cmdAt(2, &enginev1.CreateMarket{MarketId: "mkt_two", Title: "t", OutcomeLabels: []string{"A", "B"}}),
	}
	var issued []string
	for len(cmds) < n {
		seq := uint64(len(cmds) + 1)
		r := rng.Intn(100)
		switch {
		case r < 80:
			id := fmt.Sprintf("o%d", seq)
			o := &enginev1.SubmitOrder{
				OrderId: id, MarketId: "mkt_demo", UserId: users[rng.Intn(len(users))], ClientOrderId: "c" + id,
				Outcome: uint32(rng.Intn(2)), Side: BUY, Price: uint32(30 + rng.Intn(40)), Qty: uint64(1 + rng.Intn(50)),
				Tif: commonv1.TimeInForce_TIME_IN_FORCE_GTC,
			}
			if rng.Intn(3) == 0 {
				o.Side = SELL
			}
			if rng.Intn(20) == 0 {
				o.MarketId = "mkt_two"
			}
			switch t := rng.Intn(10); {
			case t < 2:
				o.Tif = IOC
			case t == 2:
				o.Tif = FOK
			}
			if rng.Intn(20) == 0 {
				o.PostOnly = true
			}
			if rng.Intn(50) == 0 && len(issued) > 0 {
				o.ClientOrderId = "c" + issued[rng.Intn(len(issued))] // duplicate cid
			}
			if rng.Intn(100) == 0 {
				o.Price = 0 // INVALID
			}
			if rng.Intn(200) == 0 {
				o.Qty = PositionLimit + 1
			}
			issued = append(issued, id)
			cmds = append(cmds, cmdAt(seq, o))
		case r < 97 && len(issued) > 0:
			id := issued[rng.Intn(len(issued))]
			c := &enginev1.CancelOrder{OrderId: id, MarketId: "mkt_demo", UserId: users[rng.Intn(len(users))]}
			if rng.Intn(4) == 0 {
				c.UserId = ""
			}
			cmds = append(cmds, cmdAt(seq, c))
		case r < 98:
			cmds = append(cmds, cmdAt(seq, &enginev1.LockMarket{MarketId: "mkt_two"}))
		default:
			cmds = append(cmds, cmdAt(seq, &enginev1.CreateMarket{MarketId: "mkt_two", OutcomeLabels: []string{"A", "B"}}))
		}
	}
	// finish with a resolve so the terminal path runs on every stream
	cmds = append(cmds, cmdAt(uint64(len(cmds)+1), &enginev1.ResolveMarket{MarketId: "mkt_demo", WinningOutcome: 1}))
	return cmds
}

func TestPropertyDeterminismAndInvariants(t *testing.T) {
	for seed := int64(1); seed <= 25; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			cmds := genStream(rand.New(rand.NewSource(seed)), 1500)
			a, b := New(0), New(0)
			var fills, accepted int
			for _, c := range cmds {
				ea := a.Apply(proto.Clone(c).(*enginev1.Command))
				eb := b.Apply(proto.Clone(c).(*enginev1.Command))
				if len(ea) != len(eb) {
					t.Fatalf("seq %d: %d vs %d events", c.Seq, len(ea), len(eb))
				}
				for i := range ea {
					if !proto.Equal(ea[i], eb[i]) {
						t.Fatalf("seq %d event %d differs:\n%v\n%v", c.Seq, i, ea[i], eb[i])
					}
					if ea[i].Idx != uint32(i) || ea[i].Seq != c.Seq {
						t.Fatalf("seq %d: idx/seq stamping wrong: %v", c.Seq, ea[i])
					}
					switch ea[i].Body.(type) {
					case *enginev1.Event_Fill:
						fills++
					case *enginev1.Event_OrderAccepted:
						accepted++
					}
				}
				checkInvariants(t, a)
			}
			for _, id := range []string{"mkt_demo", "mkt_two"} {
				la, sa, sta, oka := a.Book(id)
				lb, sb, stb, okb := b.Book(id)
				if oka != okb || sa != sb || sta != stb || len(la) != len(lb) {
					t.Fatalf("%s: books differ", id)
				}
				for i := range la {
					if !proto.Equal(la[i], lb[i]) {
						t.Fatalf("%s: level %d differs", id, i)
					}
				}
			}
			if fills == 0 || accepted == 0 {
				t.Fatalf("stream too tame: fills=%d accepted=%d", fills, accepted)
			}
		})
	}
}
