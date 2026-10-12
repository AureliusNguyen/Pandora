package engine

import (
	"testing"

	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
)

// mint gives bob YES shares and carol NO shares: carol rests NO bid at 40,
// bob buys YES at 60 -> MINT at 60/40.
func seedMint(t *testing.T, te *tengine, qty uint64) {
	t.Helper()
	te.apply(sub("seedN", "u_carol", 1, BUY, 40, qty))
	wantKinds(t, te.apply(sub("seedY", "u_bob", 0, BUY, 60, qty)),
		"accepted(0) fill(M"+itoa(qty)+":seedY*@60,seedN@40) level(1 BUY 40 0)")
}

func TestMatchTable(t *testing.T) {
	type step struct {
		o    *enginev1.SubmitOrder
		want string   // kinds()
		book []string // nil = do not check
	}
	cases := []struct {
		name  string
		seed  bool
		steps []step
	}{
		{"transfer at resting price with remainder resting", true, []step{
			{sub("s1", "u_bob", 0, SELL, 55, 4), "accepted(4) level(0 SELL 55 4)", nil},
			{sub("b1", "u_alice", 0, BUY, 60, 10), "accepted(6) fill(T4:b1*@55,s1@55) level(0 SELL 55 0) level(0 BUY 60 6)", []string{"0 BUY 60 6"}},
		}},
		{"FIFO within level then next level", true, []step{
			{sub("s1", "u_bob", 0, SELL, 55, 3), "accepted(3) level(0 SELL 55 3)", nil},
			{sub("s2", "u_bob", 0, SELL, 55, 3), "accepted(3) level(0 SELL 55 6)", nil},
			{sub("s3", "u_bob", 0, SELL, 56, 3), "accepted(3) level(0 SELL 56 3)", nil},
			{sub("b1", "u_alice", 0, BUY, 56, 8), "accepted(0) fill(T3:b1*@55,s1@55) fill(T3:b1*@55,s2@55) fill(T2:b1*@56,s3@56) level(0 SELL 55 0) level(0 SELL 56 1)", []string{"0 SELL 56 1"}},
		}},
		{"taker sell transfers against best bid", true, []step{
			{sub("b1", "u_alice", 0, BUY, 58, 5), "accepted(5) level(0 BUY 58 5)", nil},
			{sub("b2", "u_alice", 0, BUY, 57, 5), "accepted(5) level(0 BUY 57 5)", nil},
			{sub("s1", "u_bob", 0, SELL, 57, 7), "accepted(0) fill(T5:b1@58,s1*@58) fill(T2:b2@57,s1*@57) level(0 BUY 58 0) level(0 BUY 57 3)", []string{"0 BUY 57 3"}},
		}},
		{"mint when p plus best other bid reaches 100", false, []step{
			{sub("n1", "u_carol", 1, BUY, 45, 10), "accepted(10) level(1 BUY 45 10)", nil},
			{sub("y1", "u_alice", 0, BUY, 54, 10), "accepted(10) level(0 BUY 54 10)", nil}, // 54+45 < 100
			{sub("y2", "u_bob", 0, BUY, 60, 4), "accepted(0) fill(M4:y2*@55,n1@45) level(1 BUY 45 6)", []string{"0 BUY 54 10", "1 BUY 45 6"}},
		}},
		{"tie goes to transfer", true, []step{
			{sub("n1", "u_carol", 1, BUY, 45, 10), "accepted(10) level(1 BUY 45 10)", nil},
			{sub("s1", "u_bob", 0, SELL, 55, 2), "accepted(2) level(0 SELL 55 2)", nil},
			{sub("y1", "u_alice", 0, BUY, 60, 5), "accepted(0) fill(T2:y1*@55,s1@55) fill(M3:y1*@55,n1@45) level(0 SELL 55 0) level(1 BUY 45 7)", nil},
		}},
		{"mint beats higher ask", true, []step{
			{sub("n1", "u_carol", 1, BUY, 45, 10), "accepted(10) level(1 BUY 45 10)", nil},
			{sub("s1", "u_bob", 0, SELL, 56, 2), "accepted(2) level(0 SELL 56 2)", nil},
			{sub("y1", "u_alice", 0, BUY, 60, 5), "accepted(0) fill(M5:y1*@55,n1@45) level(1 BUY 45 5)", []string{"0 SELL 56 2", "1 BUY 45 5"}},
		}},
		// Rewritten per the 1B ruling: a foreign fill (alice's ask at 53)
		// executes before bob's BUY reaches his own ask at 55; that fill
		// stands, the remainder is canceled with reason "stp" and bob's
		// resting ask is untouched.
		{"stp cancels newest after earlier fills stand", true, []step{
			{sub("s2", "u_bob", 0, SELL, 54, 2), "accepted(2) level(0 SELL 54 2)", nil},
			{sub("s3", "u_bob", 0, SELL, 55, 2), "accepted(2) level(0 SELL 55 2)", nil},
			{sub("b0", "u_alice", 0, BUY, 55, 2), "accepted(0) fill(T2:b0*@54,s2@54) level(0 SELL 54 0)", nil},
			{sub("as", "u_alice", 0, SELL, 53, 2), "accepted(2) level(0 SELL 53 2)", nil},
			{sub("b1", "u_bob", 0, BUY, 60, 10), "accepted(0) fill(T2:b1*@53,as@53) canceled(stp,8) level(0 SELL 53 0)", []string{"0 SELL 55 2"}},
			{sub("b2", "u_alice", 0, BUY, 55, 1), "accepted(0) fill(T1:b2*@55,s3@55) level(0 SELL 55 1)", nil},
		}},
		{"stp in mint leg", false, []step{
			{sub("n1", "u_alice", 1, BUY, 45, 10), "accepted(10) level(1 BUY 45 10)", nil},
			{sub("y1", "u_alice", 0, BUY, 60, 5), "accepted(0) canceled(stp,5) ", []string{"1 BUY 45 10"}},
		}},
		{"ioc partial cancels remainder", true, []step{
			{sub("s1", "u_bob", 0, SELL, 55, 3), "accepted(3) level(0 SELL 55 3)", nil},
			{withTif(sub("b1", "u_alice", 0, BUY, 60, 10), IOC), "accepted(0) fill(T3:b1*@55,s1@55) canceled(ioc,7) level(0 SELL 55 0)", []string{}},
		}},
		{"ioc no match cancels all", false, []step{
			{withTif(sub("b1", "u_alice", 0, BUY, 60, 10), IOC), "accepted(0) canceled(ioc,10) ", []string{}},
		}},
		{"fok unfillable rejects and leaves book", true, []step{
			{sub("s1", "u_bob", 0, SELL, 55, 3), "accepted(3) level(0 SELL 55 3)", nil},
			{withTif(sub("b1", "u_alice", 0, BUY, 60, 4), FOK), "rejected(FOK_UNFILLABLE)", []string{"0 SELL 55 3"}},
			{withTif(sub("b2", "u_alice", 0, BUY, 60, 3), FOK), "accepted(0) fill(T3:b2*@55,s1@55) level(0 SELL 55 0)", []string{}},
		}},
		{"fok blocked by own order is unfillable", true, []step{
			{sub("s1", "u_bob", 0, SELL, 55, 3), "accepted(3) level(0 SELL 55 3)", nil},
			{withTif(sub("b1", "u_bob", 0, BUY, 60, 3), FOK), "rejected(FOK_UNFILLABLE)", []string{"0 SELL 55 3"}},
		}},
		{"post only rejects when it would cross, rests otherwise", true, []step{
			{sub("s1", "u_bob", 0, SELL, 55, 3), "accepted(3) level(0 SELL 55 3)", nil},
			{postOnly(sub("b1", "u_alice", 0, BUY, 55, 1)), "rejected(POST_ONLY_WOULD_CROSS)", nil},
			{postOnly(sub("b2", "u_alice", 0, BUY, 54, 1)), "accepted(1) level(0 BUY 54 1)", nil},
			{postOnly(sub("b3", "u_bob", 0, BUY, 55, 1)), "rejected(POST_ONLY_WOULD_CROSS)", nil}, // own order still counts as crossing
		}},
		{"post only mint would cross", false, []step{
			{sub("n1", "u_carol", 1, BUY, 45, 10), "accepted(10) level(1 BUY 45 10)", nil},
			{postOnly(sub("y1", "u_alice", 0, BUY, 55, 1)), "rejected(POST_ONLY_WOULD_CROSS)", nil},
		}},
		{"sell locks shares while resting", true, []step{
			{sub("s1", "u_bob", 0, SELL, 70, 6), "accepted(6) level(0 SELL 70 6)", nil},
			{sub("s2", "u_bob", 0, SELL, 71, 5), "rejected(INSUFFICIENT_SHARES)", nil},
			{sub("s3", "u_bob", 0, SELL, 71, 4), "accepted(4) level(0 SELL 71 4)", nil},
		}},
		{"taker sell stp cancels newest and leaves own bid", true, []step{
			{sub("b1", "u_bob", 0, BUY, 50, 3), "accepted(3) level(0 BUY 50 3)", nil},
			{sub("s1", "u_bob", 0, SELL, 40, 2), "accepted(0) canceled(stp,2)", []string{"0 BUY 50 3"}},
			{withTif(sub("s2", "u_bob", 0, SELL, 40, 2), FOK), "rejected(FOK_UNFILLABLE)", []string{"0 BUY 50 3"}},
			{postOnly(sub("s3", "u_bob", 0, SELL, 40, 2)), "rejected(POST_ONLY_WOULD_CROSS)", []string{"0 BUY 50 3"}},
		}},
		{"mint walks two levels in one command", false, []step{
			{sub("n1", "u_carol", 1, BUY, 45, 5), "accepted(5) level(1 BUY 45 5)", nil},
			{sub("n2", "u_dave", 1, BUY, 44, 5), "accepted(5) level(1 BUY 44 5)", nil},
			{sub("y1", "u_alice", 0, BUY, 60, 8), "accepted(0) fill(M5:y1*@55,n1@45) fill(M3:y1*@56,n2@44) level(1 BUY 45 0) level(1 BUY 44 2)", []string{"1 BUY 44 2"}},
		}},
		{"three outcome mint legs sum to 100", false, []step{
			{sub("t0", "u_carol", 0, BUY, 30, 5), "accepted(5) level(0 BUY 30 5)", nil},
			{sub("t2", "u_dave", 2, BUY, 25, 5), "accepted(5) level(2 BUY 25 5)", nil},
			{sub("t1", "u_alice", 1, BUY, 45, 5), "accepted(0) fill(M5:t0@30,t1*@45,t2@25) level(0 BUY 30 0) level(2 BUY 25 0)", []string{}},
		}},
	}
	// extras gives a case a non-YES/NO market and/or post-run checks.
	type extra struct {
		outcomes []string                      // nil = YES/NO
		after    func(t *testing.T, e *Engine) // nil = no extra checks
	}
	extras := map[string]extra{
		"taker sell stp cancels newest and leaves own bid": {after: func(t *testing.T, e *Engine) {
			if e.Position("mkt_demo", "u_bob", 0) != 10 {
				t.Fatal("stp must not move bob's shares")
			}
		}},
		"mint walks two levels in one command": {after: func(t *testing.T, e *Engine) {
			if e.Position("mkt_demo", "u_alice", 0) != 8 || e.Position("mkt_demo", "u_carol", 1) != 5 || e.Position("mkt_demo", "u_dave", 1) != 3 {
				t.Fatal("multi-level mint must credit each leg")
			}
		}},
		"three outcome mint legs sum to 100": {outcomes: []string{"A", "B", "C"}, after: func(t *testing.T, e *Engine) {
			for _, c := range []struct {
				user    string
				outcome uint32
			}{{"u_carol", 0}, {"u_alice", 1}, {"u_dave", 2}} {
				if e.Position("mkt_demo", c.user, c.outcome) != 5 {
					t.Fatalf("%s outcome %d: want 5 shares", c.user, c.outcome)
				}
			}
		}},
	}
	for name := range extras {
		found := false
		for _, tc := range cases {
			found = found || tc.name == name
		}
		if !found {
			t.Fatalf("extras key %q matches no case", name)
		}
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			x := extras[tc.name]
			var te *tengine
			if x.outcomes != nil {
				te = &tengine{t: t, e: New(0)}
				te.create("mkt_demo", x.outcomes...)
			} else {
				te = newT(t)
			}
			if tc.seed {
				seedMint(t, te, 10)
			}
			for i, st := range tc.steps {
				evs := te.apply(st.o)
				if got := kinds(evs); got != trimSpace(st.want) {
					t.Fatalf("step %d (%s):\n got %s\nwant %s", i, st.o.OrderId, got, trimSpace(st.want))
				}
				if st.book != nil {
					wantBook(t, te.e, "mkt_demo", st.book...)
				}
				checkInvariants(t, te.e)
			}
			if x.after != nil {
				x.after(t, te.e)
			}
		})
	}
}

func TestMintAndTransferMovePositions(t *testing.T) {
	te := newT(t)
	seedMint(t, te, 10)
	if te.e.Position("mkt_demo", "u_bob", 0) != 10 || te.e.Position("mkt_demo", "u_carol", 1) != 10 {
		t.Fatal("mint must credit each leg's outcome")
	}
	te.apply(sub("s1", "u_bob", 0, SELL, 55, 4))
	te.apply(sub("b1", "u_alice", 0, BUY, 55, 4))
	if te.e.Position("mkt_demo", "u_bob", 0) != 6 || te.e.Position("mkt_demo", "u_alice", 0) != 4 {
		t.Fatal("transfer must move shares seller -> buyer")
	}
	// Taker SELL into a resting bid moves shares the other way.
	te.apply(sub("b2", "u_carol", 0, BUY, 70, 2))
	te.apply(sub("s2", "u_alice", 0, SELL, 65, 2))
	if te.e.Position("mkt_demo", "u_alice", 0) != 2 || te.e.Position("mkt_demo", "u_carol", 0) != 2 {
		t.Fatal("taker sell must move shares seller -> resting buyer")
	}
}

func TestFillIDAndEventHeaders(t *testing.T) {
	te := newT(t)
	te.apply(sub("n1", "u_carol", 1, BUY, 45, 10))     // seq 2
	evs := te.apply(sub("y1", "u_bob", 0, BUY, 60, 4)) // seq 3
	f := evs[1].GetFill()
	if f.GetFillId() != "f0-3-1" || f.GetMarketId() != "mkt_demo" || evs[1].Idx != 1 {
		t.Fatalf("fill id/idx: %s idx=%d", f.GetFillId(), evs[1].Idx)
	}
	sum := uint32(0)
	for _, l := range f.Legs {
		if l.Side != BUY {
			t.Fatal("mint legs are all BUY")
		}
		sum += l.Price
	}
	if sum != 100 || len(f.Legs) != 2 || f.Legs[0].Outcome != 0 || f.Legs[1].Outcome != 1 {
		t.Fatalf("mint legs must be ordered by outcome and sum to 100: %+v", f.Legs)
	}
	for i, ev := range evs {
		if ev.Idx != uint32(i) || ev.Seq != 3 || ev.MarketSeq != 3 {
			t.Fatalf("event %d header: %+v", i, ev)
		}
	}
}
