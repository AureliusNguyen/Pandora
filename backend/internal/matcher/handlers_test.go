package matcher

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"

	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
	matcherv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/matcher/v1"
	"github.com/aureliusnguyen/pandora/backend/gen/go/pandora/matcher/v1/matcherv1connect"
)

const validULID = "01ARZ3NDEKTSV4RRFFQ69G5FAV"

func newClientT(t *testing.T) (matcherv1connect.MatcherServiceClient, *Service, *FakeWAL) {
	t.Helper()
	return newClientIDT(t, nil)
}

// newClientIDT is newClientT with newID (when non-nil) assigning order ids.
func newClientIDT(t *testing.T, newID func() string) (matcherv1connect.MatcherServiceClient, *Service, *FakeWAL) {
	t.Helper()
	w := NewFakeWAL()
	svc := newServiceT(t, w, true)
	if newID != nil {
		svc.o.NewID = newID // set before any handler goroutine exists
	}
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(svc.Stop)
	mux := http.NewServeMux()
	mux.Handle(matcherv1connect.NewMatcherServiceHandler(svc))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return matcherv1connect.NewMatcherServiceClient(srv.Client(), srv.URL), svc, w
}

func order(user string, side commonv1.Side, price uint32, qty uint64) *enginev1.SubmitOrder {
	return &enginev1.SubmitOrder{
		MarketId: DemoMarketID, UserId: user, ClientOrderId: user + "-" + itoa(int(price)) + "-" + itoa(int(qty)),
		Outcome: 0, Side: side, Price: price, Qty: qty, Tif: commonv1.TimeInForce_TIME_IN_FORCE_GTC,
	}
}

// mod returns a valid order after f edited it.
func mod(f func(o *enginev1.SubmitOrder)) *enginev1.SubmitOrder {
	o := order("u_alice", commonv1.Side_SIDE_BUY, 50, 1)
	f(o)
	return o
}

func code(err error) connect.Code {
	var ce *connect.Error
	if errors.As(err, &ce) {
		return ce.Code()
	}
	return connect.Code(0)
}

func TestSubmitAssignsIDAndMapsStatuses(t *testing.T) {
	c, _, _ := newClientT(t)
	ctx := context.Background()
	r1, err := c.SubmitOrder(ctx, connect.NewRequest(&matcherv1.SubmitOrderRequest{Order: order("u_carol", commonv1.Side_SIDE_BUY, 40, 10)}))
	if err != nil {
		t.Fatal(err)
	}
	m1 := r1.Msg
	if m1.OrderId == "" || m1.Seq != 2 || m1.Status != commonv1.OrderStatus_ORDER_STATUS_ACCEPTED || m1.EngineTimeUnixNanos != 7 || len(m1.Fills) != 0 {
		t.Fatalf("resting: %+v", m1)
	}
	o1 := m1.OrderId
	// crossing NO bid 40 with a YES buy at 60 -> MINT, fully filled
	no := order("u_carol", commonv1.Side_SIDE_BUY, 40, 10)
	no.Outcome = 1
	no.ClientOrderId = "carol-no"
	if _, err := c.SubmitOrder(ctx, connect.NewRequest(&matcherv1.SubmitOrderRequest{Order: no})); err != nil {
		t.Fatal(err)
	}
	yes := order("u_bob", commonv1.Side_SIDE_BUY, 60, 4)
	r2, err := c.SubmitOrder(ctx, connect.NewRequest(&matcherv1.SubmitOrderRequest{Order: yes}))
	if err != nil {
		t.Fatal(err)
	}
	if r2.Msg.OrderId == "" || r2.Msg.OrderId == o1 || r2.Msg.Status != commonv1.OrderStatus_ORDER_STATUS_FILLED || len(r2.Msg.Fills) != 1 || r2.Msg.Fills[0].Kind != commonv1.FillKind_FILL_KIND_MINT {
		t.Fatalf("filled: %+v", r2.Msg)
	}
	// bob now holds 4 YES; an IOC sell at 30 transfers against carol's resting 40 bid (o1)
	ioc := order("u_bob", commonv1.Side_SIDE_SELL, 30, 4)
	ioc.Tif = commonv1.TimeInForce_TIME_IN_FORCE_IOC
	ioc.ClientOrderId = "bob-ioc"
	r3, err := c.SubmitOrder(ctx, connect.NewRequest(&matcherv1.SubmitOrderRequest{Order: ioc}))
	if err != nil || r3.Msg.Status != commonv1.OrderStatus_ORDER_STATUS_FILLED || len(r3.Msg.Fills) != 1 || r3.Msg.Fills[0].Legs[0].OrderId != o1 {
		t.Fatalf("ioc fill: %+v err=%v", r3.Msg, err)
	}
	// engine rejections are reported as status REJECTED, not as connect errors
	dup := order("u_carol", commonv1.Side_SIDE_BUY, 40, 10)
	r4, err := c.SubmitOrder(ctx, connect.NewRequest(&matcherv1.SubmitOrderRequest{Order: dup}))
	if err != nil || r4.Msg.Status != commonv1.OrderStatus_ORDER_STATUS_REJECTED || r4.Msg.RejectReason != commonv1.RejectReason_REJECT_REASON_DUPLICATE_CLIENT_ORDER_ID {
		t.Fatalf("dup: %+v err=%v", r4.Msg, err)
	}
}

func TestSubmitPartialAndCanceledStatuses(t *testing.T) {
	c, _, _ := newClientT(t)
	ctx := context.Background()
	no := order("u_carol", commonv1.Side_SIDE_BUY, 50, 3)
	no.Outcome = 1
	if _, err := c.SubmitOrder(ctx, connect.NewRequest(&matcherv1.SubmitOrderRequest{Order: no})); err != nil {
		t.Fatal(err)
	}
	r, err := c.SubmitOrder(ctx, connect.NewRequest(&matcherv1.SubmitOrderRequest{Order: order("u_bob", commonv1.Side_SIDE_BUY, 50, 5)}))
	if err != nil || r.Msg.Status != commonv1.OrderStatus_ORDER_STATUS_PARTIALLY_FILLED || len(r.Msg.Fills) != 1 || r.Msg.Fills[0].Qty != 3 {
		t.Fatalf("partial: %+v err=%v", r.Msg, err)
	}
	ioc := order("u_alice", commonv1.Side_SIDE_BUY, 1, 1)
	ioc.Tif = commonv1.TimeInForce_TIME_IN_FORCE_IOC
	r, err = c.SubmitOrder(ctx, connect.NewRequest(&matcherv1.SubmitOrderRequest{Order: ioc}))
	if err != nil || r.Msg.Status != commonv1.OrderStatus_ORDER_STATUS_CANCELED {
		t.Fatalf("ioc unfilled: %+v err=%v", r.Msg, err)
	}
}

func TestSubmitErrorCodes(t *testing.T) {
	c, svc, w := newClientT(t)
	ctx := context.Background()
	cases := []struct {
		name string
		o    *enginev1.SubmitOrder
		want connect.Code
	}{
		{"nil order", nil, connect.CodeInvalidArgument},
		{"empty user", func() *enginev1.SubmitOrder { o := order("", commonv1.Side_SIDE_BUY, 50, 1); return o }(), connect.CodeInvalidArgument},
		{"price 0", order("u_alice", commonv1.Side_SIDE_BUY, 0, 1), connect.CodeInvalidArgument},
		{"price 100", order("u_alice", commonv1.Side_SIDE_BUY, 100, 1), connect.CodeInvalidArgument},
		{"qty 0", order("u_alice", commonv1.Side_SIDE_BUY, 50, 0), connect.CodeInvalidArgument},
		{"bad side", order("u_alice", commonv1.Side_SIDE_UNSPECIFIED, 50, 1), connect.CodeInvalidArgument},
		{"no tif", func() *enginev1.SubmitOrder {
			o := order("u_alice", commonv1.Side_SIDE_BUY, 50, 1)
			o.Tif = commonv1.TimeInForce_TIME_IN_FORCE_UNSPECIFIED
			return o
		}(), connect.CodeInvalidArgument},
		{"unknown market", func() *enginev1.SubmitOrder {
			o := order("u_alice", commonv1.Side_SIDE_BUY, 50, 1)
			o.MarketId = "ghost"
			return o
		}(), connect.CodeNotFound},
		{"bad order_id", mod(func(o *enginev1.SubmitOrder) { o.OrderId = "given-id" }), connect.CodeInvalidArgument},
		{"25-char order_id", mod(func(o *enginev1.SubmitOrder) { o.OrderId = validULID[:25] }), connect.CodeInvalidArgument},
		// The matcher always assigns ids: even a valid ULID is refused.
		{"given ULID", mod(func(o *enginev1.SubmitOrder) { o.OrderId = validULID }), connect.CodeInvalidArgument},
		{"given lowercase ULID", mod(func(o *enginev1.SubmitOrder) { o.OrderId = strings.ToLower(validULID) }), connect.CodeInvalidArgument},
		{"65-char user_id", mod(func(o *enginev1.SubmitOrder) { o.UserId = strings.Repeat("u", 65) }), connect.CodeInvalidArgument},
		{"65-char market_id", mod(func(o *enginev1.SubmitOrder) { o.MarketId = strings.Repeat("m", 65) }), connect.CodeInvalidArgument},
		{"empty client_order_id", mod(func(o *enginev1.SubmitOrder) { o.ClientOrderId = "" }), connect.CodeInvalidArgument},
		{"65-char client_order_id", mod(func(o *enginev1.SubmitOrder) { o.ClientOrderId = strings.Repeat("c", 65) }), connect.CodeInvalidArgument},
		{"outcome out of range", mod(func(o *enginev1.SubmitOrder) { o.Outcome = 2 }), connect.CodeInvalidArgument},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := c.SubmitOrder(ctx, connect.NewRequest(&matcherv1.SubmitOrderRequest{Order: tc.o}))
			if code(err) != tc.want {
				t.Fatalf("got %v (%v) want %v", code(err), err, tc.want)
			}
		})
	}
	// Rejected inputs never reach the WAL: only the demo market record exists.
	if w.AppendCount() != 1 || len(w.Records[0]) != 1 {
		t.Fatalf("invalid requests were journaled: appends=%d records=%d", w.AppendCount(), len(w.Records[0]))
	}
	// 64-char ids are accepted and the matcher assigns the order id.
	long := mod(func(o *enginev1.SubmitOrder) {
		o.UserId = strings.Repeat("u", 64)
		o.ClientOrderId = strings.Repeat("c", 64)
	})
	if r, err := c.SubmitOrder(ctx, connect.NewRequest(&matcherv1.SubmitOrderRequest{Order: long})); err != nil || r.Msg.OrderId == "" {
		t.Fatalf("64-char ids must be accepted: %+v err=%v", r, err)
	}
	svc.Stop()
	_, err := c.SubmitOrder(ctx, connect.NewRequest(&matcherv1.SubmitOrderRequest{Order: order("u_alice", commonv1.Side_SIDE_BUY, 50, 1)}))
	if code(err) != connect.CodeUnavailable {
		t.Fatalf("stopped service must be Unavailable, got %v", err)
	}
	if _, err := c.GetBook(ctx, connect.NewRequest(&matcherv1.GetBookRequest{MarketId: DemoMarketID})); code(err) != connect.CodeUnavailable {
		t.Fatalf("stopped service GetBook must be Unavailable, got %v", err)
	}
}

// Ids from NewID are uppercase ULIDs; a cancel in lowercase still matches.
func TestLowercaseCancelMatchesAssignedULID(t *testing.T) {
	c, _, _ := newClientIDT(t, func() string { return validULID })
	ctx := context.Background()
	r, err := c.SubmitOrder(ctx, connect.NewRequest(&matcherv1.SubmitOrderRequest{Order: order("u_alice", commonv1.Side_SIDE_BUY, 50, 1)}))
	if err != nil || r.Msg.OrderId != validULID {
		t.Fatalf("submit: %+v err=%v", r, err)
	}
	cr, err := c.CancelOrder(ctx, connect.NewRequest(&matcherv1.CancelOrderRequest{Cancel: &enginev1.CancelOrder{
		OrderId: strings.ToLower(validULID), MarketId: DemoMarketID, UserId: "u_alice",
	}}))
	if err != nil || !cr.Msg.Canceled || cr.Msg.CanceledQty != 1 {
		t.Fatalf("lowercase cancel must match: %+v err=%v", cr, err)
	}
}

func TestRunnerErrorMapping(t *testing.T) {
	cases := []struct {
		err     error
		want    connect.Code
		wantMsg string // "" means the underlying error's message
	}{
		{context.Canceled, connect.CodeCanceled, ""},
		{context.DeadlineExceeded, connect.CodeDeadlineExceeded, ""},
		{ErrShuttingDown, connect.CodeUnavailable, "exchange unavailable"},
		{errors.Join(ErrWALProduce, errors.New("broker down")), connect.CodeUnavailable, "exchange unavailable"},
		{errors.Join(ErrMarshal, errors.New("bad proto")), connect.CodeInternal, "internal error"},
	}
	for _, tc := range cases {
		var logBuf bytes.Buffer
		log := slog.New(slog.NewTextHandler(&logBuf, nil))
		err := runnerError(log, tc.err)
		if got := code(err); got != tc.want {
			t.Errorf("%v: got %v want %v", tc.err, got, tc.want)
		}
		var ce *connect.Error
		if !errors.As(err, &ce) {
			t.Fatalf("%v: not a connect error", tc.err)
		}
		want := tc.wantMsg
		if want == "" {
			want = tc.err.Error()
		}
		if ce.Message() != want {
			t.Errorf("%v: message %q want %q", tc.err, ce.Message(), want)
		}
		if tc.wantMsg != "" && (!strings.Contains(logBuf.String(), "level=WARN") || !strings.Contains(logBuf.String(), strings.SplitN(tc.err.Error(), "\n", 2)[0])) {
			t.Errorf("%v: underlying error not logged at WARN: %q", tc.err, logBuf.String())
		}
	}
}

func TestCancelGetBookAndAdmin(t *testing.T) {
	c, svc, w := newClientT(t)
	ctx := context.Background()
	r, err := c.SubmitOrder(ctx, connect.NewRequest(&matcherv1.SubmitOrderRequest{Order: order("u_alice", commonv1.Side_SIDE_BUY, 33, 7)}))
	if err != nil {
		t.Fatal(err)
	}
	book, err := c.GetBook(ctx, connect.NewRequest(&matcherv1.GetBookRequest{MarketId: DemoMarketID}))
	if err != nil || book.Msg.MarketSeq != 2 || len(book.Msg.Levels) != 1 || book.Msg.Levels[0].Qty != 7 || book.Msg.Status != commonv1.MarketStatus_MARKET_STATUS_OPEN {
		t.Fatalf("book: %+v err=%v", book.Msg, err)
	}
	if _, err := c.GetBook(ctx, connect.NewRequest(&matcherv1.GetBookRequest{MarketId: "ghost"})); code(err) != connect.CodeNotFound {
		t.Fatalf("ghost book: %v", err)
	}
	if _, err := c.GetBook(ctx, connect.NewRequest(&matcherv1.GetBookRequest{})); code(err) != connect.CodeInvalidArgument {
		t.Fatalf("book without market: %v", err)
	}
	// a cancel by another user leaves the order resting (ownership check)
	cr, err := c.CancelOrder(ctx, connect.NewRequest(&matcherv1.CancelOrderRequest{Cancel: &enginev1.CancelOrder{OrderId: r.Msg.OrderId, MarketId: DemoMarketID, UserId: "u_mallory"}}))
	if err != nil || cr.Msg.Canceled || cr.Msg.Seq != 3 {
		t.Fatalf("foreign cancel: %+v err=%v", cr.Msg, err)
	}
	cr, err = c.CancelOrder(ctx, connect.NewRequest(&matcherv1.CancelOrderRequest{Cancel: &enginev1.CancelOrder{OrderId: r.Msg.OrderId, MarketId: DemoMarketID, UserId: "u_alice"}}))
	if err != nil || !cr.Msg.Canceled || cr.Msg.CanceledQty != 7 || cr.Msg.Seq != 4 {
		t.Fatalf("cancel: %+v err=%v", cr.Msg, err)
	}
	cr, err = c.CancelOrder(ctx, connect.NewRequest(&matcherv1.CancelOrderRequest{Cancel: &enginev1.CancelOrder{OrderId: r.Msg.OrderId, MarketId: DemoMarketID, UserId: "u_alice"}}))
	if err != nil || cr.Msg.Canceled || cr.Msg.Seq != 5 {
		t.Fatalf("second cancel: %+v err=%v", cr.Msg, err)
	}
	if _, err := c.CancelOrder(ctx, connect.NewRequest(&matcherv1.CancelOrderRequest{Cancel: &enginev1.CancelOrder{OrderId: "x", UserId: "u_alice"}})); code(err) != connect.CodeInvalidArgument {
		t.Fatalf("cancel without market: %v", err)
	}
	if _, err := c.CancelOrder(ctx, connect.NewRequest(&matcherv1.CancelOrderRequest{Cancel: &enginev1.CancelOrder{OrderId: "x", MarketId: DemoMarketID}})); code(err) != connect.CodeInvalidArgument {
		t.Fatalf("cancel without user: %v", err)
	}
	if _, err := c.CancelOrder(ctx, connect.NewRequest(&matcherv1.CancelOrderRequest{Cancel: &enginev1.CancelOrder{OrderId: "x", MarketId: "ghost", UserId: "u_alice"}})); code(err) != connect.CodeNotFound {
		t.Fatalf("cancel on ghost market: %v", err)
	}
	if _, err := c.CancelOrder(ctx, connect.NewRequest(&matcherv1.CancelOrderRequest{})); code(err) != connect.CodeInvalidArgument {
		t.Fatalf("empty cancel: %v", err)
	}
	for name, cancel := range map[string]*enginev1.CancelOrder{
		"65-char order_id":  {OrderId: strings.Repeat("o", 65), MarketId: DemoMarketID, UserId: "u_alice"},
		"65-char user_id":   {OrderId: "x", MarketId: DemoMarketID, UserId: strings.Repeat("u", 65)},
		"65-char market_id": {OrderId: "x", MarketId: strings.Repeat("m", 65), UserId: "u_alice"},
	} {
		if _, err := c.CancelOrder(ctx, connect.NewRequest(&matcherv1.CancelOrderRequest{Cancel: cancel})); code(err) != connect.CodeInvalidArgument {
			t.Fatalf("cancel %s: %v", name, err)
		}
	}
	ar, err := c.AdminCommand(ctx, connect.NewRequest(&matcherv1.AdminCommandRequest{Body: &matcherv1.AdminCommandRequest_CreateMarket{CreateMarket: &enginev1.CreateMarket{MarketId: "mkt_two", Title: "two", OutcomeLabels: []string{"A", "B"}}}}))
	if err != nil || ar.Msg.Seq != 6 {
		t.Fatalf("create: %+v err=%v", ar.Msg, err)
	}
	records := len(w.Records[0])
	for name, cm := range map[string]*enginev1.CreateMarket{
		"one label":         {MarketId: "bad", OutcomeLabels: []string{"A"}},
		"empty label":       {MarketId: "bad", OutcomeLabels: []string{"A", ""}},
		"nine labels":       {MarketId: "bad", OutcomeLabels: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"}},
		"no market_id":      {OutcomeLabels: []string{"A", "B"}},
		"65-char market_id": {MarketId: strings.Repeat("m", 65), OutcomeLabels: []string{"A", "B"}},
	} {
		if _, err := c.AdminCommand(ctx, connect.NewRequest(&matcherv1.AdminCommandRequest{Body: &matcherv1.AdminCommandRequest_CreateMarket{CreateMarket: cm}})); code(err) != connect.CodeInvalidArgument {
			t.Fatalf("create with %s: %v", name, err)
		}
	}
	if !svc.Runner().HasMarket("mkt_two") || svc.Runner().HasMarket("bad") {
		t.Fatal("rejected create_market reached the engine")
	}
	if got := len(w.Records[0]); got != records {
		t.Fatalf("rejected create_market was journaled: records %d -> %d", records, got)
	}
	// Bounds are inclusive: a 256-char title and 64-char labels are fine.
	if _, err := c.AdminCommand(ctx, connect.NewRequest(&matcherv1.AdminCommandRequest{Body: &matcherv1.AdminCommandRequest_CreateMarket{CreateMarket: &enginev1.CreateMarket{
		MarketId: "mkt_max", Title: strings.Repeat("t", 256), OutcomeLabels: []string{strings.Repeat("l", 64), "B"},
	}}})); err != nil {
		t.Fatalf("create at the bounds: %v", err)
	}
	records = len(w.Records[0])
	create := func(cm *enginev1.CreateMarket) *matcherv1.AdminCommandRequest {
		return &matcherv1.AdminCommandRequest{Body: &matcherv1.AdminCommandRequest_CreateMarket{CreateMarket: cm}}
	}
	for _, tc := range []struct {
		name string
		req  *matcherv1.AdminCommandRequest
		want connect.Code
	}{
		{"257-char title", create(&enginev1.CreateMarket{MarketId: "bad", Title: strings.Repeat("t", 257), OutcomeLabels: []string{"A", "B"}}), connect.CodeInvalidArgument},
		{"65-char label", create(&enginev1.CreateMarket{MarketId: "bad", OutcomeLabels: []string{"A", strings.Repeat("l", 65)}}), connect.CodeInvalidArgument},
		{"1025-char void reason", &matcherv1.AdminCommandRequest{Body: &matcherv1.AdminCommandRequest_VoidMarket{VoidMarket: &enginev1.VoidMarket{
			MarketId: "mkt_two", Reason: strings.Repeat("r", 1025),
		}}}, connect.CodeInvalidArgument},
		{"existing market", create(&enginev1.CreateMarket{MarketId: "mkt_two", Title: "again", OutcomeLabels: []string{"A", "B"}}), connect.CodeAlreadyExists},
	} {
		if _, err := c.AdminCommand(ctx, connect.NewRequest(tc.req)); code(err) != tc.want {
			t.Fatalf("%s: got %v (%v) want %v", tc.name, code(err), err, tc.want)
		}
		if got := len(w.Records[0]); got != records {
			t.Fatalf("%s was journaled: records %d -> %d", tc.name, records, got)
		}
	}
	if svc.Runner().HasMarket("bad") {
		t.Fatal("rejected create_market reached the engine")
	}
	for _, win := range []uint32{2, 7} {
		if _, err := c.AdminCommand(ctx, connect.NewRequest(&matcherv1.AdminCommandRequest{Body: &matcherv1.AdminCommandRequest_ResolveMarket{ResolveMarket: &enginev1.ResolveMarket{MarketId: DemoMarketID, WinningOutcome: win}}})); code(err) != connect.CodeInvalidArgument {
			t.Fatalf("resolve with winning_outcome %d: %v", win, err)
		}
	}
	if got := len(w.Records[0]); got != records {
		t.Fatalf("out-of-range resolve was journaled: records %d -> %d", records, got)
	}
	if _, err := c.AdminCommand(ctx, connect.NewRequest(&matcherv1.AdminCommandRequest{Body: &matcherv1.AdminCommandRequest_LockMarket{LockMarket: &enginev1.LockMarket{MarketId: "ghost"}}})); code(err) != connect.CodeNotFound {
		t.Fatalf("lock ghost: %v", err)
	}
	if _, err := c.AdminCommand(ctx, connect.NewRequest(&matcherv1.AdminCommandRequest{})); code(err) != connect.CodeInvalidArgument {
		t.Fatalf("empty admin: %v", err)
	}
	if _, err := c.AdminCommand(ctx, connect.NewRequest(&matcherv1.AdminCommandRequest{Body: &matcherv1.AdminCommandRequest_ResolveMarket{ResolveMarket: &enginev1.ResolveMarket{MarketId: DemoMarketID, WinningOutcome: 0}}})); err != nil {
		t.Fatal(err)
	}
	book, err = c.GetBook(ctx, connect.NewRequest(&matcherv1.GetBookRequest{MarketId: DemoMarketID}))
	if err != nil || book.Msg.Status != commonv1.MarketStatus_MARKET_STATUS_RESOLVED {
		t.Fatalf("status after resolve: %v err=%v", book.Msg.GetStatus(), err)
	}
	if _, err := c.AdminCommand(ctx, connect.NewRequest(&matcherv1.AdminCommandRequest{Body: &matcherv1.AdminCommandRequest_VoidMarket{VoidMarket: &enginev1.VoidMarket{MarketId: "mkt_two", Reason: "test"}}})); err != nil {
		t.Fatal(err)
	}
	book, err = c.GetBook(ctx, connect.NewRequest(&matcherv1.GetBookRequest{MarketId: "mkt_two"}))
	if err != nil || book.Msg.Status != commonv1.MarketStatus_MARKET_STATUS_VOIDED {
		t.Fatalf("status after void: %v err=%v", book.Msg.GetStatus(), err)
	}
}

// Review Focus 5: GetBook concurrent with applies must be race-free.
func TestGetBookConcurrentWithSubmits(t *testing.T) {
	c, _, _ := newClientT(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			o := order("u_alice", commonv1.Side_SIDE_BUY, uint32(10+i), 1)
			if _, err := c.SubmitOrder(ctx, connect.NewRequest(&matcherv1.SubmitOrderRequest{Order: o})); err != nil {
				t.Error(err)
			}
		}(i)
		go func() {
			defer wg.Done()
			if _, err := c.GetBook(ctx, connect.NewRequest(&matcherv1.GetBookRequest{MarketId: DemoMarketID})); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	book, err := c.GetBook(ctx, connect.NewRequest(&matcherv1.GetBookRequest{MarketId: DemoMarketID}))
	if err != nil || len(book.Msg.Levels) != 50 || book.Msg.MarketSeq != 51 {
		t.Fatalf("final book: levels=%d seq=%d err=%v", len(book.Msg.GetLevels()), book.Msg.GetMarketSeq(), err)
	}
}
