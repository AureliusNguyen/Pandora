package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"

	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
	matcherv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/matcher/v1"
	"github.com/aureliusnguyen/pandora/backend/gen/go/pandora/matcher/v1/matcherv1connect"
)

type fakeMatcher struct {
	matcherv1connect.UnimplementedMatcherServiceHandler
	calls   atomic.Int32
	lastReq atomic.Pointer[matcherv1.SubmitOrderRequest]
	err     error
}

func (f *fakeMatcher) SubmitOrder(_ context.Context, r *connect.Request[matcherv1.SubmitOrderRequest]) (*connect.Response[matcherv1.SubmitOrderResponse], error) {
	f.calls.Add(1)
	f.lastReq.Store(r.Msg)
	if f.err != nil {
		return nil, f.err
	}
	return connect.NewResponse(&matcherv1.SubmitOrderResponse{
		OrderId: "o_1", Seq: 9, Status: commonv1.OrderStatus_ORDER_STATUS_FILLED, EngineTimeUnixNanos: 1_700_000_000_000_000_000,
		Fills: []*commonv1.Fill{{FillId: "f_1", Kind: commonv1.FillKind_FILL_KIND_MINT, Qty: 10, Legs: []*commonv1.FillLeg{
			{OrderId: "o_1", UserId: "u_alice", Outcome: 0, Side: commonv1.Side_SIDE_BUY, Price: 55, IsTaker: true},
		}}},
	}), nil
}

func (f *fakeMatcher) GetBook(_ context.Context, r *connect.Request[matcherv1.GetBookRequest]) (*connect.Response[matcherv1.GetBookResponse], error) {
	if f.err != nil {
		return nil, f.err
	}
	if r.Msg.GetMarketId() != DemoMarketID {
		return nil, connect.NewError(connect.CodeNotFound, nil)
	}
	return connect.NewResponse(&matcherv1.GetBookResponse{
		MarketId: DemoMarketID, MarketSeq: 42, Status: commonv1.MarketStatus_MARKET_STATUS_OPEN,
		Levels: []*commonv1.Level{{Outcome: 0, Side: commonv1.Side_SIDE_BUY, Price: 55, Qty: 10}},
	}), nil
}

func (f *fakeMatcher) CancelOrder(_ context.Context, _ *connect.Request[matcherv1.CancelOrderRequest]) (*connect.Response[matcherv1.CancelOrderResponse], error) {
	return connect.NewResponse(&matcherv1.CancelOrderResponse{Seq: 11, Canceled: true, CanceledQty: 4}), nil
}

func newTestGateway(t *testing.T, f *fakeMatcher) http.Handler {
	t.Helper()
	path, h := matcherv1connect.NewMatcherServiceHandler(f)
	mux := http.NewServeMux()
	mux.Handle(path, h)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	client := matcherv1connect.NewMatcherServiceClient(ts.Client(), ts.URL)
	return NewServer(client, slog.New(slog.NewTextHandler(io.Discard, nil))).Handler()
}

func do(h http.Handler, method, path, user, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if user != "" {
		req.Header.Set("X-User-Id", user)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

const validOrder = `{"market_id":"mkt_demo","outcome":0,"side":"BUY","price":55,"qty":10,"tif":"GTC","post_only":false,"client_order_id":"c1"}`

func TestSubmitOrderOK(t *testing.T) {
	f := &fakeMatcher{}
	rec := do(newTestGateway(t, f), "POST", "/v1/orders", "u_alice", validOrder)
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["status"] != "FILLED" || got["order_id"] != "o_1" || got["reject_reason"] != "" || got["engine_time"] != "2023-11-14T22:13:20Z" {
		t.Fatalf("body = %v", got)
	}
	fill := got["fills"].([]any)[0].(map[string]any)
	if fill["kind"] != "MINT" || fill["legs"].([]any)[0].(map[string]any)["side"] != "BUY" {
		t.Fatalf("fill = %v", fill)
	}
	if o := f.lastReq.Load().GetOrder(); o.GetUserId() != "u_alice" || o.GetTif() != commonv1.TimeInForce_TIME_IN_FORCE_GTC {
		t.Fatalf("forwarded = %v", o)
	}
}

func TestSubmitOrderValidation(t *testing.T) {
	f := &fakeMatcher{}
	h := newTestGateway(t, f)
	cases := map[string]string{
		"price0":   strings.Replace(validOrder, `"price":55`, `"price":0`, 1),
		"price100": strings.Replace(validOrder, `"price":55`, `"price":100`, 1),
		"qty0":     strings.Replace(validOrder, `"qty":10`, `"qty":0`, 1),
		"qtyneg":   strings.Replace(validOrder, `"qty":10`, `"qty":-1`, 1),
		"outcome2": strings.Replace(validOrder, `"outcome":0`, `"outcome":2`, 1),
		"side":     strings.Replace(validOrder, `"BUY"`, `"HOLD"`, 1),
		"tif":      strings.Replace(validOrder, `"GTC"`, `"DAY"`, 1),
		"coid":     strings.Replace(validOrder, `"c1"`, `"`+strings.Repeat("x", 65)+`"`, 1),
		"coidnil":  strings.Replace(validOrder, `"c1"`, `""`, 1),
		"badjson":  `{"price":"fifty"}`,
	}
	for name, body := range cases {
		if rec := do(h, "POST", "/v1/orders", "u_alice", body); rec.Code != 400 || !strings.Contains(rec.Body.String(), `"error"`) {
			t.Errorf("%s: code=%d body=%s", name, rec.Code, rec.Body)
		}
	}
	if rec := do(h, "POST", "/v1/orders", "", validOrder); rec.Code != 400 {
		t.Errorf("missing user: code=%d", rec.Code)
	}
	if f.calls.Load() != 0 {
		t.Fatalf("matcher called %d times for invalid input", f.calls.Load())
	}
}

func TestMatcherErrorMapping(t *testing.T) {
	type want struct {
		code int
		body string
	}
	for code, w := range map[connect.Code]want{
		connect.CodeInvalidArgument:  {400, "invalid order"},
		connect.CodeNotFound:         {404, "market not found"},
		connect.CodeUnavailable:      {503, "exchange unavailable"},
		connect.CodeInternal:         {500, "internal error"},
		connect.CodeDeadlineExceeded: {503, "exchange unavailable"},
		connect.CodeCanceled:         {statusClientClosed, "request canceled"},
	} {
		// The matcher's own message must never reach the client.
		f := &fakeMatcher{err: connect.NewError(code, errors.New("secret matcher detail 10.0.0.7:9001"))}
		rec := do(newTestGateway(t, f), "POST", "/v1/orders", "u_alice", validOrder)
		if rec.Code != w.code || rec.Body.String() != `{"error":"`+w.body+`"}`+"\n" {
			t.Errorf("%v: got %d %q want %d %q", code, rec.Code, rec.Body, w.code, w.body)
		}
	}
}

func TestDeadMatcherDoesNotLeakAddress(t *testing.T) {
	client, err := NewMatcherClient("http://127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	h := NewServer(client, slog.New(slog.NewTextHandler(io.Discard, nil))).Handler()
	for _, path := range []string{"/v1/markets", "/v1/markets/mkt_demo/book"} {
		rec := do(h, "GET", path, "", "")
		body := rec.Body.String()
		if rec.Code != 503 || strings.Contains(body, "127.0.0.1") || strings.Contains(body, "dial") {
			t.Fatalf("%s: %d %q", path, rec.Code, body)
		}
	}
	rec := do(h, "POST", "/v1/orders", "u_alice", validOrder)
	if rec.Code != 503 || rec.Body.String() != `{"error":"exchange unavailable"}`+"\n" {
		t.Fatalf("submit: %d %q", rec.Code, rec.Body)
	}
}

func TestOversizedBody(t *testing.T) {
	f := &fakeMatcher{}
	body := `{"client_order_id":"` + strings.Repeat("x", 1<<17) + `"}`
	rec := do(newTestGateway(t, f), "POST", "/v1/orders", "u_alice", body)
	if rec.Code != http.StatusRequestEntityTooLarge || rec.Body.String() != `{"error":"request body too large"}`+"\n" {
		t.Fatalf("oversized: %d %q", rec.Code, rec.Body)
	}
	if f.calls.Load() != 0 {
		t.Fatal("matcher called for oversized body")
	}
}

func TestDecodeErrorMessages(t *testing.T) {
	h := newTestGateway(t, &fakeMatcher{})
	cases := map[string]string{
		`{"price":"fifty"}`: "invalid JSON body: field price",
		`{"qty":1.5}`:       "invalid JSON body: field qty",
		`{not json`:         "invalid JSON body",
	}
	for body, want := range cases {
		rec := do(h, "POST", "/v1/orders", "u_alice", body)
		if rec.Code != 400 || rec.Body.String() != `{"error":"`+want+`"}`+"\n" {
			t.Errorf("%s: %d %q want %q", body, rec.Code, rec.Body, want)
		}
	}
}

func TestBookAndMarkets(t *testing.T) {
	h := newTestGateway(t, &fakeMatcher{})
	rec := do(h, "GET", "/v1/markets/mkt_demo/book", "", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"status":"open"`) || !strings.Contains(rec.Body.String(), `"side":"BUY"`) {
		t.Fatalf("book: %d %s", rec.Code, rec.Body)
	}
	if rec := do(h, "GET", "/v1/markets/nope/book", "", ""); rec.Code != 404 {
		t.Fatalf("unknown market: %d", rec.Code)
	}
	rec = do(h, "GET", "/v1/markets", "", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"outcomes":["YES","NO"]`) {
		t.Fatalf("markets: %d %s", rec.Code, rec.Body)
	}
}

func TestCancel(t *testing.T) {
	h := newTestGateway(t, &fakeMatcher{})
	if rec := do(h, "DELETE", "/v1/orders/o_1", "u_alice", ""); rec.Code != 400 {
		t.Fatalf("missing market_id: %d", rec.Code)
	}
	rec := do(h, "DELETE", "/v1/orders/o_1?market_id=mkt_demo", "u_alice", "")
	if rec.Code != 200 || rec.Body.String() != `{"seq":11,"canceled":true,"canceled_qty":4}`+"\n" {
		t.Fatalf("cancel: %d %q", rec.Code, rec.Body)
	}
}

func TestUserIDHeaderBounds(t *testing.T) {
	f := &fakeMatcher{}
	h := newTestGateway(t, f)
	if rec := do(h, "POST", "/v1/orders", "", validOrder); rec.Code != 400 {
		t.Fatalf("missing user: %d", rec.Code)
	}
	if rec := do(h, "POST", "/v1/orders", strings.Repeat("u", 65), validOrder); rec.Code != 400 {
		t.Fatalf("65-char user: %d", rec.Code)
	}
	// Synthetic k6 USER_POOL users are accepted as-is.
	if rec := do(h, "POST", "/v1/orders", "u_k6_0042", validOrder); rec.Code != 200 || f.lastReq.Load().GetOrder().GetUserId() != "u_k6_0042" {
		t.Fatalf("pool user: %d", rec.Code)
	}
}

func TestClientCanceledRequest(t *testing.T) {
	f := &fakeMatcher{}
	h := newTestGateway(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest("POST", "/v1/orders", strings.NewReader(validOrder)).WithContext(ctx)
	req.Header.Set("X-User-Id", "u_alice")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != statusClientClosed {
		t.Fatalf("canceled client: code=%d body=%s", rec.Code, rec.Body)
	}
}
