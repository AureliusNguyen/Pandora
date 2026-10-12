// Package gateway is the public REST edge of Pandora: it reads the M1 dev
// user header, validates order JSON, forwards commands to the matcher over
// connect-go and maps matcher errors to HTTP status codes.
//
// M1 dev auth is NOT authentication: the X-User-Id header is trusted as the
// caller identity, so any client can impersonate any user by design. It is
// for local and kind demos only and is replaced by JWT in M4.
package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
	matcherv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/matcher/v1"
	"github.com/aureliusnguyen/pandora/backend/gen/go/pandora/matcher/v1/matcherv1connect"
)

// DemoMarketID is the single market served in M1.
const DemoMarketID = "mkt_demo"

const (
	demoTitle      = "Demo: BTC above strike"
	matcherTimeout = 2 * time.Second
	// maxUserIDLen bounds the M1 dev-auth header; any non-empty id up to this
	// length is accepted (the ledger auto-creates unknown users).
	maxUserIDLen = 64
	// maxBodyBytes caps the POST /v1/orders body; larger bodies get 413.
	maxBodyBytes = 1 << 16
	// statusClientClosed is the nginx-style 499 "client closed request",
	// used when the caller's context is canceled before the matcher answers.
	// The contract table has no slot for it; nobody reads the body, but the
	// status keeps these out of the 5xx error rate in logs and metrics.
	statusClientClosed = 499
)

// Server holds the gateway's dependencies and builds its HTTP handler.
type Server struct {
	matcher matcherv1connect.MatcherServiceClient
	log     *slog.Logger
	origins []string // CORS allowlist; nil means DefaultAllowedOrigins
}

// NewServer returns a Server that forwards to the matcher client m.
func NewServer(m matcherv1connect.MatcherServiceClient, log *slog.Logger) *Server {
	return &Server{matcher: m, log: log}
}

// NewMatcherClient builds the connect client with OTel propagation.
func NewMatcherClient(addr string) (matcherv1connect.MatcherServiceClient, error) {
	ic, err := otelconnect.NewInterceptor()
	if err != nil {
		return nil, err
	}
	hc := &http.Client{Timeout: matcherTimeout}
	return matcherv1connect.NewMatcherServiceClient(hc, addr, connect.WithInterceptors(ic)), nil
}

// Routes returns the bare route table (no middleware).
func (s *Server) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/orders", s.submitOrder)
	mux.HandleFunc("DELETE /v1/orders/{order_id}", s.cancelOrder)
	mux.HandleFunc("GET /v1/markets/{market_id}/book", s.getBook)
	mux.HandleFunc("GET /v1/markets", s.listMarkets)
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// requireUser reads the M1 dev-auth X-User-Id header. On a missing or
// over-long value it writes the 400 and returns ok=false.
func requireUser(w http.ResponseWriter, r *http.Request) (string, bool) {
	user := r.Header.Get("X-User-Id")
	if user == "" || len(user) > maxUserIDLen {
		writeError(w, http.StatusBadRequest, "X-User-Id header must be 1..64 characters")
		return "", false
	}
	return user, true
}

// writeMatcherError maps a failed matcher call to a status and a fixed
// client message. The error text never reaches the client (it can carry the
// matcher address or internal detail); it is logged instead.
func (s *Server) writeMatcherError(w http.ResponseWriter, r *http.Request, err error) {
	code, msg := http.StatusInternalServerError, "internal error"
	switch connect.CodeOf(err) {
	case connect.CodeInvalidArgument:
		code, msg = http.StatusBadRequest, "invalid order"
	case connect.CodeNotFound:
		code, msg = http.StatusNotFound, "market not found"
	case connect.CodeUnavailable, connect.CodeDeadlineExceeded:
		code, msg = http.StatusServiceUnavailable, "exchange unavailable"
		w.Header().Set("Retry-After", "1")
	case connect.CodeCanceled:
		code, msg = statusClientClosed, "request canceled"
	}
	level := slog.LevelWarn
	if code == http.StatusInternalServerError {
		level = slog.LevelError
	}
	s.log.Log(r.Context(), level, "matcher call failed", "method", r.Method, "path", r.URL.Path, "status", code, "err", err)
	writeError(w, code, msg)
}

// writeDecodeError reports a body decode failure without echoing Go type
// names: 413 for an oversized body, else 400 with at most the field name.
func writeDecodeError(w http.ResponseWriter, err error) {
	if errors.As(err, new(*http.MaxBytesError)) {
		writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
		return
	}
	msg := "invalid JSON body"
	var te *json.UnmarshalTypeError
	if errors.As(err, &te) && te.Field != "" {
		msg += ": field " + te.Field
	}
	writeError(w, http.StatusBadRequest, msg)
}

func (s *Server) submitOrder(w http.ResponseWriter, r *http.Request) {
	user, ok := requireUser(w, r)
	if !ok {
		return
	}
	var req OrderRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&req); err != nil {
		writeDecodeError(w, err)
		return
	}
	if err := req.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), matcherTimeout)
	defer cancel()
	resp, err := s.matcher.SubmitOrder(ctx, connect.NewRequest(&matcherv1.SubmitOrderRequest{Order: req.Proto(user)}))
	if err != nil {
		s.writeMatcherError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toOrderResponse(resp.Msg))
}

func (s *Server) cancelOrder(w http.ResponseWriter, r *http.Request) {
	user, ok := requireUser(w, r)
	if !ok {
		return
	}
	market := r.URL.Query().Get("market_id")
	if market == "" {
		writeError(w, http.StatusBadRequest, "market_id query parameter is required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), matcherTimeout)
	defer cancel()
	resp, err := s.matcher.CancelOrder(ctx, connect.NewRequest(&matcherv1.CancelOrderRequest{
		Cancel: &enginev1.CancelOrder{OrderId: r.PathValue("order_id"), MarketId: market, UserId: user},
	}))
	if err != nil {
		s.writeMatcherError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, CancelResponse{Seq: resp.Msg.GetSeq(), Canceled: resp.Msg.GetCanceled(), CanceledQty: resp.Msg.GetCanceledQty()})
}

func (s *Server) book(ctx context.Context, market string) (*matcherv1.GetBookResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, matcherTimeout)
	defer cancel()
	resp, err := s.matcher.GetBook(ctx, connect.NewRequest(&matcherv1.GetBookRequest{MarketId: market}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func (s *Server) getBook(w http.ResponseWriter, r *http.Request) {
	b, err := s.book(r.Context(), r.PathValue("market_id"))
	if err != nil {
		s.writeMatcherError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toBookResponse(b))
}

func (s *Server) listMarkets(w http.ResponseWriter, r *http.Request) {
	b, err := s.book(r.Context(), DemoMarketID)
	if err != nil {
		s.writeMatcherError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, []MarketJSON{{ID: DemoMarketID, Title: demoTitle, Status: marketStatus(b.GetStatus()), Outcomes: []string{"YES", "NO"}}})
}

// WithAllowedOrigins sets the CORS allowlist (cmd/gateway passes
// config.Gateway.CORSOrigins). Empty keeps DefaultAllowedOrigins.
func (s *Server) WithAllowedOrigins(origins []string) *Server {
	s.origins = origins
	return s
}

// Handler is otelhttp -> CORS -> observe -> mux.
func (s *Server) Handler() http.Handler {
	origins := s.origins
	if len(origins) == 0 {
		origins = DefaultAllowedOrigins
	}
	return otelhttp.NewHandler(withCORS(origins, observe(s.log, s.Routes())), "gateway")
}
