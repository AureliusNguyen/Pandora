package matcher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"connectrpc.com/connect"

	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
	matcherv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/matcher/v1"
	"github.com/aureliusnguyen/pandora/backend/gen/go/pandora/matcher/v1/matcherv1connect"
	"github.com/aureliusnguyen/pandora/backend/internal/engine"
	"github.com/aureliusnguyen/pandora/backend/internal/pkg/ids"
)

// Bounds on client-supplied strings. maxIDLen covers every identifier
// (user_id, market_id, client_order_id, cancel.order_id); the admin strings
// have their own bounds.
const (
	maxIDLen     = 64
	maxTitleLen  = 256
	maxLabelLen  = 64
	maxReasonLen = 1024
)

var _ matcherv1connect.MatcherServiceHandler = (*Service)(nil)

var errNotReady = errors.New("matcher not ready")

func (s *Service) observe(rpc string, start time.Time) {
	s.o.Metrics.Request.WithLabelValues(rpc).Observe(time.Since(start).Seconds())
}

func (s *Service) checkReady() error {
	if !s.ready.Load() {
		return connect.NewError(connect.CodeUnavailable, errNotReady)
	}
	return nil
}

func invalid(msg string) error {
	return connect.NewError(connect.CodeInvalidArgument, errors.New(msg))
}

func notFound(marketID string) error {
	return connect.NewError(connect.CodeNotFound, fmt.Errorf("market %q not found", marketID))
}

// idOK reports whether s is 1..maxIDLen bytes.
func idOK(s string) bool { return s != "" && len(s) <= maxIDLen }

// requireID returns an InvalidArgument error unless idOK(s).
func requireID(field, s string) error {
	if !idOK(s) {
		return invalid(fmt.Sprintf("%s must be 1..%d characters", field, maxIDLen))
	}
	return nil
}

// requireMarket validates the id and returns the market's outcome count.
func (s *Service) requireMarket(id string) (uint32, error) {
	if err := requireID("market_id", id); err != nil {
		return 0, err
	}
	n, ok := s.runner.Outcomes(id)
	if !ok {
		return 0, notFound(id)
	}
	return n, nil
}

// Fixed client-facing messages for server-side failures; the underlying
// error (broker addresses, proto details) is logged, not returned.
var (
	errExchangeUnavailable = errors.New("exchange unavailable")
	errInternal            = errors.New("internal error")
)

// runnerError maps a Runner.Submit error to a connect error: the caller's
// own cancellation or deadline keep their codes and messages, a marshal
// failure is a bug (Internal), everything else (shutdown, WAL produce) is
// Unavailable. Internal and Unavailable carry fixed messages and the
// underlying error is logged at WARN.
func runnerError(log *slog.Logger, err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return connect.NewError(connect.CodeCanceled, err)
	case errors.Is(err, context.DeadlineExceeded):
		return connect.NewError(connect.CodeDeadlineExceeded, err)
	case errors.Is(err, ErrMarshal):
		log.Warn("submit failed", "code", connect.CodeInternal.String(), "err", err)
		return connect.NewError(connect.CodeInternal, errInternal)
	}
	log.Warn("submit failed", "code", connect.CodeUnavailable.String(), "err", err)
	return connect.NewError(connect.CodeUnavailable, errExchangeUnavailable)
}

// submit enqueues and maps runner errors to connect codes.
func (s *Service) submit(ctx context.Context, cmd *enginev1.Command) (Result, error) {
	res, err := s.runner.Submit(ctx, cmd)
	if err != nil {
		return res, runnerError(s.o.Log, err)
	}
	return res, nil
}

// validateSubmit checks the shape of o before the market is looked up. It
// mirrors the engine's own validation so an invalid order is answered with
// InvalidArgument instead of being journaled and rejected.
func validateSubmit(o *enginev1.SubmitOrder) error {
	switch {
	case o == nil:
		return errors.New("order required")
	case !idOK(o.MarketId):
		return errors.New("market_id must be 1..64 characters")
	case !idOK(o.UserId):
		return errors.New("user_id must be 1..64 characters")
	case !idOK(o.ClientOrderId):
		return errors.New("client_order_id must be 1..64 characters")
	case o.OrderId != "":
		return errors.New("order_id must be empty; the matcher assigns ids")
	case o.Price < engine.MinPrice || o.Price > engine.MaxPrice:
		return fmt.Errorf("price %d out of range %d..%d", o.Price, engine.MinPrice, engine.MaxPrice)
	case o.Qty == 0:
		return errors.New("qty must be > 0")
	case o.Side != commonv1.Side_SIDE_BUY && o.Side != commonv1.Side_SIDE_SELL:
		return errors.New("side must be BUY or SELL")
	case o.Tif == commonv1.TimeInForce_TIME_IN_FORCE_UNSPECIFIED:
		return errors.New("tif required")
	}
	return nil
}

// SubmitOrder validates the order, assigns its id (a client-supplied
// order_id is rejected), journals and applies it, and reports the engine's verdict in the
// response status (engine rejections are not connect errors).
func (s *Service) SubmitOrder(ctx context.Context, req *connect.Request[matcherv1.SubmitOrderRequest]) (*connect.Response[matcherv1.SubmitOrderResponse], error) {
	defer s.observe("SubmitOrder", time.Now())
	if err := s.checkReady(); err != nil {
		return nil, err
	}
	o := req.Msg.GetOrder()
	if err := validateSubmit(o); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	n, err := s.requireMarket(o.MarketId)
	if err != nil {
		return nil, err
	}
	if o.Outcome >= n {
		return nil, invalid(fmt.Sprintf("outcome %d out of range for a %d-outcome market", o.Outcome, n))
	}
	o.OrderId = s.o.NewID()
	res, err := s.submit(ctx, &enginev1.Command{Body: &enginev1.Command_SubmitOrder{SubmitOrder: o}})
	if err != nil {
		return nil, err
	}
	resp := &matcherv1.SubmitOrderResponse{OrderId: o.OrderId, Seq: res.Seq, EngineTimeUnixNanos: res.EngineTime}
	summarizeSubmit(res.Events, o, resp)
	if resp.RejectReason == commonv1.RejectReason_REJECT_REASON_MARKET_NOT_FOUND {
		return nil, notFound(o.MarketId)
	}
	return connect.NewResponse(resp), nil
}

// summarizeSubmit derives status, reject_reason and fills from the
// command's events. Status: REJECTED; FILLED when fills cover qty; CANCELED
// when an ioc/stp cancel ended it (fills, if any, are still reported);
// PARTIALLY_FILLED when some filled and the rest rests; else ACCEPTED.
func summarizeSubmit(events []*enginev1.Event, o *enginev1.SubmitOrder, resp *matcherv1.SubmitOrderResponse) {
	var filled uint64
	canceled := false
	for _, ev := range events {
		switch b := ev.Body.(type) {
		case *enginev1.Event_OrderRejected:
			resp.Status = commonv1.OrderStatus_ORDER_STATUS_REJECTED
			resp.RejectReason = b.OrderRejected.Reason
			return
		case *enginev1.Event_Fill:
			resp.Fills = append(resp.Fills, b.Fill)
			filled += b.Fill.Qty
		case *enginev1.Event_OrderCanceled:
			if b.OrderCanceled.OrderId == o.OrderId {
				canceled = true
			}
		}
	}
	switch {
	case filled >= o.Qty:
		resp.Status = commonv1.OrderStatus_ORDER_STATUS_FILLED
	case canceled:
		resp.Status = commonv1.OrderStatus_ORDER_STATUS_CANCELED
	case filled > 0:
		resp.Status = commonv1.OrderStatus_ORDER_STATUS_PARTIALLY_FILLED
	default:
		resp.Status = commonv1.OrderStatus_ORDER_STATUS_ACCEPTED
	}
}

// CancelOrder journals a cancel; canceled is false when the order was not
// resting or belongs to another user. user_id is required so a caller can
// only cancel its own orders.
func (s *Service) CancelOrder(ctx context.Context, req *connect.Request[matcherv1.CancelOrderRequest]) (*connect.Response[matcherv1.CancelOrderResponse], error) {
	defer s.observe("CancelOrder", time.Now())
	if err := s.checkReady(); err != nil {
		return nil, err
	}
	c := req.Msg.GetCancel()
	if c == nil {
		return nil, invalid("cancel required")
	}
	if err := requireID("cancel.order_id", c.OrderId); err != nil {
		return nil, err
	}
	if ids.Valid(c.OrderId) {
		// ULIDs are case-insensitive; ids from NewID are uppercase.
		c.OrderId = strings.ToUpper(c.OrderId)
	}
	if err := requireID("cancel.user_id", c.UserId); err != nil {
		return nil, err
	}
	if _, err := s.requireMarket(c.MarketId); err != nil {
		return nil, err
	}
	res, err := s.submit(ctx, &enginev1.Command{Body: &enginev1.Command_CancelOrder{CancelOrder: c}})
	if err != nil {
		return nil, err
	}
	resp := &matcherv1.CancelOrderResponse{Seq: res.Seq}
	for _, ev := range res.Events {
		if oc := ev.GetOrderCanceled(); oc != nil && oc.OrderId == c.OrderId {
			resp.Canceled, resp.CanceledQty = true, oc.CanceledQty
		}
	}
	return connect.NewResponse(resp), nil
}

// GetBook snapshots a market's non-empty levels, market_seq and status.
func (s *Service) GetBook(_ context.Context, req *connect.Request[matcherv1.GetBookRequest]) (*connect.Response[matcherv1.GetBookResponse], error) {
	defer s.observe("GetBook", time.Now())
	if err := s.checkReady(); err != nil {
		return nil, err
	}
	id := req.Msg.GetMarketId()
	if id == "" {
		return nil, invalid("market_id required")
	}
	levels, seq, status, ok := s.runner.Book(id)
	if !ok {
		return nil, notFound(id)
	}
	return connect.NewResponse(&matcherv1.GetBookResponse{MarketId: id, MarketSeq: seq, Status: status, Levels: levels}), nil
}

// validateCreate checks a CreateMarket's id, title and labels.
func validateCreate(cm *enginev1.CreateMarket) error {
	if err := requireID("create_market.market_id", cm.GetMarketId()); err != nil {
		return err
	}
	if len(cm.GetTitle()) > maxTitleLen {
		return invalid(fmt.Sprintf("create_market.title must be at most %d characters", maxTitleLen))
	}
	// The engine refuses (journals as a no-op) a market with fewer than 2
	// or more than engine.MaxOutcomes labels; answer InvalidArgument
	// instead of journaling a command that creates nothing.
	labels := cm.GetOutcomeLabels()
	if len(labels) < 2 || len(labels) > engine.MaxOutcomes {
		return invalid(fmt.Sprintf("create_market needs 2..%d outcome_labels", engine.MaxOutcomes))
	}
	for i, l := range labels {
		if l == "" || len(l) > maxLabelLen {
			return invalid(fmt.Sprintf("outcome_labels[%d] must be 1..%d characters", i, maxLabelLen))
		}
	}
	return nil
}

// AdminCommand journals a market lifecycle command. Lock, resolve and void
// require an existing market (resolve also an in-range winning_outcome,
// void a reason of at most maxReasonLen); create requires a new id, a title
// of at most maxTitleLen and 2..engine.MaxOutcomes labels of 1..maxLabelLen
// (an existing id is AlreadyExists and is not journaled). M1
// has no authentication on this RPC: the matcher is reachable only on the
// cluster network and the gateway does not expose it (auth lands in M4).
func (s *Service) AdminCommand(ctx context.Context, req *connect.Request[matcherv1.AdminCommandRequest]) (*connect.Response[matcherv1.AdminCommandResponse], error) {
	defer s.observe("AdminCommand", time.Now())
	if err := s.checkReady(); err != nil {
		return nil, err
	}
	var cmd *enginev1.Command
	switch b := req.Msg.GetBody().(type) {
	case *matcherv1.AdminCommandRequest_CreateMarket:
		cm := b.CreateMarket
		if err := validateCreate(cm); err != nil {
			return nil, err
		}
		if s.runner.HasMarket(cm.GetMarketId()) {
			return nil, connect.NewError(connect.CodeAlreadyExists, fmt.Errorf("market %q already exists", cm.GetMarketId()))
		}
		cmd = &enginev1.Command{Body: &enginev1.Command_CreateMarket{CreateMarket: cm}}
	case *matcherv1.AdminCommandRequest_LockMarket:
		if _, err := s.requireMarket(b.LockMarket.GetMarketId()); err != nil {
			return nil, err
		}
		cmd = &enginev1.Command{Body: &enginev1.Command_LockMarket{LockMarket: b.LockMarket}}
	case *matcherv1.AdminCommandRequest_ResolveMarket:
		n, err := s.requireMarket(b.ResolveMarket.GetMarketId())
		if err != nil {
			return nil, err
		}
		if w := b.ResolveMarket.GetWinningOutcome(); w >= n {
			return nil, invalid(fmt.Sprintf("winning_outcome %d out of range for a %d-outcome market", w, n))
		}
		cmd = &enginev1.Command{Body: &enginev1.Command_ResolveMarket{ResolveMarket: b.ResolveMarket}}
	case *matcherv1.AdminCommandRequest_VoidMarket:
		if _, err := s.requireMarket(b.VoidMarket.GetMarketId()); err != nil {
			return nil, err
		}
		if len(b.VoidMarket.GetReason()) > maxReasonLen {
			return nil, invalid(fmt.Sprintf("void_market.reason must be at most %d characters", maxReasonLen))
		}
		cmd = &enginev1.Command{Body: &enginev1.Command_VoidMarket{VoidMarket: b.VoidMarket}}
	default:
		return nil, invalid("admin command body required")
	}
	res, err := s.submit(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&matcherv1.AdminCommandResponse{Seq: res.Seq}), nil
}
