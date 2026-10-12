package matcher

import (
	"context"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"

	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
	matcherv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/matcher/v1"
	"github.com/aureliusnguyen/pandora/backend/gen/go/pandora/matcher/v1/matcherv1connect"
	"github.com/aureliusnguyen/pandora/backend/internal/engine"
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

func (s *Service) requireMarket(id string) error {
	if id == "" {
		return invalid("market_id required")
	}
	if !s.runner.HasMarket(id) {
		return notFound(id)
	}
	return nil
}

// runnerError maps a Runner.Submit error to a connect error: the caller's
// own cancellation or deadline keep their codes, a marshal failure is a
// bug (Internal), everything else (shutdown, WAL produce) is Unavailable.
func runnerError(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return connect.NewError(connect.CodeCanceled, err)
	case errors.Is(err, context.DeadlineExceeded):
		return connect.NewError(connect.CodeDeadlineExceeded, err)
	case errors.Is(err, ErrMarshal):
		return connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewError(connect.CodeUnavailable, err)
}

// submit enqueues and maps runner errors to connect codes.
func (s *Service) submit(ctx context.Context, cmd *enginev1.Command) (Result, error) {
	res, err := s.runner.Submit(ctx, cmd)
	if err != nil {
		return res, runnerError(err)
	}
	return res, nil
}

func validateSubmit(o *enginev1.SubmitOrder) error {
	switch {
	case o == nil:
		return errors.New("order required")
	case o.MarketId == "":
		return errors.New("market_id required")
	case o.UserId == "":
		return errors.New("user_id required")
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

// SubmitOrder validates the order, assigns an id when the request has
// none, journals and applies it, and reports the engine's verdict in the
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
	if err := s.requireMarket(o.MarketId); err != nil {
		return nil, err
	}
	if o.OrderId == "" {
		o.OrderId = s.o.NewID()
	}
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
	switch {
	case c == nil || c.OrderId == "":
		return nil, invalid("cancel.order_id required")
	case c.UserId == "":
		return nil, invalid("cancel.user_id required")
	}
	if err := s.requireMarket(c.MarketId); err != nil {
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

// AdminCommand journals a market lifecycle command. Lock, resolve and void
// require an existing market; create requires an id and two or more labels.
func (s *Service) AdminCommand(ctx context.Context, req *connect.Request[matcherv1.AdminCommandRequest]) (*connect.Response[matcherv1.AdminCommandResponse], error) {
	defer s.observe("AdminCommand", time.Now())
	if err := s.checkReady(); err != nil {
		return nil, err
	}
	var cmd *enginev1.Command
	switch b := req.Msg.GetBody().(type) {
	case *matcherv1.AdminCommandRequest_CreateMarket:
		cm := b.CreateMarket
		if cm.GetMarketId() == "" || len(cm.GetOutcomeLabels()) < 2 {
			return nil, invalid("create_market needs market_id and at least two outcome_labels")
		}
		cmd = &enginev1.Command{Body: &enginev1.Command_CreateMarket{CreateMarket: cm}}
	case *matcherv1.AdminCommandRequest_LockMarket:
		if err := s.requireMarket(b.LockMarket.GetMarketId()); err != nil {
			return nil, err
		}
		cmd = &enginev1.Command{Body: &enginev1.Command_LockMarket{LockMarket: b.LockMarket}}
	case *matcherv1.AdminCommandRequest_ResolveMarket:
		if err := s.requireMarket(b.ResolveMarket.GetMarketId()); err != nil {
			return nil, err
		}
		cmd = &enginev1.Command{Body: &enginev1.Command_ResolveMarket{ResolveMarket: b.ResolveMarket}}
	case *matcherv1.AdminCommandRequest_VoidMarket:
		if err := s.requireMarket(b.VoidMarket.GetMarketId()); err != nil {
			return nil, err
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
