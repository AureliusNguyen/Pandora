package engine

import (
	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
)

// planner is defined fully by the matching task.
type planner struct{}

// execute (interim): nothing ever matches; GTC rests, IOC cancels, FOK fails.
func (e *Engine) execute(m *market, o *enginev1.SubmitOrder) {
	switch o.Tif {
	case commonv1.TimeInForce_TIME_IN_FORCE_FOK:
		e.reject(o, commonv1.RejectReason_REJECT_REASON_FOK_UNFILLABLE)
	case commonv1.TimeInForce_TIME_IN_FORCE_IOC:
		e.accept(o, 0)
		e.em.push(&enginev1.Event{Body: &enginev1.Event_OrderCanceled{OrderCanceled: &enginev1.OrderCanceled{OrderId: o.OrderId, UserId: o.UserId, CanceledQty: o.Qty, Reason: "ioc"}}}, true)
	default:
		e.accept(o, o.Qty)
		e.rest(m, o, o.Qty)
	}
}
