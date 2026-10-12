package gateway

import (
	"errors"
	"strings"
	"time"

	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
	matcherv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/matcher/v1"
)

// OrderRequest is the POST /v1/orders body. It uses signed ints so negative
// input is a validation error, not a decode error.
type OrderRequest struct {
	MarketID      string `json:"market_id"`
	Outcome       int64  `json:"outcome"`
	Side          string `json:"side"`
	Price         int64  `json:"price"`
	Qty           int64  `json:"qty"`
	TIF           string `json:"tif"`
	PostOnly      bool   `json:"post_only"`
	ClientOrderID string `json:"client_order_id"`
}

var (
	sides = map[string]commonv1.Side{"BUY": commonv1.Side_SIDE_BUY, "SELL": commonv1.Side_SIDE_SELL}
	tifs  = map[string]commonv1.TimeInForce{
		"GTC": commonv1.TimeInForce_TIME_IN_FORCE_GTC, "IOC": commonv1.TimeInForce_TIME_IN_FORCE_IOC, "FOK": commonv1.TimeInForce_TIME_IN_FORCE_FOK,
	}
)

// Validate checks the request against the contract bounds and returns the
// first violation as a client-facing message.
func (o OrderRequest) Validate() error {
	switch {
	case o.MarketID == "":
		return errors.New("market_id is required")
	case o.Outcome < 0 || o.Outcome > 1:
		return errors.New("outcome must be 0 or 1")
	case sides[o.Side] == commonv1.Side_SIDE_UNSPECIFIED:
		return errors.New("side must be BUY or SELL")
	case o.Price < 1 || o.Price > 99:
		return errors.New("price must be 1..99")
	case o.Qty < 1:
		return errors.New("qty must be >= 1")
	case tifs[o.TIF] == commonv1.TimeInForce_TIME_IN_FORCE_UNSPECIFIED:
		return errors.New("tif must be GTC, IOC or FOK")
	case o.ClientOrderID == "" || len(o.ClientOrderID) > 64:
		return errors.New("client_order_id must be 1..64 characters")
	}
	return nil
}

// Proto converts a validated request into the engine command for userID.
func (o OrderRequest) Proto(userID string) *enginev1.SubmitOrder {
	return &enginev1.SubmitOrder{
		MarketId: o.MarketID, UserId: userID, ClientOrderId: o.ClientOrderID, Outcome: uint32(o.Outcome),
		Side: sides[o.Side], Price: uint32(o.Price), Qty: uint64(o.Qty), Tif: tifs[o.TIF], PostOnly: o.PostOnly,
	}
}

// LegJSON is one participant's side of a fill.
type LegJSON struct {
	OrderID string `json:"order_id"`
	UserID  string `json:"user_id"`
	Outcome uint32 `json:"outcome"`
	Side    string `json:"side"`
	Price   uint32 `json:"price"`
	IsTaker bool   `json:"is_taker"`
}

// FillJSON is one fill (TRANSFER or MINT kind) in an order response.
type FillJSON struct {
	FillID string    `json:"fill_id"`
	Kind   string    `json:"kind"`
	Qty    uint64    `json:"qty"`
	Legs   []LegJSON `json:"legs"`
}

// OrderResponse is the 200 body of POST /v1/orders.
type OrderResponse struct {
	OrderID      string     `json:"order_id"`
	Seq          uint64     `json:"seq"`
	Status       string     `json:"status"`
	RejectReason string     `json:"reject_reason"`
	Fills        []FillJSON `json:"fills"`
	EngineTime   string     `json:"engine_time"`
}

// CancelResponse is the 200 body of DELETE /v1/orders/{order_id}.
type CancelResponse struct {
	Seq         uint64 `json:"seq"`
	Canceled    bool   `json:"canceled"`
	CanceledQty uint64 `json:"canceled_qty"`
}

// LevelJSON is one aggregated price level of a book.
type LevelJSON struct {
	Outcome uint32 `json:"outcome"`
	Side    string `json:"side"`
	Price   uint32 `json:"price"`
	Qty     uint64 `json:"qty"`
}

// BookResponse is the 200 body of GET /v1/markets/{market_id}/book.
type BookResponse struct {
	MarketID  string      `json:"market_id"`
	MarketSeq uint64      `json:"market_seq"`
	Status    string      `json:"status"`
	Levels    []LevelJSON `json:"levels"`
}

// MarketJSON is one entry of GET /v1/markets.
type MarketJSON struct {
	ID       string   `json:"id"`
	Title    string   `json:"title"`
	Status   string   `json:"status"`
	Outcomes []string `json:"outcomes"`
}

func sideName(s commonv1.Side) string { return strings.TrimPrefix(s.String(), "SIDE_") }

func marketStatus(s commonv1.MarketStatus) string {
	return strings.ToLower(strings.TrimPrefix(s.String(), "MARKET_STATUS_"))
}

func toOrderResponse(m *matcherv1.SubmitOrderResponse) OrderResponse {
	out := OrderResponse{
		OrderID: m.GetOrderId(), Seq: m.GetSeq(), Status: strings.TrimPrefix(m.GetStatus().String(), "ORDER_STATUS_"),
		Fills: make([]FillJSON, 0, len(m.GetFills())), EngineTime: time.Unix(0, m.GetEngineTimeUnixNanos()).UTC().Format(time.RFC3339Nano),
	}
	if r := m.GetRejectReason(); r != commonv1.RejectReason_REJECT_REASON_UNSPECIFIED {
		out.RejectReason = strings.TrimPrefix(r.String(), "REJECT_REASON_")
	}
	for _, f := range m.GetFills() {
		fj := FillJSON{FillID: f.GetFillId(), Kind: strings.TrimPrefix(f.GetKind().String(), "FILL_KIND_"), Qty: f.GetQty(), Legs: []LegJSON{}}
		for _, l := range f.GetLegs() {
			fj.Legs = append(fj.Legs, LegJSON{l.GetOrderId(), l.GetUserId(), l.GetOutcome(), sideName(l.GetSide()), l.GetPrice(), l.GetIsTaker()})
		}
		out.Fills = append(out.Fills, fj)
	}
	return out
}

func toBookResponse(m *matcherv1.GetBookResponse) BookResponse {
	out := BookResponse{MarketID: m.GetMarketId(), MarketSeq: m.GetMarketSeq(), Status: marketStatus(m.GetStatus()), Levels: []LevelJSON{}}
	for _, l := range m.GetLevels() {
		out.Levels = append(out.Levels, LevelJSON{l.GetOutcome(), sideName(l.GetSide()), l.GetPrice(), l.GetQty()})
	}
	return out
}
