package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
)

// Deliberate deviations from the contracts' "Ledger" rules (do not re-litigate):
//   - orders.remaining: OrderAccepted sets remaining = resting_qty, which is
//     already net of the taker's immediate fills, so a Fill decrements
//     remaining only for maker legs (is_taker == false); OrderCanceled
//     subtracts canceled_qty clamped at 0 (ruling X10).
//   - TRANSFER moves cost basis with the shares (buyer +p*q, seller -p*q) so
//     sum(cost_basis) tracks escrow and void refunds net to zero; a seller
//     who sold above cost can therefore be debited on void.
//   - Order status uses partially_filled in addition to accepted/filled.
//   - Malformed events (bad fill kind, MINT prices not summing to 100,
//     TRANSFER without one buyer and one seller) are logged, counted in
//     pandora_ledger_malformed_events_total, marked applied and skipped, so
//     one bad record cannot wedge the partition.
//   - "Escrow must be 0" after settle is not asserted at runtime; invariant
//     settled_escrow_zero (invariants.go) checks it.

var errMalformed = errors.New("malformed event")

var malformedEvents = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "pandora", Subsystem: "ledger", Name: "malformed_events_total",
	Help: "Events skipped because they violate the ledger rules.",
})

// Applier applies batches of engine events to a Repo.
type Applier struct {
	repo Repo
	log  *slog.Logger
	now  func() time.Time
}

// NewApplier returns an Applier writing through repo.
func NewApplier(repo Repo, log *slog.Logger) *Applier {
	return &Applier{repo: repo, log: log, now: time.Now}
}

func keyOf(ev *enginev1.Event) EventKey {
	return EventKey{Partition: int32(ev.GetPartition()), Seq: int64(ev.GetSeq()), Idx: int32(ev.GetIdx())}
}

// relevant reports whether the ledger acts on (and therefore dedups) this
// event. A nil event (undecodable record) and LevelUpdate are not relevant.
func relevant(ev *enginev1.Event) bool {
	_, lvl := ev.GetBody().(*enginev1.Event_LevelUpdate)
	return ev.GetBody() != nil && !lvl
}

type batchState struct {
	seen    map[EventKey]bool
	users   map[string]struct{}
	escrows map[string]struct{}
	txns    []JournalTxn
	fills   []FillRow
	pos     map[PositionKey]*PositionDelta
	wallets map[string]int64
}

func newBatchState() *batchState {
	return &batchState{
		seen: map[EventKey]bool{}, users: map[string]struct{}{}, escrows: map[string]struct{}{},
		pos: map[PositionKey]*PositionDelta{}, wallets: map[string]int64{},
	}
}

func (b *batchState) addPos(user, market string, outcome uint32, qty, cost int64) {
	k := PositionKey{UserID: user, MarketID: market, Outcome: int32(outcome)}
	d := b.pos[k]
	if d == nil {
		d = &PositionDelta{PositionKey: k}
		b.pos[k] = d
	}
	d.Qty += qty
	d.CostCents += cost
}

// ApplyBatch applies all records in one transaction: claim keys in event_log,
// per-event rules for the claimed events only, aggregated flush,
// consumer_offsets. Every envelope, including one with a nil Event, advances
// its partition's stored offset.
//
// Dedup is claim-first: the event_log insert is the first write, so two
// transactions applying the same batch (a redelivery racing the original,
// two group members after a rebalance) serialize on event_log's unique index
// and the loser applies nothing. Skipped malformed and ignored events stay
// claimed, so they are never retried.
func (a *Applier) ApplyBatch(ctx context.Context, batch []Envelope) error {
	if len(batch) == 0 {
		return nil
	}
	return a.repo.InTx(ctx, func(ctx context.Context, tx Tx) error {
		keys := make([]EventKey, 0, len(batch))
		for _, env := range batch {
			if relevant(env.Event) {
				keys = append(keys, keyOf(env.Event))
			}
		}
		claimed := map[EventKey]bool{}
		if len(keys) > 0 {
			var err error
			if claimed, err = tx.ClaimKeys(ctx, keys); err != nil {
				return fmt.Errorf("claim keys: %w", err)
			}
		}
		b := newBatchState()
		offsets := map[int32]int64{}
		for _, env := range batch {
			offsets[env.KafkaPartition] = max(offsets[env.KafkaPartition], env.Offset+1)
			k := keyOf(env.Event)
			if !relevant(env.Event) || !claimed[k] || b.seen[k] {
				continue
			}
			b.seen[k] = true
			if err := a.applyEvent(ctx, tx, b, k, env.Event); err != nil {
				if errors.Is(err, errMalformed) {
					malformedEvents.Inc()
					a.log.Error("skipping malformed event", "key", k, "err", err)
					continue
				}
				return fmt.Errorf("apply %+v: %w", k, err)
			}
		}
		if err := a.flush(ctx, tx, b); err != nil {
			return err
		}
		for _, p := range slices.Sorted(maps.Keys(offsets)) {
			if err := tx.UpsertOffset(ctx, ConsumerName, p, offsets[p]); err != nil {
				return fmt.Errorf("upsert offset: %w", err)
			}
		}
		return nil
	})
}

func (a *Applier) applyEvent(ctx context.Context, tx Tx, b *batchState, k EventKey, ev *enginev1.Event) error {
	switch body := ev.GetBody().(type) {
	case *enginev1.Event_MarketCreated:
		m := body.MarketCreated.GetMarket()
		row := MarketRow{ID: m.GetMarketId(), Title: m.GetTitle(), Outcomes: m.GetOutcomeLabels()}
		if n := m.GetLockAtUnixNanos(); n > 0 {
			t := time.Unix(0, n).UTC()
			row.LockAt = &t
		}
		return tx.UpsertMarket(ctx, row)
	case *enginev1.Event_OrderAccepted:
		o := body.OrderAccepted.GetOrder()
		resting := body.OrderAccepted.GetRestingQty()
		status := "accepted"
		switch {
		case resting == 0:
			status = "filled" // an IOC remainder cancel that follows flips it to canceled
		case resting < o.GetQty():
			status = "partially_filled"
		}
		b.users[o.GetUserId()] = struct{}{}
		return tx.InsertOrder(ctx, orderRow(o, int64(resting), status, k.Seq))
	case *enginev1.Event_OrderRejected:
		o := body.OrderRejected.GetOrder()
		b.users[o.GetUserId()] = struct{}{}
		return tx.InsertOrder(ctx, orderRow(o, 0, "rejected", k.Seq))
	case *enginev1.Event_OrderCanceled:
		return tx.CancelOrder(ctx, body.OrderCanceled.GetOrderId(), int64(body.OrderCanceled.GetCanceledQty()))
	case *enginev1.Event_Fill:
		return a.applyFill(ctx, tx, b, k, ev, body.Fill)
	case *enginev1.Event_MarketLocked:
		return tx.SetMarketStatus(ctx, ev.GetMarketId(), "locked", nil)
	case *enginev1.Event_MarketResolved:
		if done, err := a.alreadySettled(ctx, tx, k, ev.GetMarketId()); done || err != nil {
			return err
		}
		return a.settle(ctx, tx, b, k, ev.GetMarketId(), int32(body.MarketResolved.GetWinningOutcome()))
	case *enginev1.Event_MarketVoided:
		if done, err := a.alreadySettled(ctx, tx, k, ev.GetMarketId()); done || err != nil {
			return err
		}
		return a.void(ctx, tx, b, k, ev.GetMarketId())
	}
	return nil
}

type legJSON struct {
	OrderID string `json:"order_id"`
	UserID  string `json:"user_id"`
	Outcome uint32 `json:"outcome"`
	Side    string `json:"side"`
	Price   uint32 `json:"price"`
	IsTaker bool   `json:"is_taker"`
}

func (a *Applier) applyFill(ctx context.Context, tx Tx, b *batchState, k EventKey, ev *enginev1.Event, f *commonv1.Fill) error {
	q := int64(f.GetQty())
	market := f.GetMarketId()
	if market == "" {
		market = ev.GetMarketId()
	}
	legs := f.GetLegs()
	var entries []Entry
	var kind string
	switch f.GetKind() {
	case commonv1.FillKind_FILL_KIND_TRANSFER:
		var buyer, seller *commonv1.FillLeg
		for _, l := range legs {
			if l.GetSide() == commonv1.Side_SIDE_BUY {
				buyer = l
			} else {
				seller = l
			}
		}
		if len(legs) != 2 || buyer == nil || seller == nil {
			return fmt.Errorf("%w: transfer needs one buyer and one seller", errMalformed)
		}
		amt := int64(buyer.GetPrice()) * q
		entries = []Entry{{CashAccount(buyer.GetUserId()), -amt}, {CashAccount(seller.GetUserId()), amt}}
		b.wallets[buyer.GetUserId()] -= amt
		b.wallets[seller.GetUserId()] += amt
		b.addPos(buyer.GetUserId(), market, buyer.GetOutcome(), q, amt)
		// Seller cost basis drops by proceeds, so sum(cost_basis) == escrow and void refunds net to zero.
		b.addPos(seller.GetUserId(), market, seller.GetOutcome(), -q, -amt)
		kind = "fill"
	case commonv1.FillKind_FILL_KIND_MINT:
		// Sum in int64 and bound each price: uint32 prices can wrap to 100.
		var sum int64
		for _, l := range legs {
			if p := l.GetPrice(); p < 1 || p > 99 {
				return fmt.Errorf("%w: mint leg price %d outside 1..99", errMalformed, p)
			}
			sum += int64(l.GetPrice())
		}
		if len(legs) < 2 || sum != 100 {
			return fmt.Errorf("%w: mint leg prices sum to %d", errMalformed, sum)
		}
		for _, l := range legs {
			amt := int64(l.GetPrice()) * q
			entries = append(entries, Entry{CashAccount(l.GetUserId()), -amt})
			b.wallets[l.GetUserId()] -= amt
			b.addPos(l.GetUserId(), market, l.GetOutcome(), q, amt)
		}
		entries = append(entries, Entry{EscrowAccount(market), 100 * q})
		b.escrows[market] = struct{}{}
		kind = "mint"
	default:
		return fmt.Errorf("%w: fill kind %v", errMalformed, f.GetKind())
	}
	js := make([]legJSON, len(legs))
	for i, l := range legs {
		b.users[l.GetUserId()] = struct{}{}
		js[i] = legJSON{l.GetOrderId(), l.GetUserId(), l.GetOutcome(), sideName(l.GetSide()), l.GetPrice(), l.GetIsTaker()}
		// Taker remaining was already net of these fills in OrderAccepted.resting_qty.
		if !l.GetIsTaker() {
			if err := tx.FillOrder(ctx, l.GetOrderId(), q); err != nil {
				return err
			}
		}
	}
	raw, err := json.Marshal(js)
	if err != nil {
		return err
	}
	b.fills = append(b.fills, FillRow{
		ID: f.GetFillId(), MarketID: market, Kind: strings.ToLower(strings.TrimPrefix(f.GetKind().String(), "FILL_KIND_")),
		Qty: q, Key: k, Legs: raw, EngineTime: time.Unix(0, ev.GetEngineTimeUnixNanos()).UTC(),
	})
	b.txns = append(b.txns, JournalTxn{Key: k, Kind: kind, Ref: f.GetFillId(), Entries: entries})
	return nil
}

func (a *Applier) flushPositions(ctx context.Context, tx Tx, b *batchState) error {
	if len(b.pos) == 0 {
		return nil
	}
	ds := make([]PositionDelta, 0, len(b.pos))
	for _, d := range b.pos {
		ds = append(ds, *d)
	}
	slices.SortFunc(ds, func(x, y PositionDelta) int {
		return strings.Compare(fmt.Sprintf("%s/%s/%d", x.UserID, x.MarketID, x.Outcome), fmt.Sprintf("%s/%s/%d", y.UserID, y.MarketID, y.Outcome))
	})
	b.pos = map[PositionKey]*PositionDelta{}
	return tx.AddPositions(ctx, ds)
}

// alreadySettled reports whether market is already resolved or voided. A
// second MarketResolved/MarketVoided is then a logged no-op: no txn, no
// settlements row, resolution_outcome untouched; its key stays claimed.
func (a *Applier) alreadySettled(ctx context.Context, tx Tx, k EventKey, market string) (bool, error) {
	status, ok, err := tx.MarketStatus(ctx, market)
	if err != nil {
		return false, fmt.Errorf("market status: %w", err)
	}
	if ok && (status == "resolved" || status == "voided") {
		a.log.Warn("ignoring settlement of an already settled market", "key", k, "market", market, "status", status)
		return true, nil
	}
	return false, nil
}

func (a *Applier) settle(ctx context.Context, tx Tx, b *batchState, k EventKey, market string, winning int32) error {
	start := a.now()
	if err := a.flushPositions(ctx, tx, b); err != nil {
		return err
	}
	pos, err := tx.OpenPositions(ctx, market)
	if err != nil {
		return err
	}
	var entries []Entry
	var total, count int64
	for _, p := range pos {
		if p.Outcome != winning || p.Qty <= 0 {
			continue
		}
		amt := 100 * p.Qty
		entries = append(entries, Entry{CashAccount(p.UserID), amt})
		b.wallets[p.UserID] += amt
		total += amt
		count++
	}
	if total > 0 {
		entries = append(entries, Entry{EscrowAccount(market), -total})
	}
	b.escrows[market] = struct{}{}
	b.txns = append(b.txns, JournalTxn{Key: k, Kind: "settle", Ref: market, Entries: entries, Settlement: &Settlement{
		MarketID: market, WinningOutcome: winning, PositionCount: count, PayoutCents: total,
		DurationMs: a.now().Sub(start).Milliseconds(),
	}})
	if err := tx.SettlePositions(ctx, market); err != nil {
		return err
	}
	return tx.SetMarketStatus(ctx, market, "resolved", &winning)
}

func (a *Applier) void(ctx context.Context, tx Tx, b *batchState, k EventKey, market string) error {
	if err := a.flushPositions(ctx, tx, b); err != nil {
		return err
	}
	pos, err := tx.OpenPositions(ctx, market)
	if err != nil {
		return err
	}
	var entries []Entry
	var total int64
	for _, p := range pos {
		if p.CostBasisCents == 0 {
			continue
		}
		entries = append(entries, Entry{CashAccount(p.UserID), p.CostBasisCents})
		b.wallets[p.UserID] += p.CostBasisCents
		total += p.CostBasisCents
	}
	if total != 0 {
		entries = append(entries, Entry{EscrowAccount(market), -total})
	}
	b.escrows[market] = struct{}{}
	b.txns = append(b.txns, JournalTxn{Key: k, Kind: "void", Ref: market, Entries: entries})
	if err := tx.SettlePositions(ctx, market); err != nil {
		return err
	}
	return tx.SetMarketStatus(ctx, market, "voided", nil)
}

func (a *Applier) flush(ctx context.Context, tx Tx, b *batchState) error {
	if len(b.users) > 0 {
		if err := tx.EnsureUsers(ctx, slices.Sorted(maps.Keys(b.users))); err != nil {
			return fmt.Errorf("ensure users: %w", err)
		}
	}
	if len(b.escrows) > 0 {
		if err := tx.EnsureEscrow(ctx, slices.Sorted(maps.Keys(b.escrows))); err != nil {
			return fmt.Errorf("ensure escrow: %w", err)
		}
	}
	if len(b.txns) > 0 {
		if err := tx.InsertLedgerTxns(ctx, b.txns); err != nil {
			return fmt.Errorf("ledger txns: %w", err)
		}
	}
	if len(b.fills) > 0 {
		if err := tx.InsertFills(ctx, b.fills); err != nil {
			return fmt.Errorf("fills: %w", err)
		}
	}
	if err := a.flushPositions(ctx, tx, b); err != nil {
		return fmt.Errorf("positions: %w", err)
	}
	var ds []WalletDelta
	for _, u := range slices.Sorted(maps.Keys(b.wallets)) {
		if d := b.wallets[u]; d != 0 {
			ds = append(ds, WalletDelta{UserID: u, DeltaCents: d})
		}
	}
	if len(ds) > 0 {
		if err := tx.ApplyWalletDeltas(ctx, ds); err != nil {
			return fmt.Errorf("wallets: %w", err)
		}
	}
	return nil
}

func sideName(s commonv1.Side) string { return strings.TrimPrefix(s.String(), "SIDE_") }

func orderRow(o *enginev1.SubmitOrder, remaining int64, status string, seq int64) OrderRow {
	return OrderRow{
		ID: o.GetOrderId(), UserID: o.GetUserId(), MarketID: o.GetMarketId(), ClientOrderID: o.GetClientOrderId(),
		Outcome: int32(o.GetOutcome()), Side: sideName(o.GetSide()), Price: int32(o.GetPrice()),
		Qty: int64(o.GetQty()), Remaining: remaining, Status: status,
		TIF: strings.TrimPrefix(o.GetTif().String(), "TIME_IN_FORCE_"), Seq: seq,
	}
}
