package ledger

import (
	"context"
	"fmt"
	"sort"
)

type fakeMarket struct {
	Status   string
	Winning  *int32
	Outcomes []string
}

type fakePos struct {
	Qty, Cost int64
	Settled   bool
}

type fakeTxn struct {
	ID int64
	JournalTxn
}

// fakeRepo is an in-memory Tx. It emulates the FKs, unique keys, ON CONFLICT
// clauses and the balanced-txn trigger that matter to the applier, so a test
// cannot pass here while the same batch would fail on Postgres.
type fakeRepo struct {
	applied     map[EventKey]bool
	users       map[string]bool
	accounts    map[string]bool
	markets     map[string]*fakeMarket
	orders      map[string]*OrderRow
	coids       map[string]string // user/coid -> order id
	fills       map[EventKey]FillRow
	txns        []fakeTxn
	txnKeys     map[EventKey]bool
	positions   map[PositionKey]*fakePos
	wallets     map[string]int64
	offsets     map[string]int64
	settlements []Settlement
	calls       []string // Tx method names in call order
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		applied: map[EventKey]bool{}, users: map[string]bool{}, accounts: map[string]bool{},
		markets: map[string]*fakeMarket{}, orders: map[string]*OrderRow{}, coids: map[string]string{},
		fills: map[EventKey]FillRow{}, txnKeys: map[EventKey]bool{}, positions: map[PositionKey]*fakePos{},
		wallets: map[string]int64{}, offsets: map[string]int64{},
	}
}

func (f *fakeRepo) InTx(ctx context.Context, fn func(context.Context, Tx) error) error {
	f.calls = append(f.calls, "InTx")
	return fn(ctx, f)
}

// ClaimKeys mirrors INSERT INTO event_log ... ON CONFLICT DO NOTHING
// RETURNING: only keys not yet present (including earlier in the same call)
// come back. Like the rest of the fake, it does not roll back on error.
func (f *fakeRepo) ClaimKeys(_ context.Context, keys []EventKey) (map[EventKey]bool, error) {
	f.calls = append(f.calls, "ClaimKeys")
	out := map[EventKey]bool{}
	for _, k := range keys {
		if !f.applied[k] {
			f.applied[k] = true
			out[k] = true
		}
	}
	return out, nil
}

// MarketStatus mirrors SELECT status FROM markets WHERE id = $1.
func (f *fakeRepo) MarketStatus(_ context.Context, id string) (string, bool, error) {
	f.calls = append(f.calls, "MarketStatus")
	m, ok := f.markets[id]
	if !ok {
		return "", false, nil
	}
	return m.Status, true, nil
}

func (f *fakeRepo) EnsureUsers(_ context.Context, ids []string) error {
	f.calls = append(f.calls, "EnsureUsers")
	for _, id := range ids {
		if !f.users[id] {
			f.users[id] = true
			f.wallets[id] = 0
			f.accounts[CashAccount(id)] = true
		}
	}
	return nil
}

func (f *fakeRepo) EnsureEscrow(_ context.Context, ids []string) error {
	f.calls = append(f.calls, "EnsureEscrow")
	for _, id := range ids {
		f.accounts[EscrowAccount(id)] = true
	}
	return nil
}

// UpsertMarket mirrors ON CONFLICT (id) DO UPDATE: a re-upsert keeps status.
func (f *fakeRepo) UpsertMarket(_ context.Context, m MarketRow) error {
	f.calls = append(f.calls, "UpsertMarket")
	if old, ok := f.markets[m.ID]; ok {
		old.Outcomes = m.Outcomes
	} else {
		f.markets[m.ID] = &fakeMarket{Status: "open", Outcomes: m.Outcomes}
	}
	f.accounts[EscrowAccount(m.ID)] = true
	return nil
}

// SetMarketStatus mirrors an UPDATE: a missing market is a no-op, and only
// ResolveMarket (w != nil) writes the winning outcome.
func (f *fakeRepo) SetMarketStatus(_ context.Context, id, status string, w *int32) error {
	f.calls = append(f.calls, "SetMarketStatus")
	m, ok := f.markets[id]
	if !ok {
		return nil
	}
	m.Status = status
	if w != nil {
		m.Winning = w
	}
	return nil
}

// InsertOrder mirrors ON CONFLICT (user_id, client_order_id) DO UPDATE ...
// WHERE orders.status = 'rejected': a rejected row is replaced, any other
// existing row wins, and a bare primary-key clash is an error.
func (f *fakeRepo) InsertOrder(_ context.Context, o OrderRow) error {
	f.calls = append(f.calls, "InsertOrder")
	ck := o.UserID + "/" + o.ClientOrderID
	if oldID, ok := f.coids[ck]; ok {
		if f.orders[oldID].Status != "rejected" {
			return nil
		}
		delete(f.orders, oldID)
	} else if _, ok := f.orders[o.ID]; ok {
		return fmt.Errorf("unique: order id %s exists", o.ID)
	}
	f.coids[ck] = o.ID
	row := o
	f.orders[o.ID] = &row
	return nil
}

func (f *fakeRepo) FillOrder(_ context.Context, id string, qty int64) error {
	f.calls = append(f.calls, "FillOrder")
	if o, ok := f.orders[id]; ok {
		o.Remaining = max(o.Remaining-qty, 0)
		o.Status = "partially_filled"
		if o.Remaining == 0 {
			o.Status = "filled"
		}
	}
	return nil
}

func (f *fakeRepo) CancelOrder(_ context.Context, id string, qty int64) error {
	f.calls = append(f.calls, "CancelOrder")
	if o, ok := f.orders[id]; ok {
		o.Remaining = max(o.Remaining-qty, 0)
		if o.Remaining == 0 {
			o.Status = "canceled"
		}
	}
	return nil
}

// InsertFills mirrors ON CONFLICT DO NOTHING: existing keys are skipped.
func (f *fakeRepo) InsertFills(_ context.Context, fills []FillRow) error {
	f.calls = append(f.calls, "InsertFills")
	for _, r := range fills {
		if _, ok := f.fills[r.Key]; !ok {
			f.fills[r.Key] = r
		}
	}
	return nil
}

func (f *fakeRepo) InsertLedgerTxns(_ context.Context, txns []JournalTxn) error {
	f.calls = append(f.calls, "InsertLedgerTxns")
	for _, t := range txns {
		if f.txnKeys[t.Key] {
			continue
		}
		var sum int64
		for _, e := range t.Entries {
			if !f.accounts[e.AccountID] {
				return fmt.Errorf("fk: account %s missing", e.AccountID)
			}
			sum += e.AmountCents
		}
		if sum != 0 {
			return fmt.Errorf("ledger txn %v unbalanced: %d", t.Key, sum)
		}
		f.txnKeys[t.Key] = true
		f.txns = append(f.txns, fakeTxn{ID: int64(len(f.txns) + 1), JournalTxn: t})
		if t.Settlement != nil {
			f.settlements = append(f.settlements, *t.Settlement)
		}
	}
	return nil
}

func (f *fakeRepo) AddPositions(_ context.Context, ds []PositionDelta) error {
	f.calls = append(f.calls, "AddPositions")
	for _, d := range ds {
		p := f.positions[d.PositionKey]
		if p == nil {
			p = &fakePos{}
			f.positions[d.PositionKey] = p
		}
		p.Qty += d.Qty
		p.Cost += d.CostCents
	}
	return nil
}

func (f *fakeRepo) OpenPositions(_ context.Context, marketID string) ([]Position, error) {
	f.calls = append(f.calls, "OpenPositions")
	var out []Position
	for k, p := range f.positions {
		if k.MarketID == marketID && !p.Settled {
			out = append(out, Position{UserID: k.UserID, Outcome: k.Outcome, Qty: p.Qty, CostBasisCents: p.Cost})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UserID < out[j].UserID })
	return out, nil
}

func (f *fakeRepo) SettlePositions(_ context.Context, marketID string) error {
	f.calls = append(f.calls, "SettlePositions")
	for k, p := range f.positions {
		if k.MarketID == marketID {
			p.Settled = true
		}
	}
	return nil
}

func (f *fakeRepo) ApplyWalletDeltas(_ context.Context, ds []WalletDelta) error {
	f.calls = append(f.calls, "ApplyWalletDeltas")
	for i, d := range ds {
		if i > 0 && ds[i-1].UserID >= d.UserID {
			return fmt.Errorf("wallet deltas not in lock order at %s", d.UserID)
		}
		if !f.users[d.UserID] {
			return fmt.Errorf("fk: wallet %s missing", d.UserID)
		}
		f.wallets[d.UserID] += d.DeltaCents
	}
	return nil
}

// UpsertOffset mirrors GREATEST(old, new): stored offsets never regress.
func (f *fakeRepo) UpsertOffset(_ context.Context, c string, p int32, off int64) error {
	f.calls = append(f.calls, "UpsertOffset")
	k := fmt.Sprintf("%s/%d", c, p)
	f.offsets[k] = max(f.offsets[k], off)
	return nil
}

func (f *fakeRepo) balance(account string) int64 {
	var s int64
	for _, t := range f.txns {
		for _, e := range t.Entries {
			if e.AccountID == account {
				s += e.AmountCents
			}
		}
	}
	return s
}
