package ledger

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aureliusnguyen/pandora/backend/internal/ledger/db"
)

// PGRepo is the Postgres Repo backed by the sqlc queries in package db.
type PGRepo struct{ pool *pgxpool.Pool }

// NewPGRepo returns a Repo over pool.
func NewPGRepo(pool *pgxpool.Pool) *PGRepo { return &PGRepo{pool: pool} }

// InTx runs fn in one READ COMMITTED transaction; the deferred
// balanced-txn trigger fires at its commit.
func (r *PGRepo) InTx(ctx context.Context, fn func(context.Context, Tx) error) error {
	return pgx.BeginTxFunc(ctx, r.pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		return fn(ctx, &pgTx{q: db.New(tx)})
	})
}

type pgTx struct{ q *db.Queries }

func splitKeys(keys []EventKey) (ps []int32, ss []int64, is []int32) {
	for _, k := range keys {
		ps, ss, is = append(ps, k.Partition), append(ss, k.Seq), append(is, k.Idx)
	}
	return ps, ss, is
}

func (t *pgTx) AppliedKeys(ctx context.Context, keys []EventKey) (map[EventKey]bool, error) {
	out := map[EventKey]bool{}
	if len(keys) == 0 {
		return out, nil
	}
	ps, ss, is := splitKeys(keys)
	rows, err := t.q.AppliedKeys(ctx, db.AppliedKeysParams{Partitions: ps, Seqs: ss, Idxs: is})
	for _, r := range rows {
		out[EventKey{r.Partition, r.Seq, r.Idx}] = true
	}
	return out, err
}

func (t *pgTx) EnsureUsers(ctx context.Context, ids []string) error {
	if err := t.q.EnsureUsers(ctx, ids); err != nil {
		return err
	}
	if err := t.q.EnsureWallets(ctx, ids); err != nil {
		return err
	}
	return t.ensureAccounts(ctx, "user_cash", ids, CashAccount)
}

func (t *pgTx) EnsureEscrow(ctx context.Context, ids []string) error {
	return t.ensureAccounts(ctx, "market_escrow", ids, EscrowAccount)
}

// ensureAccounts creates the accounts of one kind for owners, idempotently.
func (t *pgTx) ensureAccounts(ctx context.Context, kind string, owners []string, accountID func(string) string) error {
	p := db.EnsureAccountsParams{}
	for _, id := range owners {
		p.AccountIds, p.Kinds, p.OwnerIds = append(p.AccountIds, accountID(id)), append(p.Kinds, kind), append(p.OwnerIds, id)
	}
	return t.q.EnsureAccounts(ctx, p)
}

func (t *pgTx) UpsertMarket(ctx context.Context, m MarketRow) error {
	lock := pgtype.Timestamptz{}
	if m.LockAt != nil {
		lock = pgtype.Timestamptz{Time: *m.LockAt, Valid: true}
	}
	if err := t.q.UpsertMarket(ctx, db.UpsertMarketParams{ID: m.ID, Title: m.Title, LockAt: lock}); err != nil {
		return err
	}
	idxs := make([]int32, len(m.Outcomes))
	for i := range m.Outcomes {
		idxs[i] = int32(i)
	}
	if err := t.q.UpsertOutcomes(ctx, db.UpsertOutcomesParams{MarketID: m.ID, Idxs: idxs, Labels: m.Outcomes}); err != nil {
		return err
	}
	return t.EnsureEscrow(ctx, []string{m.ID})
}

func (t *pgTx) SetMarketStatus(ctx context.Context, id, status string, w *int32) error {
	if w != nil {
		return t.q.ResolveMarket(ctx, db.ResolveMarketParams{Outcome: *w, ID: id})
	}
	return t.q.SetMarketStatus(ctx, db.SetMarketStatusParams{Status: status, ID: id})
}

func (t *pgTx) InsertOrder(ctx context.Context, o OrderRow) error {
	return t.q.InsertOrder(ctx, db.InsertOrderParams{
		ID: o.ID, UserID: o.UserID, MarketID: o.MarketID, ClientOrderID: o.ClientOrderID, Outcome: o.Outcome,
		Side: o.Side, Price: o.Price, Qty: o.Qty, Remaining: o.Remaining, Status: o.Status, Tif: o.TIF, Seq: o.Seq,
	})
}

func (t *pgTx) FillOrder(ctx context.Context, id string, qty int64) error {
	return t.q.FillOrder(ctx, db.FillOrderParams{Qty: qty, ID: id})
}

func (t *pgTx) CancelOrder(ctx context.Context, id string, qty int64) error {
	return t.q.CancelOrder(ctx, db.CancelOrderParams{Qty: qty, ID: id})
}

func (t *pgTx) InsertFills(ctx context.Context, fills []FillRow) error {
	p := db.InsertFillsParams{}
	for _, f := range fills {
		p.Ids, p.MarketIds, p.Kinds, p.Qtys = append(p.Ids, f.ID), append(p.MarketIds, f.MarketID), append(p.Kinds, f.Kind), append(p.Qtys, f.Qty)
		p.Partitions, p.Seqs, p.Idxs = append(p.Partitions, f.Key.Partition), append(p.Seqs, f.Key.Seq), append(p.Idxs, f.Key.Idx)
		p.Legs = append(p.Legs, string(f.Legs))
		p.EngineTimes = append(p.EngineTimes, pgtype.Timestamptz{Time: f.EngineTime, Valid: true})
	}
	return t.q.InsertFills(ctx, p)
}

func (t *pgTx) InsertLedgerTxns(ctx context.Context, txns []JournalTxn) error {
	p := db.InsertLedgerTxnsParams{}
	for _, x := range txns {
		p.Kinds, p.Refs = append(p.Kinds, x.Kind), append(p.Refs, x.Ref)
		p.Partitions, p.Seqs, p.Idxs = append(p.Partitions, x.Key.Partition), append(p.Seqs, x.Key.Seq), append(p.Idxs, x.Key.Idx)
	}
	rows, err := t.q.InsertLedgerTxns(ctx, p)
	if err != nil {
		return err
	}
	ids := make(map[EventKey]int64, len(rows)) // keyed lookup: never rely on RETURNING order
	for _, r := range rows {
		ids[EventKey{r.Partition, r.Seq, r.Idx}] = r.ID
	}
	e := db.InsertEntriesParams{}
	for _, x := range txns {
		id, ok := ids[x.Key]
		if !ok {
			continue // already present: second dedup guard
		}
		for _, en := range x.Entries {
			e.TxnIds, e.AccountIds, e.Amounts = append(e.TxnIds, id), append(e.AccountIds, en.AccountID), append(e.Amounts, en.AmountCents)
		}
		if s := x.Settlement; s != nil {
			if err := t.q.InsertSettlement(ctx, db.InsertSettlementParams{
				MarketID: s.MarketID, WinningOutcome: s.WinningOutcome, PositionCount: s.PositionCount,
				PayoutCents: s.PayoutCents, TxnID: id, DurationMs: s.DurationMs,
			}); err != nil {
				return fmt.Errorf("settlement: %w", err)
			}
		}
	}
	if len(e.TxnIds) == 0 {
		return nil
	}
	return t.q.InsertEntries(ctx, e)
}

func (t *pgTx) AddPositions(ctx context.Context, ds []PositionDelta) error {
	p := db.AddPositionsParams{}
	for _, d := range ds {
		p.UserIds, p.MarketIds, p.Outcomes = append(p.UserIds, d.UserID), append(p.MarketIds, d.MarketID), append(p.Outcomes, d.Outcome)
		p.Qtys, p.Costs = append(p.Qtys, d.Qty), append(p.Costs, d.CostCents)
	}
	return t.q.AddPositions(ctx, p)
}

func (t *pgTx) OpenPositions(ctx context.Context, marketID string) ([]Position, error) {
	rows, err := t.q.OpenPositions(ctx, marketID)
	out := make([]Position, len(rows))
	for i, r := range rows {
		out[i] = Position{UserID: r.UserID, Outcome: r.Outcome, Qty: r.Qty, CostBasisCents: r.CostBasisCents}
	}
	return out, err
}

func (t *pgTx) SettlePositions(ctx context.Context, marketID string) error {
	return t.q.SettlePositions(ctx, marketID)
}

func (t *pgTx) ApplyWalletDeltas(ctx context.Context, ds []WalletDelta) error {
	p := db.ApplyWalletDeltasParams{}
	for _, d := range ds {
		p.UserIds, p.Deltas = append(p.UserIds, d.UserID), append(p.Deltas, d.DeltaCents)
	}
	if _, err := t.q.LockWallets(ctx, p.UserIds); err != nil { // FOR UPDATE in user/account-id order
		return err
	}
	return t.q.ApplyWalletDeltas(ctx, p)
}

func (t *pgTx) MarkApplied(ctx context.Context, keys []EventKey) error {
	ps, ss, is := splitKeys(keys)
	return t.q.MarkApplied(ctx, db.MarkAppliedParams{Partitions: ps, Seqs: ss, Idxs: is})
}

func (t *pgTx) UpsertOffset(ctx context.Context, c string, p int32, off int64) error {
	return t.q.UpsertConsumerOffset(ctx, db.UpsertConsumerOffsetParams{Consumer: c, Partition: p, NextOffset: off})
}
