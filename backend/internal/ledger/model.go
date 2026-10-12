// Package ledger turns engine.events into the double-entry Postgres ledger:
// one journal txn per Fill (MINT, TRANSFER), settle and void txns on
// MarketResolved and MarketVoided, plus the orders, fills, positions,
// wallets and markets read models. Batches are applied in one database
// transaction and deduplicated by (partition, seq, idx).
package ledger

import (
	"context"
	"time"

	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
)

// ConsumerName is the Kafka consumer group and the consumer_offsets key.
const ConsumerName = "pandora-ledger"

// CashAccount returns the cash account id of a user.
func CashAccount(userID string) string { return "acct_cash_" + userID }

// EscrowAccount returns the escrow account id of a market.
func EscrowAccount(marketID string) string { return "acct_escrow_" + marketID }

// EventKey is the engine's unique event identity, used for dedup.
type EventKey struct {
	Partition int32
	Seq       int64
	Idx       int32
}

// Envelope is one consumed Kafka record. Event is nil when the record could
// not be decoded; such envelopes only advance the stored consumer offset.
type Envelope struct {
	KafkaPartition int32
	Offset         int64
	Event          *enginev1.Event
}

// Entry is one leg of a journal txn; amounts across a txn sum to zero.
type Entry struct {
	AccountID   string
	AmountCents int64
}

// Settlement is the settlements row written alongside a settle txn.
type Settlement struct {
	MarketID       string
	WinningOutcome int32
	PositionCount  int64
	PayoutCents    int64
	DurationMs     int64
}

// JournalTxn is one balanced ledger_txns row with its entries.
type JournalTxn struct {
	Key        EventKey
	Kind       string
	Ref        string
	Entries    []Entry
	Settlement *Settlement
}

// OrderRow is an orders table row.
type OrderRow struct {
	ID, UserID, MarketID, ClientOrderID string
	Outcome                             int32
	Side                                string
	Price                               int32
	Qty, Remaining                      int64
	Status, TIF                         string
	Seq                                 int64
}

// FillRow is a fills table row; Legs is the JSON-encoded leg list.
type FillRow struct {
	ID, MarketID, Kind string
	Qty                int64
	Key                EventKey
	Legs               []byte
	EngineTime         time.Time
}

// PositionKey identifies a positions row.
type PositionKey struct {
	UserID, MarketID string
	Outcome          int32
}

// PositionDelta is an additive change to a position's qty and cost basis.
type PositionDelta struct {
	PositionKey
	Qty, CostCents int64
}

// Position is an unsettled position of one market.
type Position struct {
	UserID         string
	Outcome        int32
	Qty            int64
	CostBasisCents int64
}

// WalletDelta is an additive change to a user's wallet balance.
type WalletDelta struct {
	UserID     string
	DeltaCents int64
}

// MarketRow is the markets/outcomes data carried by MarketCreated.
type MarketRow struct {
	ID, Title string
	Outcomes  []string
	LockAt    *time.Time
}

// Tx is everything the applier needs inside one database transaction.
type Tx interface {
	// ClaimKeys must be the first write of the transaction: it inserts the
	// keys into event_log (ON CONFLICT DO NOTHING) and returns only those this
	// transaction inserted. A concurrent transaction claiming the same keys
	// blocks on the unique index until this one ends, then gets none back.
	ClaimKeys(ctx context.Context, keys []EventKey) (map[EventKey]bool, error)
	// MarketStatus returns the market's status and whether the market exists.
	MarketStatus(ctx context.Context, marketID string) (status string, ok bool, err error)
	EnsureUsers(ctx context.Context, userIDs []string) error
	EnsureEscrow(ctx context.Context, marketIDs []string) error
	UpsertMarket(ctx context.Context, m MarketRow) error
	SetMarketStatus(ctx context.Context, marketID, status string, winning *int32) error
	InsertOrder(ctx context.Context, o OrderRow) error
	FillOrder(ctx context.Context, orderID string, qty int64) error
	CancelOrder(ctx context.Context, orderID string, qty int64) error
	InsertFills(ctx context.Context, fills []FillRow) error
	InsertLedgerTxns(ctx context.Context, txns []JournalTxn) error
	AddPositions(ctx context.Context, deltas []PositionDelta) error
	OpenPositions(ctx context.Context, marketID string) ([]Position, error)
	SettlePositions(ctx context.Context, marketID string) error
	// ApplyWalletDeltas receives deltas sorted by UserID (account-id lock order).
	ApplyWalletDeltas(ctx context.Context, deltas []WalletDelta) error
	UpsertOffset(ctx context.Context, consumer string, partition int32, nextOffset int64) error
}

// Repo runs a function inside one database transaction, committing when it
// returns nil and rolling back otherwise.
type Repo interface {
	InTx(ctx context.Context, fn func(ctx context.Context, tx Tx) error) error
}
