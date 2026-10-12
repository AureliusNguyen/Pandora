//go:build integration

package ledger

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	tcpg "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
)

func startPG(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	ctr, err := tcpg.Run(ctx, "postgres:17",
		tcpg.WithDatabase("pandora"), tcpg.WithUsername("pandora"), tcpg.WithPassword("pandora"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(90*time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(ctx) })
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatal(err)
	}
	sqlDB := stdlib.OpenDBFromPool(pool)
	defer sqlDB.Close()
	if err := goose.Up(sqlDB, "../../migrations"); err != nil {
		t.Fatal(err)
	}
	return pool
}

func count(t *testing.T, pool *pgxpool.Pool, sql string) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(context.Background(), sql).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestPGMintResolveReplayInvariants(t *testing.T) {
	pool := startPG(t)
	ctx := context.Background()
	a := NewApplier(NewPGRepo(pool), slog.New(slog.NewTextHandler(io.Discard, nil)))
	evs := append(mintEvents(), at(6, 0, &enginev1.Event{Body: &enginev1.Event_MarketResolved{
		MarketResolved: &enginev1.MarketResolved{WinningOutcome: 0},
	}}))
	for i := 0; i < 2; i++ { // second pass is a replay and must be a no-op
		if err := a.ApplyBatch(ctx, batch(evs...)); err != nil {
			t.Fatalf("pass %d: %v", i, err)
		}
	}
	if n := count(t, pool, "SELECT count(*) FROM ledger_txns WHERE kind IN ('mint','settle')"); n != 2 {
		t.Fatalf("txns = %d, want 2", n)
	}
	if n := count(t, pool, "SELECT balance_cents FROM wallets WHERE user_id='u_alice'"); n != 1000000-600+1000 {
		t.Fatalf("alice = %d", n)
	}
	if n := count(t, pool, "SELECT count(*) FROM ledger_entries WHERE txn_id=(SELECT id FROM ledger_txns WHERE kind='mint')"); n != 3 {
		t.Fatalf("mint entries = %d", n)
	}
	if n := count(t, pool, `SELECT "offset" FROM consumer_offsets WHERE consumer='pandora-ledger' AND partition=0`); n != 5 {
		t.Fatalf("offset = %d", n)
	}
	vs, err := CheckInvariants(ctx, pool)
	if err != nil || len(vs) != 0 {
		t.Fatalf("invariants: %v %+v", err, vs)
	}
}

func TestPGInvariantDetectsDrift(t *testing.T) {
	pool := startPG(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, "UPDATE wallets SET balance_cents = balance_cents + 1 WHERE user_id='u_bob'"); err != nil {
		t.Fatal(err)
	}
	vs, err := CheckInvariants(ctx, pool)
	if err != nil || len(vs) != 1 || vs[0].Check != "wallet_matches_entries" {
		t.Fatalf("want one wallet violation, got %v %+v", err, vs)
	}
}

func TestPGRejectThenAcceptSameCOIDAndOffsetNeverRegresses(t *testing.T) {
	pool := startPG(t)
	ctx := context.Background()
	a := NewApplier(NewPGRepo(pool), slog.New(slog.NewTextHandler(io.Discard, nil)))
	evs := []*enginev1.Event{
		at(1, 0, created()),
		at(2, 0, rejected(order("o_rej", "u_alice", "c1", 0, buy, 60, 10))),
		at(3, 0, accepted(order("o_a", "u_alice", "c1", 0, buy, 60, 10), 10)),
		at(4, 0, rejected(order("o_dup", "u_alice", "c1", 0, buy, 60, 10))),
	}
	if err := a.ApplyBatch(ctx, batch(evs...)); err != nil {
		t.Fatal(err)
	}
	if n := count(t, pool, "SELECT count(*) FROM orders WHERE id='o_a' AND status='accepted' AND remaining=10"); n != 1 {
		t.Fatalf("accepted order rows = %d", n)
	}
	if n := count(t, pool, "SELECT count(*) FROM orders WHERE id IN ('o_rej','o_dup')"); n != 0 {
		t.Fatalf("stale rejected rows = %d", n)
	}
	// A redelivery of only the first record must not move the offset back.
	if err := a.ApplyBatch(ctx, batch(evs[0])); err != nil {
		t.Fatal(err)
	}
	if n := count(t, pool, `SELECT "offset" FROM consumer_offsets WHERE consumer='pandora-ledger' AND partition=0`); n != 4 {
		t.Fatalf("offset = %d, want 4", n)
	}
}

// holdRepo wraps PGRepo so the transaction pauses right after ClaimKeys
// until released, holding its event_log rows uncommitted.
type holdRepo struct {
	inner   *PGRepo
	claimed chan struct{}
	release chan struct{}
}

type holdTx struct {
	Tx
	r *holdRepo
}

func (h *holdRepo) InTx(ctx context.Context, fn func(context.Context, Tx) error) error {
	return h.inner.InTx(ctx, func(ctx context.Context, tx Tx) error {
		return fn(ctx, &holdTx{Tx: tx, r: h})
	})
}

func (t *holdTx) ClaimKeys(ctx context.Context, keys []EventKey) (map[EventKey]bool, error) {
	got, err := t.Tx.ClaimKeys(ctx, keys)
	close(t.r.claimed)
	<-t.r.release
	return got, err
}

// Two transactions applying the same batch at once (a rebalance that let a
// second member take the partition) must apply it exactly once.
func TestPGConcurrentSameBatchAppliesOnce(t *testing.T) {
	pool := startPG(t)
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	evs := append(mintEvents(), at(6, 0, &enginev1.Event{Body: &enginev1.Event_MarketResolved{
		MarketResolved: &enginev1.MarketResolved{WinningOutcome: 0},
	}}))
	hold := &holdRepo{inner: NewPGRepo(pool), claimed: make(chan struct{}), release: make(chan struct{})}
	errA, errB := make(chan error, 1), make(chan error, 1)
	go func() { errA <- NewApplier(hold, log).ApplyBatch(ctx, batch(evs...)) }()
	<-hold.claimed // A holds its uncommitted claim
	go func() { errB <- NewApplier(NewPGRepo(pool), log).ApplyBatch(ctx, batch(evs...)) }()
	// Wait until B is blocked on A's event_log rows, then let A commit.
	deadline := time.Now().Add(30 * time.Second)
	for count(t, pool, "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock'") < 1 {
		if time.Now().After(deadline) {
			t.Fatal("second transaction never blocked on the claim")
		}
		time.Sleep(20 * time.Millisecond)
	}
	close(hold.release)
	if err := <-errA; err != nil {
		t.Fatalf("A: %v", err)
	}
	if err := <-errB; err != nil {
		t.Fatalf("B: %v", err)
	}
	if n := count(t, pool, "SELECT balance_cents FROM wallets WHERE user_id='u_alice'"); n != 1000000-600+1000 {
		t.Fatalf("alice = %d", n)
	}
	if n := count(t, pool, "SELECT balance_cents FROM wallets WHERE user_id='u_bob'"); n != 1000000-400 {
		t.Fatalf("bob = %d", n)
	}
	if n := count(t, pool, "SELECT COALESCE(SUM(qty),0) FROM positions WHERE market_id='mkt_t'"); n != 20 {
		t.Fatalf("position qty = %d, want 20", n)
	}
	if n := count(t, pool, "SELECT count(*) FROM ledger_entries e JOIN ledger_txns x ON x.id = e.txn_id WHERE x.kind IN ('mint','settle')"); n != 5 {
		t.Fatalf("entries = %d, want 5 (3 mint + 2 settle)", n)
	}
	if n := count(t, pool, "SELECT count(*) FROM settlements"); n != 1 {
		t.Fatalf("settlements = %d", n)
	}
	if n := count(t, pool, "SELECT remaining FROM orders WHERE id='o_a'"); n != 0 {
		t.Fatalf("maker remaining = %d", n)
	}
	vs, err := CheckInvariants(ctx, pool)
	if err != nil || len(vs) != 0 {
		t.Fatalf("invariants: %v %+v", err, vs)
	}
}

func TestPGApplyWalletDeltasMissingWalletFails(t *testing.T) {
	pool := startPG(t)
	err := NewPGRepo(pool).InTx(context.Background(), func(ctx context.Context, tx Tx) error {
		return tx.ApplyWalletDeltas(ctx, []WalletDelta{{UserID: "u_alice", DeltaCents: 0}, {UserID: "u_nobody", DeltaCents: 5}})
	})
	if err == nil {
		t.Fatal("missing wallet must fail the transaction")
	}
}

func TestPGSecondResolutionIsNoop(t *testing.T) {
	pool := startPG(t)
	ctx := context.Background()
	evs := append(mintEvents(),
		at(6, 0, &enginev1.Event{Body: &enginev1.Event_MarketResolved{MarketResolved: &enginev1.MarketResolved{WinningOutcome: 0}}}),
		at(7, 0, &enginev1.Event{Body: &enginev1.Event_MarketResolved{MarketResolved: &enginev1.MarketResolved{WinningOutcome: 1}}}),
	)
	if err := NewApplier(NewPGRepo(pool), slog.New(slog.NewTextHandler(io.Discard, nil))).ApplyBatch(ctx, batch(evs...)); err != nil {
		t.Fatal(err)
	}
	if n := count(t, pool, "SELECT count(*) FROM settlements"); n != 1 {
		t.Fatalf("settlements = %d", n)
	}
	if n := count(t, pool, "SELECT resolution_outcome::bigint FROM markets WHERE id='mkt_t'"); n != 0 {
		t.Fatalf("resolution_outcome = %d", n)
	}
	if n := count(t, pool, "SELECT count(*) FROM event_log WHERE seq = 7"); n != 1 {
		t.Fatal("ignored resolution not claimed")
	}
}
