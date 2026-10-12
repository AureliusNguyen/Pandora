-- name: AppliedKeys :many
SELECT partition, seq, idx FROM event_log
WHERE (partition, seq, idx) IN (SELECT unnest(@partitions::int[]), unnest(@seqs::bigint[]), unnest(@idxs::int[]));

-- name: MarkApplied :exec
INSERT INTO event_log (partition, seq, idx)
SELECT unnest(@partitions::int[]), unnest(@seqs::bigint[]), unnest(@idxs::int[])
ON CONFLICT DO NOTHING;

-- name: EnsureUsers :exec
INSERT INTO users (id, display_name) SELECT unnest(@ids::text[]), unnest(@ids::text[]) ON CONFLICT DO NOTHING;

-- name: EnsureWallets :exec
INSERT INTO wallets (user_id) SELECT unnest(@ids::text[]) ON CONFLICT DO NOTHING;

-- name: EnsureAccounts :exec
INSERT INTO accounts (id, kind, owner_id)
SELECT unnest(@account_ids::text[]), unnest(@kinds::text[]), unnest(@owner_ids::text[])
ON CONFLICT DO NOTHING;

-- name: UpsertMarket :exec
INSERT INTO markets (id, title, lock_at) VALUES (@id, @title, @lock_at)
ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, lock_at = EXCLUDED.lock_at;

-- name: UpsertOutcomes :exec
INSERT INTO outcomes (market_id, idx, label)
SELECT @market_id::text, unnest(@idxs::int[]), unnest(@labels::text[])
ON CONFLICT (market_id, idx) DO UPDATE SET label = EXCLUDED.label;

-- name: SetMarketStatus :exec
UPDATE markets SET status = @status::text WHERE id = @id;

-- name: ResolveMarket :exec
UPDATE markets SET status = 'resolved', resolution_outcome = @outcome::int WHERE id = @id;

-- name: InsertOrder :exec
INSERT INTO orders (id, user_id, market_id, client_order_id, outcome, side, price, qty, remaining, status, tif, seq)
VALUES (@id, @user_id, @market_id, @client_order_id, @outcome, @side, @price, @qty, @remaining, @status, @tif, @seq)
ON CONFLICT (user_id, client_order_id) DO UPDATE SET
  id = EXCLUDED.id, market_id = EXCLUDED.market_id, outcome = EXCLUDED.outcome, side = EXCLUDED.side,
  price = EXCLUDED.price, qty = EXCLUDED.qty, remaining = EXCLUDED.remaining, status = EXCLUDED.status,
  tif = EXCLUDED.tif, seq = EXCLUDED.seq, updated_at = now()
WHERE orders.status = 'rejected';

-- name: FillOrder :exec
UPDATE orders SET remaining = GREATEST(remaining - @qty::bigint, 0),
  status = CASE WHEN remaining - @qty::bigint <= 0 THEN 'filled' ELSE 'partially_filled' END, updated_at = now()
WHERE id = @id;

-- name: CancelOrder :exec
UPDATE orders SET remaining = GREATEST(remaining - @qty::bigint, 0),
  status = CASE WHEN remaining - @qty::bigint <= 0 THEN 'canceled' ELSE status END, updated_at = now()
WHERE id = @id;

-- name: InsertFills :exec
INSERT INTO fills (id, market_id, kind, qty, partition, seq, idx, legs, engine_time)
SELECT unnest(@ids::text[]), unnest(@market_ids::text[]), unnest(@kinds::text[]), unnest(@qtys::bigint[]),
  unnest(@partitions::int[]), unnest(@seqs::bigint[]), unnest(@idxs::int[]), unnest(@legs::text[])::jsonb,
  unnest(@engine_times::timestamptz[])
ON CONFLICT DO NOTHING;

-- name: InsertLedgerTxns :many
INSERT INTO ledger_txns (kind, ref, partition, seq, idx)
SELECT unnest(@kinds::text[]), unnest(@refs::text[]), unnest(@partitions::int[]), unnest(@seqs::bigint[]), unnest(@idxs::int[])
ON CONFLICT (partition, seq, idx) DO NOTHING
RETURNING id, partition, seq, idx;

-- name: InsertEntries :exec
INSERT INTO ledger_entries (txn_id, account_id, amount_cents)
SELECT unnest(@txn_ids::bigint[]), unnest(@account_ids::text[]), unnest(@amounts::bigint[]);

-- name: InsertSettlement :exec
INSERT INTO settlements (market_id, winning_outcome, position_count, payout_cents, txn_id, duration_ms)
VALUES (@market_id, @winning_outcome, @position_count, @payout_cents, @txn_id::bigint, @duration_ms);

-- name: AddPositions :exec
INSERT INTO positions (user_id, market_id, outcome, qty, cost_basis_cents)
SELECT unnest(@user_ids::text[]), unnest(@market_ids::text[]), unnest(@outcomes::int[]), unnest(@qtys::bigint[]), unnest(@costs::bigint[])
ON CONFLICT (user_id, market_id, outcome) DO UPDATE
SET qty = positions.qty + EXCLUDED.qty, cost_basis_cents = positions.cost_basis_cents + EXCLUDED.cost_basis_cents;

-- name: OpenPositions :many
SELECT user_id, outcome, qty, cost_basis_cents FROM positions
WHERE market_id = @market_id AND settled_at IS NULL ORDER BY user_id, outcome;

-- name: SettlePositions :exec
UPDATE positions SET settled_at = now() WHERE market_id = @market_id AND settled_at IS NULL;

-- name: LockWallets :many
SELECT user_id FROM wallets WHERE user_id = ANY(@user_ids::text[]) ORDER BY user_id FOR UPDATE;

-- name: ApplyWalletDeltas :exec
UPDATE wallets w SET balance_cents = w.balance_cents + v.delta, version = w.version + 1
FROM (SELECT unnest(@user_ids::text[]) AS user_id, unnest(@deltas::bigint[]) AS delta) AS v
WHERE w.user_id = v.user_id;

-- name: UpsertConsumerOffset :exec
INSERT INTO consumer_offsets (consumer, partition, "offset") VALUES (@consumer, @partition, @next_offset::bigint)
ON CONFLICT (consumer, partition) DO UPDATE SET "offset" = GREATEST(consumer_offsets."offset", EXCLUDED."offset");
