-- +goose Up
CREATE TABLE users (id TEXT PRIMARY KEY, display_name TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE accounts (id TEXT PRIMARY KEY, kind TEXT NOT NULL CHECK (kind IN ('user_cash','market_escrow','faucet','dispute')),
  owner_id TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE (kind, owner_id));
CREATE TABLE wallets (user_id TEXT PRIMARY KEY REFERENCES users(id), balance_cents BIGINT NOT NULL DEFAULT 0, version BIGINT NOT NULL DEFAULT 0);
CREATE TABLE ledger_txns (id BIGSERIAL PRIMARY KEY, kind TEXT NOT NULL, ref TEXT NOT NULL, partition INT NOT NULL, seq BIGINT NOT NULL, idx INT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE (partition, seq, idx));
CREATE TABLE ledger_entries (id BIGSERIAL PRIMARY KEY, txn_id BIGINT NOT NULL REFERENCES ledger_txns(id), account_id TEXT NOT NULL REFERENCES accounts(id), amount_cents BIGINT NOT NULL);
CREATE INDEX ON ledger_entries (account_id);
CREATE TABLE markets (id TEXT PRIMARY KEY, kind TEXT NOT NULL DEFAULT 'demo', title TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'open',
  partition INT NOT NULL DEFAULT 0, lock_at TIMESTAMPTZ, resolve_at TIMESTAMPTZ, oracle_payload JSONB, resolution_outcome INT, created_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE outcomes (market_id TEXT NOT NULL REFERENCES markets(id), idx INT NOT NULL, label TEXT NOT NULL, PRIMARY KEY (market_id, idx));
CREATE TABLE orders (id TEXT PRIMARY KEY, user_id TEXT NOT NULL, market_id TEXT NOT NULL, client_order_id TEXT NOT NULL, outcome INT NOT NULL,
  side TEXT NOT NULL, price INT NOT NULL, qty BIGINT NOT NULL, remaining BIGINT NOT NULL, status TEXT NOT NULL, tif TEXT NOT NULL,
  seq BIGINT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE (user_id, client_order_id));
CREATE TABLE fills (id TEXT PRIMARY KEY, market_id TEXT NOT NULL, kind TEXT NOT NULL, qty BIGINT NOT NULL, partition INT NOT NULL, seq BIGINT NOT NULL, idx INT NOT NULL,
  legs JSONB NOT NULL, engine_time TIMESTAMPTZ NOT NULL, UNIQUE (partition, seq, idx));
CREATE TABLE positions (user_id TEXT NOT NULL, market_id TEXT NOT NULL, outcome INT NOT NULL, qty BIGINT NOT NULL DEFAULT 0, cost_basis_cents BIGINT NOT NULL DEFAULT 0,
  settled_at TIMESTAMPTZ, PRIMARY KEY (user_id, market_id, outcome));
CREATE TABLE settlements (id BIGSERIAL PRIMARY KEY, market_id TEXT NOT NULL, winning_outcome INT NOT NULL, position_count BIGINT NOT NULL, payout_cents BIGINT NOT NULL, txn_id BIGINT, duration_ms BIGINT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE consumer_offsets (consumer TEXT NOT NULL, partition INT NOT NULL, "offset" BIGINT NOT NULL, PRIMARY KEY (consumer, partition));
CREATE TABLE event_log (partition INT NOT NULL, seq BIGINT NOT NULL, idx INT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (partition, seq, idx));

-- Every ledger txn must sum to zero. Checked at commit.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION assert_txn_balanced() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE s BIGINT; BEGIN
  SELECT COALESCE(SUM(amount_cents),0) INTO s FROM ledger_entries WHERE txn_id = COALESCE(NEW.txn_id, OLD.txn_id);
  IF s <> 0 THEN RAISE EXCEPTION 'ledger txn % unbalanced: %', COALESCE(NEW.txn_id, OLD.txn_id), s; END IF; RETURN NULL; END $$;
-- +goose StatementEnd
CREATE CONSTRAINT TRIGGER ledger_entries_balanced AFTER INSERT OR UPDATE OR DELETE ON ledger_entries
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION assert_txn_balanced();

INSERT INTO users (id, display_name) VALUES ('u_alice','Alice'),('u_bob','Bob'),('u_carol','Carol');
INSERT INTO wallets (user_id, balance_cents) VALUES ('u_alice',1000000),('u_bob',1000000),('u_carol',1000000);
INSERT INTO accounts (id, kind, owner_id) VALUES ('acct_cash_u_alice','user_cash','u_alice'),('acct_cash_u_bob','user_cash','u_bob'),('acct_cash_u_carol','user_cash','u_carol'),('acct_faucet','faucet','system');
INSERT INTO ledger_txns (kind, ref, partition, seq, idx) VALUES ('faucet','seed',-1,0,0);
INSERT INTO ledger_entries (txn_id, account_id, amount_cents) VALUES (1,'acct_faucet',-3000000),(1,'acct_cash_u_alice',1000000),(1,'acct_cash_u_bob',1000000),(1,'acct_cash_u_carol',1000000);

-- +goose Down
DROP TABLE IF EXISTS event_log, consumer_offsets, settlements, positions, fills, orders, outcomes, markets, ledger_entries, ledger_txns, wallets, accounts, users CASCADE;
DROP FUNCTION IF EXISTS assert_txn_balanced();
