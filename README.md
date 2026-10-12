# Pandora

Pandora is a play-money prediction exchange with a central limit order book,
in the style of Polymarket and Kalshi. Users trade YES/NO shares on 5-minute
crypto up/down markets (resolved by a median-of-feeds oracle over Binance,
Coinbase and Kraken) and on live Lichess games. It is built as a set of Go
microservices where latency, idempotency and tail behavior are product
requirements: order acks are journaled to Kafka and matched with a p99 under
20 ms, and every load test ends with SQL invariants proving no money was
created or lost.

## Architecture

- gateway: public REST edge, validates and routes orders to the owning matcher
- matcher: sequencer + matching engine, sole producer of the `engine.wal` partition, replays the WAL on start
- ledger: the only writer of money tables; double-entry Postgres consumer of `engine.events`
- fanout: WebSocket edge streaming book snapshots and deltas
- lifecycle, ingest-crypto, ingest-chess, stockfish, auth: later milestones
- web: Next.js App Router frontend
- Data: Redpanda (Kafka API), Postgres, Redis, MongoDB
- Internal RPC: connect-go (gRPC wire compatible); observability: OpenTelemetry + Prometheus
- Deploy: kind + Helm; CI: GitHub Actions with k6 latency gates

## Run locally

Requirements: Go (any version >= 1.21; the 1.25 toolchain is auto-downloaded),
buf, goose (for `make migrate`), Docker with Compose v2 (`docker compose`),
pnpm, k6, kind, helm. `make test` runs with `-race`, which needs a C compiler
(cgo); without one, run `make test RACE=`.

```bash
make infra-up          # Redpanda, Postgres, Redis, Mongo via docker compose
make proto             # regenerate protobuf + connect code
make lint test         # golangci-lint, buf lint, unit tests
make test-integration  # testcontainers tests (Docker required)
make kind-up deploy    # kind cluster + Helm install
make smoke             # k6 smoke against the gateway
make invariants        # SQL ledger invariants against PG_DSN (cmd/ledger --invariants)
make infra-down
```

Redpanda console: http://localhost:8085

## Status

Milestone 1 (vertical slice) in progress.
