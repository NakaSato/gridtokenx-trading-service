# gridtokenx-trading-service

> The energy-trading and ERC (Energy Renewable Certificate) microservice of GridTokenX.
> A **modular-monolith Cargo workspace** whose pure Continuous-Double-Auction (CDA) matching
> engine clears GRID/GRX orders and settles them on Solana **through Chain Bridge** — never via
> direct RPC.

This repo is a **git submodule** of the [`gridtokenx-coresystem`](../) superproject. Platform-wide
rules (services, gateways, Chain Bridge as the only Solana RPC client) live in the superproject.

- **Architecture** → [`ARCHITECTURE.md`](ARCHITECTURE.md) — citation-backed map (module layout,
  match→settlement pipeline, invariants).
- **REST reference** → [`API.md`](API.md) — narrative conventions; live OpenAPI at
  `GET /api-docs/openapi.json`, Swagger UI at `/docs`.
- **LLM working rules** → [`CLAUDE.md`](CLAUDE.md).

---

## What it is

- **One Cargo workspace, one binary** (`bin/trading-service`). Kept **out** of the superproject root
  workspace because Solana deps pull in BPF target conflicts — always run `cargo` from this
  directory, never the repo root.
- **Modular monolith**: a single process boots the API server (REST + ConnectRPC) and all background
  workers.
- **Sync core, async edges**: the hot path — order matching (`trading-engine`) — is a **pure,
  synchronous, I/O-free** CDA engine; everything async (DB, blockchain, events, HTTP) lives at the
  edges.
- Workspace version `0.1.1`, edition 2021. Strict workspace lints: `unsafe_code = "deny"`,
  `clippy::unwrap_used = "deny"`, `clippy::pedantic = "warn"` — **code that `.unwrap()`s will not
  compile**.

## Crate layout

```
trading-core → trading-engine / persistence / infra → trading-logic → trading-api → bin/trading-service
```

| Crate | Role | Async? |
|-------|------|--------|
| `trading-core` | Domain primitives: `types`, `models`, `error::ApiError`, `traits` (all repo/gateway DI traits), `config`, `fast_price` (fixed-point), `events`. Every crate depends on it. | sync |
| `trading-engine` | **Pure** CDA matcher (`MatchingEngine::match_cycle`). Zero async, zero I/O, zero DB — the hot path. | sync |
| `trading-persistence` | Postgres repos implementing the `core` traits. **Runtime** SQLx. | async |
| `trading-infra` | External adapters: `blockchain` (Chain Bridge), `events` (Kafka/Redis EventBus + outbox), `cache` (Redis), `audit`, `identity` (IAM gateway), `telemetry`, `metrics`. | async |
| `trading-logic` | Services & workers: `MatcherService`, `SettlementService`, `vpp` + forecasting, `clearing`, conditional/recurring evaluators, `workers/`. | async |
| `trading-api` | ConnectRPC handlers + REST (`rest.rs`), `startup`, `state::AppState`, auth/middleware. | async |
| `trading-protocol` | Wire types; `proto/trading.proto` compiled by `connectrpc_build` (buffa), not prost/tonic. | — |

Cross-service crates (`iam-protocol`, `gridtokenx-blockchain-core`) are pulled by **path** from
sibling submodules — those must be checked out for the build.

## Architecture highlights

- **CDA matching engine** (`trading-engine/src/engine.rs`) — pure sync function. Zone-segmented
  order books enforcing price-time priority; grid topology (wheeling charges, loss factors, flow
  accommodation, intra-zone discount) injected via the `TopologySnapshot` trait. `FastPrice`
  fixed-point — no floats on the hot path.
- **Settlement via Chain Bridge** — no direct Solana RPC. `SettlementService` settles through
  `Arc<dyn BlockchainGateway>` → `BlockchainService` → Chain Bridge (`CHAIN_BRIDGE_URL`, default
  `http://127.0.0.1:5040`). On-chain step is an **atomic swap, never a mint** (GRID is minted
  upstream by meter-service). Idempotent, replay-safe (per-match `TradeNullifier`), gated on finality.
- **Transactional outbox** — domain events written to a DB outbox in the same tx as state changes,
  relayed by `OutboxWorker` to the `EventBus` (Redis Streams always, Kafka when
  `KAFKA_EVENTS_ENABLED=true`). Services never publish to Kafka/Redis directly.
- **ServiceBuilder** (`bin/trading-service/src/builder.rs`) — the single wiring point. All
  collaborators are `Arc<dyn Trait>` from `trading-core/src/traits.rs`.

### Background workers (`bin/trading-service/src/main.rs`)

| Worker | Cadence | Role |
|--------|---------|------|
| `MatcherWorker` | every 1s | drains/matches realtime orders → `trading-engine` (CDA) |
| `ClearingWorker` | every 60s | uniform-price clears + closes 15-min interval epochs |
| `ReaperWorker` | every 10s | expires orders past `expires_at` |
| `SettlementWorker` | every 10s, batch 10 | settles through Chain Bridge |
| `SupplySyncWorker` | polling | syncs tokenized supply from blockchain; graceful-degrades |
| `OracleConsumer` | event-driven | consumes oracle readings off the EventBus, feeds settlement |

Beyond spot orders: **futures**, **carbon/REC**, **VPP** (with forecasting), and
**conditional/recurring** orders — each with its own repo + logic module.

## Build & test

Standalone workspace — run from this directory, never the repo root.

```bash
cargo check                              # fast feedback (no DATABASE_URL / DB needed)
cargo build --release                    # binary -> target/release/trading-service
cargo clippy -- -D warnings              # strict lints (unwrap_used = deny, pedantic = warn)
cargo test                               # unit tests
cargo test -p trading-engine             # one crate
cargo test test_order_matching -- --nocapture   # one test by name

# Integration tests in bin/trading-service/tests/ (run any by --test <file stem>):
cargo test -p trading-service --test repository_integration_test   # needs Postgres
cargo test -p trading-service --test settlement_integration_test   # needs Postgres
cargo test -p trading-service --test api_routing_test
cargo test -p trading-service --test settlement_cas_retry_test     # self-contained (CAS retry)
```

**SQLx is runtime, not macros** (overrides root CLAUDE.md): persistence uses `sqlx::query(...)` — no
`query!`/`query_as!`, no `.sqlx/` dir, nothing to `cargo sqlx prepare`, no `DATABASE_URL` needed to
build. **Migrations are not owned here** (`migrations/` holds only `.keep`); the trading schema is
provisioned externally (superproject `just migrate` / IAM). Point `DATABASE_URL` at an
already-migrated DB.

## Configuration

Loaded by `trading_core::config::Config::from_env()`. `.env` is auto-loaded via `dotenvy`.

**Ports** (override via env): `HTTP_PORT` = `8093`, `GRPC_PORT` = `8092`. (The Dockerfile
`EXPOSE 5020 4020` is the deploy-time mapping — code defaults differ.)

**Required** (build fine without them; startup fails if missing): `DATABASE_URL` (or
`TRADING_DATABASE_URL`), `REDIS_URL`, `SOLANA_RPC_URL`, `SOLANA_WS_URL`, `ENERGY_TOKEN_MINT`.

**Notable optional**: `CHAIN_BRIDGE_URL`, `IAM_GRPC_URL`/`IAM_SERVICE_URL`, `INTERNAL_API_KEY`,
`ENCRYPTION_SECRET`, `KAFKA_EVENTS_ENABLED` + `KAFKA_BOOTSTRAP_SERVERS` + `KAFKA_TOPIC_PREFIX`,
`TRADING_ROLE` (`api`|`matcher`), `PLATFORM_USER_ID`, `ORACLE_FEED_IN_TARIFF`,
`AGGREGATOR_BRIDGE_PUBLIC_KEY`, and the `SOLANA_*_PROGRAM_ID` set (localnet defaults baked in).

`test-api.sh` is a manual REST smoke test against `:8093`.

## API surface

REST under `/api/v1/*` (JWT `UserContext` + `ServiceRole` RBAC per handler; `/health`, `/metrics`
open). Money/energy amounts are decimal **strings** (`rust_decimal`), not JSON numbers. Timestamps
are RFC-3339 UTC. Full reference in [`API.md`](API.md); live spec at `/api-docs/openapi.json`,
Swagger UI at `/docs`.

Domain areas: **Orders** · **Quotes** · **Order book / market stats** · **Markets (read-only)** ·
**Trades** (+ CSV/JSON export) · **Price alerts** · **Recurring orders** · **Futures** · **User
analytics** · **Carbon/ESG**. Ops: `GET /health`, `GET /health/ready`, `GET /metrics`.

Reached by the Trading UI (`gridtokenx-trading`, Next.js) via the APISIX gateway.

## Load-bearing invariants

1. **Standalone workspace, never the root** — run `cargo` from this dir.
2. **Keep `trading-engine` pure** — no async, I/O, or DB in the matcher.
3. **No floats on the price path** — `FastPrice` / `rust_decimal` only.
4. **No direct Solana RPC** — all blockchain access via `BlockchainGateway` → Chain Bridge.
5. **Events go through the outbox** — never publish to Kafka/Redis directly.
6. **`.unwrap()` does not compile** — use `?`, `.expect("reason")`, or `.context()`.
7. **Wire dependencies in `builder.rs` only.**

## Domain notes

- **GRID** = energy token, **GRX** = currency token (superproject `docs/glossary.md`).
- **Trading never mints tokens** — it only **settles** (token swap). GRID supply is minted upstream
  (meter-service via Chain Bridge).
