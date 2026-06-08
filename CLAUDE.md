# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

This is the **Trading Service** — an independent Cargo workspace (a git submodule of the
`gridtokenx-coresystem` superproject). The superproject root has its own `CLAUDE.md` with
cross-service rules; this file covers what is specific to this service and, where noted,
**overrides** the root.

---

## Build & Test

This is a standalone workspace — run `cargo` from this directory, never the repo root.

```bash
cargo check                              # fast feedback across all crates
cargo build --release                    # binary -> target/release/trading-service
cargo clippy -- -D warnings              # lints are strict (see below)
cargo test                               # unit tests
cargo test -p trading-engine             # one crate
cargo test test_order_matching -- --nocapture   # one test

# Integration tests (need a live Postgres) live in bin/trading-service/tests/
cargo test -p trading-service --test repository_integration_test
cargo test -p trading-service --test settlement_integration_test
cargo test -p trading-service --test api_routing_test
```

**Lints are enforced workspace-wide** (`Cargo.toml [workspace.lints]`): `unsafe_code = "deny"`,
`clippy::unwrap_used = "deny"`, `clippy::pedantic = "warn"`. Code that `.unwrap()`s will not
compile — use `?`, `.expect("reason")`, or `.context()`.

### SQLx — overrides root CLAUDE.md

The root CLAUDE.md says to use the compile-time `sqlx::query_as!` macros and run
`cargo sqlx prepare`. **This service does NOT.** Persistence uses the **runtime** `sqlx::query(...)`
API (zero `query!`/`query_as!` macros, no `.sqlx/` dir). Consequences:

- `cargo check`/`build` need **no** `DATABASE_URL` and no running database.
- There is **nothing to `cargo sqlx prepare`** — don't add it to the build.
- New repository code should follow the existing runtime-query pattern in
  `crates/trading-persistence/src/repositories/`, not the macro form.

### Migrations

`migrations/` here is empty (only `.keep`). The trading schema is **not** owned by this service —
the DB is provisioned externally (superproject `just migrate` / IAM). Don't add a
`sqlx::migrate!` call expecting local migrations; point `DATABASE_URL` at an already-migrated DB.

---

## Architecture

**Modular monolith, single binary.** `bin/trading-service` boots everything; there are no
separate per-crate processes. The crates enforce the layered dependency direction
(`core → … → api → bin`); never reverse it.

### Crate layout (`crates/`)

| Crate | Role | Async? |
|-------|------|--------|
| `trading-core` | Domain primitives: `types`, `models`, `error::ApiError`, `traits` (all repo/gateway traits), `config`, `fast_price` (fixed-point), `events`. Every crate depends on this. | sync |
| `trading-engine` | **Pure** Continuous Double Auction (CDA) matcher. Zero async, zero I/O, zero DB — the hot path. Keep it that way. | sync |
| `trading-persistence` | Postgres repos implementing the `core` traits. Runtime SQLx. | async |
| `trading-infra` | External adapters: `blockchain` (Chain Bridge), `events` (Kafka/Redis EventBus + outbox), `cache` (Redis), `audit`, `identity` (IAM gateway), `telemetry`, `metrics`. | async |
| `trading-logic` | Services & background workers: `MatcherService`, `SettlementService`, `vpp`, `forecasting`, `clearing`, conditional/recurring order evaluators, `workers/` (matcher, settlement, supply_sync, oracle_consumer). | async |
| `trading-api` | ConnectRPC handlers + REST (`rest.rs`, `handlers.rs`), `startup`, `state::AppState`, auth/middleware. | async |
| `iam-protocol-compat` / `blockchain-core-compat` | Local shims standing in for the cross-service crates so this workspace builds in isolation (BPF target conflicts keep it out of the root workspace). | — |

### Dependency injection

All collaborators are `Arc<dyn Trait>` traits defined in `trading-core/src/traits.rs`
(`OrderRepository`, `SettlementRepository`, `BlockchainGateway`, `EventPublisher`,
`IdentityGateway`, `AuditLog`, `CacheStore`, `VppRepository`, …). Concrete impls are wired in
**one place**: `bin/trading-service/src/builder.rs` (`ServiceBuilder::build` → `Infrastructure`
+ `AppServices`). To add a dependency: define the trait in `core`, implement in
`persistence`/`infra`, wire in `builder.rs`, surface on `AppState` if a handler needs it.

### Runtime topology (`bin/trading-service/src/main.rs`)

`main` builds the system, then `tokio::spawn`s long-running workers alongside the API server:

- **MatcherWorker** — drains/matches orders every 1s (calls `MatcherService` → `trading-engine`).
- **SettlementWorker** — settles in batches of 10 every 10s via Chain Bridge.
- **SupplySyncWorker** — polls blockchain to sync tokenized supply; graceful-degrades on repeated failure.
- **OracleConsumer** — consumes oracle readings off the EventBus, feeds settlement.
- **AuditWorker / OutboxWorker** — async audit logging and transactional-outbox event publishing.

### Events — transactional outbox

Domain events are written to a DB outbox in the same transaction as state changes
(`OutboxPublisher`), then `OutboxWorker` relays them to the `EventBus`. The EventBus fans out to
**Redis Streams** always and **Kafka** when `KAFKA_EVENTS_ENABLED=true`. Don't publish to
Kafka/Redis directly from services — go through the outbox so events stay consistent with DB writes.

### Blockchain

No direct Solana RPC. `BlockchainGateway` (`trading-infra::blockchain`) talks to **Chain Bridge**
(`CHAIN_BRIDGE_URL`, default `http://127.0.0.1:5040`). Settlement and supply-sync go through it.

### Protobuf / RPC

`crates/trading-protocol/build.rs` compiles `crates/trading-protocol/proto/trading.proto` via
**`connectrpc_build`** (the `buffa` codegen) into `_trading_include.rs` — **not** prost/tonic,
despite the root CLAUDE.md's general note. The compat crates supply hand-written `buffa::Message`
impls (e.g. `google.protobuf.Empty`). (There is no service-root proto: the workspace manifest has
no `[package]`, so a root `build.rs` would never run — the authoritative proto lives in the
`trading-protocol` crate.)

---

## Configuration (env)

Loaded by `trading_core::config::Config::from_env()` (`crates/trading-core/src/config/mod.rs`).
`.env` is auto-loaded via `dotenvy`.

**Ports** (from `main.rs`, override via env): `HTTP_PORT` = `8093`, `GRPC_PORT` = `8092`.
(The Dockerfile `EXPOSE 5020 4020` is the deploy-time gRPC/metrics mapping — the code defaults differ.)

**Required** (build fine without them, but startup fails): `DATABASE_URL` (or `TRADING_DATABASE_URL`),
`REDIS_URL`, `SOLANA_RPC_URL`, `SOLANA_WS_URL`, `ENERGY_TOKEN_MINT`.

**Notable optional**: `CHAIN_BRIDGE_URL`, `IAM_GRPC_URL`/`IAM_SERVICE_URL`, `INTERNAL_API_KEY`,
`ENCRYPTION_SECRET`, `KAFKA_EVENTS_ENABLED` + `KAFKA_BOOTSTRAP_SERVERS` + `KAFKA_TOPIC_PREFIX`,
`TRADING_ROLE` (`api`|`matcher`), `PLATFORM_USER_ID`, `ORACLE_FEED_IN_TARIFF`,
`AGGREGATOR_BRIDGE_PUBLIC_KEY`, and the `SOLANA_*_PROGRAM_ID` set (sensible localnet defaults baked in).

`test-api.sh` is a manual REST smoke test against `:8093`.

---

## Domain notes

- **GRID** = energy token, **GRX** = currency token (see superproject `docs/glossary.md`).
- Matching is CDA; price math uses `fast_price::FastPrice` fixed-point (no floats on the hot path).
- Beyond spot orders: **futures**, **carbon/REC**, **VPP** (virtual power plant with forecasting),
  and **conditional/recurring** orders each have their own repo + logic module.
- `skill.md` is a long architecture essay on the on-chain/off-chain split and crate-boundary
  rationale — read it when deciding *where new code belongs* (program vs SDK vs service).

<!-- code-review-graph MCP tools -->
## MCP Tools: code-review-graph

**IMPORTANT: This project has a knowledge graph. ALWAYS use the
code-review-graph MCP tools BEFORE using Grep/Glob/Read to explore
the codebase.** The graph is faster, cheaper (fewer tokens), and gives
you structural context (callers, dependents, test coverage) that file
scanning cannot.

### When to use graph tools FIRST

- **Exploring code**: `semantic_search_nodes` or `query_graph` instead of Grep
- **Understanding impact**: `get_impact_radius` instead of manually tracing imports
- **Code review**: `detect_changes` + `get_review_context` instead of reading entire files
- **Finding relationships**: `query_graph` with callers_of/callees_of/imports_of/tests_for
- **Architecture questions**: `get_architecture_overview` + `list_communities`

Fall back to Grep/Glob/Read **only** when the graph doesn't cover what you need.

### Key Tools

| Tool | Use when |
| ------ | ---------- |
| `detect_changes` | Reviewing code changes — gives risk-scored analysis |
| `get_review_context` | Need source snippets for review — token-efficient |
| `get_impact_radius` | Understanding blast radius of a change |
| `get_affected_flows` | Finding which execution paths are impacted |
| `query_graph` | Tracing callers, callees, imports, tests, dependencies |
| `semantic_search_nodes` | Finding functions/classes by name or keyword |
| `get_architecture_overview` | Understanding high-level codebase structure |
| `refactor_tool` | Planning renames, finding dead code |

### Workflow

1. The graph auto-updates on file changes (via hooks).
2. Use `detect_changes` for code review.
3. Use `get_affected_flows` to understand impact.
4. Use `query_graph` pattern="tests_for" to check coverage.
