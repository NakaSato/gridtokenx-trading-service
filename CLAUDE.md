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

# Integration tests live in bin/trading-service/tests/ (run any by --test <file stem>).
# The DB-backed ones run against `<db>_test`, NEVER the service's own database —
# `tests/common::test_db_url()` rewrites the name even when DATABASE_URL points at
# the live one, because the running trading-service's settlement/matcher workers
# mutate fixture rows mid-test. Provision it once:
#   ./scripts/setup-test-db.sh            # clone the schema (idempotent)
#   ./scripts/setup-test-db.sh --recreate # rebuild it
# Override wholesale with TRADING_TEST_DATABASE_URL.
cargo test -p trading-service --test repository_integration_test   # needs Postgres
cargo test -p trading-service --test settlement_integration_test   # needs Postgres
cargo test -p trading-service --test api_routing_test
cargo test -p trading-service --test settlement_cas_retry_test     # CAS retry on tx-hash write-back
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
| `iam-protocol` / `gridtokenx-blockchain-core` | Cross-service crates pulled by **path** from sibling submodules (`../gridtokenx-iam-service/crates/iam-protocol`, `../gridtokenx-blockchain-core` — see `Cargo.toml` `[workspace.dependencies]`). Not vendored shims: these siblings must be checked out for the build. The workspace is excluded from the root workspace due to BPF target conflicts. | — |

### Dependency injection

All collaborators are `Arc<dyn Trait>` traits defined in `trading-core/src/traits.rs`
(`OrderRepository`, `SettlementRepository`, `BlockchainGateway`, `EventPublisher`,
`IdentityGateway`, `AuditLog`, `CacheStore`, `VppRepository`, …). Concrete impls are wired in
**one place**: `bin/trading-service/src/builder.rs` (`ServiceBuilder::build` → `Infrastructure`
+ `AppServices`). To add a dependency: define the trait in `core`, implement in
`persistence`/`infra`, wire in `builder.rs`, surface on `AppState` if a handler needs it.

### Runtime topology (`bin/trading-service/src/main.rs`)

`main` builds the system, then `tokio::spawn`s long-running workers alongside the API server:

- **MatcherWorker** — drains/matches orders (calls `MatcherService` → `trading-engine`). Event-driven:
  every insert path (REST, gRPC, `RecurringEvaluator` via the `MatchTrigger` trait) calls
  `MatcherService::request_cycle`, waking it within `MATCHER_DEBOUNCE_MS` (default 5ms) of an order
  landing; `MATCHER_INTERVAL_MS` (default 1s) is only a safety-net tick.
  `MATCHER_REALTIME=false` reverts to tick-only polling.
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
despite the root CLAUDE.md's general note. Hand-written `buffa::Message` impls for the well-known
types the generated code references (e.g. `google.protobuf.Empty`) live in
`crates/trading-protocol/src/lib.rs`. (There is no service-root proto: the workspace manifest has
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
`TRADING_ROLE` (`api`|`matcher`), `MATCHER_REALTIME` + `MATCHER_DEBOUNCE_MS` + `MATCHER_INTERVAL_MS`
(matching cadence — see `MatcherConfig`), `ORDER_DEFAULT_TTL_SECS` + `ORDER_MAX_TTL_SECS`
(order lifetime — see `OrderExpiryConfig`), `PLATFORM_USER_ID`, `ORACLE_FEED_IN_TARIFF`,
`AGGREGATOR_BRIDGE_PUBLIC_KEY`, and the `SOLANA_*_PROGRAM_ID` set (sensible localnet defaults baked in).

`test-api.sh` is a manual REST smoke test against `:8093` (`BASE_URL=... ./test-api.sh`; needs a
running service + reachable Postgres). Guarded routes need **three** headers, not one:
`x-gridtokenx-role`, `x-gridtokenx-user-id`, **and** `x-gridtokenx-gateway-secret` — the
`api-gateway` role fails *closed* to `Unknown` (⇒ 403) without the secret, whose dev default is
only honoured when the service runs with `CHAIN_BRIDGE_INSECURE=true` and `GATEWAY_SECRET` unset
(`ServiceRole::from_headers`, `../gridtokenx-blockchain-core/crates/blockchain-auth/src/lib.rs`).

---

## Domain notes

- **GRID** = energy token, **GRX** = currency token (see superproject `docs/glossary.md`).
- **Trading never mints tokens.** Token issuance was removed (commit `9587dd3`); GRID supply is
  minted upstream (meter-service via Chain Bridge). This service only **settles** trades —
  `BlockchainSettlement::execute_atomic_settlement` does a token *swap*, never a mint. ATA
  derivation is program-aware per mint (classic SPL vs Token-2022) in `settlement.rs`.
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

## Search Tooling

> **Use `rg` (ripgrep), never `grep`.** When shelling out to search files, run `rg` —
> it respects `.gitignore`, skips binaries, and is far faster than `grep`/`find -exec grep`.
> Reserve plain `grep` only for piping non-file streams.
