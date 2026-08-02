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

`migrations/` is **no longer empty** — since the DB-per-service split Trading owns the
`gridtokenx_trading` schema and its migrations live here (`docs/db-split-phase1.md`).

**Nothing applies them automatically.** The service has no `sqlx::migrate!` call at boot, and
there is no CI: apply them yourself with `sqlx migrate run` against `gridtokenx_trading`, then
point `DATABASE_URL` at the migrated DB. A missing migration surfaces as a runtime column
error, not a build failure — `cargo` needs no database (runtime SQLx, see above), so the build
stays green against a stale schema.

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
- **SettlementWorker** — settles in batches of 10 every 10s via Chain Bridge. A claimed
  settlement whose buy or sell order has **lapsed** is parked `permanently_failed` before any
  on-chain attempt (`SettlementService::park_lapsed_settlements`): the on-chain paths reject a
  lapsed order (`OrderExpired`), so retrying is guaranteed to fail and would otherwise burn all
  5 retries and then blame "not included in on-chain batch result". The pre-flight is an
  optimisation, never a gate — if its expiry lookup fails, every settlement is attempted as
  before, because refusing to settle live trades over a broken helper query would be a
  self-inflicted outage.
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
(order lifetime — see `OrderExpiryConfig`; both REST and gRPC accept a per-order
`expires_at`/`expires_in_secs`), `PLATFORM_USER_ID`, `ORACLE_FEED_IN_TARIFF`,
`AGGREGATOR_BRIDGE_PUBLIC_KEY`, and the `SOLANA_*_PROGRAM_ID` set (sensible localnet defaults baked in).

`test-api.sh` is a manual REST smoke test against `:8093` (`BASE_URL=... ./test-api.sh`; needs a
running service + reachable Postgres). Guarded routes need **three** headers, not one:
`x-gridtokenx-role`, `x-gridtokenx-user-id`, **and** `x-gridtokenx-gateway-secret` — the
`api-gateway` role fails *closed* to `Unknown` (⇒ 403) without the secret, whose dev default is
only honoured when the service runs with `CHAIN_BRIDGE_INSECURE=true` and `GATEWAY_SECRET` unset
(`ServiceRole::from_headers`, `../gridtokenx-blockchain-core/crates/blockchain-auth/src/lib.rs`).

---

## Order placement: a program rejection is final, and nothing retries placement

Custodial placement (`place_order_on_chain`, REST `rest/orders.rs` + gRPC `handlers.rs`) used
to be uniformly "best-effort: left for retry". Two things made that wrong, and both are now
handled — read this before relaxing it back:

- **No retry exists.** No worker looks for `order_pda IS NULL`; the workers are matcher,
  settlement, clearing, read_model_feed, reaper, recurring, supply_sync, trigger. So an order
  that fails placement is never placed later.
- **A program rejection is deterministic.** `trading_core::error::is_deterministic_chain_rejection`
  splits "the program executed and refused this" (`InstructionError`) from "no verdict was
  reached" (timeout, blockhash, connection). The first can never succeed on resubmission.

Behaviour: a deterministic rejection now **refuses the order** — REST returns 422 before the row
is inserted, gRPC cancels the already-durable row. A transport failure keeps the old best-effort
path (a validator blip must not reject a customer's order), and its log now states the real cost.

Why it matters, from a real incident: 18 orders were submitted for zones with no initialized
`ZoneMarket` and rejected `{"InstructionError":[0,{"Custom":3007}]}` (`AccountOwnedByWrongProgram`).
Each was accepted with `order_pda = NULL`, matched by the CDA, and its settlement then failed 5×
(*"buy order … has no on-chain PDA; skipping"*) before landing in `permanently_failed` with
`"not included in on-chain batch result"` — a message naming the symptom, not the cause. `zone_id`
comes straight from the request and is not validated against initialized markets, so any zone id a
client invents reproduces it; `scripts/init-zones.sh` (in the superproject) creates missing markets.

## A sell order requires a verified meter

Selling energy is a claim to have produced it, and the meter is the only thing that
substantiates the claim. Both submit edges refuse a sell (**403** / Connect
`PermissionDenied`) unless the seller has one, **before** the row is inserted and before any
on-chain placement — so an ungrounded ask never reaches the book. Buys are untouched: a
consumer needs no meter to bid.

- **The rule lives once**, in `trading_core::order_policy::check_sell_eligibility` (pure,
  unit-tested), with `needs_any_verified_meter_lookup` deciding when the extra query is worth
  taking. REST (`rest/orders.rs`) and gRPC (`handlers.rs`) both call the pair, so the two
  transports cannot drift — the same reason `resolve_order_price`/`resolve_expires_at` live
  there.
- **A sell naming no meter still needs one.** The seller must own at least one verified meter
  (`MeterRepository::has_verified_meter`), otherwise omitting `meter_serial` would be a
  one-field bypass of the entire rule.
- **A meter the seller does not own reports "not yours", never "not verified"** — the second
  would confirm the serial is registered and disclose its state. An unknown `meter_serial` /
  `meter_id` is a 400: it used to be stored as NULL, silently downgrading a meter-bound sell
  into an ungated meterless one.
- **The input is `meter_read_model.is_verified`**, mirrored from metering `meters.is_verified`
  by the `MeterRegistered`/`MeterUpdated` feed + boot backfill. It is deliberately **not**
  derived from `meter_read_model.status`: that column holds three vocabularies (`'active'`
  from the backfill = the meter's *operating* status; `'verified'`/`'unverified'` from the
  event; `'active'` again as the DDL default), so reading it would treat a backfilled meter as
  verified and fail the gate **open**. The feed prefers the event's explicit `is_verified` and
  falls back to `status == "verified"` only for older producers; anything unrecognised reads
  as unverified.
- **Migration `20260801000000` grandfathers pre-existing rows to verified.** Every meter
  mirrored before this feature was auto-verified at registration under the old rule; starting
  them false would have revoked the sell right of every prosumer already trading. The column
  DEFAULT is `false`, so any future insert path that forgets it fails closed.
- Meters become verified via meter-service `POST /api/v1/me/meters/{serial}/verify`, which
  attests against signature-verified telemetry. Test harnesses that only exercise the CDA seed
  the projection directly — `tests/e2e/lib/db.py::ensure_sellable` in the superproject.

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
