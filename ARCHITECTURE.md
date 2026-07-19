# gridtokenx-trading-service — Architecture

> The energy-trading and ERC microservice: a modular-monolith Cargo workspace whose pure
> Continuous-Double-Auction (CDA) matching engine clears GRID/GRX orders and settles them on Solana
> **through Chain Bridge** — never via direct RPC.
>
> This repo is a **git submodule** of the `gridtokenx-coresystem` superproject. Platform-wide rules
> live in the superproject. This doc covers **only** the contents of this folder.

---

## 1. What This Is

`gridtokenx-trading-service` is an **independent Cargo workspace** that builds a **single binary**
(`bin/trading-service`). It is deliberately kept **out of the superproject root workspace** because
its Solana dependencies pull in BPF target conflicts; always run `cargo` from this directory, never
the repo root.

It is a **modular monolith**: one process boots the API server and all background workers. The
workspace is split into seven library crates plus the binary, wired along a strict layered
dependency direction. The hot path — order matching — is a **pure, synchronous, I/O-free** engine
(`trading-engine`); everything async (DB, blockchain, events, HTTP) lives at the edges.

Workspace `version` 0.1.1, `edition` 2021. Lints are enforced workspace-wide
(`[workspace.lints]`): `unsafe_code = "deny"`, `clippy::unwrap_used = "deny"`,
`clippy::pedantic = "warn"` — code that `.unwrap()`s will not compile.

## 2. Module Layout

```
crates/
├── trading-core/                    domain primitives — every crate depends on this (sync)
│   └── src/
│       ├── lib.rs
│       ├── types.rs                 core enums/IDs (TimeInForce, …)
│       ├── models.rs                domain models
│       ├── traits.rs                ALL repo/gateway traits (the DI contracts)
│       ├── error.rs                 ApiError
│       ├── config/                  Config::from_env(), tokenization config
│       ├── fast_price.rs            FastPrice fixed-point (no floats on hot path)
│       ├── numeric.rs               numeric helpers
│       └── events.rs                domain event types
├── trading-engine/                  PURE CDA matcher — zero async, zero I/O, zero DB (sync)
│   └── src/
│       ├── engine.rs                MatchingEngine::match_cycle, TopologySnapshot trait
│       ├── types.rs                 FastOrder, MatchResult, OrderMetadata, CycleStats
│       └── lib.rs
├── trading-persistence/             Postgres repos implementing core traits — runtime SQLx (async)
│   └── src/
│       ├── pool.rs
│       └── repositories/            order, settlement, outbox, futures, carbon, conditional,
│                                    recurring, vpp, epoch, analytics, price_alert, meter,
│                                    read_model (wallet/meter read-models, post DB-split)
├── trading-infra/                   external adapters (async)
│   └── src/
│       ├── blockchain/              BlockchainService → Chain Bridge (rpc, settlement, wallet)
│       ├── events/                  Kafka + outbox EventBus, kafka_consumer, outbox_worker
│       ├── cache/                   Redis
│       ├── identity/                IAM gateway + signer
│       ├── audit/                   audit log + worker
│       ├── telemetry/               init_telemetry
│       └── metrics.rs               Prometheus metrics
├── trading-logic/                   services + background workers (async)
│   └── src/
│       ├── matcher_service.rs       MatcherService (drives trading-engine)
│       ├── settlement.rs            SettlementService (settles via BlockchainGateway)
│       ├── vpp.rs                   VppService (virtual power plant)
│       ├── forecasting.rs           VPP forecasting
│       ├── clearing.rs              market clearing
│       ├── futures_service.rs       futures
│       ├── erc.rs / energy.rs       carbon/REC + energy logic
│       ├── trigger_evaluator.rs     conditional-order evaluation
│       ├── recurring_evaluator.rs   recurring-order evaluation
│       ├── rehydration.rs           order-book rehydration on boot
│       ├── market_data.rs / participant.rs / p2p_config.rs
│       └── workers/                 matcher, clearing, settlement, supply_sync, reaper,
│                                    recurring, trigger, read_model_feed
├── trading-api/                     ConnectRPC + REST surface (async)
│   └── src/
│       ├── startup.rs               run(state, port, grpc_port, token) — serves HTTP + gRPC
│       ├── state.rs                 AppState (DI surface for handlers)
│       ├── handlers.rs              ConnectRPC handlers
│       ├── rest.rs                  REST routes
│       ├── auth.rs                  header-based auth (APISIX-injected)
│       └── middleware.rs
└── trading-protocol/                wire types
    ├── proto/trading.proto          the authoritative proto
    ├── build.rs                     connectrpc_build → OUT_DIR/_trading_include.rs
    └── src/lib.rs                   generated stubs + hand-written google.protobuf.Empty

bin/trading-service/                 the single binary
└── src/
    ├── main.rs                      boots Config, DB pool, ServiceBuilder, spawns workers + server
    ├── builder.rs                   ServiceBuilder — the one wiring point
    └── lib.rs
    tests/                           repository / settlement / api_routing integration tests

migrations/                          empty (.keep only) — schema owned externally, NOT this service
```

## 3. Architecture

### Layered dependency direction

```
trading-core → trading-engine / persistence / infra → trading-logic → trading-api → bin/trading-service
```

Never reversed. `trading-core` owns the domain primitives and **all** the DI traits; every other
crate depends on it. Business logic never imports HTTP types; handlers never import SQL.

### The CDA matching engine (`trading-engine`)

`MatchingEngine::match_cycle` (`crates/trading-engine/src/engine.rs`) is a **pure synchronous
function** — no async, no I/O, no DB — implementing a **Continuous Double Auction**. It segments
active orders into **zone-segmented order books** (`HashMap<Option<i32>, BTreeMap<(price, created_at,
id), idx>>`) enforcing **price-time priority**, applies grid topology via the injected
`TopologySnapshot` trait (wheeling charges, loss factors, flow accommodation, intra-zone discount),
and returns `Vec<MatchResult>` + `CycleStats`. Price math uses `FastPrice` fixed-point — **no floats
on the hot path**. Keep this crate pure and dependency-light.

When there is a **single zone book**, the per-buy scan takes a fast path: that book's `range` already
yields sells in landed-cost order (zone-constant fees), so the engine skips the per-buy candidate sort
and stops building candidates once it has gathered enough resting energy to fill the buy — turning the
scan from O(all crossing sells) into O(sells actually needed) and the full-cross cycle from ~O(N²) to
~O(N·log N). Multi-zone cycles keep the full build-and-sort (the global landed order interleaves books);
a rare grid-capacity skip in the fast path falls back to one full-scan rebuild for that buy so results
are identical to the exhaustive path.

### Settlement via Chain Bridge

There is **no direct Solana RPC**. `SettlementService` (`trading-logic/src/settlement.rs`) settles
through an `Arc<dyn BlockchainGateway>` whose concrete impl is `BlockchainService`
(`trading-infra/src/blockchain/`), which talks to **Chain Bridge** (`CHAIN_BRIDGE_URL`, default
`http://127.0.0.1:5040`). Supply-sync reads tokenized supply through the same gateway.

### Match → settlement pipeline (end-to-end, verified)

A trade travels two independent worker loops. The matcher persists intent; the settlement drain
mints it on-chain. Every stage is crash-safe and replay-safe.

**1. Match → persist** — same matcher cycle, every 1s. `MatcherWorker` →
`MatcherService::run_matching_cycle` (`crates/trading-logic/src/matcher_service.rs:28`). The CDA
engine `MatchingEngine::match_cycle` returns matches (pure, no I/O); `clearing_support::persist_matches`
(`crates/trading-logic/src/clearing_support.rs:71`) writes **atomically per match**: order fill
deltas → `PartiallyFilled`/`Filled` (`:214`), a match row (`insert_match_with_event`, status
pending, `:187`), and a `Settlement` row status `Pending` (`insert_settlement`, `:123`). IOC
remainders are cancelled last.

**2. Drain loop** — `SettlementWorker`, every 10s, batch 10 (`crates/trading-logic/src/workers/settlement.rs:34`).
`reclaim_stale_settlements` runs **first** — recovers rows orphaned in `processing` beyond
`STALE_PROCESSING_SECS` (300s, `settlement.rs:244`; crash between claim and finalize) — then
`SettlementService::process_pending_settlements` (`settlement.rs:45`).

**3. Claim → on-chain** (`crates/trading-logic/src/settlement.rs`). `get_pending_settlements(limit)`
then the atomic `claim_settlements_for_processing` flips `Pending → Processing` (`:54`). Concurrent
workers / RPC get disjoint subsets → no double-mint. `settle_claimed` (`:101`) →
`blockchain.execute_batched_settlements(claimed)`.

**4. On-chain atomic swap** (`crates/trading-infra/src/blockchain/settlement.rs:266`). Builds **one
batched tx**: per match a `build_atomic_settlement_instruction` (trading program atomic settlement —
a **swap, never a mint**). Funds source = platform pooled escrow ATAs (`buyer_currency_escrow`,
`seller_energy_escrow`); `escrow_authority == market.authority == platform` → single bridge
signature (`:148-153`). Idempotent create-ATA for seller-currency + buyer-energy receivers. Amounts:
energy `*1e9`, price/wheeling/loss `*1e6` atomic (`:131`, `:135`, `:138`, `:143`). Per-match id =
settlement UUID → on-chain `TradeNullifier` (F3c) rejects replay (`:196`, `:405`, `:431`). Submitted
via `execute_batched_instructions` → Chain Bridge (no direct Solana RPC). **F3a**: gates on
**finality** — `wait_for_confirmation` (default 30s, `:439`), not bridge RPC-accept; timeout → `Err`.

**5. Finalize** (`settle_claimed`). Minted (in `tx_results`) → `Completed` + tx sig,
`SettlementProcessed` outbox event, audit log (`:150-181`); oracle-direct settlements (no
`trade_id`) also get ERC issued. Batch call errored → nothing minted → whole batch
`reset_settlements_for_retry` back to `Pending` (or `permanently_failed` after
`MAX_SETTLEMENT_RETRIES = 5`, `:240`) via `:122`. Claimed but absent from results → never minted →
released for retry (`:225`). **Post-mint DB write fails → does NOT reset to pending** (would
double-mint); forces the row out of `processing` via a plain status write, logs loud (`:169-193`).

Events ride the transactional outbox (below) → `OutboxWorker` → `EventBus` (Redis always, Kafka if
`KAFKA_EVENTS_ENABLED`).

### Transactional outbox for events

Domain events are written to a DB **outbox** in the same transaction as state changes
(`OutboxRepository` / outbox repo), then `OutboxWorker` relays them to the `EventBus`. The EventBus
fans out to **Redis Streams** always and **Kafka** when `KAFKA_EVENTS_ENABLED=true`. Services never
publish to Kafka/Redis directly — they go through the outbox so events stay consistent with DB writes.

### Read-model feed (DB-per-service Phase 1)

Post DB-split the trading pool no longer sees IAM/meter tables, so wallet and meter data trading
needs are mirrored into **local read-model tables** (`iam_wallet_read_model`, `meter_read_model`).
Two halves, both gated on `TRADING_READMODEL_FEED=true`:

- **Boot backfill** — runs once in `ServiceBuilder::build`, snapshots the source tables via
  **read-only** connections `READMODEL_IAM_DATABASE_URL` / `READMODEL_METER_DATABASE_URL`
  (`Config::readmodel_iam_db_url` / `readmodel_meter_db_url`,
  `crates/trading-core/src/config/mod.rs`). Missing URL → that backfill is skipped (the event feed
  still applies deltas).
- **`ReadModelFeedWorker`** — a stable-group Kafka `StreamConsumer`
  (`crates/trading-logic/src/workers/read_model_feed.rs`) that streams wallet/meter change events
  and applies them to the read-models. Feeds into `WalletReadModelRepository` /
  `MeterReadModelRepository`. Never `.unwrap()`s the consumer: a build/subscribe failure disables
  the feed rather than crashing the process.

### ServiceBuilder — the single wiring point

All collaborators are `Arc<dyn Trait>` traits defined in `trading-core/src/traits.rs`
(`OrderRepository`, `SettlementRepository`, `FuturesRepository`, `CarbonRepository`,
`AnalyticsRepository`, `VppRepository`, `OutboxRepository`, `BlockchainGateway`, `EventPublisher`,
`IdentityGateway`, `AuditLog`, `CacheStore`, …). Concrete impls are constructed in **exactly one
place**: `ServiceBuilder::build` (`bin/trading-service/src/builder.rs`), which returns an
`Infrastructure` (repos + gateways, incl. the optional `ReadModelFeedWorker`) and `AppServices`
(`SettlementService`, `MatcherService`, `ClearingService`, `VppService`, `RecurringEvaluator`,
`TriggerEvaluator`). To add a dependency: define the trait in `core`, implement it in
`persistence`/`infra`, wire it in `builder.rs`, and surface it on `AppState` if a handler needs it.

### Runtime topology (`bin/trading-service/src/main.rs`)

`main` loads `Config::from_env()`, opens the Postgres pool, calls `ServiceBuilder::build`, then
`tokio::spawn`s long-running workers alongside the API server under a `CancellationToken`:

| Worker | Cadence | Role |
| :--- | :--- | :--- |
| `MatcherWorker` | every 1s | drains/matches realtime orders via `MatcherService` → `trading-engine` (CDA) |
| `ClearingWorker` | every 60s | uniform-price clears + closes interval epochs whose 15-min window elapsed |
| `ReaperWorker` | every 10s | flips orders past `expires_at` to `Expired`; sole expiry mechanism (the active-order queries no longer filter on `expires_at`), supervised with respawn + a 2×-cadence DB-call timeout |
| `SettlementWorker` | every 10s, batch 10 | settles through Chain Bridge |
| `SupplySyncWorker` | polling interval | syncs tokenized supply from blockchain; graceful-degrades |
| `RecurringEvaluatorWorker` | periodic | materializes due recurring orders |
| `TriggerEvaluatorWorker` | periodic | evaluates conditional/trigger orders |
| `ReadModelFeedWorker` | Kafka-streamed | present only when `TRADING_READMODEL_FEED=true`; mirrors IAM/meter events into local read-models (see below) |

The API server (`trading_api::startup::run`) serves both REST and ConnectRPC; default ports are
`HTTP_PORT = 8093` and `GRPC_PORT = 8092` (override via env).

## 4. Load-Bearing Invariants

1. **Standalone workspace, never the root.** This service is excluded from the superproject root
   workspace due to BPF target conflicts. Run `cargo` from this directory only.
2. **Keep `trading-engine` pure.** The CDA matcher is the hot path — zero async, zero I/O, zero DB.
   Don't introduce side effects; feed it data and consume its `MatchResult`s.
3. **No floats on the price path.** Use `FastPrice` / `rust_decimal` fixed-point, never `f64`.
4. **No direct Solana RPC.** All blockchain access goes through `BlockchainGateway` → Chain Bridge.
5. **Events go through the outbox.** Never publish to Kafka/Redis directly from services — write to
   the DB outbox in the same transaction, let `OutboxWorker` relay.
6. **Runtime SQLx, not macros — overrides root CLAUDE.md.** Persistence uses the **runtime**
   `sqlx::query(...)` API (no `query!`/`query_as!`, no `.sqlx/` dir). Consequence: `cargo
   check`/`build` need **no** `DATABASE_URL` and no running DB; there is **nothing to `cargo sqlx
   prepare`**. Follow the existing pattern in `crates/trading-persistence/src/repositories/`.
7. **Migrations are not owned here.** `migrations/` holds only `.keep`; the trading schema is
   provisioned externally (superproject `just migrate` / IAM). Point `DATABASE_URL` at an
   already-migrated DB; don't add a `sqlx::migrate!` call expecting local migrations.
8. **`.unwrap()` does not compile.** `clippy::unwrap_used = "deny"` and `unsafe_code = "deny"` are
   workspace lints. Use `?`, `.expect("reason")`, or `.context()`.
9. **Proto codegen is connectrpc_build (buffa), not prost/tonic — overrides root CLAUDE.md.**
   `crates/trading-protocol/build.rs` compiles `proto/trading.proto` into
   `OUT_DIR/_trading_include.rs`; `trading-protocol` supplies a hand-written `buffa::Message` impl
   for `google.protobuf.Empty`. The authoritative proto lives in the `trading-protocol` crate.
10. **Wire dependencies in `builder.rs` only.** `ServiceBuilder::build` is the single composition
    root; keep construction of `Arc<dyn Trait>` impls there.

## 5. Commands

Standalone workspace — run from this directory, never the repo root.

```bash
cargo check                              # fast feedback across all crates (no DATABASE_URL needed)
cargo build --release                    # binary -> target/release/trading-service
cargo clippy -- -D warnings              # strict lints (unwrap_used = deny, pedantic = warn)
cargo test                               # unit tests
cargo test -p trading-engine             # one crate
cargo test test_order_matching -- --nocapture   # one test by name

# Integration tests (need a live Postgres) live in bin/trading-service/tests/
cargo test -p trading-service --test repository_integration_test
cargo test -p trading-service --test settlement_integration_test
cargo test -p trading-service --test api_routing_test
```

## Further Reading (in this repo)

| File | Covers |
| :--- | :--- |
| `CLAUDE.md` | Fast orientation + LLM working rules (build, SQLx/migrations overrides, env) |
| `AGENTS.md` | Contributor guide |
| `Cargo.toml` | Workspace members, lints, shared dependency versions |
| `Dockerfile` | Container build + deploy-time port mapping |
| `crates/trading-protocol/proto/trading.proto` | ConnectRPC wire contract |
