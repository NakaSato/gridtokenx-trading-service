---
name: gridtokenx-trading-architecture
description: Guidance for structuring the Rust codebase behind GridTokenX's trading services — the Solana/Anchor on-chain programs, the off-chain matching and settlement pipeline, the oracle bridge, and the operator/API tier. Use this skill whenever the user asks how to organize, lay out, or refactor any part of GridTokenX's trading stack; where a piece of code belongs (on-chain vs off-chain, program vs SDK vs service); how to split or combine crates across the ten programs (Token Mint, Order Book CDA, Atomic Settlement, REC NFT, Dual-Tracker, V2G Fleet Dispatch, Grid Reliability Oracle, Demand Response Incentive, Forecasting Oracle, Demand Profile); how the GRID/GRX dual-token code should be structured; how to handle `no_std` on-chain vs tokio off-chain; or how to share types between programs, clients, oracles, and the matching engine. Trigger this even when the question is casual ("where does the NILM oracle client live?", "should settlement be one crate or three?") or scoped to a subpart — the on-chain/off-chain boundary and the cross-program type sharing story are interlinked and benefit from the broader frame.
---

# GridTokenX Trading Services Architecture

A skill for structuring the Rust codebase that powers GridTokenX's P2P solar trading platform: ten Anchor programs on Solana, the off-chain CDA matching engine, the oracle bridge feeding meter and forecast data on-chain, and the operator-facing API and SDK tiers. The core idea: structure decisions are trade-offs shaped by the on-chain/off-chain split, Anchor's conventions, and the dual-token model — not generic Rust prescriptions.

## Core Stance: The Chain Boundary Drives Everything

The single most consequential axis in this codebase is **on-chain vs off-chain**. It determines `no_std` vs `std`, SBF vs native target, strict account-size budgets vs normal memory, deterministic execution vs async I/O, and what "a crate" even means (Anchor programs are crates with fixed shape; off-chain services are free-form workspaces).

Three questions worth holding throughout any architecture conversation on GridTokenX:
1. **Does this need to execute on-chain, or can it live off-chain and produce a signed transaction?** On-chain is expensive (compute units, account rent, audit surface). Default off-chain; move on-chain only when trust-minimization or atomic settlement genuinely requires it.
2. **Which side of the oracle boundary is this?** Data flowing onto the chain has to be signed, bounded, and replay-safe. Data flowing off the chain is just an RPC read.
3. **Is this shared across programs, across services, or across the chain boundary?** Each answer has a different home.

## The Trading Stack: What Lives Where

Before any crate layout, fix the mental model of the stack. GridTokenX has five tiers; every piece of code belongs to exactly one.

| Tier | Runtime | Purpose | Example components |
|------|---------|---------|--------------------|
| **On-chain programs** | Solana BPF, `no_std`-ish, Anchor | Custody, settlement, token mint, NFT issuance, on-chain registries | Token Mint, Order Book CDA state, Atomic Settlement, REC NFT, Dual-Tracker, Demand Response Incentive |
| **Oracle bridge** | Off-chain tokio, signs transactions | Ingests meter/forecast/grid data, publishes to on-chain oracles | Forecasting Oracle publisher, Grid Reliability Oracle publisher, Demand Profile/NILM publisher |
| **Matching engine** | Off-chain tokio, high-throughput | Continuous Double Auction, order ingestion, fill production, handoff to settlement | Order book, matcher, fill builder, settlement dispatcher |
| **API / operator tier** | Off-chain tokio, HTTP/gRPC | Prosumer/consumer clients, PEA operator dashboards, auth, rate limiting | REST/gRPC gateways, auth, admin endpoints |
| **SDK / shared** | Either | Wire types, program IDs, instruction builders, client helpers | Anchor-generated IDLs, typed instruction builders, domain primitives |

The **V2G Fleet Dispatch** program is a partial exception: its dispatch decisions are DRL-driven off-chain but the commitment and reward distribution are on-chain. Treat it as a pair — off-chain policy crate + on-chain reward/commitment program.

## Workspace Layout: The Recommended Shape

GridTokenX is a layered workspace, not microservices (yet — see the microservices section for when that changes). Ten programs plus off-chain services plus shared SDK plus operator tooling is the right scale for a single workspace with 15–25 crates. Reth and Foundry are the reference points, not a constellation of repos.

```
gridtokenx/
├── Cargo.toml                          # virtual manifest — no code at root
├── rust-toolchain.toml                 # pin for BPF compatibility
├── Anchor.toml                         # Anchor workspace config
├── ARCHITECTURE.md                     # the one-page map
│
├── programs/                           # on-chain Anchor programs (one crate each)
│   ├── token-mint/                     # GRID token (1 kWh = 1 GRID), mint authority
│   ├── order-book/                     # CDA state, open/cancel order instructions
│   ├── settlement/                     # atomic settlement, escrow release
│   ├── rec-nft/                        # ERC-1155-analog REC issuance on Solana
│   ├── dual-tracker/                   # dual-tracker protocol state
│   ├── v2g-dispatch/                   # commitment + reward distribution
│   ├── reliability-oracle/             # LOLE/SAIDI on-chain publish target
│   ├── demand-response/                # gamified DR incentive distribution
│   ├── forecasting-oracle/             # CTT–ViT–Transformer output publish target
│   └── demand-profile/                 # NILM-derived profile commitments
│
├── crates/                             # off-chain libraries
│   ├── primitives/                     # no_std: energy units, errors, IDs, GRID/GRX amounts
│   ├── protocol/                       # wire types shared on-chain ↔ off-chain (no_std)
│   ├── sdk/                            # instruction builders, account helpers, IDL re-exports
│   ├── matching/
│   │   ├── engine/                     # pure sync CDA matcher — no I/O, no async
│   │   └── orderbook/                  # in-memory order book data structures
│   ├── oracle/
│   │   ├── bridge/                     # common publisher scaffolding, retry, signing
│   │   ├── forecasting/                # CTT–ViT–Transformer client + publisher
│   │   ├── reliability/                # grid reliability source + publisher
│   │   └── demand-profile/             # NILM client + publisher
│   ├── services/
│   │   ├── matcher-service/            # wraps matching engine with tokio + chain handoff
│   │   ├── settlement-service/         # watches matcher output, submits settlement tx
│   │   └── dr-coordinator/             # gamification + DR event orchestration
│   ├── api/
│   │   ├── gateway/                    # HTTP/gRPC for prosumers, consumers, PEA ops
│   │   └── types/                      # API request/response types (no domain logic)
│   ├── storage/                        # Postgres schemas, migrations, repositories
│   ├── telemetry/                      # tracing, metrics, structured logging setup
│   └── grx/
│       ├── pricing/                    # USD-anchored pricing, TWAP oracle client
│       └── dex-integration/            # Raydium CPMM swap quote builder
│
├── bin/                                # binaries — thin main.rs + wiring
│   ├── matcher-service/
│   ├── settlement-service/
│   ├── oracle-publishers/              # one bin with subcommands, or split per oracle
│   ├── api-gateway/
│   └── admin-cli/                      # operator CLI: PEA ops, ERC sandbox reports
│
├── xtask/                              # repo automation in Rust
├── migrations/                         # SQL migrations
├── tests/                              # workspace-level integration tests
│   ├── integration/                    # validator-backed end-to-end
│   └── contracts/                      # cross-program invariant tests
└── deploy/                             # Docker, k8s, anchor deploy scripts
```

Push back on two temptations:
- **Splitting every domain into its own crate** ("nilm-types", "nilm-client", "nilm-publisher", "nilm-schema"). Start with one crate per oracle, split only when compile time or team boundaries make it necessary.
- **A giant `shared` or `common` crate.** Route shared types through `primitives` (no_std, pure types), `protocol` (on-chain/off-chain wire), or `sdk` (client-side Anchor helpers). If something doesn't fit those three, it probably doesn't belong in shared.

## The Dependency Direction Rule (GridTokenX Flavor)

Strict acyclic layering, bottom to top. Lower layers must never depend on higher ones:

```
   bin/*  (matcher-service, settlement-service, api-gateway, oracle-publishers)
        │
   api/*, services/*                    ← adapters, tokio, I/O
        │
   matching/*, oracle/*, grx/*, storage ← domain services, still async at edges
        │
   sdk, protocol                        ← on-chain/off-chain bridge types
        │
   primitives                           ← no_std, pure types, energy units, errors
        │
   (programs/* depend only on: primitives, protocol, anchor-lang)
```

**The on-chain programs sit on a parallel track.** They depend on `primitives` and `protocol` only — never on `sdk`, `services`, `storage`, or `api`. This is enforced by the fact that programs compile to SBF and those upper crates pull in tokio, reqwest, sqlx. The type system gives you this for free if you keep `primitives` and `protocol` `no_std`.

**The `sdk` crate is the unique bidirectional seam.** It re-exports Anchor IDLs (generated from the programs), builds instructions, parses account data. Off-chain services depend on `sdk`. Programs do not.

## Crate Splitting: The Ten Programs

Each Anchor program is its own crate under `programs/`. This is not a design choice — Anchor requires it. What *is* a choice: how much logic lives inside the program crate vs a sibling off-chain crate.

Rule of thumb for each program:
- **Program crate**: instructions, account validation, CPI calls, state structs. Nothing that isn't needed on-chain.
- **Pure logic that both on-chain and off-chain need** (e.g., CDA matching math, fill price calculation, REC eligibility rules): put it in a `no_std` library under `crates/` and depend on it from both the program and the off-chain service.

**Concrete example — Order Book CDA:**
- `programs/order-book/` — place_order, cancel_order, account state, access control. Does NOT match orders (matching happens off-chain).
- `crates/matching/engine/` — the actual CDA algorithm, pure sync, `no_std`-compatible if feasible. The off-chain matcher imports this.
- `crates/sdk/` — instruction builders so clients can place orders without hand-rolling the instruction.

**Concrete example — Atomic Settlement:**
- `programs/settlement/` — the atomic swap instruction, escrow accounts, transfer CPIs to Token Mint.
- `crates/services/settlement-service/` — off-chain watcher that consumes matcher fills and submits settlement transactions with proper retry and confirmation handling.
- No shared business logic crate — settlement atomicity is entirely on-chain.

**Concrete example — Forecasting Oracle:**
- `programs/forecasting-oracle/` — on-chain storage of forecasts, access control on who can publish.
- `crates/oracle/forecasting/` — the CTT–ViT–Transformer client (calls the model service), validation, publisher loop.
- `crates/oracle/bridge/` — common scaffolding (signing, retry, publish cadence) shared with other oracles.

## Primitives and Protocol: The Two Crates You Cannot Get Wrong

If anything in this architecture deserves over-investment, it's `primitives` and `protocol`. Everything else can be refactored; these two sit at the bottom and churn is expensive.

**`crates/primitives/`** — `no_std`, minimal dependencies, pure types:
- Energy units: `KilowattHours`, `Watts`, `Joules` with `Serialize`/`Deserialize` and conversions
- Token amounts: `GridAmount`, `GrxAmount` — newtypes over `u64` with explicit decimals
- Identifiers: `ProsumerId`, `MeterId`, `OrderId`, `FillId`
- Domain errors (the `thiserror`-friendly ones that both on-chain and off-chain code raise)
- Time types compatible with Solana's `Clock` sysvar semantics
- No async. No I/O. No logging. No tokio. No reqwest. No serde_json (use serde + borsh).

**`crates/protocol/`** — `no_std`, wire types on the on-chain/off-chain boundary:
- Account state shapes (mirror of Anchor `#[account]` structs, when Borsh-compatible sharing is viable)
- Instruction argument types
- Event types emitted by programs
- Oracle payload schemas (what the publisher serializes; what the on-chain program deserializes)

Keeping these `no_std` means they compile into the programs *and* the services, so there is literally one definition of `KilowattHours` across the entire stack. Drift between on-chain and off-chain representations is the most common bug class in blockchain-energy systems. Kill it structurally.

## GRID and GRX: Keep the Token Models Separate

GRID and GRX have different tokenomics, different regulatory posture, and different lifecycles. Don't share crates across them unless there's a concrete reason.

- **GRID** (1 kWh = 1 GRID, minted from verified trades): `programs/token-mint/` and amount types in `primitives`. The minting logic is tightly coupled to settlement and REC issuance — all on the same Solana program set.
- **GRX** (100M fixed supply, SEC Group 1, AMM-priced, burn-on-redemption): `crates/grx/pricing/` and `crates/grx/dex-integration/` on the off-chain side. The GRX token itself is an SPL Token-2022 mint — you don't need a bespoke program for it. Raydium CPMM integration, Switchboard TWAP, Jupiter quote pattern all live in `crates/grx/`.

The hybrid USD-anchored pricing logic (60-second quote window, 2% slippage, TWAP oracle) belongs in `crates/grx/pricing/` as a pure library that the API gateway and the redemption flow both call. Resist putting it on-chain — the USD anchor is off-chain state, and putting AMM-dependent pricing on-chain multiplies the oracle attack surface. This is the single highest-risk design decision in the GRX stack; flag it clearly if a user proposes otherwise.

## Async Strategy: Sync Core, Async Edges — Strictly

Default position across the entire off-chain stack:

- **`matching/engine/`** — pure sync. The CDA matcher takes orders in, produces fills out, no async, no I/O. Testable with plain unit tests, no runtime needed. This is the hottest path in the system; keeping it sync also keeps it fast.
- **`services/*`** — async. Wraps the sync matcher with tokio channels for order ingestion and fill dispatch. Handles backpressure, retries, chain submission.
- **`oracle/*`** — async at the publisher boundary, sync in the validation/transformation logic. The ML inference clients are async (HTTP calls to model services), but the validation that outputs a signed payload is pure.
- **`api/*`** — async throughout (axum or tonic).

The mistake to avoid: making the matcher async because "everything else is async." Async infects call graphs; keep it out of pure logic. For a CDA matching engine specifically, sync + channels at the boundary is the dominant production pattern.

On-chain programs are irrelevant to this discussion — Solana execution is synchronous and deterministic, full stop.

## Errors: Hybrid, Oriented by Layer

Use the standard Rust hybrid, calibrated to GridTokenX's layers:

- **`primitives` errors** — `thiserror`, typed. Every domain error variant named explicitly. These will be matched on by callers across both programs and services.
- **Program errors** — Anchor's `#[error_code]` enum, mapped one-to-one from `primitives` errors where possible. Anchor constrains this; don't fight it.
- **`matching/engine`, `oracle/*`, `grx/*`** — `thiserror`, typed. These are libraries; callers need to distinguish errors programmatically (e.g., matcher-service needs to tell "order rejected: self-trade" from "order rejected: insufficient balance").
- **`services/*`, `api/*`, `bin/*`** — `anyhow` at the outer layer, with `.context()` on every `?`. The binary doesn't need typed errors; it needs good error messages in logs and tracing spans.

Never `pub use anyhow::Error` from a library crate. Services and binaries can use it freely.

## Visibility and SDK Stability

- **Program crates**: Anchor dictates most of this. Public instructions are public; state structs are typically `pub` because clients deserialize them. No ceremony needed.
- **`primitives`, `protocol`, `sdk`**: these are your stable contract. Once other teams (or external prosumers writing integrations) depend on them, `pub` is a commitment. Default to `pub(crate)`; promote to `pub` deliberately.
- **Internal service crates (`services/*`, `api/gateway`, etc.)**: default to `pub` for ergonomics. Don't over-engineer visibility for code only you and the binary consume.
- **`matching/engine`**: treat as a library. Public surface is the matcher API, fill types, and error types. Order book internals stay `pub(crate)`.

## Feature Flags: A Short List

GridTokenX does not need many feature flags. The ones that are defensible:

- `primitives`: `std` (default on for off-chain, off for program builds)
- `sdk`: `mainnet` / `devnet` / `localnet` for embedded cluster configs, or use runtime config instead (prefer runtime — see the generic rule that modes should be runtime, not compile-time)
- `oracle/forecasting`: `mock` for swapping out the CTT–ViT–Transformer client in tests
- `grx/dex-integration`: `raydium-cpmm` (default) — leave the door open for other AMMs without committing

Avoid feature flags for "enable/disable the NILM oracle" — that's a deployment-time config, not a compile-time choice.

## The Oracle Bridge: The Hardest Part of the Codebase

You've already identified oracle integrity as the hardest unsolved challenge in blockchain-energy. The architecture should reflect that. Put disproportionate care into `crates/oracle/bridge/`:

- **Signed payloads end-to-end**: the meter/forecast source signs, the bridge verifies, the bridge re-signs for chain submission. Keep the signing abstraction in `bridge` and inject per-oracle implementations.
- **Replay protection**: every oracle payload carries a monotonic sequence number and a timestamp bounded by Solana `Clock`. The on-chain program rejects out-of-order or stale payloads.
- **Publisher = one tokio task per oracle**, all built on the same `bridge::Publisher` scaffolding. Metrics, retries, and failure handling live in `bridge`; per-oracle code only provides the source and the payload schema.
- **Model inference lives behind a trait**. The CTT–ViT–Transformer may run as a separate Python service accessed over gRPC; the NILM model similarly. Don't compile ML into Rust crates — wrap them.

This is also where the proposed Edge Intelligence and Oracle Bridge sublayers of the six-sublayer architecture physically land. `oracle/bridge/` is the Oracle Bridge sublayer; any edge-intelligence preprocessing that happens before the publisher (e.g., NILM disaggregation) is its own crate under `oracle/` and feeds the publisher.

## Testing Strategy: Three Layers

- **Unit tests** inside every crate. `matching/engine/` should have the densest test suite in the repo — it's pure logic with clear invariants.
- **Program tests** via Anchor's test framework (`anchor test`) for each program. These run against `solana-test-validator`. Keep them focused on instruction-level correctness and access control.
- **Workspace integration tests** under `tests/integration/` that spin up `solana-test-validator`, deploy all programs, run the matcher and settlement services, and exercise end-to-end flows (order → match → settle → GRID mint → REC NFT). Slow; run in CI, not on every save.
- **Cross-program invariant tests** under `tests/contracts/` that assert properties across programs (e.g., "for every settled fill, GRID supply increased by exactly the settled kWh amount"). These are the tests that catch protocol-level bugs.

`proptest` or `quickcheck` on the matcher is high-leverage. Energy trading has obvious invariants (total volume conserved across a fill, price-time priority) that generative testing catches quickly.

## When (If) to Split into Microservices

GridTokenX is a single workspace today and should stay that way through the PEA Hackathon 2026 submission and through initial sandbox deployment. Real reasons to split later:

- **Matcher and settlement have radically different scaling profiles.** Matcher is latency-sensitive, CPU-heavy; settlement is throughput-bound by Solana confirmation time. Eventually these deploy independently, but the crate split already reflects that — splitting repos is the smaller step.
- **PEA operator tier has stricter reliability guarantees** than prosumer-facing API. This is the likely first real split: extract `bin/api-gateway` and its direct dependencies into a separate deployment (still same repo) before separating repos.
- **ERC sandbox compliance may require isolating GRX redemption flows.** If the regulator requires separate access controls and audit logs, the GRX redemption service becomes its own deployable.

Until those pressures are concrete, resist. The modular-monolith workspace already gives you internal boundaries that become service boundaries when needed. A 5-person team deploying 8 services to meet a hackathon deadline is how hackathon submissions fail.

## Projects Worth Reading

Match the reference to what you're working on:

- **Building or reviewing the overall workspace**: read `reth`'s `docs/repo/layout.md` and its `crates/` tree. Ten+ programs, layered workspace, shared primitives — closest analog.
- **Matching engine patterns**: `openbook-v2` (Solana CDA) for on-chain layout, though GridTokenX matches off-chain. For the off-chain engine itself, look at `databento`-style sync-core patterns or any HFT-adjacent Rust matching engine on GitHub.
- **Anchor program organization at scale**: `drift-v2` and `marginfi-v2` — large Anchor program sets with sibling SDK crates and shared primitives. The cleanest examples of the "programs + crates + sdk" triad.
- **Oracle and bridge patterns**: Pyth's `pyth-crosschain` and Switchboard v3 for publisher scaffolding, retry, and signing conventions.
- **Cross-cutting library design**: `tokio` and `foundry` for feature gating and stable API discipline.
- **`ARCHITECTURE.md` as a practice**: copy `rust-analyzer`'s format. A one-page map of the workspace is worth more than any amount of prose documentation.

## Starter Snippets

### Workspace `Cargo.toml`

```toml
[workspace]
resolver = "2"
members = [
    "programs/*",
    "crates/*",
    "crates/matching/*",
    "crates/oracle/*",
    "crates/services/*",
    "crates/api/*",
    "crates/grx/*",
    "bin/*",
    "xtask",
]

[workspace.package]
version = "0.1.0"
edition = "2021"
rust-version = "1.80"
license = "Apache-2.0"

[workspace.dependencies]
# Solana / Anchor
anchor-lang = "0.30"
anchor-spl = "0.30"
solana-program = "1.18"
solana-sdk = "1.18"
solana-client = "1.18"
spl-token-2022 = "3"

# async / runtime (off-chain only)
tokio = { version = "1.40", features = ["full"] }
tower = "0.5"
axum = "0.7"
tonic = "0.12"

# serialization
serde = { version = "1", features = ["derive"] }
borsh = "1"

# storage (off-chain only)
sqlx = { version = "0.8", features = ["postgres", "runtime-tokio-rustls"] }

# observability
tracing = "0.1"
tracing-subscriber = "0.3"
opentelemetry = "0.24"

# errors
thiserror = "1"
anyhow = "1"

# internal
gridtokenx-primitives = { path = "crates/primitives" }
gridtokenx-protocol = { path = "crates/protocol" }
gridtokenx-sdk = { path = "crates/sdk" }

[workspace.lints.rust]
unsafe_code = "forbid"

[workspace.lints.clippy]
pedantic = { level = "warn", priority = -1 }
unwrap_used = "deny"
expect_used = "warn"
```

### Program crate `Cargo.toml` (e.g., `programs/order-book/`)

```toml
[package]
name = "gridtokenx-order-book"
version.workspace = true
edition.workspace = true

[lib]
crate-type = ["cdylib", "lib"]
name = "gridtokenx_order_book"

[features]
no-entrypoint = []
no-idl = []
no-log-ix-name = []
cpi = ["no-entrypoint"]
default = []

[dependencies]
anchor-lang.workspace = true
gridtokenx-primitives = { workspace = true, default-features = false }
gridtokenx-protocol  = { workspace = true, default-features = false }
```

### `primitives` crate `Cargo.toml`

```toml
[package]
name = "gridtokenx-primitives"
version.workspace = true
edition.workspace = true

[features]
default = ["std"]
std = ["serde/std", "borsh/std"]

[dependencies]
serde = { workspace = true, default-features = false, features = ["derive"] }
borsh = { workspace = true, default-features = false, features = ["derive"] }
thiserror = { version = "1", optional = true }

[lints]
workspace = true
```

### Recommended supporting files
- `rust-toolchain.toml` — pin to a version known-good for Anchor 0.30 + Solana 1.18
- `Anchor.toml` — declare all ten programs, localnet and devnet clusters, wallet paths
- `.cargo/config.toml` — BPF target aliases, clippy settings
- `clippy.toml`, `rustfmt.toml`
- `deny.toml` — critical when your audit surface includes financial primitives
- `ARCHITECTURE.md` — one page: tier diagram, program inventory with one-sentence purposes, the on-chain/off-chain boundary explained

## Pitfalls Specific to GridTokenX

Watch for these when reviewing or writing:

- **Duplicate type definitions between program and service.** `KilowattHours` defined in `primitives` and also redefined in the matcher because `primitives` wasn't `no_std`-clean. Fix by keeping `primitives` strictly `no_std` and dependency-free.
- **Tokio or reqwest leaking into a program crate.** If `cargo build-sbf` fails on a program, check dependency graphs — a transitive tokio dep is the usual culprit.
- **Shared domain types in a `common` crate across services.** Route through `primitives`/`protocol`/`sdk`; don't create a fourth shared crate for "things that didn't fit."
- **On-chain matching logic.** A 2,000-line CDA in an Anchor program is a red flag: compute unit budgets will bite, and auditors will charge for it. Off-chain matcher + on-chain settlement is the pattern.
- **Unbounded `Vec` in account structs.** Solana accounts have fixed rent-exempt sizes; any `Vec` or `String` in `#[account]` state needs a documented maximum and a compile-time size calculation.
- **`unwrap()` in program code.** Panics in on-chain code are silently converted to generic failures. Every `unwrap` in `programs/*` is a correctness bug waiting to happen. Use clippy `unwrap_used = "deny"` on program crates.
- **GRX pricing logic leaking on-chain.** USD-anchored pricing depends on off-chain oracles (Switchboard TWAP). Keep it off-chain; the on-chain program accepts the quote as an input, doesn't recompute it.
- **No `ARCHITECTURE.md`.** At ten programs plus 15+ off-chain crates, a new contributor (or a judge reading the submission) cannot orient without one.

## How to Run a Conversation

When a user asks for structural advice on GridTokenX:

1. **Locate the question in the stack.** Is this on-chain, off-chain, or cross-boundary? The answer to almost every "where does this live?" question falls out of that.
2. **Check whether a generic Rust answer works.** If yes, give it — don't invent GridTokenX-specific nuance where none is needed. If no (account sizing, CPI constraints, no_std requirements, Anchor conventions), flag the specifics clearly.
3. **Recommend the smallest structure that fits.** Ten programs already forces a certain workspace shape; resist adding more crates without concrete justification (compile time, team boundary, `no_std` requirement, distinct deployable).
4. **Show the on-chain/off-chain boundary explicitly.** For any code under discussion, say which side it lives on and why. This is the seam where mistakes are most expensive.
5. **Be concrete.** Reference the actual program names (Token Mint, Order Book CDA, …) and the actual off-chain components (matcher, oracle bridge, GRX pricing). Don't abstract into "the trading service" when "the matcher-service consuming fills from matching::engine and handing off to settlement-service" is clearer.
6. **Suggest `ARCHITECTURE.md` early.** A one-page map of tiers + program inventory is the single highest-leverage artifact for both onboarding and competition submission.
