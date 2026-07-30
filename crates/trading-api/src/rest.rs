use crate::state::AppState;
use axum::{
    extract::{Path, Query, State},
    http::header,
    response::{IntoResponse, Response},
    Json,
};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use tracing::info;
use utoipa::{IntoParams, ToSchema};
use trading_core::models::{
    MarketPrice, NewPriceAlert, NewRecurringOrder, OrderBookEntry, PriceAlert, RecurringOrder,
    Settlement, SettlementStats, TradingOrder,
};
use trading_core::recurring::next_execution_at;
use trading_core::types::{
    AlertCondition, AlertStatus, IntervalType, OrderSide, OrderStatus, OrderType, RecurringStatus,
    TimeInForce,
};
use uuid::Uuid;

#[derive(Debug, Deserialize, ToSchema)]
pub struct SubmitOrderRequest {
    pub side: String,
    pub order_type: String,
    pub energy_amount_kwh: String,
    /// Required for limit orders. For a market buy it is optional and, if given,
    /// acts as the maximum acceptable price (slippage cap); absent → a wide
    /// default ceiling. A market buy still fills at the resting ask, not this value.
    pub price_per_kwh: Option<String>,
    pub zone_id: i32,
    /// The metering `meters.id`. Callers holding a `serial_number` (the grid
    /// map's node id) must send `meter_serial` instead — the two id spaces are
    /// not interchangeable and `trading_orders.meter_id` FKs to `meters.id`.
    pub meter_id: Option<Uuid>,
    /// The meter's `serial_number`, resolved server-side to `meters.id`.
    /// Takes precedence over `meter_id` when both are sent.
    pub meter_serial: Option<String>,
    pub custodial_sign: Option<bool>,
    /// Time-in-force: "gtc" (default), "ioc", or "fok".
    pub time_in_force: Option<String>,
    /// Market segment: "realtime" (default, CDA matcher) or "interval"
    /// (15-min uniform-price clearing).
    pub market_segment: Option<String>,

    // ── Order lifetime (both optional; send at most one) ─────────────────────
    //
    // Omitted → `ORDER_DEFAULT_TTL_SECS` (15m, one interval-clearing window) from
    // now. A resting order past its expiry is skipped
    // by the matcher immediately and flipped to `Expired` by the ReaperWorker.
    // Inert for ioc/fok, which never rest.
    /// Absolute expiry (RFC 3339, e.g. `2026-07-30T12:00:00Z`). Use when the
    /// deadline is an instant that matters — an epoch boundary, a delivery window.
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Expiry as a lifetime in seconds from now. Preferred by default: unlike
    /// `expires_at` it cannot be thrown off by client clock skew.
    pub expires_in_secs: Option<i64>,

    // ── Per-user escrow settlement (config `per_user_escrow_settlement`) ──────
    //
    // Required only when that flag is on. The three fields below are inputs to
    // the signed payload, so the client must send exactly what it signed; the
    // server re-derives the message from them and verifies the signature before
    // accepting the order (see `crate::order_signature`).
    /// Order UUID. The client generates it because the id is part of the signed
    /// payload — the server cannot mint one after the fact and still verify.
    pub order_id: Option<Uuid>,
    /// Base58 Ed25519 signature over the canonical payload
    /// ([`trading_core::offchain_payload`]), produced by the user's wallet.
    /// Distinct from the HMAC `signature` on `CreateOrderRequest`.
    pub wallet_signature: Option<String>,
    /// The exact `expires_at` (unix seconds) that was signed. Sent explicitly
    /// because the server's own default TTL would not match the signed bytes.
    pub signed_expires_at: Option<i64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SubmitOrderResponse {
    pub id: Uuid,
    pub status: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OrderBookResponse {
    pub zone_id: i32,
    /// Sequence the `/ws/trading` stream had reached for this zone when the
    /// snapshot was read.
    ///
    /// **A staleness hint, not a splice point.** This book is read from
    /// Postgres while the sequence counts Kafka frames, and events reach
    /// Postgres before they reach Kafka (the outbox relay,
    /// `trading-infra/src/events/outbox_worker.rs`). So the snapshot can
    /// already contain an order this sequence has not counted yet. A client
    /// that resumed deltas from exactly `last_update_id + 1` would re-apply
    /// that order.
    ///
    /// Apply frames keyed by `order_id`/`match_id` so a repeat is a no-op, and
    /// use this only to notice you have fallen behind. Note it also **resets on
    /// gateway restart** — the WS pump replays Kafka from earliest with a fresh
    /// consumer group — so treat a decrease as "resync", not as an error.
    pub last_update_id: u64,
    pub asks: Vec<[String; 2]>,
    pub bids: Vec<[String; 2]>,
}

/// One meter that currently has at least one resting order, and on which sides.
///
/// Deliberately minimal: no user id, amount or price. It answers "is this meter
/// trading right now, and which way" — enough for the map to filter markers —
/// without turning a map read into an order-flow feed.
#[derive(Debug, Serialize, ToSchema)]
pub struct ActiveOrderMeter {
    /// The metering `meters.id` the order was placed against.
    pub meter_id: Uuid,
    /// The same meter's `serial_number` — the id the grid map keys its nodes on.
    /// Clients matching against map nodes must use this, not `meter_id`.
    pub meter_serial: String,
    pub zone_id: i32,
    pub has_open_buy: bool,
    pub has_open_sell: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ActiveOrderMetersResponse {
    pub data: Vec<ActiveOrderMeter>,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct ListOrdersParams {
    pub status: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListOrdersResponse {
    pub data: Vec<OrderData>,
    pub pagination: Pagination,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OrderData {
    pub id: Uuid,
    pub zone_id: i32,
    pub side: String,
    pub order_type: String,
    pub status: String,
    pub energy_amount_kwh: String,
    /// `None` for market orders: their stored `price_per_kwh` is a synthetic bid
    /// (ceiling or slippage cap), not the price the order executed at — surfacing
    /// it would mislead. Limit orders carry the user's real price.
    pub price_per_kwh: Option<String>,
    pub filled_amount_kwh: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl OrderData {
    fn from_order(o: &TradingOrder) -> Self {
        // Market orders price at the resting ask; the row's price_per_kwh is a
        // synthetic bid the user never set, so don't expose it as the price.
        let price_per_kwh = match o.order_type {
            OrderType::Market => None,
            _ => Some(o.price_per_kwh.to_string()),
        };
        OrderData {
            id: o.id,
            zone_id: o.zone_id.unwrap_or(0),
            side: o.side.to_string().to_lowercase(),
            order_type: o.order_type.to_string().to_lowercase(),
            status: o.status.to_string().to_lowercase(),
            energy_amount_kwh: o.energy_amount.to_string(),
            price_per_kwh,
            filled_amount_kwh: o.filled_amount.to_string(),
            created_at: o.created_at.unwrap_or_else(gridtokenx_telemetry::time::now),
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Pagination {
    pub total: usize,
    pub limit: usize,
    pub offset: usize,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct QuoteRequest {
    pub buyer_zone_id: i32,
    pub seller_zone_id: i32,
    pub energy_amount_kwh: String,
    pub agreed_price: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct QuoteResponse {
    pub quote_id: String,
    pub expires_at: chrono::DateTime<chrono::Utc>,
    pub breakdown: QuoteBreakdown,
    pub grid_metrics: GridMetrics,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct QuoteBreakdown {
    pub energy_cost: String,
    pub wheeling_charge: String,
    pub loss_cost: String,
    pub total_cost: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct GridMetrics {
    pub effective_energy_kwh: String,
    pub loss_factor: String,
    pub zone_distance_km: String,
    pub is_grid_compliant: bool,
}

use crate::auth::UserContext;
use gridtokenx_blockchain_auth::ServiceRole;

/// Submit a spot order (limit or market) into the CDA or interval market.
#[utoipa::path(
    post,
    path = "/api/v1/orders",
    tag = "orders",
    request_body = SubmitOrderRequest,
    responses(
        (status = 200, description = "Order accepted (status `open`)", body = SubmitOrderResponse),
        (status = 400, description = "Invalid side/type/amount/price/TIF/segment combination", body = String),
        (status = 401, description = "Missing or invalid user id header", body = String),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Database or epoch resolution error", body = String),
    ),
    security(("gateway_role" = [], "user_id" = [])),
)]
pub async fn submit_order(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Json(req): Json<SubmitOrderRequest>,
) -> Result<Json<SubmitOrderResponse>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let submit_started = std::time::Instant::now();
    tracing::info!("Submit order request: {:?}", req);

    let amount = Decimal::from_str(&req.energy_amount_kwh).map_err(|e| {
        (
            axum::http::StatusCode::BAD_REQUEST,
            format!("Invalid energy_amount_kwh: {}", e),
        )
    })?;

    let side = match req.side.to_lowercase().as_str() {
        "buy" => OrderSide::Buy,
        "sell" => OrderSide::Sell,
        _ => {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                "Invalid side".to_string(),
            ))
        }
    };

    let order_type = match req.order_type.to_lowercase().as_str() {
        "limit" => OrderType::Limit,
        "market" => OrderType::Market,
        _ => OrderType::Limit,
    };

    let time_in_force = match req.time_in_force.as_deref().map(str::to_lowercase).as_deref() {
        // A market order with no explicit TIF defaults to IOC — it should fill at
        // whatever's resting and never sit in the book as a price-less GTC order.
        // A limit order defaults to GTC.
        None => match order_type {
            OrderType::Market => TimeInForce::Ioc,
            _ => TimeInForce::Gtc,
        },
        Some("gtc") => TimeInForce::Gtc,
        Some("ioc") => TimeInForce::Ioc,
        Some("fok") => TimeInForce::Fok,
        Some(other) => {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                format!("Invalid time_in_force: {other} (expected gtc|ioc|fok)"),
            ))
        }
    };

    // Parse the optional price input (limit price OR market-buy slippage cap),
    // then apply the shared admission policy. See
    // `trading_core::order_policy::resolve_order_price` for the rules.
    // A present-but-non-positive value (e.g. "0") is kept present so the policy
    // REJECTS it — REST can distinguish "0" from an omitted field. (The gRPC edge
    // can't: its proto f64 default 0.0 is indistinguishable from unset, so there
    // a market buy's 0.0 is treated as "no cap". That literal-zero divergence is
    // inherent to the proto and is the safe direction — REST never silently drops
    // slippage protection.)
    let price_input = match req.price_per_kwh.as_deref() {
        Some(raw) => Some(Decimal::from_str(raw).map_err(|e| {
            (
                axum::http::StatusCode::BAD_REQUEST,
                format!("Invalid price_per_kwh: {}", e),
            )
        })?),
        None => None,
    };
    let price = trading_core::order_policy::resolve_order_price(
        order_type,
        side,
        time_in_force,
        price_input,
    )
    .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.message().to_string()))?;

    let market_segment = match req.market_segment.as_deref().map(str::to_lowercase).as_deref() {
        None | Some("realtime") => trading_core::types::MarketSegment::Realtime,
        Some("interval") => trading_core::types::MarketSegment::Interval,
        Some(other) => {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                format!("Invalid market_segment: {other} (expected realtime|interval)"),
            ))
        }
    };

    // Interval orders clear in a 15-min uniform-price batch, not continuously, so
    // the "immediate" time-in-force modes (IOC/FOK) have no meaning there — and the
    // CDA IOC sweep never sees interval orders (the matcher filters to Realtime), so
    // an interval IOC remainder would never be cancelled. Reject the combination.
    if market_segment == trading_core::types::MarketSegment::Interval
        && time_in_force != TimeInForce::Gtc
    {
        return Err((
            axum::http::StatusCode::BAD_REQUEST,
            "interval orders must be gtc (ioc/fok require continuous matching)".to_string(),
        ));
    }

    // `meter_serial` is the id space every user-facing surface holds (the grid
    // map's node ids). Resolve it to `meters.id` here — sending a serial through
    // as `meter_id` violates `trading_orders_meter_id_fkey`.
    let meter_id = match req.meter_serial.as_deref() {
        Some(serial) => {
            let resolved = state
                .meter_repo
                .resolve_id_by_serial(serial)
                .await
                .map_err(|e| {
                    (
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Database error: {}", e),
                    )
                })?;
            Some(resolved.ok_or_else(|| {
                (
                    axum::http::StatusCode::BAD_REQUEST,
                    format!("unknown meter_serial: {serial}"),
                )
            })?)
        }
        None => req.meter_id,
    };

    // ── Per-user escrow settlement: verify the wallet signature ──────────────
    // With this flag on, settlement spends the parties' OWN escrow PDAs via
    // `settle_offchain_match`, which verifies an Ed25519 signature over the order
    // terms on-chain. Reject anything unsigned or mis-signed here, at placement,
    // rather than letting it rest in the book and fail at settlement.
    //
    // The message is rebuilt from the values this handler is about to persist —
    // never from client-supplied bytes — so a client cannot sign one price and
    // submit another. See `crate::order_signature` for the full rationale.
    let signed_order: Option<(Uuid, String, i64)> = if state.config.per_user_escrow_settlement {
        let order_id = req.order_id.ok_or((
            axum::http::StatusCode::BAD_REQUEST,
            "order_id is required when per-user escrow settlement is enabled".to_string(),
        ))?;
        let wallet_signature = req.wallet_signature.clone().ok_or((
            axum::http::StatusCode::BAD_REQUEST,
            "wallet_signature is required when per-user escrow settlement is enabled".to_string(),
        ))?;
        let signed_expires_at = req.signed_expires_at.ok_or((
            axum::http::StatusCode::BAD_REQUEST,
            "signed_expires_at is required when per-user escrow settlement is enabled".to_string(),
        ))?;

        let wallet = state
            .blockchain
            .get_user_wallet(user.user_id)
            .await
            .map_err(|e| {
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to resolve wallet: {e}"),
                )
            })?
            .ok_or((
                axum::http::StatusCode::BAD_REQUEST,
                "no on-chain wallet linked to this account".to_string(),
            ))?;

        let wallet_bytes: [u8; 32] = bs58::decode(&wallet)
            .into_vec()
            .ok()
            .and_then(|v| v.try_into().ok())
            .ok_or((
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "linked wallet is not a valid Ed25519 public key".to_string(),
            ))?;

        // Same truncating conversion the on-chain instruction and the browser use.
        let energy_base = trading_core::offchain_payload::energy_to_base_units(amount).ok_or((
            axum::http::StatusCode::BAD_REQUEST,
            "energy_amount_kwh is out of range".to_string(),
        ))?;
        let price_base = trading_core::offchain_payload::currency_to_base_units(price).ok_or((
            axum::http::StatusCode::BAD_REQUEST,
            "price_per_kwh is out of range".to_string(),
        ))?;

        let message = trading_core::offchain_payload::message_for(
            order_id.as_bytes(),
            &wallet_bytes,
            energy_base,
            price_base,
            match side {
                OrderSide::Buy => trading_core::offchain_payload::SIDE_BUY,
                OrderSide::Sell => trading_core::offchain_payload::SIDE_SELL,
            },
            req.zone_id as u32,
            signed_expires_at,
        );

        crate::order_signature::verify_order_signature(&wallet, &wallet_signature, &message)
            .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

        Some((order_id, wallet_signature, signed_expires_at))
    } else {
        None
    };

    // Resolve the order's lifetime before building it: an inadmissible expiry is
    // a 400, not an order that rests unmatchable until the reaper collects it.
    let expires_at = trading_core::order_policy::resolve_expires_at(
        gridtokenx_telemetry::time::now(),
        req.expires_at,
        req.expires_in_secs,
        signed_order.as_ref().map(|(_, _, exp)| *exp),
        state.config.order_expiry.default_ttl_secs,
        state.config.order_expiry.max_ttl_secs,
    )
    .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.message().to_string()))?;

    let mut order = TradingOrder {
        id: signed_order.as_ref().map(|(id, _, _)| *id).unwrap_or_else(Uuid::new_v4),
        user_id: user.user_id,
        order_type,
        side,
        energy_amount: amount,
        price_per_kwh: price,
        filled_amount: Decimal::ZERO,
        status: OrderStatus::Pending,
        // Client expiry, the signed expiry, or the configured default — resolved
        // by `order_policy::resolve_expires_at` above so REST and gRPC cannot
        // drift. A signed expiry still wins: the settlement-time payload has to
        // match the bytes the user signed.
        expires_at: Some(expires_at),
        created_at: Some(gridtokenx_telemetry::time::now()),
        filled_at: None,
        epoch_id: None,
        zone_id: Some(req.zone_id),
        meter_id,
        refund_tx_signature: None,
        order_pda: None,
        order_index: None,
        session_token: None,
        blockchain_status: None,
        blockchain_tx_hash: None,
        blockchain_error: None,
        retry_count: 0,
        time_in_force,
        market_segment,
    };

    // ── Custodial On-Chain Placement (Option A) ───────
    // Record the order PDA + fund its escrow on the user's behalf (platform-signed,
    // no user signature). Fires when settlement is enabled or explicitly requested.
    // Best-effort: a failure leaves order_pda NULL so the settlement worker skips it
    // (unchanged behaviour) — it never fails the API.
    //
    // Skipped entirely under `per_user_escrow_settlement`: that is the whole point
    // of the flag. Platform funding is what makes a seller's own GRX never move —
    // the escrow is filled from the platform's ATA, so selling debits nobody and
    // the pool drains by the traded amount on every match. With the flag on, each
    // party funds their own `[b"escrow", user, mint]` PDA by wallet-signed
    // `deposit_escrow`, and `settle_offchain_match` spends those.
    if !state.config.per_user_escrow_settlement
        && (state.config.trade_settlement_enabled || req.custodial_sign.unwrap_or(false))
    {
        let seed = u64::from_le_bytes(
            order.id.as_bytes()[0..8].try_into().expect("uuid has 16 bytes"),
        );
        let is_buy = matches!(side, OrderSide::Buy);
        match state
            .blockchain
            // The same expiry stored on the row, so the Order PDA states this
            // order's real lifetime instead of the program's old 24h default.
            // `None` maps to the on-chain no-expiry sentinel (0).
            .place_order_on_chain(
                user.user_id,
                is_buy,
                amount,
                price,
                req.zone_id,
                seed,
                order.expires_at.map_or(0, |t| t.timestamp()),
            )
            .await
        {
            Ok((sig, pda)) => {
                info!("✅ On-chain order placed. Sig: {}, PDA: {}", sig, pda);
                order.order_pda = Some(pda);
                order.order_index = Some(seed as i64);
                order.blockchain_tx_hash = Some(sig);
                order.blockchain_status = Some("confirmed".to_string());
            }
            Err(e) => {
                tracing::warn!("order {}: on-chain placement failed (left for retry): {}", order.id, e);
            }
        }
    }

    // Stamp the order with the active market epoch so the matcher's settlement
    // and order_matches inserts satisfy their NOT NULL FK to market_epochs.
    order.epoch_id = Some(
        state
            .order_repo
            .get_or_create_active_epoch()
            .await
            .map_err(|e| {
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to resolve active epoch: {}", e),
                )
            })?,
    );

    // Insert the order and its OrderCreated event in one transaction so the
    // event can never be lost relative to the state change (the outbox row is
    // written atomically with the order; OutboxWorker relays it later). Mirrors
    // the ConnectRPC submit path in handlers.rs.
    let event = trading_core::events::Event::OrderCreated(trading_core::events::OrderCreatedPayload {
        id: order.id,
        user_id: order.user_id,
        order_type: order.order_type.to_string(),
        side: order.side.to_string(),
        energy_amount: order.energy_amount,
        price_per_kwh: order.price_per_kwh,
        status: order.status.to_string(),
        zone_id: order.zone_id,
        created_at: order.created_at,
    });

    let insert_res = state
        .order_repo
        .insert_order_with_event(&order, &event)
        .await;

    trading_infra::metrics::record_order_submission(
        &order.order_type.to_string(),
        &order.side.to_string(),
        insert_res.is_ok(),
        submit_started.elapsed().as_secs_f64() * 1000.0,
    );

    insert_res.map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Database error: {}", e),
        )
    })?;

    // Store the wallet signature the settlement builder will replay into the
    // Ed25519 verify instruction. Written after the insert (not as part of it)
    // because `TradingOrder` does not map the column — see `set_wallet_signature`.
    // Hard-fail on error: without the signature this order can never settle on the
    // per-user-escrow path, and silently resting an unsettleable order in the book
    // is exactly the failure mode that produced the endless re-match loop before.
    if let Some((_, signature, _)) = signed_order.as_ref() {
        state
            .order_repo
            .set_wallet_signature(order.id, signature)
            .await
            .map_err(|e| {
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to persist order signature: {e}"),
                )
            })?;
    }

    // Realtime matching: wake the matcher now that the order is fully durable —
    // deliberately AFTER `set_wallet_signature`, because a cycle that matched the
    // order before its signature landed would persist a settlement that can never
    // execute on the per-user-escrow path. Fire-and-forget: `request_cycle` neither
    // awaits nor fails, so submit latency is unchanged, and a wake-up arriving
    // mid-cycle is held as a permit and served immediately after. Interval-segment
    // orders are cleared by the uniform-price path, not the CDA matcher, so waking
    // it for them would only buy a wasted book scan.
    if order.market_segment == trading_core::types::MarketSegment::Realtime {
        state.matcher.request_cycle();
    }

    Ok(Json(SubmitOrderResponse {
        id: order.id,
        status: "open".to_string(),
        created_at: order.created_at.unwrap_or_else(|| gridtokenx_telemetry::time::now()),
    }))
}

/// Zone order book: remaining energy aggregated by price level.
#[utoipa::path(
    get,
    path = "/api/v1/zones/{zone_id}/book",
    tag = "orders",
    params(("zone_id" = i32, Path, description = "Grid zone id")),
    responses(
        (status = 200, description = "Price-level book; asks ascend, bids descend; entries are [price, amount] decimal strings", body = OrderBookResponse),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [])),
)]
pub async fn get_order_book(
    role: ServiceRole,
    State(state): State<AppState>,
    Path(zone_id): Path<i32>,
) -> Result<Json<OrderBookResponse>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let entries = state
        .order_repo
        .get_active_orders_by_zone(zone_id)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {}", e),
            )
        })?;

    // Aggregate remaining (unfilled) energy by price level. BTreeMap keeps the
    // levels ordered by price; asks (sells) ascend from the best (lowest) ask,
    // bids (buys) descend from the best (highest) bid.
    let mut ask_levels: std::collections::BTreeMap<Decimal, Decimal> = Default::default();
    let mut bid_levels: std::collections::BTreeMap<Decimal, Decimal> = Default::default();
    for e in &entries {
        let book = match e.side {
            trading_core::types::OrderSide::Sell => &mut ask_levels,
            trading_core::types::OrderSide::Buy => &mut bid_levels,
        };
        *book.entry(e.price_per_kwh).or_insert(Decimal::ZERO) += e.energy_amount;
    }

    let asks: Vec<[String; 2]> = ask_levels
        .iter()
        .map(|(price, amount)| [price.to_string(), amount.to_string()])
        .collect();
    let bids: Vec<[String; 2]> = bid_levels
        .iter()
        .rev()
        .map(|(price, amount)| [price.to_string(), amount.to_string()])
        .collect();

    Ok(Json(OrderBookResponse {
        zone_id,
        // Was `entries.len()` — the resting-order count, a placeholder standing in
        // for the sequence source that did not exist yet. It does now: the WS
        // gateway stamps a per-zone sequence. See the field docs for why this is a
        // staleness hint rather than a resume point.
        last_update_id: state.ws_hub.current_seq(zone_id),
        asks,
        bids,
    }))
}

/// Meters that currently have resting orders, market-wide, grouped by meter.
///
/// The map uses this to show only meters that are actually trading, matching on
/// `meter_serial` (its node id space), not `meter_id`.
///
/// Orders carry a meter only when placed against a specific one (the map's node
/// form) — orders without one are simply absent here, so a meter silent on this
/// endpoint is not proof it has no orders at all.
#[utoipa::path(
    get,
    path = "/api/v1/markets/active-order-meters",
    tag = "markets",
    responses(
        (status = 200, description = "Meters with at least one resting (pending/active/partially-filled) order", body = ActiveOrderMetersResponse),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [])),
)]
pub async fn list_active_order_meters(
    role: ServiceRole,
    State(state): State<AppState>,
) -> Result<Json<ActiveOrderMetersResponse>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let data = fetch_active_order_meters(&state).await?;
    Ok(Json(ActiveOrderMetersResponse { data }))
}

/// Public, unauthenticated variant of [`list_active_order_meters`] for the grid
/// map. Returns only market-level order *presence* — which meters have a resting
/// buy/sell order — which is strictly less than the already-public order book
/// (`/api/v1/zones/{zone_id}/book`): no prices, amounts, or account identity. The
/// logged-out map needs it to hide non-trading meters without a JWT.
#[utoipa::path(
    get,
    path = "/api/v1/public/active-order-meters",
    tag = "markets",
    responses(
        (status = 200, description = "Meters with at least one resting (pending/active/partially-filled) order", body = ActiveOrderMetersResponse),
        (status = 500, description = "Database error", body = String),
    ),
)]
pub async fn list_public_active_order_meters(
    State(state): State<AppState>,
) -> Result<Json<ActiveOrderMetersResponse>, (axum::http::StatusCode, String)> {
    let data = fetch_active_order_meters(&state).await?;
    Ok(Json(ActiveOrderMetersResponse { data }))
}

/// Shared query behind both the authed and public active-order-meters endpoints:
/// every meter with at least one resting order, grouped, with `meter_id`
/// translated to the map's `meter_serial` id space.
async fn fetch_active_order_meters(
    state: &AppState,
) -> Result<Vec<ActiveOrderMeter>, (axum::http::StatusCode, String)> {
    // `bootstrap_active_orders` is named for the matcher's warm-up path, but it is
    // exactly this query: every order in ('pending','active','partially_filled'),
    // as a full `TradingOrder` (so `meter_id` survives — `get_all_active_orders`
    // projects to `OrderBookEntry`, which drops it).
    let orders = state
        .order_repo
        .bootstrap_active_orders()
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {}", e),
            )
        })?;

    // Group by meter first, then translate ids in one round-trip rather than
    // per-order.
    let mut sides: HashMap<Uuid, (i32, bool, bool)> = HashMap::new();
    for o in &orders {
        let Some(meter_id) = o.meter_id else { continue };
        let entry = sides
            .entry(meter_id)
            .or_insert((o.zone_id.unwrap_or(0), false, false));
        match o.side {
            OrderSide::Buy => entry.1 = true,
            OrderSide::Sell => entry.2 = true,
        }
    }

    let ids: Vec<Uuid> = sides.keys().copied().collect();
    let serials = state
        .meter_repo
        .get_serials_for_ids(&ids)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {}", e),
            )
        })?;

    // A meter_id with no `meters` row can't be matched to a map node, so it is
    // dropped rather than emitted with a placeholder serial.
    let mut data: Vec<ActiveOrderMeter> = sides
        .into_iter()
        .filter_map(|(meter_id, (zone_id, has_open_buy, has_open_sell))| {
            Some(ActiveOrderMeter {
                meter_id,
                meter_serial: serials.get(&meter_id)?.clone(),
                zone_id,
                has_open_buy,
                has_open_sell,
            })
        })
        .collect();
    // Stable order so clients can diff responses without re-sorting.
    data.sort_by_key(|m| m.meter_id);

    Ok(data)
}

/// List the authenticated user's orders (optionally filtered by status).
#[utoipa::path(
    get,
    path = "/api/v1/orders",
    tag = "orders",
    params(ListOrdersParams),
    responses(
        (status = 200, description = "Page of the user's orders (status filter applies after pagination)", body = ListOrdersResponse),
        (status = 401, description = "Missing or invalid user id header", body = String),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [], "user_id" = [])),
)]
pub async fn list_orders(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Query(params): Query<ListOrdersParams>,
) -> Result<Json<ListOrdersResponse>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;
    let limit = params.limit.unwrap_or(20);
    let offset = params.offset.unwrap_or(0);

    let orders = state
        .order_repo
        .get_orders_by_user(user.user_id, limit as i64, offset as i64)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {}", e),
            )
        })?;

    // `params.status` was previously accepted but never read — every cancelled/filled
    // order kept showing in `?status=active` callers (e.g. the trading UI's "My
    // Orders" list) forever. Filtered post-fetch since get_orders_by_user has no
    // status-aware query variant; note this applies after the repo's limit/offset, so
    // a page can return fewer than `limit` matches once a user has enough non-matching
    // orders to span pages — acceptable for the common (small, mostly-active) case.
    let orders: Vec<_> = match params.status.as_deref() {
        Some(status) => orders
            .into_iter()
            .filter(|o| o.status.as_str() == status)
            .collect(),
        None => orders,
    };

    let data = orders.iter().map(OrderData::from_order).collect::<Vec<_>>();

    let total = data.len();
    Ok(Json(ListOrdersResponse {
        data,
        pagination: Pagination {
            total,
            limit,
            offset,
        },
    }))
}

/// Fetch one order. Non-admin callers only see their own orders (404 otherwise).
#[utoipa::path(
    get,
    path = "/api/v1/orders/{id}",
    tag = "orders",
    params(("id" = Uuid, Path, description = "Order id")),
    responses(
        (status = 200, description = "The order", body = OrderData),
        (status = 401, description = "Missing or invalid user id header", body = String),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 404, description = "Not found (or owned by another user)", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [], "user_id" = [])),
)]
pub async fn get_order_by_id(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<OrderData>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let order = state
        .order_repo
        .get_order(id)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {}", e),
            )
        })?
        .ok_or((
            axum::http::StatusCode::NOT_FOUND,
            "Order not found".to_string(),
        ))?;

    // Ownership scoping: a gateway-scoped caller may only read its own user's
    // order; admins may read any. 404 (not 403) so an id's existence isn't
    // leaked across users.
    if role != ServiceRole::Admin && order.user_id != user.user_id {
        return Err((
            axum::http::StatusCode::NOT_FOUND,
            "Order not found".to_string(),
        ));
    }

    Ok(Json(OrderData::from_order(&order)))
}

/// Cancel an order owned by the authenticated user.
#[utoipa::path(
    delete,
    path = "/api/v1/orders/{id}",
    tag = "orders",
    params(("id" = Uuid, Path, description = "Order id")),
    responses(
        (status = 200, description = "`{\"status\": \"cancelled\", \"order_id\": ...}`", body = serde_json::Value),
        (status = 401, description = "Missing or invalid user id header", body = String),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [], "user_id" = [])),
)]
pub async fn cancel_order(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    state
        .order_repo
        .cancel_order(id, user.user_id)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {}", e),
            )
        })?;

    Ok(Json(serde_json::json!({
        "status": "cancelled",
        "order_id": id,
    })))
}

/// Price quote with wheeling/loss breakdown, computed from the request and the
/// service market config (`state.config.market`) — the same schedule surfaced by
/// `/api/v1/markets/p2p/market-prices`.
///
/// Model (per kWh, THB):
/// - `energy_cost   = energy * price`  (price defaults to `base_price` when ≤ 0)
/// - `wheeling      = energy * wheeling_rate`  (intra- vs cross-zone)
/// - `loss_fraction = loss_factor - 1`  (config stores 1.01 / 1.03 multipliers)
/// - `loss_cost     = energy_cost * loss_fraction`
/// - `effective_kwh = energy * (1 - loss_fraction)`
/// - `total         = energy_cost + wheeling + loss_cost`
///
/// `zone_distance_km` is a 10 km-per-zone-hop heuristic (config carries no grid
/// topology). `is_grid_compliant` = price within `[min_price, max_price]`.
#[utoipa::path(
    post,
    path = "/api/v1/quotes",
    tag = "quotes",
    request_body = QuoteRequest,
    responses(
        (status = 200, description = "Computed quote for the requested trade", body = QuoteResponse),
        (status = 400, description = "Invalid energy amount", body = String),
        (status = 403, description = "Caller role not allowed", body = String),
    ),
    security(("gateway_role" = [])),
)]
pub async fn create_quote(
    role: ServiceRole,
    State(state): State<AppState>,
    Json(req): Json<QuoteRequest>,
) -> Result<Json<QuoteResponse>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let m = &state.config.market;

    let energy = Decimal::from_str(req.energy_amount_kwh.trim()).map_err(|e| {
        (
            axum::http::StatusCode::BAD_REQUEST,
            format!("invalid energy_amount_kwh: {e}"),
        )
    })?;
    if energy <= Decimal::ZERO {
        return Err((
            axum::http::StatusCode::BAD_REQUEST,
            "energy_amount_kwh must be positive".to_string(),
        ));
    }

    // An absent/zero agreed price falls back to the real market price (24h VWAP,
    // widened to all-time), never a static config default. If the market has
    // never traded there is no price to quote — reject rather than invent one.
    let mut price = Decimal::from_str(req.agreed_price.trim()).unwrap_or(Decimal::ZERO);
    if price <= Decimal::ZERO {
        let quote_err = |e: String| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e);
        let mut mp = state
            .settlement_repo
            .get_market_price(24)
            .await
            .map_err(|e| quote_err(format!("Database error: {e}")))?;
        if mp.trade_count == 0 {
            mp = state
                .settlement_repo
                .get_market_price(0)
                .await
                .map_err(|e| quote_err(format!("Database error: {e}")))?;
        }
        if mp.trade_count == 0 {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                "no agreed_price supplied and no market price yet (no completed trades); supply agreed_price".to_string(),
            ));
        }
        price = mp.vwap;
    }

    let same_zone = req.buyer_zone_id == req.seller_zone_id;
    let wheeling_rate = if same_zone {
        m.intra_zone_wheeling_charge
    } else {
        m.cross_zone_wheeling_charge
    };
    let loss_mult = if same_zone {
        m.intra_zone_loss_factor
    } else {
        m.cross_zone_loss_factor
    };
    let loss_fraction = loss_mult - Decimal::ONE;

    let energy_cost = energy * price;
    let wheeling_charge = energy * wheeling_rate;
    let loss_cost = energy_cost * loss_fraction;
    let total_cost = energy_cost + wheeling_charge + loss_cost;
    let effective_energy = energy * (Decimal::ONE - loss_fraction);

    let zone_gap = (req.buyer_zone_id - req.seller_zone_id).abs();
    let zone_distance_km = Decimal::from(zone_gap) * Decimal::from(10);

    let is_grid_compliant = price >= m.min_price_per_kwh && price <= m.max_price_per_kwh;

    let qid = format!("q_{}", &Uuid::new_v4().to_string()[..8]);
    Ok(Json(QuoteResponse {
        quote_id: qid,
        expires_at: gridtokenx_telemetry::time::now() + chrono::Duration::minutes(5),
        breakdown: QuoteBreakdown {
            energy_cost: format!("{:.2}", dec_f64(energy_cost)),
            wheeling_charge: format!("{:.2}", dec_f64(wheeling_charge)),
            loss_cost: format!("{:.2}", dec_f64(loss_cost)),
            total_cost: format!("{:.2}", dec_f64(total_cost)),
        },
        grid_metrics: GridMetrics {
            effective_energy_kwh: format!("{:.4}", dec_f64(effective_energy)),
            loss_factor: format!("{:.4}", dec_f64(loss_fraction)),
            zone_distance_km: format!("{:.1}", dec_f64(zone_distance_km)),
            is_grid_compliant,
        },
    }))
}

#[derive(Debug, Serialize, ToSchema)]
pub struct MarketStatsResponse {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Total settled energy over the last 24h (kWh), decimal string.
    pub total_volume_24h_kwh: String,
    /// 24h VWAP price (THB/kWh), decimal string. "0" when no trades in 24h.
    pub avg_price_24h: String,
    /// Distinct users who traded (buyer or seller) in the last 24h.
    pub active_users: i64,
    /// Number of completed settlements in the last 24h.
    pub trade_count_24h: i64,
}

/// Real 24h market statistics, derived from completed settlements — no mock or
/// static values. Price/volume come from the market-price aggregate (VWAP);
/// `active_users` is the distinct buyer∪seller count. Grid-stability and
/// renewable-ratio were removed: trading has no real source for them (they
/// belong to telemetry), and a fabricated constant is worse than their absence.
#[utoipa::path(
    get,
    path = "/api/v1/stats",
    tag = "markets",
    responses(
        (status = 200, description = "Real 24h market stats from settled trades", body = MarketStatsResponse),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [])),
)]
pub async fn get_market_stats(
    role: ServiceRole,
    State(state): State<AppState>,
) -> Result<Json<MarketStatsResponse>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let price = state.settlement_repo.get_market_price(24).await.map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Database error: {}", e),
        )
    })?;
    let active_users = state.settlement_repo.count_active_traders(24).await.map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Database error: {}", e),
        )
    })?;

    Ok(Json(MarketStatsResponse {
        timestamp: gridtokenx_telemetry::time::now(),
        total_volume_24h_kwh: price.volume_kwh.to_string(),
        avg_price_24h: price.vwap.to_string(),
        active_users,
        trade_count_24h: price.trade_count,
    }))
}

// =============================================================================
// Futures Mock Handlers
// =============================================================================

/// List futures products.
#[utoipa::path(
    get,
    path = "/api/v1/futures/products",
    tag = "futures",
    responses(
        (status = 200, description = "All futures products", body = Vec<trading_core::models::FuturesProduct>),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [])),
)]
pub async fn get_futures_products(
    role: ServiceRole,
    State(state): State<AppState>,
) -> Result<Json<Vec<trading_core::models::FuturesProduct>>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let products = state.futures_repo.get_products().await.map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Database error: {}", e),
        )
    })?;

    Ok(Json(products))
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateFuturesOrderRequest {
    pub product_id: String,
    pub side: String,
    pub order_type: String,
    pub quantity: f64,
    pub price: f64,
    pub leverage: u32,
}

/// Create a futures order. STUB — body accepted but ignored; response is mock.
#[utoipa::path(
    post,
    path = "/api/v1/futures/orders",
    tag = "futures",
    request_body = CreateFuturesOrderRequest,
    responses(
        (status = 200, description = "`{\"order_id\": ..., \"status\": \"open\"}` (mock)", body = serde_json::Value),
        (status = 403, description = "Caller role not allowed", body = String),
    ),
    security(("gateway_role" = [])),
)]
pub async fn create_futures_order(
    role: ServiceRole,
    Json(_req): Json<CreateFuturesOrderRequest>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;
    Ok(Json(serde_json::json!({
        "order_id": Uuid::new_v4().to_string(),
        "status": "open",
    })))
}

/// List the authenticated user's futures positions.
#[utoipa::path(
    get,
    path = "/api/v1/futures/positions",
    tag = "futures",
    responses(
        (status = 200, description = "User's futures positions", body = Vec<trading_core::models::FuturesPosition>),
        (status = 401, description = "Missing or invalid user id header", body = String),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [], "user_id" = [])),
)]
pub async fn get_futures_positions(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
) -> Result<Json<Vec<trading_core::models::FuturesPosition>>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let positions = state
        .futures_repo
        .get_positions_by_user(user.user_id)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {}", e),
            )
        })?;

    Ok(Json(positions))
}

/// List the authenticated user's futures orders.
#[utoipa::path(
    get,
    path = "/api/v1/futures/orders",
    tag = "futures",
    responses(
        (status = 200, description = "User's futures orders", body = Vec<trading_core::models::FuturesOrder>),
        (status = 401, description = "Missing or invalid user id header", body = String),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [], "user_id" = [])),
)]
pub async fn get_futures_orders(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
) -> Result<Json<Vec<trading_core::models::FuturesOrder>>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let orders = state
        .futures_repo
        .get_orders_by_user(user.user_id)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {}", e),
            )
        })?;

    Ok(Json(orders))
}

/// Close a futures position by id.
#[utoipa::path(
    delete,
    path = "/api/v1/futures/positions/{id}",
    tag = "futures",
    params(("id" = Uuid, Path, description = "Position id")),
    responses(
        (status = 200, description = "`{\"status\": \"closed\", \"position_id\": ...}`", body = serde_json::Value),
        (status = 401, description = "Missing or invalid user id header", body = String),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [], "user_id" = [])),
)]
pub async fn close_futures_position(
    role: ServiceRole,
    _user: UserContext,
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    state.futures_repo.close_position(id).await.map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Database error: {}", e),
        )
    })?;

    Ok(Json(serde_json::json!({
        "status": "closed",
        "position_id": id,
    })))
}

/// Futures candles. STUB — always returns an empty array.
#[utoipa::path(
    get,
    path = "/api/v1/futures/candles",
    tag = "futures",
    responses(
        (status = 200, description = "Empty array (stub)", body = serde_json::Value),
        (status = 403, description = "Caller role not allowed", body = String),
    ),
    security(("gateway_role" = [])),
)]
pub async fn get_futures_candles(
    role: ServiceRole,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;
    Ok(Json(serde_json::json!([])))
}

/// Futures order book. STUB — always returns empty asks/bids.
#[utoipa::path(
    get,
    path = "/api/v1/futures/book",
    tag = "futures",
    responses(
        (status = 200, description = "`{\"asks\": [], \"bids\": []}` (stub)", body = serde_json::Value),
        (status = 403, description = "Caller role not allowed", body = String),
    ),
    security(("gateway_role" = [])),
)]
pub async fn get_futures_order_book(
    role: ServiceRole,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;
    Ok(Json(serde_json::json!({
        "asks": [],
        "bids": [],
    })))
}

// =============================================================================
// User Data Handlers (Modernized)
// =============================================================================

/// GRID token balance for a wallet address (via Chain Bridge).
#[utoipa::path(
    get,
    path = "/api/v1/wallets/{address}/balance",
    tag = "wallets",
    params(("address" = String, Path, description = "Solana wallet address")),
    responses(
        (status = 200, description = "Token balance (decimal string + raw), mint and decimals", body = serde_json::Value),
        (status = 401, description = "Missing or invalid user id header", body = String),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Blockchain error", body = String),
    ),
    security(("gateway_role" = [], "user_id" = [])),
)]
pub async fn get_wallet_balance(
    role: ServiceRole,
    _user: UserContext,
    State(state): State<AppState>,
    Path(address): Path<String>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let balance_raw = state
        .blockchain
        .get_token_balance(&address)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Blockchain error: {}", e),
            )
        })?;

    let decimals = 9;
    let balance_decimal = Decimal::new(balance_raw as i64, decimals);

    // Native SOL is a best-effort read: a chain hiccup here must not fail the
    // whole balance call, so fall back to 0.0 and log rather than 500.
    let balance_sol = match state.blockchain.get_sol_balance(&address).await {
        Ok(sol) => sol,
        Err(e) => {
            tracing::warn!("Failed to read native SOL for {}: {}", address, e);
            0.0
        }
    };

    // The currency (THBC) leg. A trade moves energy one way and baht the other,
    // but this endpoint used to return only the energy side — so a wallet could
    // show the kWh it held and not the money it was paid or spent.
    //
    // Best-effort like SOL: a wallet that has never touched the currency mint has
    // no ATA, and that is a legitimate zero, not a 500. Never fail the whole call
    // on this leg.
    //
    // CURRENCY_DECIMALS is 6 against energy's 9. They are reported explicitly, per
    // leg, rather than left for the caller to infer — assuming one scale for both
    // is exactly the mistake that produces 1000x errors.
    const CURRENCY_DECIMALS: u32 = 6;
    let currency_raw = match state.blockchain.get_currency_balance(&address).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("Failed to read currency balance for {}: {}", address, e);
            0
        }
    };
    let currency_decimal = Decimal::new(currency_raw as i64, CURRENCY_DECIMALS);

    Ok(Json(serde_json::json!({
        "wallet_address": address,
        // Energy leg — kept at the original key names so existing callers,
        // including the trading UI's getWalletBalance, keep working unchanged.
        "token_balance": balance_decimal.to_string(),
        "token_balance_raw": balance_raw,
        "balance_sol": balance_sol,
        "decimals": decimals,
        "token_mint": std::env::var("ENERGY_TOKEN_MINT").unwrap_or_default(),
        // Currency leg (THBC).
        "currency_balance": currency_decimal.to_string(),
        "currency_balance_raw": currency_raw,
        "currency_decimals": CURRENCY_DECIMALS,
        "currency_mint": std::env::var("CURRENCY_TOKEN_MINT").unwrap_or_default(),
    })))
}

/// Aggregate trading analytics for the authenticated user.
#[utoipa::path(
    get,
    path = "/api/v1/analytics/stats",
    tag = "analytics",
    responses(
        (status = 200, description = "User trading analytics", body = trading_core::models::UserAnalytics),
        (status = 401, description = "Missing or invalid user id header", body = String),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [], "user_id" = [])),
)]
pub async fn get_user_analytics_stats(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
) -> Result<Json<trading_core::models::UserAnalytics>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let stats = state
        .analytics_repo
        .get_user_stats(user.user_id)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {}", e),
            )
        })?;

    Ok(Json(stats))
}

/// Analytics history. STUB — always returns `{"history": []}`.
#[utoipa::path(
    get,
    path = "/api/v1/analytics/history",
    tag = "analytics",
    responses(
        (status = 200, description = "`{\"history\": []}` (stub)", body = serde_json::Value),
        (status = 403, description = "Caller role not allowed", body = String),
    ),
    security(("gateway_role" = [])),
)]
pub async fn get_user_analytics_history(
    role: ServiceRole,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    Ok(Json(serde_json::json!({
        "history": []
    })))
}

/// Transaction history for the authenticated user.
#[utoipa::path(
    get,
    path = "/api/v1/transactions",
    tag = "analytics",
    responses(
        (status = 200, description = "User's transactions", body = Vec<trading_core::models::TransactionData>),
        (status = 401, description = "Missing or invalid user id header", body = String),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [], "user_id" = [])),
)]
pub async fn get_user_transactions(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
) -> Result<Json<Vec<trading_core::models::TransactionData>>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let txs = state
        .analytics_repo
        .get_user_transactions(user.user_id)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {}", e),
            )
        })?;

    Ok(Json(txs))
}

// =============================================================================
// Carbon / ESG Handlers (Modernized)
// =============================================================================

/// Carbon credit balance for the authenticated user.
#[utoipa::path(
    get,
    path = "/api/v1/carbon/balance",
    tag = "carbon",
    responses(
        (status = 200, description = "Total/available/retired credits (decimal strings; retired always \"0.0\")", body = serde_json::Value),
        (status = 401, description = "Missing or invalid user id header", body = String),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [], "user_id" = [])),
)]
pub async fn get_carbon_balance(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let balance = state
        .carbon_repo
        .get_balance(user.user_id)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {}", e),
            )
        })?;

    Ok(Json(serde_json::json!({
        "total_credits": balance.to_string(),
        "available_credits": balance.to_string(),
        "retired_credits": "0.0",
        "last_updated": gridtokenx_telemetry::time::now(),
    })))
}

/// Carbon credit history for the authenticated user.
#[utoipa::path(
    get,
    path = "/api/v1/carbon/history",
    tag = "carbon",
    responses(
        (status = 200, description = "User's carbon credits", body = Vec<trading_core::models::CarbonCredit>),
        (status = 401, description = "Missing or invalid user id header", body = String),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [], "user_id" = [])),
)]
pub async fn get_carbon_history(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
) -> Result<Json<Vec<trading_core::models::CarbonCredit>>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let history = state
        .carbon_repo
        .get_history(user.user_id)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {}", e),
            )
        })?;

    Ok(Json(history))
}

/// Carbon transactions. STUB — always returns an empty array.
#[utoipa::path(
    get,
    path = "/api/v1/carbon/transactions",
    tag = "carbon",
    responses(
        (status = 200, description = "Empty array (stub)", body = serde_json::Value),
        (status = 403, description = "Caller role not allowed", body = String),
    ),
    security(("gateway_role" = [])),
)]
pub async fn get_carbon_transactions(
    role: ServiceRole,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    Ok(Json(serde_json::json!([])))
}

/// Transfer carbon credits. STUB — body accepted but ignored; response is mock.
#[utoipa::path(
    post,
    path = "/api/v1/carbon/transfers",
    tag = "carbon",
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "`{\"transaction_id\": ..., \"status\": \"pending\"}` (mock)", body = serde_json::Value),
        (status = 403, description = "Caller role not allowed", body = String),
    ),
    security(("gateway_role" = [])),
)]
pub async fn transfer_carbon_credits(
    role: ServiceRole,
    Json(_req): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    Ok(Json(serde_json::json!({
        "transaction_id": Uuid::new_v4().to_string(),
        "status": "pending",
    })))
}

// =============================================================================
// Markets — Config, P2P Prices, Matching Status (Phase 1, read-only)
// =============================================================================

fn dec_f64(d: Decimal) -> f64 {
    d.to_f64().unwrap_or(0.0)
}

#[derive(Debug, Serialize, ToSchema)]
pub struct MarketConfigResponse {
    pub base_price_thb_kwh: f64,
    pub grid_import_price_thb_kwh: f64,
    pub grid_export_price_thb_kwh: f64,
    pub transaction_fee_bps: u32,
    pub min_price_per_kwh: f64,
    pub max_price_per_kwh: f64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct P2PMarketPricesResponse {
    pub base_price_thb_kwh: f64,
    pub grid_import_price_thb_kwh: f64,
    pub grid_export_price_thb_kwh: f64,
    pub loss_allocation_model: String,
    pub wheeling_charges: HashMap<String, f64>,
    pub loss_factors: HashMap<String, f64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PriceRange {
    pub min: f64,
    pub max: f64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct MatchingStatusResponse {
    pub pending_buy_orders: usize,
    pub pending_sell_orders: usize,
    pub pending_matches: usize,
    pub buy_price_range: PriceRange,
    pub sell_price_range: PriceRange,
    pub can_match: bool,
    pub match_reason: String,
}

/// Static market pricing parameters. NOTE: real JSON floats, not decimal strings.
#[utoipa::path(
    get,
    path = "/api/v1/markets/config",
    tag = "markets",
    responses(
        (status = 200, description = "Market pricing config", body = MarketConfigResponse),
        (status = 403, description = "Caller role not allowed", body = String),
    ),
    security(("gateway_role" = [])),
)]
pub async fn get_market_config(
    role: ServiceRole,
    State(state): State<AppState>,
) -> Result<Json<MarketConfigResponse>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let m = &state.config.market;
    Ok(Json(MarketConfigResponse {
        base_price_thb_kwh: dec_f64(m.base_price_thb_kwh),
        grid_import_price_thb_kwh: dec_f64(m.grid_import_price_thb_kwh),
        grid_export_price_thb_kwh: dec_f64(m.grid_export_price_thb_kwh),
        transaction_fee_bps: m.transaction_fee_bps,
        min_price_per_kwh: dec_f64(m.min_price_per_kwh),
        max_price_per_kwh: dec_f64(m.max_price_per_kwh),
    }))
}

/// P2P pricing + wheeling/loss schedules. NOTE: real JSON floats, not decimal strings.
#[utoipa::path(
    get,
    path = "/api/v1/markets/p2p/market-prices",
    tag = "markets",
    responses(
        (status = 200, description = "P2P prices with intra/cross-zone wheeling charges and loss factors", body = P2PMarketPricesResponse),
        (status = 403, description = "Caller role not allowed", body = String),
    ),
    security(("gateway_role" = [])),
)]
pub async fn get_p2p_market_prices(
    role: ServiceRole,
    State(state): State<AppState>,
) -> Result<Json<P2PMarketPricesResponse>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let m = &state.config.market;
    let mut wheeling_charges = HashMap::new();
    wheeling_charges.insert("intra_zone".to_string(), dec_f64(m.intra_zone_wheeling_charge));
    wheeling_charges.insert("cross_zone".to_string(), dec_f64(m.cross_zone_wheeling_charge));
    let mut loss_factors = HashMap::new();
    loss_factors.insert("intra_zone".to_string(), dec_f64(m.intra_zone_loss_factor));
    loss_factors.insert("cross_zone".to_string(), dec_f64(m.cross_zone_loss_factor));

    Ok(Json(P2PMarketPricesResponse {
        base_price_thb_kwh: dec_f64(m.base_price_thb_kwh),
        grid_import_price_thb_kwh: dec_f64(m.grid_import_price_thb_kwh),
        grid_export_price_thb_kwh: dec_f64(m.grid_export_price_thb_kwh),
        loss_allocation_model: m.loss_allocation_model.clone(),
        wheeling_charges,
        loss_factors,
    }))
}

/// Live order-book crossing summary (expired-but-unreaped orders excluded).
#[utoipa::path(
    get,
    path = "/api/v1/markets/matching-status",
    tag = "markets",
    responses(
        (status = 200, description = "Pending counts, price ranges, crossability", body = MatchingStatusResponse),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [])),
)]
pub async fn get_matching_status(
    role: ServiceRole,
    State(state): State<AppState>,
) -> Result<Json<MatchingStatusResponse>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let buys = state.order_repo.get_active_buy_orders().await.map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Database error: {}", e),
        )
    })?;
    let sells = state.order_repo.get_active_sell_orders().await.map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Database error: {}", e),
        )
    })?;

    Ok(Json(build_matching_status(
        &buys,
        &sells,
        gridtokenx_telemetry::time::now(),
    )))
}

/// Pure aggregation over active orders — unit-testable without a DB.
///
/// `get_active_buy_orders`/`get_active_sell_orders` no longer filter expired rows
/// at the DB level (expiry is reaped asynchronously by the `ReaperWorker`), so a
/// row can be returned here after its `expires_at` but before the next reap tick.
/// The matcher skips such orders, so counting them as crossable would report
/// phantom liquidity. Filter them out with the same expiry rule the engine uses.
fn build_matching_status(
    buys: &[TradingOrder],
    sells: &[TradingOrder],
    now: chrono::DateTime<chrono::Utc>,
) -> MatchingStatusResponse {
    let buys: Vec<&TradingOrder> = buys.iter().filter(|o| o.is_live(now)).collect();
    let sells: Vec<&TradingOrder> = sells.iter().filter(|o| o.is_live(now)).collect();

    let buy_min = buys.iter().map(|o| o.price_per_kwh).min();
    let buy_max = buys.iter().map(|o| o.price_per_kwh).max();
    let sell_min = sells.iter().map(|o| o.price_per_kwh).min();
    let sell_max = sells.iter().map(|o| o.price_per_kwh).max();

    let can_match = matches!((buy_max, sell_min), (Some(b), Some(s)) if b >= s);

    let pending_matches = match (buy_max, sell_min) {
        (Some(b_max), Some(s_min)) if b_max >= s_min => {
            let crossable_buys = buys.iter().filter(|o| o.price_per_kwh >= s_min).count();
            let crossable_sells = sells.iter().filter(|o| o.price_per_kwh <= b_max).count();
            crossable_buys.min(crossable_sells)
        }
        _ => 0,
    };

    let match_reason = if buys.is_empty() && sells.is_empty() {
        "no orders"
    } else if sells.is_empty() {
        "no sell liquidity"
    } else if buys.is_empty() {
        "no buy liquidity"
    } else if can_match {
        "orders crossing"
    } else {
        "spread too wide"
    }
    .to_string();

    MatchingStatusResponse {
        pending_buy_orders: buys.len(),
        pending_sell_orders: sells.len(),
        pending_matches,
        buy_price_range: PriceRange {
            min: buy_min.map(dec_f64).unwrap_or(0.0),
            max: buy_max.map(dec_f64).unwrap_or(0.0),
        },
        sell_price_range: PriceRange {
            min: sell_min.map(dec_f64).unwrap_or(0.0),
            max: sell_max.map(dec_f64).unwrap_or(0.0),
        },
        can_match,
        match_reason,
    }
}

// =============================================================================
// Markets — Settlement Stats + P2P Order Book (Phase 2, read-only)
// =============================================================================

#[derive(Debug, Serialize, ToSchema)]
pub struct SettlementStatsResponse {
    pub pending_count: i64,
    pub processing_count: i64,
    pub confirmed_count: i64,
    pub failed_count: i64,
    pub total_settled_value: f64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct P2POrderBookResponse {
    pub asks: Vec<[String; 2]>,
    pub bids: Vec<[String; 2]>,
}

fn build_settlement_stats_response(s: &SettlementStats) -> SettlementStatsResponse {
    SettlementStatsResponse {
        pending_count: s.pending_count,
        processing_count: s.processing_count,
        confirmed_count: s.confirmed_count,
        failed_count: s.failed_count,
        total_settled_value: dec_f64(s.total_settled_value),
    }
}

/// Aggregate active orders into price-level book entries (`[price, amount]`).
/// Bids (buys) descending, asks (sells) ascending. DB-free, unit-testable.
fn build_p2p_orderbook(entries: &[OrderBookEntry]) -> P2POrderBookResponse {
    use std::collections::BTreeMap;
    let mut bids: BTreeMap<Decimal, Decimal> = BTreeMap::new();
    let mut asks: BTreeMap<Decimal, Decimal> = BTreeMap::new();
    for e in entries {
        let book = match e.side {
            OrderSide::Buy => &mut bids,
            OrderSide::Sell => &mut asks,
        };
        *book.entry(e.price_per_kwh).or_insert(Decimal::ZERO) += e.energy_amount;
    }
    let bids_vec = bids
        .iter()
        .rev()
        .map(|(p, a)| [p.to_string(), a.to_string()])
        .collect();
    let asks_vec = asks
        .iter()
        .map(|(p, a)| [p.to_string(), a.to_string()])
        .collect();
    P2POrderBookResponse {
        asks: asks_vec,
        bids: bids_vec,
    }
}

/// Settlement counts by status.
#[utoipa::path(
    get,
    path = "/api/v1/markets/settlement-stats",
    tag = "markets",
    responses(
        (status = 200, description = "Settlement pipeline counters", body = SettlementStatsResponse),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [])),
)]
pub async fn get_settlement_stats(
    role: ServiceRole,
    State(state): State<AppState>,
) -> Result<Json<SettlementStatsResponse>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let stats = state.settlement_repo.get_settlement_stats().await.map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Database error: {}", e),
        )
    })?;
    Ok(Json(build_settlement_stats_response(&stats)))
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct MarketPriceQuery {
    /// Trailing window in hours (default 24, clamped to 0..=720).
    /// `0` means all-time (no time bound) — used to widen when a 24h window is empty.
    pub window_hours: Option<i64>,
}

/// Real market price (THB/kWh) computed from completed settlements — VWAP, last,
/// high/low, volume and trade count over a trailing window. Replaces static/mock
/// pricing: when `trade_count` is 0 there is no price yet (all fields 0), so
/// callers must not render 0 as a real value.
#[utoipa::path(
    get,
    path = "/api/v1/markets/price",
    tag = "markets",
    params(MarketPriceQuery),
    responses(
        (status = 200, description = "Real trade-derived market price; prices are decimal strings", body = MarketPrice),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [])),
)]
pub async fn get_market_price(
    role: ServiceRole,
    State(state): State<AppState>,
    axum::extract::Query(q): axum::extract::Query<MarketPriceQuery>,
) -> Result<Json<MarketPrice>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let window_hours = q.window_hours.unwrap_or(24).clamp(0, 720);
    let price = state
        .settlement_repo
        .get_market_price(window_hours)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {}", e),
            )
        })?;
    Ok(Json(price))
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct ClearingEpochsQuery {
    /// Max rows (default 20, clamped to 1..=100).
    pub limit: Option<i64>,
}

/// One uniform-price clearing result. Decimal fields are stringified to avoid
/// float drift, matching the trade/stats responses.
#[derive(Debug, Serialize, ToSchema)]
pub struct ClearingEpochResponse {
    pub epoch_id: Uuid,
    pub epoch_number: i64,
    pub start_time: chrono::DateTime<chrono::Utc>,
    pub end_time: chrono::DateTime<chrono::Utc>,
    pub status: String,
    pub clearing_price: Option<String>,
    pub total_volume: Option<String>,
    pub total_orders: Option<i64>,
    pub matched_orders: Option<i64>,
}

/// Recent uniform-price (Interval) clearing results, newest first.
#[utoipa::path(
    get,
    path = "/api/v1/markets/clearing-epochs",
    tag = "markets",
    params(ClearingEpochsQuery),
    responses(
        (status = 200, description = "Cleared epochs (decimal fields as strings)", body = Vec<ClearingEpochResponse>),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [])),
)]
pub async fn get_clearing_epochs(
    role: ServiceRole,
    State(state): State<AppState>,
    axum::extract::Query(q): axum::extract::Query<ClearingEpochsQuery>,
) -> Result<Json<Vec<ClearingEpochResponse>>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let limit = q.limit.unwrap_or(20).clamp(1, 100);
    let epochs = state
        .order_repo
        .list_recent_cleared_epochs(limit)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {}", e),
            )
        })?;

    let out = epochs
        .into_iter()
        .map(|e| ClearingEpochResponse {
            epoch_id: e.id,
            epoch_number: e.epoch_number,
            start_time: e.start_time,
            end_time: e.end_time,
            status: e.status.to_string(),
            clearing_price: e.clearing_price.map(|p| p.to_string()),
            total_volume: e.total_volume.map(|v| v.to_string()),
            total_orders: e.total_orders,
            matched_orders: e.matched_orders,
        })
        .collect();
    Ok(Json(out))
}

/// Cross-zone P2P aggregate order book.
#[utoipa::path(
    get,
    path = "/api/v1/markets/orderbook",
    tag = "markets",
    responses(
        (status = 200, description = "Aggregate book; entries are [price, amount] decimal strings", body = P2POrderBookResponse),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [])),
)]
pub async fn get_p2p_orderbook(
    role: ServiceRole,
    State(state): State<AppState>,
) -> Result<Json<P2POrderBookResponse>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let entries = state.order_repo.get_all_active_orders().await.map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Database error: {}", e),
        )
    })?;
    Ok(Json(build_p2p_orderbook(&entries)))
}

// =============================================================================
// Trades — history (JSON) + export (CSV) (Phase 3)
// =============================================================================
//
// Backed by the `settlements` table (no dedicated `trades` table exists). A
// settlement IS a completed trade: it carries buyer/seller, energy, price,
// total, fees and zones. Always scoped to the authenticated user (buyer OR
// seller); `role`/`counterparty_id` are computed relative to that user.

#[derive(Debug, Deserialize, IntoParams)]
pub struct TradesQuery {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    /// Export only: `csv` (default) | `json`.
    pub format: Option<String>,
}

/// One trade row. Superset of the frontend `TradeRecord` (string decimals,
/// `role`/`counterparty_id`, `executed_at`) plus aliases consumed by
/// `getTradeHistory` (`buyer_id`, `seller_id`, `energy_amount`,
/// `price_per_kwh`, `fee_amount`, `transaction_hash`, `created_at`).
#[derive(Debug, Serialize, ToSchema)]
pub struct TradeRecordResponse {
    pub id: Uuid,
    pub buyer_id: Uuid,
    pub seller_id: Uuid,
    pub counterparty_id: Uuid,
    pub role: String,
    pub quantity: String,
    pub energy_amount: String,
    pub price: String,
    pub price_per_kwh: String,
    pub total_value: String,
    pub fee_amount: String,
    pub wheeling_charge: String,
    pub loss_cost: String,
    pub effective_energy: String,
    pub status: String,
    pub transaction_hash: Option<String>,
    pub buy_order_id: Uuid,
    pub sell_order_id: Uuid,
    pub buyer_zone_id: Option<i32>,
    pub seller_zone_id: Option<i32>,
    pub executed_at: chrono::DateTime<chrono::Utc>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Settlement retry attempts so far. Non-zero means the settlement
    /// bounced at least once; at `MAX_SETTLEMENT_RETRIES` the row is
    /// parked in `permanently_failed`.
    pub retry_count: i32,
    /// Why the settlement failed, verbatim from the settlement worker.
    /// `None` unless the row failed.
    pub error_message: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TradesListResponse {
    pub trades: Vec<TradeRecordResponse>,
    /// `getTrades` (TradeHistory) reads `total_count`.
    pub total_count: i64,
    /// `getTradeHistory` reads `total`.
    pub total: i64,
}

fn dec_opt_str(d: Option<Decimal>) -> String {
    d.unwrap_or(Decimal::ZERO).to_string()
}

/// Map a settlement to a trade row from the perspective of `user`.
fn build_trade_record(s: &Settlement, user: Uuid) -> TradeRecordResponse {
    let is_buyer = s.buyer_id == user;
    let (role, counterparty_id) = if is_buyer {
        ("buyer".to_string(), s.seller_id)
    } else {
        ("seller".to_string(), s.buyer_id)
    };
    TradeRecordResponse {
        id: s.id,
        buyer_id: s.buyer_id,
        seller_id: s.seller_id,
        counterparty_id,
        role,
        quantity: s.energy_amount.to_string(),
        energy_amount: s.energy_amount.to_string(),
        price: s.price.to_string(),
        price_per_kwh: s.price.to_string(),
        total_value: s.total_amount.to_string(),
        fee_amount: s.fee_amount.to_string(),
        wheeling_charge: dec_opt_str(s.wheeling_charge),
        loss_cost: dec_opt_str(s.loss_cost),
        effective_energy: dec_opt_str(s.effective_energy),
        status: s.status.to_string(),
        transaction_hash: s.blockchain_tx.clone(),
        buy_order_id: s.buy_order_id,
        sell_order_id: s.sell_order_id,
        buyer_zone_id: s.buyer_zone_id,
        seller_zone_id: s.seller_zone_id,
        executed_at: s.created_at,
        created_at: s.created_at,
        retry_count: s.retry_count,
        error_message: s.error_message.clone(),
    }
}

fn build_trades_response(
    settlements: &[Settlement],
    total: i64,
    user: Uuid,
) -> TradesListResponse {
    let trades = settlements.iter().map(|s| build_trade_record(s, user)).collect();
    TradesListResponse {
        trades,
        total_count: total,
        total,
    }
}

/// RFC-4180 field escaping: quote if the value holds a comma, quote, CR or LF;
/// double any embedded quotes.
fn csv_field(v: &str) -> String {
    if v.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", v.replace('"', "\"\""))
    } else {
        v.to_string()
    }
}

/// Serialize trade rows to a CSV document (header + one row each). DB-free.
fn trades_to_csv(records: &[TradeRecordResponse]) -> String {
    let mut out = String::from(
        "id,executed_at,role,counterparty_id,quantity,price,total_value,fee_amount,wheeling_charge,loss_cost,effective_energy,status,transaction_hash,buyer_zone_id,seller_zone_id\n",
    );
    for r in records {
        let cols = [
            r.id.to_string(),
            r.executed_at.to_rfc3339(),
            r.role.clone(),
            r.counterparty_id.to_string(),
            r.quantity.clone(),
            r.price.clone(),
            r.total_value.clone(),
            r.fee_amount.clone(),
            r.wheeling_charge.clone(),
            r.loss_cost.clone(),
            r.effective_energy.clone(),
            r.status.clone(),
            r.transaction_hash.clone().unwrap_or_default(),
            r.buyer_zone_id.map(|z| z.to_string()).unwrap_or_default(),
            r.seller_zone_id.map(|z| z.to_string()).unwrap_or_default(),
        ];
        let line: Vec<String> = cols.iter().map(|c| csv_field(c)).collect();
        out.push_str(&line.join(","));
        out.push('\n');
    }
    out
}

/// Authenticated user's trade history (newest first). A trade = a settlement row.
#[utoipa::path(
    get,
    path = "/api/v1/trades",
    tag = "trades",
    params(TradesQuery),
    responses(
        (status = 200, description = "Page of trades scoped to the user (buyer or seller)", body = TradesListResponse),
        (status = 401, description = "Missing or invalid user id header", body = String),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [], "user_id" = [])),
)]
pub async fn get_trades(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Query(params): Query<TradesQuery>,
) -> Result<Json<TradesListResponse>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let limit = params.limit.unwrap_or(50).clamp(1, 500);
    let offset = params.offset.unwrap_or(0).max(0);

    let (settlements, total) = state
        .settlement_repo
        .list_settlements_for_user(user.user_id, limit, offset)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {}", e),
            )
        })?;

    Ok(Json(build_trades_response(&settlements, total, user.user_id)))
}

/// Export trade history as CSV (default) or JSON (`?format=json`).
#[utoipa::path(
    get,
    path = "/api/v1/trades/export",
    tag = "trades",
    params(TradesQuery),
    responses(
        (status = 200, description = "CSV attachment `trades.csv`, or a JSON array of trades when format=json", content_type = "text/csv", body = String),
        (status = 401, description = "Missing or invalid user id header", body = String),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [], "user_id" = [])),
)]
pub async fn export_trades(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Query(params): Query<TradesQuery>,
) -> Result<Response, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    // Export the full history (capped) rather than a single page.
    let limit = params.limit.unwrap_or(10_000).clamp(1, 50_000);

    let (settlements, _total) = state
        .settlement_repo
        .list_settlements_for_user(user.user_id, limit, 0)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {}", e),
            )
        })?;

    let records: Vec<TradeRecordResponse> = settlements
        .iter()
        .map(|s| build_trade_record(s, user.user_id))
        .collect();

    if params.format.as_deref() == Some("json") {
        return Ok(Json(records).into_response());
    }

    let csv = trades_to_csv(&records);
    Ok((
        [
            (header::CONTENT_TYPE, "text/csv; charset=utf-8"),
            (
                header::CONTENT_DISPOSITION,
                "attachment; filename=\"trades.csv\"",
            ),
        ],
        csv,
    )
        .into_response())
}

// ── Price Alerts (Phase 4) ───────────────────────────────────────────────────

/// POST body for creating a price alert. Frontend sends `symbol` (no DB column —
/// stored in `note`), `target_price` (string decimal), `condition` (above/below).
#[derive(Debug, Deserialize, ToSchema)]
pub struct CreatePriceAlertRequest {
    pub symbol: Option<String>,
    pub target_price: String,
    pub condition: String,
}

/// Wire shape matching frontend `PriceAlert` (`types/features.ts`): `symbol`
/// echoed from `note`, `is_active` derived from status, decimals as strings.
#[derive(Debug, Serialize, ToSchema)]
pub struct PriceAlertResponse {
    pub id: Uuid,
    pub user_id: Uuid,
    pub symbol: String,
    pub target_price: String,
    pub condition: String,
    pub is_active: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Map a stored alert to the frontend wire shape. `symbol` falls back to "" when
/// `note` is null; `is_active` is true only while status == active.
pub fn build_price_alert_response(a: &PriceAlert) -> PriceAlertResponse {
    PriceAlertResponse {
        id: a.id,
        user_id: a.user_id,
        symbol: a.note.clone().unwrap_or_default(),
        target_price: a.target_price.to_string(),
        condition: a.condition.to_string(),
        is_active: matches!(a.status, AlertStatus::Active),
        created_at: a.created_at,
    }
}

/// Create a price alert for the authenticated user.
#[utoipa::path(
    post,
    path = "/api/v1/price-alerts",
    tag = "price-alerts",
    request_body = CreatePriceAlertRequest,
    responses(
        (status = 200, description = "Created alert", body = PriceAlertResponse),
        (status = 400, description = "Invalid target_price or condition", body = String),
        (status = 401, description = "Missing or invalid user id header", body = String),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [], "user_id" = [])),
)]
pub async fn create_price_alert(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Json(req): Json<CreatePriceAlertRequest>,
) -> Result<Json<PriceAlertResponse>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let target_price = Decimal::from_str(req.target_price.trim()).map_err(|_| {
        (
            axum::http::StatusCode::BAD_REQUEST,
            format!("Invalid target_price: {}", req.target_price),
        )
    })?;

    let condition = AlertCondition::from_str(req.condition.trim().to_lowercase().as_str())
        .map_err(|_| {
            (
                axum::http::StatusCode::BAD_REQUEST,
                format!("Invalid condition: {} (expected above|below|crosses)", req.condition),
            )
        })?;

    let note = req.symbol.and_then(|s| {
        let t = s.trim().to_string();
        if t.is_empty() {
            None
        } else {
            Some(t)
        }
    });

    let alert = state
        .price_alert_repo
        .create_price_alert(NewPriceAlert {
            user_id: user.user_id,
            target_price,
            condition,
            note,
        })
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {}", e),
            )
        })?;

    Ok(Json(build_price_alert_response(&alert)))
}

/// List the authenticated user's price alerts (newest first).
#[utoipa::path(
    get,
    path = "/api/v1/price-alerts",
    tag = "price-alerts",
    responses(
        (status = 200, description = "User's alerts", body = Vec<PriceAlertResponse>),
        (status = 401, description = "Missing or invalid user id header", body = String),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [], "user_id" = [])),
)]
pub async fn list_price_alerts(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
) -> Result<Json<Vec<PriceAlertResponse>>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let alerts = state
        .price_alert_repo
        .list_price_alerts_for_user(user.user_id)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {}", e),
            )
        })?;

    Ok(Json(alerts.iter().map(build_price_alert_response).collect()))
}

/// Delete a price alert owned by the user.
#[utoipa::path(
    delete,
    path = "/api/v1/price-alerts/{id}",
    tag = "price-alerts",
    params(("id" = Uuid, Path, description = "Alert id")),
    responses(
        (status = 200, description = "`{\"success\": true}`", body = serde_json::Value),
        (status = 401, description = "Missing or invalid user id header", body = String),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 404, description = "Alert not found (or owned by another user)", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [], "user_id" = [])),
)]
pub async fn delete_price_alert(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let deleted = state
        .price_alert_repo
        .delete_price_alert(id, user.user_id)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {}", e),
            )
        })?;

    if deleted {
        Ok(Json(serde_json::json!({ "success": true })))
    } else {
        Err((
            axum::http::StatusCode::NOT_FOUND,
            "Price alert not found".to_string(),
        ))
    }
}

// ── Recurring Orders (Phase 5) ───────────────────────────────────────────────

/// POST body for creating a recurring order. Decimals arrive as strings and are
/// parsed manually (the workspace `rust_decimal` uses `serde-float`, so a JSON
/// string would otherwise fail to deserialize). `session_token` is accepted for
/// forward-compat with auto-trading but not persisted by the CRUD path.
#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateRecurringRequest {
    pub side: String,
    pub energy_amount: String,
    pub max_price_per_kwh: Option<String>,
    pub min_price_per_kwh: Option<String>,
    pub interval_type: String,
    pub interval_value: Option<i32>,
    pub max_executions: Option<i32>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub session_token: Option<String>,
}

/// Wire shape mirroring frontend `RecurringOrder` (`types/features.ts:99`).
/// Decimals are emitted as strings for an exact contract match (the float
/// serialization would otherwise drop trailing zeros / vary by locale).
#[derive(Debug, Serialize, ToSchema)]
pub struct RecurringOrderWire {
    pub id: Uuid,
    pub user_id: Uuid,
    pub side: String,
    pub energy_amount: String,
    pub max_price_per_kwh: Option<String>,
    pub min_price_per_kwh: Option<String>,
    pub interval_type: String,
    pub interval_value: i32,
    pub next_execution_at: chrono::DateTime<chrono::Utc>,
    pub last_executed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub status: String,
    pub total_executions: i32,
    pub max_executions: Option<i32>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// Map a stored recurring order to the frontend wire shape (decimals → strings,
/// enums → their serde-rename'd lowercase forms).
pub fn build_recurring_response(o: &RecurringOrder) -> RecurringOrderWire {
    RecurringOrderWire {
        id: o.id,
        user_id: o.user_id,
        side: o.side.to_string(),
        energy_amount: o.energy_amount.to_string(),
        max_price_per_kwh: o.max_price_per_kwh.map(|d| d.to_string()),
        min_price_per_kwh: o.min_price_per_kwh.map(|d| d.to_string()),
        interval_type: o.interval_type.to_string(),
        interval_value: o.interval_value,
        next_execution_at: o.next_execution_at,
        last_executed_at: o.last_executed_at,
        status: o.status.to_string(),
        total_executions: o.total_executions,
        max_executions: o.max_executions,
        name: o.name.clone(),
        description: o.description.clone(),
        created_at: o.created_at,
        updated_at: o.updated_at,
    }
}

fn parse_side(s: &str) -> Result<OrderSide, (axum::http::StatusCode, String)> {
    match s.trim().to_lowercase().as_str() {
        "buy" => Ok(OrderSide::Buy),
        "sell" => Ok(OrderSide::Sell),
        _ => Err((
            axum::http::StatusCode::BAD_REQUEST,
            format!("Invalid side: {s} (expected buy|sell)"),
        )),
    }
}

fn parse_interval(s: &str) -> Result<IntervalType, (axum::http::StatusCode, String)> {
    match s.trim().to_lowercase().as_str() {
        "hourly" => Ok(IntervalType::Hourly),
        "daily" => Ok(IntervalType::Daily),
        "weekly" => Ok(IntervalType::Weekly),
        "monthly" => Ok(IntervalType::Monthly),
        _ => Err((
            axum::http::StatusCode::BAD_REQUEST,
            format!("Invalid interval_type: {s} (expected hourly|daily|weekly|monthly)"),
        )),
    }
}

fn parse_opt_decimal(
    field: &str,
    value: Option<&str>,
) -> Result<Option<Decimal>, (axum::http::StatusCode, String)> {
    match value {
        None => Ok(None),
        Some(v) => Decimal::from_str(v.trim())
            .map(Some)
            .map_err(|_| (axum::http::StatusCode::BAD_REQUEST, format!("Invalid {field}: {v}"))),
    }
}

/// Create a recurring order for the authenticated user.
#[utoipa::path(
    post,
    path = "/api/v1/orders/recurring",
    tag = "recurring-orders",
    request_body = CreateRecurringRequest,
    responses(
        (status = 200, description = "Created recurring order", body = RecurringOrderWire),
        (status = 400, description = "Invalid side/interval/amount/price or interval_value < 1", body = String),
        (status = 401, description = "Missing or invalid user id header", body = String),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [], "user_id" = [])),
)]
pub async fn create_recurring_order(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Json(req): Json<CreateRecurringRequest>,
) -> Result<Json<RecurringOrderWire>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let side = parse_side(&req.side)?;
    let interval_type = parse_interval(&req.interval_type)?;
    let energy_amount = Decimal::from_str(req.energy_amount.trim()).map_err(|_| {
        (
            axum::http::StatusCode::BAD_REQUEST,
            format!("Invalid energy_amount: {}", req.energy_amount),
        )
    })?;
    let max_price_per_kwh = parse_opt_decimal("max_price_per_kwh", req.max_price_per_kwh.as_deref())?;
    let min_price_per_kwh = parse_opt_decimal("min_price_per_kwh", req.min_price_per_kwh.as_deref())?;

    // Cadence: default every 1 interval; DB CHECK enforces > 0.
    let interval_value = req.interval_value.unwrap_or(1);
    if interval_value < 1 {
        return Err((
            axum::http::StatusCode::BAD_REQUEST,
            "interval_value must be >= 1".to_string(),
        ));
    }
    let next_exec = next_execution_at(gridtokenx_telemetry::time::now(), interval_type, interval_value);

    let name = req.name.and_then(|s| {
        let t = s.trim().to_string();
        if t.is_empty() { None } else { Some(t) }
    });
    let description = req.description.and_then(|s| {
        let t = s.trim().to_string();
        if t.is_empty() { None } else { Some(t) }
    });

    let order = state
        .recurring_repo
        .create_recurring_order(NewRecurringOrder {
            user_id: user.user_id,
            side,
            energy_amount,
            max_price_per_kwh,
            min_price_per_kwh,
            interval_type,
            interval_value,
            next_execution_at: next_exec,
            max_executions: req.max_executions,
            name,
            description,
        })
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {e}"),
            )
        })?;

    Ok(Json(build_recurring_response(&order)))
}

/// List the user's recurring orders (newest first).
#[utoipa::path(
    get,
    path = "/api/v1/orders/recurring",
    tag = "recurring-orders",
    responses(
        (status = 200, description = "User's recurring orders", body = Vec<RecurringOrderWire>),
        (status = 401, description = "Missing or invalid user id header", body = String),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [], "user_id" = [])),
)]
pub async fn list_recurring_orders(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
) -> Result<Json<Vec<RecurringOrderWire>>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let orders = state
        .recurring_repo
        .list_recurring_orders_for_user(user.user_id)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {e}"),
            )
        })?;

    Ok(Json(orders.iter().map(build_recurring_response).collect()))
}

/// Fetch one recurring order owned by the user.
#[utoipa::path(
    get,
    path = "/api/v1/orders/recurring/{id}",
    tag = "recurring-orders",
    params(("id" = Uuid, Path, description = "Recurring order id")),
    responses(
        (status = 200, description = "The recurring order", body = RecurringOrderWire),
        (status = 401, description = "Missing or invalid user id header", body = String),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 404, description = "Not found (or owned by another user)", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [], "user_id" = [])),
)]
pub async fn get_recurring_order(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<RecurringOrderWire>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let order = state
        .recurring_repo
        .get_recurring_order(id, user.user_id)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {e}"),
            )
        })?;

    match order {
        Some(o) => Ok(Json(build_recurring_response(&o))),
        None => Err((
            axum::http::StatusCode::NOT_FOUND,
            "Recurring order not found".to_string(),
        )),
    }
}

/// Delete a recurring order owned by the user.
#[utoipa::path(
    delete,
    path = "/api/v1/orders/recurring/{id}",
    tag = "recurring-orders",
    params(("id" = Uuid, Path, description = "Recurring order id")),
    responses(
        (status = 200, description = "`{\"success\": true}`", body = serde_json::Value),
        (status = 401, description = "Missing or invalid user id header", body = String),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 404, description = "Not found (or owned by another user)", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [], "user_id" = [])),
)]
pub async fn delete_recurring_order(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let deleted = state
        .recurring_repo
        .delete_recurring_order(id, user.user_id)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {e}"),
            )
        })?;

    if deleted {
        Ok(Json(serde_json::json!({ "success": true })))
    } else {
        Err((
            axum::http::StatusCode::NOT_FOUND,
            "Recurring order not found".to_string(),
        ))
    }
}

/// Shared body for pause/resume: flip status scoped to the owner; 404 if absent.
async fn set_recurring_status_handler(
    state: &AppState,
    id: Uuid,
    user_id: Uuid,
    status: RecurringStatus,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let updated = state
        .recurring_repo
        .set_recurring_status(id, user_id, status)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {e}"),
            )
        })?;

    if updated {
        Ok(Json(serde_json::json!({ "success": true })))
    } else {
        Err((
            axum::http::StatusCode::NOT_FOUND,
            "Recurring order not found".to_string(),
        ))
    }
}

/// Pause a recurring order (status → `paused`).
#[utoipa::path(
    post,
    path = "/api/v1/orders/recurring/{id}/pause",
    tag = "recurring-orders",
    params(("id" = Uuid, Path, description = "Recurring order id")),
    responses(
        (status = 200, description = "`{\"success\": true}`", body = serde_json::Value),
        (status = 401, description = "Missing or invalid user id header", body = String),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 404, description = "Not found (or owned by another user)", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [], "user_id" = [])),
)]
pub async fn pause_recurring_order(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    set_recurring_status_handler(&state, id, user.user_id, RecurringStatus::Paused).await
}

/// Resume a paused recurring order (status → `active`).
#[utoipa::path(
    post,
    path = "/api/v1/orders/recurring/{id}/resume",
    tag = "recurring-orders",
    params(("id" = Uuid, Path, description = "Recurring order id")),
    responses(
        (status = 200, description = "`{\"success\": true}`", body = serde_json::Value),
        (status = 401, description = "Missing or invalid user id header", body = String),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 404, description = "Not found (or owned by another user)", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [], "user_id" = [])),
)]
pub async fn resume_recurring_order(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    set_recurring_status_handler(&state, id, user.user_id, RecurringStatus::Active).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn order(side: OrderSide, price: i64) -> TradingOrder {
        TradingOrder {
            id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            order_type: OrderType::Limit,
            side,
            energy_amount: Decimal::new(100, 0),
            price_per_kwh: Decimal::new(price, 2),
            filled_amount: Decimal::ZERO,
            status: OrderStatus::Pending,
            expires_at: None,
            created_at: None,
            filled_at: None,
            epoch_id: None,
            zone_id: Some(1),
            meter_id: None,
            refund_tx_signature: None,
            order_pda: None,
            order_index: None,
            session_token: None,
            blockchain_status: None,
            blockchain_tx_hash: None,
            blockchain_error: None,
            retry_count: 0,
            time_in_force: TimeInForce::Gtc,
            market_segment: trading_core::types::MarketSegment::Realtime,
        }
    }

    fn price_alert(status: AlertStatus, note: Option<&str>) -> PriceAlert {
        PriceAlert {
            id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            target_price: Decimal::new(125, 1), // 12.5
            condition: AlertCondition::Above,
            status,
            triggered_at: None,
            triggered_price: None,
            repeat: false,
            note: note.map(str::to_string),
            created_at: gridtokenx_telemetry::time::now(),
            updated_at: None,
        }
    }

    #[test]
    fn price_alert_active_maps_symbol_and_decimal() {
        let r = build_price_alert_response(&price_alert(AlertStatus::Active, Some("GRID")));
        assert_eq!(r.symbol, "GRID"); // note -> symbol
        assert_eq!(r.target_price, "12.5");
        assert_eq!(r.condition, "above");
        assert!(r.is_active);
    }

    #[test]
    fn price_alert_triggered_inactive_empty_symbol() {
        let r = build_price_alert_response(&price_alert(AlertStatus::Triggered, None));
        assert_eq!(r.symbol, ""); // null note -> ""
        assert!(!r.is_active);
    }

    fn recurring(status: RecurringStatus) -> RecurringOrder {
        RecurringOrder {
            id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            side: OrderSide::Buy,
            energy_amount: Decimal::new(1050, 2), // 10.50
            max_price_per_kwh: Some(Decimal::new(20, 2)), // 0.20
            min_price_per_kwh: None,
            interval_type: IntervalType::Daily,
            interval_value: 2,
            next_execution_at: gridtokenx_telemetry::time::now(),
            last_executed_at: None,
            status,
            total_executions: 0,
            max_executions: Some(5),
            name: Some("dca".to_string()),
            description: None,
            created_at: gridtokenx_telemetry::time::now(),
            updated_at: gridtokenx_telemetry::time::now(),
        }
    }

    #[test]
    fn recurring_response_maps_decimals_and_enums() {
        let r = build_recurring_response(&recurring(RecurringStatus::Active));
        assert_eq!(r.side, "buy");
        assert_eq!(r.energy_amount, "10.50"); // decimal preserved as string
        assert_eq!(r.max_price_per_kwh.as_deref(), Some("0.20"));
        assert_eq!(r.min_price_per_kwh, None);
        assert_eq!(r.interval_type, "daily");
        assert_eq!(r.status, "active");
        assert_eq!(r.interval_value, 2);
    }

    #[test]
    fn recurring_response_paused_status() {
        let r = build_recurring_response(&recurring(RecurringStatus::Paused));
        assert_eq!(r.status, "paused");
    }

    #[test]
    fn matching_status_empty() {
        let r = build_matching_status(&[], &[], gridtokenx_telemetry::time::now());
        assert_eq!(r.pending_buy_orders, 0);
        assert_eq!(r.pending_sell_orders, 0);
        assert_eq!(r.pending_matches, 0);
        assert_eq!(r.buy_price_range.min, 0.0);
        assert_eq!(r.sell_price_range.max, 0.0);
        assert!(!r.can_match);
        assert_eq!(r.match_reason, "no orders");
    }

    #[test]
    fn matching_status_buys_only() {
        let r = build_matching_status(&[order(OrderSide::Buy, 500)], &[], gridtokenx_telemetry::time::now());
        assert_eq!(r.pending_buy_orders, 1);
        assert_eq!(r.pending_sell_orders, 0);
        assert!(!r.can_match);
        assert_eq!(r.pending_matches, 0);
        assert_eq!(r.match_reason, "no sell liquidity");
    }

    #[test]
    fn matching_status_sells_only() {
        let r = build_matching_status(&[], &[order(OrderSide::Sell, 400)], gridtokenx_telemetry::time::now());
        assert_eq!(r.pending_sell_orders, 1);
        assert!(!r.can_match);
        assert_eq!(r.match_reason, "no buy liquidity");
    }

    #[test]
    fn matching_status_crossing() {
        // buy_max 500 >= sell_min 400 → crossing
        let buys = vec![order(OrderSide::Buy, 450), order(OrderSide::Buy, 500)];
        let sells = vec![order(OrderSide::Sell, 400), order(OrderSide::Sell, 600)];
        let r = build_matching_status(&buys, &sells, gridtokenx_telemetry::time::now());
        assert!(r.can_match);
        assert!(r.pending_matches > 0);
        assert_eq!(r.buy_price_range.min, 4.5);
        assert_eq!(r.buy_price_range.max, 5.0);
        assert_eq!(r.sell_price_range.min, 4.0);
        assert_eq!(r.sell_price_range.max, 6.0);
        assert_eq!(r.match_reason, "orders crossing");
    }

    #[test]
    fn matching_status_non_crossing() {
        // buy_max 300 < sell_min 400 → spread too wide
        let buys = vec![order(OrderSide::Buy, 300)];
        let sells = vec![order(OrderSide::Sell, 400)];
        let r = build_matching_status(&buys, &sells, gridtokenx_telemetry::time::now());
        assert!(!r.can_match);
        assert_eq!(r.pending_matches, 0);
        assert_eq!(r.match_reason, "spread too wide");
    }

    /// Expired-but-unreaped orders must not count as liquidity: the DB query no
    /// longer filters them, so build_matching_status filters by expires_at itself.
    #[test]
    fn matching_status_excludes_expired_orders() {
        let now = gridtokenx_telemetry::time::now();
        let mut expired_sell = order(OrderSide::Sell, 400);
        expired_sell.expires_at = Some(now - chrono::Duration::seconds(1));
        // Crossing buy (500) vs an expired sell (400): without filtering this would
        // report can_match=true; the expired sell must be dropped → no liquidity.
        let r = build_matching_status(&[order(OrderSide::Buy, 500)], &[expired_sell], now);
        assert_eq!(r.pending_sell_orders, 0, "expired sell excluded");
        assert!(!r.can_match, "no live counterparty → cannot match");
        assert_eq!(r.pending_matches, 0);
        assert_eq!(r.match_reason, "no sell liquidity");
    }

    #[test]
    fn matching_status_pending_matches_min_side() {
        // 3 crossable buys, 1 crossable sell → min = 1
        let buys = vec![
            order(OrderSide::Buy, 500),
            order(OrderSide::Buy, 510),
            order(OrderSide::Buy, 520),
        ];
        let sells = vec![order(OrderSide::Sell, 400)];
        let r = build_matching_status(&buys, &sells, gridtokenx_telemetry::time::now());
        assert_eq!(r.pending_matches, 1);
    }

    fn book_entry(side: OrderSide, price: i64, amount: i64) -> OrderBookEntry {
        OrderBookEntry {
            order_id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            side,
            energy_amount: Decimal::new(amount, 0),
            original_amount: Decimal::new(amount, 0),
            price_per_kwh: Decimal::new(price, 2),
            created_at: gridtokenx_telemetry::time::now(),
            zone_id: Some(1),
            session_token: None,
            signature: None,
            payload_bytes: None,
            time_in_force: TimeInForce::Gtc,
        }
    }

    #[test]
    fn settlement_stats_mapping() {
        let s = SettlementStats {
            pending_count: 3,
            processing_count: 1,
            confirmed_count: 5,
            failed_count: 2,
            total_settled_value: Decimal::new(12550, 2),
        };
        let r = build_settlement_stats_response(&s);
        assert_eq!(r.pending_count, 3);
        assert_eq!(r.processing_count, 1);
        assert_eq!(r.confirmed_count, 5);
        assert_eq!(r.failed_count, 2);
        assert_eq!(r.total_settled_value, 125.5);
    }

    #[test]
    fn orderbook_empty() {
        let r = build_p2p_orderbook(&[]);
        assert!(r.asks.is_empty());
        assert!(r.bids.is_empty());
    }

    #[test]
    fn orderbook_split_and_sort() {
        let entries = vec![
            book_entry(OrderSide::Buy, 450, 10),
            book_entry(OrderSide::Buy, 500, 5),
            book_entry(OrderSide::Sell, 600, 8),
            book_entry(OrderSide::Sell, 550, 3),
        ];
        let r = build_p2p_orderbook(&entries);
        // bids descending by price
        assert_eq!(r.bids[0][0], "5.00");
        assert_eq!(r.bids[1][0], "4.50");
        // asks ascending by price
        assert_eq!(r.asks[0][0], "5.50");
        assert_eq!(r.asks[1][0], "6.00");
    }

    #[test]
    fn orderbook_aggregates_price_level() {
        // two buys at same price → summed amount
        let entries = vec![
            book_entry(OrderSide::Buy, 500, 10),
            book_entry(OrderSide::Buy, 500, 15),
        ];
        let r = build_p2p_orderbook(&entries);
        assert_eq!(r.bids.len(), 1);
        assert_eq!(r.bids[0][0], "5.00");
        assert_eq!(r.bids[0][1], "25");
    }

    // ── Trades (Phase 3) ────────────────────────────────────────────────────

    fn settlement(buyer: Uuid, seller: Uuid) -> trading_core::models::Settlement {
        use trading_core::models::SettlementStatus;
        trading_core::models::Settlement {
            id: Uuid::new_v4(),
            trade_id: None,
            epoch_id: Uuid::nil(),
            buyer_id: buyer,
            seller_id: seller,
            buy_order_id: Uuid::new_v4(),
            sell_order_id: Uuid::new_v4(),
            energy_amount: Decimal::new(105, 1), // 10.5
            price: Decimal::new(12, 1),          // 1.2
            total_amount: Decimal::new(126, 1),  // 12.6
            fee_amount: Decimal::new(5, 2),      // 0.05
            net_amount: Decimal::new(1255, 2),
            status: SettlementStatus::Completed,
            blockchain_tx: Some("sig123".to_string()),
            created_at: gridtokenx_telemetry::time::now(),
            confirmed_at: None,
            wheeling_charge: Some(Decimal::new(2, 2)),
            loss_factor: None,
            loss_cost: Some(Decimal::new(1, 2)),
            effective_energy: Some(Decimal::new(104, 1)),
            buyer_zone_id: Some(1),
            seller_zone_id: Some(2),
            buyer_session_token: None,
            seller_session_token: None,
            erc_certificate_id: None,
            erc_transfer_tx: None,
            retry_count: 0,
            error_message: None,
        }
    }

    #[test]
    fn trade_record_role_buyer() {
        let me = Uuid::new_v4();
        let other = Uuid::new_v4();
        let r = build_trade_record(&settlement(me, other), me);
        assert_eq!(r.role, "buyer");
        assert_eq!(r.counterparty_id, other);
        assert_eq!(r.quantity, "10.5");
        assert_eq!(r.price, "1.2");
        assert_eq!(r.total_value, "12.6");
        assert_eq!(r.status, "completed");
        assert_eq!(r.transaction_hash.as_deref(), Some("sig123"));
    }

    /// A parked settlement must carry its diagnostics to the client — the
    /// UI renders `error_message`/`retry_count` on `permanently_failed` rows.
    #[test]
    fn trade_record_surfaces_permanent_failure_diagnostics() {
        use trading_core::models::SettlementStatus;
        let me = Uuid::new_v4();
        let other = Uuid::new_v4();
        let mut s = settlement(me, other);
        s.status = SettlementStatus::PermanentlyFailed;
        s.retry_count = 5;
        s.error_message = Some("chain bridge: blockhash expired".to_string());

        let r = build_trade_record(&s, me);

        assert_eq!(r.status, "permanently_failed");
        assert_eq!(r.retry_count, 5);
        assert_eq!(
            r.error_message.as_deref(),
            Some("chain bridge: blockhash expired")
        );
    }

    /// A healthy settlement reports no failure reason.
    #[test]
    fn trade_record_healthy_has_no_error_message() {
        let me = Uuid::new_v4();
        let r = build_trade_record(&settlement(me, Uuid::new_v4()), me);
        assert_eq!(r.retry_count, 0);
        assert!(r.error_message.is_none());
    }

    #[test]
    fn trade_record_role_seller() {
        let me = Uuid::new_v4();
        let other = Uuid::new_v4();
        let r = build_trade_record(&settlement(other, me), me);
        assert_eq!(r.role, "seller");
        assert_eq!(r.counterparty_id, other);
    }

    #[test]
    fn trade_record_null_optionals_zero() {
        let me = Uuid::new_v4();
        let mut s = settlement(me, Uuid::new_v4());
        s.wheeling_charge = None;
        s.loss_cost = None;
        s.effective_energy = None;
        let r = build_trade_record(&s, me);
        assert_eq!(r.wheeling_charge, "0");
        assert_eq!(r.loss_cost, "0");
        assert_eq!(r.effective_energy, "0");
    }

    #[test]
    fn trades_response_dual_totals() {
        let me = Uuid::new_v4();
        let resp = build_trades_response(&[settlement(me, Uuid::new_v4())], 7, me);
        assert_eq!(resp.trades.len(), 1);
        assert_eq!(resp.total, 7);
        assert_eq!(resp.total_count, 7);
    }

    #[test]
    fn csv_field_escapes() {
        assert_eq!(csv_field("plain"), "plain");
        assert_eq!(csv_field("a,b"), "\"a,b\"");
        assert_eq!(csv_field("a\"b"), "\"a\"\"b\"");
        assert_eq!(csv_field("a\nb"), "\"a\nb\"");
    }

    #[test]
    fn csv_has_header_and_row() {
        let me = Uuid::new_v4();
        let rec = build_trade_record(&settlement(me, Uuid::new_v4()), me);
        let csv = trades_to_csv(&[rec]);
        let mut lines = csv.lines();
        assert!(lines
            .next()
            .expect("csv header line")
            .starts_with("id,executed_at,role,"));
        let row = lines.next().expect("csv data row");
        assert!(row.contains("buyer"));
        assert!(row.contains("12.6"));
        // exactly header + 1 data row
        assert_eq!(csv.lines().count(), 2);
    }
}
