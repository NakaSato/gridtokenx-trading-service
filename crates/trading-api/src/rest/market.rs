//! Market-wide data — stats, config, P2P prices/orderbook, matching, settlement, clearing.
//!
//! Split out of the former 3.3k-line `rest.rs` for readability. Pure code move:
//! handlers are re-exported from `rest/mod.rs`, so every `crate::rest::<name>`
//! path (router wiring, openapi.rs) resolves exactly as before.

use super::{Serialize, ToSchema, Deserialize, IntoParams, MarketStatsResponse, State, ServiceRole, AppState, Json, MarketConfigResponse, dec_f64, P2PMarketPricesResponse, HashMap, MatchingStatusResponse, TradingOrder, PriceRange, SettlementStats, OrderBookEntry, Decimal, OrderSide, MarketPrice, Uuid};

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
            format!("Database error: {e}"),
        )
    })?;
    let active_users = state.settlement_repo.count_active_traders(24).await.map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Database error: {e}"),
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
            format!("Database error: {e}"),
        )
    })?;
    let sells = state.order_repo.get_active_sell_orders().await.map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Database error: {e}"),
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
pub(super) fn build_matching_status(
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
            min: buy_min.map_or(0.0, dec_f64),
            max: buy_max.map_or(0.0, dec_f64),
        },
        sell_price_range: PriceRange {
            min: sell_min.map_or(0.0, dec_f64),
            max: sell_max.map_or(0.0, dec_f64),
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

pub(super) fn build_settlement_stats_response(s: &SettlementStats) -> SettlementStatsResponse {
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
pub(super) fn build_p2p_orderbook(entries: &[OrderBookEntry]) -> P2POrderBookResponse {
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

    let settlement_stats = state.settlement_repo.get_settlement_stats().await.map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Database error: {e}"),
        )
    })?;
    Ok(Json(build_settlement_stats_response(&settlement_stats)))
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
                format!("Database error: {e}"),
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
                format!("Database error: {e}"),
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
            format!("Database error: {e}"),
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
    /// `getTrades` (`TradeHistory`) reads `total_count`.
    pub total_count: i64,
    /// `getTradeHistory` reads `total`.
    pub total: i64,
}
