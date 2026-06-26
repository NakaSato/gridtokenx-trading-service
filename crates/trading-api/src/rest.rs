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
use tracing::{error, info};
use trading_core::models::{
    NewPriceAlert, NewRecurringOrder, OrderBookEntry, PriceAlert, RecurringOrder, Settlement,
    SettlementStats, TradingOrder,
};
use trading_core::recurring::next_execution_at;
use trading_core::types::{
    AlertCondition, AlertStatus, IntervalType, OrderSide, OrderStatus, OrderType, RecurringStatus,
    TimeInForce,
};
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct SubmitOrderRequest {
    pub side: String,
    pub order_type: String,
    pub energy_amount_kwh: String,
    pub price_per_kwh: String,
    pub zone_id: i32,
    pub meter_id: Option<Uuid>,
    pub custodial_sign: Option<bool>,
    /// Time-in-force: "gtc" (default), "ioc", or "fok".
    pub time_in_force: Option<String>,
    /// Market segment: "realtime" (default, CDA matcher) or "interval"
    /// (15-min uniform-price clearing).
    pub market_segment: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SubmitOrderResponse {
    pub id: Uuid,
    pub status: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize)]
pub struct OrderBookResponse {
    pub zone_id: i32,
    pub last_update_id: u64,
    pub asks: Vec<[String; 2]>,
    pub bids: Vec<[String; 2]>,
}

#[derive(Debug, Deserialize)]
pub struct ListOrdersParams {
    pub status: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct ListOrdersResponse {
    pub data: Vec<OrderData>,
    pub pagination: Pagination,
}

#[derive(Debug, Serialize)]
pub struct OrderData {
    pub id: Uuid,
    pub zone_id: i32,
    pub side: String,
    pub status: String,
    pub energy_amount_kwh: String,
    pub price_per_kwh: String,
    pub filled_amount_kwh: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize)]
pub struct Pagination {
    pub total: usize,
    pub limit: usize,
    pub offset: usize,
}

#[derive(Debug, Deserialize)]
pub struct QuoteRequest {
    pub buyer_zone_id: i32,
    pub seller_zone_id: i32,
    pub energy_amount_kwh: String,
    pub agreed_price: String,
}

#[derive(Debug, Serialize)]
pub struct QuoteResponse {
    pub quote_id: String,
    pub expires_at: chrono::DateTime<chrono::Utc>,
    pub breakdown: QuoteBreakdown,
    pub grid_metrics: GridMetrics,
}

#[derive(Debug, Serialize)]
pub struct QuoteBreakdown {
    pub energy_cost: String,
    pub wheeling_charge: String,
    pub loss_cost: String,
    pub total_cost: String,
}

#[derive(Debug, Serialize)]
pub struct GridMetrics {
    pub effective_energy_kwh: String,
    pub loss_factor: String,
    pub zone_distance_km: String,
    pub is_grid_compliant: bool,
}

use crate::auth::UserContext;
use gridtokenx_blockchain_core::auth::ServiceRole;

pub async fn submit_order(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Json(req): Json<SubmitOrderRequest>,
) -> Result<Json<SubmitOrderResponse>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

    tracing::info!("Submit order request: {:?}", req);

    let amount = Decimal::from_str(&req.energy_amount_kwh).map_err(|e| {
        (
            axum::http::StatusCode::BAD_REQUEST,
            format!("Invalid energy_amount_kwh: {}", e),
        )
    })?;
    let price = Decimal::from_str(&req.price_per_kwh).map_err(|e| {
        (
            axum::http::StatusCode::BAD_REQUEST,
            format!("Invalid price_per_kwh: {}", e),
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
        None | Some("gtc") => TimeInForce::Gtc,
        Some("ioc") => TimeInForce::Ioc,
        Some("fok") => TimeInForce::Fok,
        Some(other) => {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                format!("Invalid time_in_force: {other} (expected gtc|ioc|fok)"),
            ))
        }
    };

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

    let mut order = TradingOrder {
        id: Uuid::new_v4(),
        user_id: user.user_id,
        order_type,
        side,
        energy_amount: amount,
        price_per_kwh: price,
        filled_amount: Decimal::ZERO,
        status: OrderStatus::Pending,
        expires_at: Some(gridtokenx_telemetry::time::now() + chrono::Duration::hours(24)),
        created_at: Some(gridtokenx_telemetry::time::now()),
        filled_at: None,
        epoch_id: None,
        zone_id: Some(req.zone_id),
        meter_id: req.meter_id,
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

    // ── Optional On-Chain Execution ───────
    if req.custodial_sign.unwrap_or(false) {
        info!("🔗 On-chain order creation requested for user {}", user.user_id);
        
        let market_pubkey = &state.config.solana_programs.trading_market_id;
        let amount_u64 = (amount * Decimal::from(1_000_000_000i64)).to_u64().unwrap_or(0);
        let price_u64 = (price * Decimal::from(1_000_000i64)).to_u64().unwrap_or(0);
        
        match state.blockchain.execute_create_order(
            user.user_id,
            market_pubkey,
            amount_u64,
            price_u64,
            &side.to_string(),
            None,
            req.zone_id as u32,
        ).await {
            Ok((sig, pda, index)) => {
                info!("✅ On-chain order created. Sig: {}, PDA: {}", sig, pda);
                order.order_pda = Some(pda);
                order.order_index = Some(index as i64);
                order.blockchain_tx_hash = Some(sig);
                order.blockchain_status = Some("confirmed".to_string());
            }
            Err(e) => {
                error!("❌ On-chain order creation failed: {}", e);
                return Err((
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("On-chain execution failed: {}", e),
                ));
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

    state
        .order_repo
        .insert_order_with_event(&order, &event)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {}", e),
            )
        })?;

    Ok(Json(SubmitOrderResponse {
        id: order.id,
        status: "open".to_string(),
        created_at: order.created_at.unwrap_or_else(|| gridtokenx_telemetry::time::now()),
    }))
}

pub async fn get_order_book(
    role: ServiceRole,
    State(state): State<AppState>,
    Path(zone_id): Path<i32>,
) -> Result<Json<OrderBookResponse>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

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
        // No exchange-wide sequence source yet; expose the resting-order count as
        // a change proxy (replace with a real sequence when the matcher emits one).
        last_update_id: entries.len() as u64,
        asks,
        bids,
    }))
}

pub async fn list_orders(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Query(params): Query<ListOrdersParams>,
) -> Result<Json<ListOrdersResponse>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;
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

    let data = orders
        .into_iter()
        .map(|o| OrderData {
            id: o.id,
            zone_id: o.zone_id.unwrap_or(0),
            side: o.side.to_string().to_lowercase(),
            status: o.status.to_string().to_lowercase(),
            energy_amount_kwh: o.energy_amount.to_string(),
            price_per_kwh: o.price_per_kwh.to_string(),
            filled_amount_kwh: o.filled_amount.to_string(),
            created_at: o.created_at.unwrap_or_else(|| gridtokenx_telemetry::time::now()),
        })
        .collect::<Vec<_>>();

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

pub async fn get_order_by_id(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<OrderData>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

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

    Ok(Json(OrderData {
        id: order.id,
        zone_id: order.zone_id.unwrap_or(0),
        side: order.side.to_string().to_lowercase(),
        status: order.status.to_string().to_lowercase(),
        energy_amount_kwh: order.energy_amount.to_string(),
        price_per_kwh: order.price_per_kwh.to_string(),
        filled_amount_kwh: order.filled_amount.to_string(),
        created_at: order.created_at.unwrap_or_else(|| gridtokenx_telemetry::time::now()),
    }))
}

pub async fn cancel_order(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

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

pub async fn create_quote(
    role: ServiceRole,
    Json(_req): Json<QuoteRequest>,
) -> Result<Json<QuoteResponse>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;
    let qid = format!("q_{}", &Uuid::new_v4().to_string()[..8]);
    // Mock for now to match the redesign spec
    Ok(Json(QuoteResponse {
        quote_id: qid,
        expires_at: gridtokenx_telemetry::time::now() + chrono::Duration::minutes(5),
        breakdown: QuoteBreakdown {
            energy_cost: "450.00".to_string(),
            wheeling_charge: "12.50".to_string(),
            loss_cost: "5.20".to_string(),
            total_cost: "467.70".to_string(),
        },
        grid_metrics: GridMetrics {
            effective_energy_kwh: "98.50".to_string(),
            loss_factor: "0.015".to_string(),
            zone_distance_km: "15.2".to_string(),
            is_grid_compliant: true,
        },
    }))
}

#[derive(Debug, Serialize)]
pub struct MarketStatsResponse {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub total_volume_24h_kwh: String,
    pub avg_price_24h: String,
    pub active_users: u32,
    pub grid_stability_index: String,
    pub renewable_ratio: String,
}

pub async fn get_market_stats(
    role: ServiceRole,
) -> Result<Json<MarketStatsResponse>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;
    Ok(Json(MarketStatsResponse {
        timestamp: gridtokenx_telemetry::time::now(),
        total_volume_24h_kwh: "12500.50".to_string(),
        avg_price_24h: "4.45".to_string(),
        active_users: 156,
        grid_stability_index: "0.98".to_string(),
        renewable_ratio: "0.85".to_string(),
    }))
}

// =============================================================================
// Futures Mock Handlers
// =============================================================================

pub async fn get_futures_products(
    role: ServiceRole,
    State(state): State<AppState>,
) -> Result<Json<Vec<trading_core::models::FuturesProduct>>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

    let products = state.futures_repo.get_products().await.map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Database error: {}", e),
        )
    })?;

    Ok(Json(products))
}

#[derive(Debug, Deserialize)]
pub struct CreateFuturesOrderRequest {
    pub product_id: String,
    pub side: String,
    pub order_type: String,
    pub quantity: f64,
    pub price: f64,
    pub leverage: u32,
}

pub async fn create_futures_order(
    role: ServiceRole,
    Json(_req): Json<CreateFuturesOrderRequest>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;
    Ok(Json(serde_json::json!({
        "order_id": Uuid::new_v4().to_string(),
        "status": "open",
    })))
}

pub async fn get_futures_positions(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
) -> Result<Json<Vec<trading_core::models::FuturesPosition>>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

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

pub async fn get_futures_orders(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
) -> Result<Json<Vec<trading_core::models::FuturesOrder>>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

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

pub async fn close_futures_position(
    role: ServiceRole,
    _user: UserContext,
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

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

pub async fn get_futures_candles(
    role: ServiceRole,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;
    Ok(Json(serde_json::json!([])))
}

pub async fn get_futures_order_book(
    role: ServiceRole,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;
    Ok(Json(serde_json::json!({
        "asks": [],
        "bids": [],
    })))
}

// =============================================================================
// User Data Handlers (Modernized)
// =============================================================================

pub async fn get_wallet_balance(
    role: ServiceRole,
    _user: UserContext,
    State(state): State<AppState>,
    Path(address): Path<String>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

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

    Ok(Json(serde_json::json!({
        "wallet_address": address,
        "token_balance": balance_decimal.to_string(),
        "token_balance_raw": balance_raw,
        "balance_sol": 0.0, // Should be fetched from blockchain as well if needed
        "decimals": decimals,
        "token_mint": "GridTokenMint111111111111111111111111111",
    })))
}

pub async fn get_user_analytics_stats(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
) -> Result<Json<trading_core::models::UserAnalytics>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

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

pub async fn get_user_analytics_history(
    role: ServiceRole,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

    Ok(Json(serde_json::json!({
        "history": []
    })))
}

pub async fn get_user_transactions(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
) -> Result<Json<Vec<trading_core::models::TransactionData>>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

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

pub async fn get_carbon_balance(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

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

pub async fn get_carbon_history(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
) -> Result<Json<Vec<trading_core::models::CarbonCredit>>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

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

pub async fn get_carbon_transactions(
    role: ServiceRole,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

    Ok(Json(serde_json::json!([])))
}

pub async fn transfer_carbon_credits(
    role: ServiceRole,
    Json(_req): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

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

#[derive(Debug, Serialize)]
pub struct MarketConfigResponse {
    pub base_price_thb_kwh: f64,
    pub grid_import_price_thb_kwh: f64,
    pub grid_export_price_thb_kwh: f64,
    pub transaction_fee_bps: u32,
    pub min_price_per_kwh: f64,
    pub max_price_per_kwh: f64,
}

#[derive(Debug, Serialize)]
pub struct P2PMarketPricesResponse {
    pub base_price_thb_kwh: f64,
    pub grid_import_price_thb_kwh: f64,
    pub grid_export_price_thb_kwh: f64,
    pub loss_allocation_model: String,
    pub wheeling_charges: HashMap<String, f64>,
    pub loss_factors: HashMap<String, f64>,
}

#[derive(Debug, Serialize)]
pub struct PriceRange {
    pub min: f64,
    pub max: f64,
}

#[derive(Debug, Serialize)]
pub struct MatchingStatusResponse {
    pub pending_buy_orders: usize,
    pub pending_sell_orders: usize,
    pub pending_matches: usize,
    pub buy_price_range: PriceRange,
    pub sell_price_range: PriceRange,
    pub can_match: bool,
    pub match_reason: String,
}

/// GET /api/v1/markets/config — static market pricing parameters.
pub async fn get_market_config(
    role: ServiceRole,
    State(state): State<AppState>,
) -> Result<Json<MarketConfigResponse>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

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

/// GET /api/v1/markets/p2p/market-prices — P2P pricing + wheeling/loss schedules.
pub async fn get_p2p_market_prices(
    role: ServiceRole,
    State(state): State<AppState>,
) -> Result<Json<P2PMarketPricesResponse>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

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

/// GET /api/v1/markets/matching-status — live order-book crossing summary.
pub async fn get_matching_status(
    role: ServiceRole,
    State(state): State<AppState>,
) -> Result<Json<MatchingStatusResponse>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

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

    Ok(Json(build_matching_status(&buys, &sells)))
}

/// Pure aggregation over active orders — unit-testable without a DB.
fn build_matching_status(buys: &[TradingOrder], sells: &[TradingOrder]) -> MatchingStatusResponse {
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

#[derive(Debug, Serialize)]
pub struct SettlementStatsResponse {
    pub pending_count: i64,
    pub processing_count: i64,
    pub confirmed_count: i64,
    pub failed_count: i64,
    pub total_settled_value: f64,
}

#[derive(Debug, Serialize)]
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

/// GET /api/v1/markets/settlement-stats — settlement counts by status.
pub async fn get_settlement_stats(
    role: ServiceRole,
    State(state): State<AppState>,
) -> Result<Json<SettlementStatsResponse>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

    let stats = state.settlement_repo.get_settlement_stats().await.map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Database error: {}", e),
        )
    })?;
    Ok(Json(build_settlement_stats_response(&stats)))
}

/// GET /api/v1/markets/orderbook — cross-zone P2P aggregate order book.
pub async fn get_p2p_orderbook(
    role: ServiceRole,
    State(state): State<AppState>,
) -> Result<Json<P2POrderBookResponse>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

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

#[derive(Debug, Deserialize)]
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
#[derive(Debug, Serialize)]
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
}

#[derive(Debug, Serialize)]
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

/// GET /api/v1/trades — authenticated user's trade history (newest first).
pub async fn get_trades(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Query(params): Query<TradesQuery>,
) -> Result<Json<TradesListResponse>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

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

/// GET /api/v1/trades/export — same data as CSV (or JSON via `?format=json`).
pub async fn export_trades(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Query(params): Query<TradesQuery>,
) -> Result<Response, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

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
#[derive(Debug, Deserialize)]
pub struct CreatePriceAlertRequest {
    pub symbol: Option<String>,
    pub target_price: String,
    pub condition: String,
}

/// Wire shape matching frontend `PriceAlert` (`types/features.ts`): `symbol`
/// echoed from `note`, `is_active` derived from status, decimals as strings.
#[derive(Debug, Serialize)]
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

/// POST /api/v1/price-alerts — create an alert for the authenticated user.
pub async fn create_price_alert(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Json(req): Json<CreatePriceAlertRequest>,
) -> Result<Json<PriceAlertResponse>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

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

/// GET /api/v1/price-alerts — list the authenticated user's alerts (newest first).
pub async fn list_price_alerts(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
) -> Result<Json<Vec<PriceAlertResponse>>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

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

/// DELETE /api/v1/price-alerts/{id} — delete an alert owned by the user.
pub async fn delete_price_alert(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

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
#[derive(Debug, Deserialize)]
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
#[derive(Debug, Serialize)]
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

/// POST /api/v1/orders/recurring — create a recurring order for the user.
pub async fn create_recurring_order(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Json(req): Json<CreateRecurringRequest>,
) -> Result<Json<RecurringOrderWire>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

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

/// GET /api/v1/orders/recurring — list the user's recurring orders (newest first).
pub async fn list_recurring_orders(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
) -> Result<Json<Vec<RecurringOrderWire>>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

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

/// GET /api/v1/orders/recurring/{id} — fetch one recurring order owned by the user.
pub async fn get_recurring_order(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<RecurringOrderWire>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

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

/// DELETE /api/v1/orders/recurring/{id} — delete a recurring order owned by the user.
pub async fn delete_recurring_order(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

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

/// POST /api/v1/orders/recurring/{id}/pause — set status to `paused`.
pub async fn pause_recurring_order(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

    set_recurring_status_handler(&state, id, user.user_id, RecurringStatus::Paused).await
}

/// POST /api/v1/orders/recurring/{id}/resume — set status to `active`.
pub async fn resume_recurring_order(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()))?;

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
        let r = build_matching_status(&[], &[]);
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
        let r = build_matching_status(&[order(OrderSide::Buy, 500)], &[]);
        assert_eq!(r.pending_buy_orders, 1);
        assert_eq!(r.pending_sell_orders, 0);
        assert!(!r.can_match);
        assert_eq!(r.pending_matches, 0);
        assert_eq!(r.match_reason, "no sell liquidity");
    }

    #[test]
    fn matching_status_sells_only() {
        let r = build_matching_status(&[], &[order(OrderSide::Sell, 400)]);
        assert_eq!(r.pending_sell_orders, 1);
        assert!(!r.can_match);
        assert_eq!(r.match_reason, "no buy liquidity");
    }

    #[test]
    fn matching_status_crossing() {
        // buy_max 500 >= sell_min 400 → crossing
        let buys = vec![order(OrderSide::Buy, 450), order(OrderSide::Buy, 500)];
        let sells = vec![order(OrderSide::Sell, 400), order(OrderSide::Sell, 600)];
        let r = build_matching_status(&buys, &sells);
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
        let r = build_matching_status(&buys, &sells);
        assert!(!r.can_match);
        assert_eq!(r.pending_matches, 0);
        assert_eq!(r.match_reason, "spread too wide");
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
        let r = build_matching_status(&buys, &sells);
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
