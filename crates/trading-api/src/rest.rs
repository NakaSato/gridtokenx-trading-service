use crate::state::AppState;
use axum::{
    extract::{Path, Query, State},
    Json,
};
use chrono::Utc;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use tracing::{error, info};
use trading_core::models::TradingOrder;
use trading_core::types::{OrderSide, OrderStatus, OrderType, TimeInForce};
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

    let mut order = TradingOrder {
        id: Uuid::new_v4(),
        user_id: user.user_id,
        order_type,
        side,
        energy_amount: amount,
        price_per_kwh: price,
        filled_amount: Decimal::ZERO,
        status: OrderStatus::Pending,
        expires_at: Some(Utc::now() + chrono::Duration::hours(24)),
        created_at: Some(Utc::now()),
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
        time_in_force: TimeInForce::Gtc,
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

    state.order_repo.insert_order(&order).await.map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Database error: {}", e),
        )
    })?;

    // Publish Event for Event Sourcing
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

    if let Err(e) = state.events.publish(event).await {
        error!("Failed to publish OrderCreated event: {}", e);
    }

    Ok(Json(SubmitOrderResponse {
        id: order.id,
        status: "open".to_string(),
        created_at: order.created_at.unwrap_or_else(Utc::now),
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
            created_at: o.created_at.unwrap_or_else(Utc::now),
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
        created_at: order.created_at.unwrap_or_else(Utc::now),
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
        expires_at: Utc::now() + chrono::Duration::minutes(5),
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
        timestamp: Utc::now(),
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
        "last_updated": Utc::now(),
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
// Settlement Handlers (Oracle Bridge)
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct GenerationMintRequest {
    pub user_id: Uuid,
    pub meter_serial: String,
    pub energy_generated_kwh: Decimal,
    pub start_time: i64,
    pub end_time: i64,
    pub signature: String,
}

#[derive(Debug, Serialize)]
pub struct GenerationMintResponse {
    pub success: bool,
    pub signature: String,
    pub amount_minted: String,
}

pub async fn settle_generation_mint(
    State(state): State<AppState>,
    Json(req): Json<GenerationMintRequest>,
) -> Result<Json<GenerationMintResponse>, (axum::http::StatusCode, String)> {
    info!(
        "Processing generation mint settlement for user: {}",
        req.user_id
    );

    // 1. Verify Signature (Oracle Bridge Public Key)
    let public_key_str = &state.settlement.oracle_bridge_public_key;
    let public_key_bytes = bs58::decode(public_key_str).into_vec().map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Invalid public key config: {}", e),
        )
    })?;

    use ed25519_dalek::{Signature as EdSignature, Verifier, VerifyingKey};

    let verifying_key = VerifyingKey::from_bytes(&public_key_bytes.try_into().map_err(|_| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "Invalid public key length".to_string(),
        )
    })?)
    .map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to create verifying key: {}", e),
        )
    })?;

    let signature_bytes = bs58::decode(&req.signature).into_vec().map_err(|e| {
        (
            axum::http::StatusCode::BAD_REQUEST,
            format!("Invalid signature format: {}", e),
        )
    })?;

    let signature = EdSignature::from_bytes(&signature_bytes.try_into().map_err(|_| {
        (
            axum::http::StatusCode::BAD_REQUEST,
            "Invalid signature length".to_string(),
        )
    })?);

    // Construct payload for verification (must match Oracle Bridge)
    let message = format!(
        "{}:{}:{}:{}:{}",
        req.user_id, req.meter_serial, req.energy_generated_kwh, req.start_time, req.end_time
    );

    if let Err(e) = verifying_key.verify(message.as_bytes(), &signature) {
        error!("Signature verification failed for oracle settlement: {}", e);
        return Err((
            axum::http::StatusCode::UNAUTHORIZED,
            "Invalid oracle signature".to_string(),
        ));
    }

    // 2. Execute Minting Logic
    let result = state
        .settlement
        .execute_generation_mint(req.user_id, req.energy_generated_kwh, req.end_time)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Settlement failed: {}", e),
            )
        })?;

    Ok(Json(GenerationMintResponse {
        success: true,
        signature: result,
        amount_minted: req.energy_generated_kwh.to_string(),
    }))
}

#[derive(Debug, Deserialize)]
pub struct BatchGenerationMintRequest {
    pub requests: Vec<GenerationMintRequest>,
}

#[derive(Debug, Serialize)]
pub struct BatchGenerationMintResponse {
    pub success: bool,
    pub tx_signature: String,
    pub meter_serials: Vec<String>,
}

pub async fn batch_settle_generation_mint(
    State(state): State<AppState>,
    Json(req): Json<BatchGenerationMintRequest>,
) -> Result<Json<BatchGenerationMintResponse>, (axum::http::StatusCode, String)> {
    info!(
        "Processing batched generation mint settlement: count={}",
        req.requests.len()
    );

    let mut settlements = Vec::new();
    let mut serials = Vec::new();

    // Resolve the live market epoch once for the whole batch (settlements.epoch_id
    // is a NOT NULL FK to market_epochs; a nil epoch FK-fails the insert).
    let epoch_id = state
        .order_repo
        .get_or_create_active_epoch()
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to resolve active epoch: {}", e),
            )
        })?;

    // 1. Verify and prepare all requests
    for r in req.requests {
        // Verification logic (reused from single settlement)
        let public_key_str = &state.settlement.oracle_bridge_public_key;
        let public_key_bytes = bs58::decode(public_key_str).into_vec().map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Invalid public key config: {}", e),
            )
        })?;

        use ed25519_dalek::{Signature as EdSignature, Verifier, VerifyingKey};

        let verifying_key = VerifyingKey::from_bytes(&public_key_bytes.try_into().map_err(|_| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Invalid public key length".to_string(),
            )
        })?)
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to create verifying key: {}", e),
            )
        })?;

        let signature_bytes = bs58::decode(&r.signature).into_vec().map_err(|e| {
            (
                axum::http::StatusCode::BAD_REQUEST,
                format!("Invalid signature format: {}", e),
            )
        })?;

        let signature = EdSignature::from_bytes(&signature_bytes.try_into().map_err(|_| {
            (
                axum::http::StatusCode::BAD_REQUEST,
                "Invalid signature length".to_string(),
            )
        })?);

        let message = format!(
            "{}:{}:{}:{}:{}",
            r.user_id, r.meter_serial, r.energy_generated_kwh, r.start_time, r.end_time
        );

        if let Err(e) = verifying_key.verify(message.as_bytes(), &signature) {
            return Err((
                axum::http::StatusCode::UNAUTHORIZED,
                format!("Invalid oracle signature for meter {}", r.meter_serial),
            ));
        }

        let settlement = trading_core::models::Settlement {
            id: Uuid::new_v4(),
            trade_id: None,
            epoch_id,
            buyer_id: state.settlement.platform_user_id(),
            seller_id: r.user_id,
            buy_order_id: Uuid::nil(),
            sell_order_id: Uuid::nil(),
            energy_amount: r.energy_generated_kwh,
            price: Decimal::ZERO,
            total_amount: Decimal::ZERO,
            fee_amount: Decimal::ZERO,
            net_amount: Decimal::ZERO,
            status: trading_core::models::SettlementStatus::Pending,
            blockchain_tx: None,
            created_at: chrono::Utc::now(),
            confirmed_at: None,
            wheeling_charge: None,
            loss_factor: None,
            loss_cost: None,
            effective_energy: None,
            buyer_zone_id: None,
            seller_zone_id: None,
            buyer_session_token: None,
            seller_session_token: None,
            erc_certificate_id: None,
            erc_transfer_tx: None,
            retry_count: 0,
            error_message: None,
        };

        settlements.push(settlement);
        serials.push(r.meter_serial);
    }

    if settlements.is_empty() {
        return Ok(Json(BatchGenerationMintResponse {
            success: true,
            tx_signature: "".to_string(),
            meter_serials: Vec::new(),
        }));
    }

    // 2. Execute via SettlementService
    let results = state
        .settlement
        .execute_batched_settlements(settlements)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Batch settlement failed: {}", e),
            )
        })?;

    let tx_sig = results.first().map(|r| r.signature.clone()).unwrap_or_default();

    Ok(Json(BatchGenerationMintResponse {
        success: true,
        tx_signature: tx_sig,
        meter_serials: serials,
    }))
}
