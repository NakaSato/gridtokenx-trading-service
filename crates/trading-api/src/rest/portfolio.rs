//! Per-user views — wallet balance, analytics, transactions, carbon.
//!
//! Split out of the former 3.3k-line `rest.rs` for readability. Pure code move:
//! handlers are re-exported from `rest/mod.rs`, so every `crate::rest::<name>`
//! path (router wiring, openapi.rs) resolves exactly as before.

use super::*;

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

pub(super) fn dec_f64(d: Decimal) -> f64 {
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
