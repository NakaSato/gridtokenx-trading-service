//! Futures products, orders, positions and candles.
//!
//! Split out of the former 3.3k-line `rest.rs` for readability. Pure code move:
//! handlers are re-exported from `rest/mod.rs`, so every `crate::rest::<name>`
//! path (router wiring, openapi.rs) resolves exactly as before.

use super::*;

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
