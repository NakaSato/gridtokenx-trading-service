//! Price alerts.
//!
//! Split out of the former 3.3k-line `rest.rs` for readability. Pure code move:
//! handlers are re-exported from `rest/mod.rs`, so every `crate::rest::<name>`
//! path (router wiring, openapi.rs) resolves exactly as before.

use super::{
    AlertCondition, AlertStatus, AppState, CreatePriceAlertRequest, Decimal, Deserialize, FromStr,
    Json, NewPriceAlert, Path, PriceAlert, PriceAlertResponse, Serialize, ServiceRole, State,
    ToSchema, UserContext, Uuid,
};

/// Map a stored alert to the frontend wire shape. `symbol` falls back to "" when
/// `note` is null; `is_active` is true only while status == active.
#[must_use]
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
                format!(
                    "Invalid condition: {} (expected above|below|crosses)",
                    req.condition
                ),
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
                format!("Database error: {e}"),
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
                format!("Database error: {e}"),
            )
        })?;

    Ok(Json(
        alerts.iter().map(build_price_alert_response).collect(),
    ))
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
                format!("Database error: {e}"),
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
