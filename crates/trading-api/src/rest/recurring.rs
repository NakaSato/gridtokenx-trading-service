//! Recurring (scheduled) orders.
//!
//! Split out of the former 3.3k-line `rest.rs` for readability. Pure code move:
//! handlers are re-exported from `rest/mod.rs`, so every `crate::rest::<name>`
//! path (router wiring, openapi.rs) resolves exactly as before.

use super::{
    next_execution_at, AppState, CreateRecurringRequest, Decimal, FromStr, IntervalType, Json,
    NewRecurringOrder, OrderSide, Path, RecurringOrder, RecurringOrderWire, RecurringStatus,
    ServiceRole, State, UserContext, Uuid,
};

/// Map a stored recurring order to the frontend wire shape (decimals → strings,
/// enums → their serde-rename'd lowercase forms).
#[must_use]
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

pub(super) fn parse_side(s: &str) -> Result<OrderSide, (axum::http::StatusCode, String)> {
    match s.trim().to_lowercase().as_str() {
        "buy" => Ok(OrderSide::Buy),
        "sell" => Ok(OrderSide::Sell),
        _ => Err((
            axum::http::StatusCode::BAD_REQUEST,
            format!("Invalid side: {s} (expected buy|sell)"),
        )),
    }
}

pub(super) fn parse_interval(s: &str) -> Result<IntervalType, (axum::http::StatusCode, String)> {
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

pub(super) fn parse_opt_decimal(
    field: &str,
    value: Option<&str>,
) -> Result<Option<Decimal>, (axum::http::StatusCode, String)> {
    match value {
        None => Ok(None),
        Some(v) => Decimal::from_str(v.trim()).map(Some).map_err(|_| {
            (
                axum::http::StatusCode::BAD_REQUEST,
                format!("Invalid {field}: {v}"),
            )
        }),
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
    let max_price_per_kwh =
        parse_opt_decimal("max_price_per_kwh", req.max_price_per_kwh.as_deref())?;
    let min_price_per_kwh =
        parse_opt_decimal("min_price_per_kwh", req.min_price_per_kwh.as_deref())?;

    // Cadence: default every 1 interval; DB CHECK enforces > 0.
    let interval_value = req.interval_value.unwrap_or(1);
    if interval_value < 1 {
        return Err((
            axum::http::StatusCode::BAD_REQUEST,
            "interval_value must be >= 1".to_string(),
        ));
    }
    let next_exec = next_execution_at(
        gridtokenx_telemetry::time::now(),
        interval_type,
        interval_value,
    );

    let name = req.name.and_then(|s| {
        let t = s.trim().to_string();
        if t.is_empty() {
            None
        } else {
            Some(t)
        }
    });
    let description = req.description.and_then(|s| {
        let t = s.trim().to_string();
        if t.is_empty() {
            None
        } else {
            Some(t)
        }
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
pub(super) async fn set_recurring_status_handler(
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
