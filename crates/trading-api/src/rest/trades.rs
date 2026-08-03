//! Trade history and CSV export.
//!
//! Split out of the former 3.3k-line `rest.rs` for readability. Pure code move:
//! handlers are re-exported from `rest/mod.rs`, so every `crate::rest::<name>`
//! path (router wiring, openapi.rs) resolves exactly as before.

use super::{Deserialize, ToSchema, Serialize, Decimal, Settlement, Uuid, TradeRecordResponse, TradesListResponse, TradesQuery, State, Query, ServiceRole, UserContext, AppState, Json, Response, IntoResponse, header};

pub(super) fn dec_opt_str(d: Option<Decimal>) -> String {
    d.unwrap_or(Decimal::ZERO).to_string()
}

/// Map a settlement to a trade row from the perspective of `user`.
pub(super) fn build_trade_record(s: &Settlement, user: Uuid) -> TradeRecordResponse {
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

pub(super) fn build_trades_response(
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
pub(super) fn csv_field(v: &str) -> String {
    if v.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", v.replace('"', "\"\""))
    } else {
        v.to_string()
    }
}

/// Serialize trade rows to a CSV document (header + one row each). DB-free.
pub(super) fn trades_to_csv(records: &[TradeRecordResponse]) -> String {
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
                format!("Database error: {e}"),
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
                format!("Database error: {e}"),
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
