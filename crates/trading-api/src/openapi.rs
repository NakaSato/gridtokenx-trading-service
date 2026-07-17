//! OpenAPI document for the REST surface (`rest.rs` + health/metrics).
//!
//! Served at `/api-docs/openapi.json` with Swagger UI at `/docs` (wired in
//! `startup::build_router`). Schemas referenced from the `#[utoipa::path]`
//! annotations are collected automatically; only the security schemes need
//! explicit registration here.

use utoipa::openapi::security::{ApiKey, ApiKeyValue, SecurityScheme};
use utoipa::{Modify, OpenApi};

/// Registers the gateway header schemes. Auth is header-based: APISIX injects
/// `x-gridtokenx-role` (RBAC, see `gridtokenx_blockchain_auth`) and
/// `x-gridtokenx-user-id` (the JWT-resolved user, see `auth::UserContext`).
struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "gateway_role",
            SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::with_description(
                gridtokenx_blockchain_auth::INTERNAL_ROLE_HEADER,
                "Service role injected by the gateway (RBAC)",
            ))),
        );
        components.add_security_scheme(
            "user_id",
            SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::with_description(
                crate::auth::USER_ID_HEADER,
                "JWT-resolved user id injected by the gateway",
            ))),
        );
    }
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "GridTokenX Trading Service — REST API",
        description = "P2P energy trading: spot orders (CDA + interval clearing), trades, \
            price alerts, recurring orders, futures, carbon credits, wallets and analytics. \
            Money/energy amounts are decimal strings unless a schema says otherwise. \
            A gRPC (ConnectRPC) surface exists alongside this REST API and is not covered here.",
        version = env!("CARGO_PKG_VERSION"),
    ),
    paths(
        // Spot / core orders
        crate::rest::submit_order,
        crate::rest::list_orders,
        crate::rest::get_order_by_id,
        crate::rest::cancel_order,
        crate::rest::create_quote,
        crate::rest::get_order_book,
        crate::rest::get_market_stats,
        // Markets (read-only)
        crate::rest::get_market_config,
        crate::rest::get_p2p_market_prices,
        crate::rest::get_matching_status,
        crate::rest::get_settlement_stats,
        crate::rest::get_p2p_orderbook,
        crate::rest::get_clearing_epochs,
        crate::rest::list_active_order_meters,
        // Trades
        crate::rest::get_trades,
        crate::rest::export_trades,
        // Price alerts
        crate::rest::create_price_alert,
        crate::rest::list_price_alerts,
        crate::rest::delete_price_alert,
        // Recurring orders
        crate::rest::create_recurring_order,
        crate::rest::list_recurring_orders,
        crate::rest::get_recurring_order,
        crate::rest::delete_recurring_order,
        crate::rest::pause_recurring_order,
        crate::rest::resume_recurring_order,
        // Futures
        crate::rest::get_futures_products,
        crate::rest::get_futures_candles,
        crate::rest::get_futures_order_book,
        crate::rest::create_futures_order,
        crate::rest::get_futures_orders,
        crate::rest::get_futures_positions,
        crate::rest::close_futures_position,
        // User data
        crate::rest::get_wallet_balance,
        crate::rest::get_user_analytics_stats,
        crate::rest::get_user_analytics_history,
        crate::rest::get_user_transactions,
        // Carbon / ESG
        crate::rest::get_carbon_balance,
        crate::rest::get_carbon_history,
        crate::rest::get_carbon_transactions,
        crate::rest::transfer_carbon_credits,
        // System
        crate::startup::health_check,
        crate::startup::metrics_handler,
    ),
    modifiers(&SecurityAddon),
    tags(
        (name = "orders", description = "Spot orders (CDA realtime + 15-min interval clearing)"),
        (name = "quotes", description = "Price quotes with wheeling/loss breakdown"),
        (name = "markets", description = "Market config, prices, order books, clearing epochs"),
        (name = "trades", description = "Trade (settlement) history and export"),
        (name = "price-alerts", description = "User price alerts"),
        (name = "recurring-orders", description = "Recurring order CRUD and pause/resume"),
        (name = "futures", description = "Futures products, orders and positions"),
        (name = "wallets", description = "On-chain wallet balances (via Chain Bridge)"),
        (name = "analytics", description = "Per-user trading analytics and transactions"),
        (name = "carbon", description = "Carbon credit balances, history and transfers"),
        (name = "system", description = "Health and Prometheus metrics (unauthenticated)"),
    ),
)]
pub struct ApiDoc;

#[cfg(test)]
mod tests {
    use super::*;

    /// The doc must serialize and carry every REST route in `startup::build_router`
    /// (7 op-groups under /api/v1/orders*, markets, trades, alerts, futures, carbon,
    /// user data) plus /health and /metrics.
    #[test]
    fn openapi_doc_generates_and_covers_rest_surface() {
        let doc = ApiDoc::openapi();
        let json = doc.to_json().expect("openapi doc serializes");

        let paths = &doc.paths.paths;
        assert!(
            paths.len() >= 30,
            "expected >= 30 documented paths, got {}",
            paths.len()
        );
        for p in [
            "/api/v1/orders",
            "/api/v1/orders/{id}",
            "/api/v1/zones/{zone_id}/book",
            "/api/v1/markets/clearing-epochs",
            "/api/v1/markets/active-order-meters",
            "/api/v1/trades/export",
            "/api/v1/price-alerts/{id}",
            "/api/v1/orders/recurring/{id}/pause",
            "/api/v1/futures/positions/{id}",
            "/api/v1/carbon/transfers",
            "/api/v1/wallets/{address}/balance",
            "/health",
            "/metrics",
        ] {
            assert!(paths.contains_key(p), "missing path {p}");
        }

        // Security schemes registered by the modifier.
        assert!(json.contains("gateway_role") && json.contains("x-gridtokenx-role"));
        assert!(json.contains("user_id") && json.contains("x-gridtokenx-user-id"));
    }
}
