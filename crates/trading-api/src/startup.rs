use axum::{http::StatusCode, response::IntoResponse, Json, Router};
use connectrpc::Server;
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;
use tracing::{error, info};

use crate::handlers::TradingGrpcService;
use crate::state::AppState;
use trading_protocol::trading_proto::TradingServiceExt;

pub async fn run(
    state: AppState,
    port: u16,
    grpc_port: u16,
    token: CancellationToken,
) -> anyhow::Result<()> {
    // 1. Build REST Router
    let app = build_router(state.clone());

    // 2. Initialize gRPC Service
    let grpc_service = TradingGrpcService::new(state);
    let grpc_router = std::sync::Arc::new(grpc_service).register(connectrpc::Router::new());
    let grpc_server = Server::new(grpc_router);

    // 3. Start Servers
    let rest_addr: std::net::SocketAddr = format!("0.0.0.0:{}", port).parse()?;
    let grpc_addr: std::net::SocketAddr = format!("0.0.0.0:{}", grpc_port).parse()?;

    info!("🚀 Trading REST API listening on {}", rest_addr);
    info!("🚀 Trading gRPC/ConnectRPC listening on {}", grpc_addr);

    let rest_listener = tokio::net::TcpListener::bind(&rest_addr).await?;

    let rest_token = token.clone();
    let rest_handle = axum::serve(rest_listener, app).with_graceful_shutdown(async move {
        rest_token.cancelled().await;
    });

    let grpc_token = token.clone();
    let grpc_handle = async move {
        tokio::select! {
            res = grpc_server.serve(grpc_addr) => {
                res.map_err(|e| anyhow::anyhow!("gRPC failed: {}", e))
            }
            _ = grpc_token.cancelled() => {
                info!("🔄 Trading gRPC Service shutting down...");
                Ok(())
            }
        }
    };

    tokio::select! {
        res = rest_handle => {
            if let Err(e) = res {
                error!("REST server failed: {}", e);
            }
        }
        res = grpc_handle => {
            if let Err(e) = res {
                error!("gRPC server failed: {}", e);
            }
        }
    };

    Ok(())
}

/// Liveness/readiness probe. Also served at `/health/ready`.
#[utoipa::path(
    get,
    path = "/health",
    tag = "system",
    responses((status = 200, description = "`{\"status\": \"ok\", \"service\": \"trading\"}`", body = serde_json::Value)),
)]
pub(crate) async fn health_check() -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(serde_json::json!({"status": "ok", "service": "trading"})),
    )
}

/// Prometheus metrics in text exposition format.
#[utoipa::path(
    get,
    path = "/metrics",
    tag = "system",
    responses((status = 200, description = "Prometheus text format", content_type = "text/plain", body = String)),
)]
pub(crate) async fn metrics_handler() -> impl IntoResponse {
    (StatusCode::OK, trading_infra::metrics::render())
}

pub fn build_router(state: AppState) -> Router {
    use utoipa::OpenApi;
    Router::new()
        // OpenAPI: Swagger UI at /docs, raw spec at /api-docs/openapi.json.
        .merge(
            utoipa_swagger_ui::SwaggerUi::new("/docs")
                .url("/api-docs/openapi.json", crate::openapi::ApiDoc::openapi()),
        )
        // Spot / Core Orders
        .route(
            "/api/v1/orders",
            axum::routing::post(crate::rest::submit_order).get(crate::rest::list_orders),
        )
        .route(
            "/api/v1/orders/{id}",
            axum::routing::get(crate::rest::get_order_by_id).delete(crate::rest::cancel_order),
        )
        .route(
            "/api/v1/quotes",
            axum::routing::post(crate::rest::create_quote),
        )
        .route(
            "/api/v1/zones/{zone_id}/book",
            axum::routing::get(crate::rest::get_order_book),
        )
        .route(
            "/api/v1/stats",
            axum::routing::get(crate::rest::get_market_stats),
        )
        // Markets (read-only)
        .route(
            "/api/v1/markets/config",
            axum::routing::get(crate::rest::get_market_config),
        )
        .route(
            "/api/v1/markets/p2p/market-prices",
            axum::routing::get(crate::rest::get_p2p_market_prices),
        )
        .route(
            "/api/v1/markets/matching-status",
            axum::routing::get(crate::rest::get_matching_status),
        )
        .route(
            "/api/v1/markets/settlement-stats",
            axum::routing::get(crate::rest::get_settlement_stats),
        )
        .route(
            "/api/v1/markets/orderbook",
            axum::routing::get(crate::rest::get_p2p_orderbook),
        )
        .route(
            "/api/v1/markets/clearing-epochs",
            axum::routing::get(crate::rest::get_clearing_epochs),
        )
        .route(
            "/api/v1/markets/active-order-meters",
            axum::routing::get(crate::rest::list_active_order_meters),
        )
        // Trades — history (JSON) + export (CSV)
        .route(
            "/api/v1/trades",
            axum::routing::get(crate::rest::get_trades),
        )
        .route(
            "/api/v1/trades/export",
            axum::routing::get(crate::rest::export_trades),
        )
        // Price alerts — CRUD
        .route(
            "/api/v1/price-alerts",
            axum::routing::post(crate::rest::create_price_alert)
                .get(crate::rest::list_price_alerts),
        )
        .route(
            "/api/v1/price-alerts/{id}",
            axum::routing::delete(crate::rest::delete_price_alert),
        )
        // Recurring orders — CRUD + pause/resume
        .route(
            "/api/v1/orders/recurring",
            axum::routing::post(crate::rest::create_recurring_order)
                .get(crate::rest::list_recurring_orders),
        )
        .route(
            "/api/v1/orders/recurring/{id}",
            axum::routing::get(crate::rest::get_recurring_order)
                .delete(crate::rest::delete_recurring_order),
        )
        .route(
            "/api/v1/orders/recurring/{id}/pause",
            axum::routing::post(crate::rest::pause_recurring_order),
        )
        .route(
            "/api/v1/orders/recurring/{id}/resume",
            axum::routing::post(crate::rest::resume_recurring_order),
        )
        // Futures
        .nest(
            "/api/v1/futures",
            Router::new()
                .route(
                    "/products",
                    axum::routing::get(crate::rest::get_futures_products),
                )
                .route(
                    "/candles",
                    axum::routing::get(crate::rest::get_futures_candles),
                )
                .route(
                    "/book",
                    axum::routing::get(crate::rest::get_futures_order_book),
                )
                .route(
                    "/orders",
                    axum::routing::post(crate::rest::create_futures_order)
                        .get(crate::rest::get_futures_orders),
                )
                .route(
                    "/positions",
                    axum::routing::get(crate::rest::get_futures_positions),
                )
                .route(
                    "/positions/{id}",
                    axum::routing::delete(crate::rest::close_futures_position),
                ),
        )
        // User Data & Analytics
        .route(
            "/api/v1/wallets/{address}/balance",
            axum::routing::get(crate::rest::get_wallet_balance),
        )
        .route(
            "/api/v1/analytics/stats",
            axum::routing::get(crate::rest::get_user_analytics_stats),
        )
        .route(
            "/api/v1/analytics/history",
            axum::routing::get(crate::rest::get_user_analytics_history),
        )
        .route(
            "/api/v1/transactions",
            axum::routing::get(crate::rest::get_user_transactions),
        )
        // Carbon / ESG
        .nest(
            "/api/v1/carbon",
            Router::new()
                .route(
                    "/balance",
                    axum::routing::get(crate::rest::get_carbon_balance),
                )
                .route(
                    "/history",
                    axum::routing::get(crate::rest::get_carbon_history),
                )
                .route(
                    "/transactions",
                    axum::routing::get(crate::rest::get_carbon_transactions),
                )
                .route(
                    "/transfers",
                    axum::routing::post(crate::rest::transfer_carbon_credits),
                ),
        )
        .route("/health", axum::routing::get(health_check))
        .route("/health/ready", axum::routing::get(health_check))
        .route("/metrics", axum::routing::get(metrics_handler))
        // INFO-level request span so traces export to Tempo (the default
        // make_span is DEBUG and is filtered out under the standard `info` env).
        .layer(TraceLayer::new_for_http().make_span_with(
            |request: &axum::http::Request<axum::body::Body>| {
                tracing::info_span!(
                    "http_request",
                    method = %request.method(),
                    uri = %request.uri().path(),
                )
            },
        ))
        .with_state(state)
}
