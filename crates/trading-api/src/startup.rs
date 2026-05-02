use axum::{Router, response::IntoResponse, http::StatusCode, Json};
use connectrpc::Server;
use tracing::{info, error};
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;

use crate::state::AppState;
use crate::handlers::TradingGrpcService;
use trading_protocol::trading_proto::TradingServiceExt;

pub async fn run(state: AppState, port: u16, grpc_port: u16, token: CancellationToken) -> anyhow::Result<()> {
    // 1. Build REST Router
    let app = Router::new()
        .route("/health", axum::routing::get(health_check))
        .route("/health/ready", axum::routing::get(health_check))
        .route("/metrics", axum::routing::get(metrics_handler))
        .route("/api/v1/orders", axum::routing::post(crate::rest::submit_order))
        .route("/api/v1/users/me/orders", axum::routing::get(crate::rest::list_orders))
        .route("/api/v1/markets/zones/{zone_id}/order-book", axum::routing::get(crate::rest::get_order_book))
        .route("/api/v1/quotes", axum::routing::post(crate::rest::create_quote))
        .route("/api/v1/markets/stats", axum::routing::get(crate::rest::get_market_stats))
        // Futures
        .route("/api/v1/markets/futures/products", axum::routing::get(crate::rest::get_futures_products))
        .route("/api/v1/markets/futures/candles", axum::routing::get(crate::rest::get_futures_candles))
        .route("/api/v1/markets/futures/order-book", axum::routing::get(crate::rest::get_futures_order_book))
        .route("/api/v1/users/me/futures/orders", axum::routing::post(crate::rest::create_futures_order))
        .route("/api/v1/users/me/futures/orders", axum::routing::get(crate::rest::get_futures_orders))
        .route("/api/v1/users/me/futures/positions", axum::routing::get(crate::rest::get_futures_positions))
        .route("/api/v1/users/me/futures/positions/{id}/close", axum::routing::post(crate::rest::close_futures_position))
        // User Data (Modernized)
        .route("/api/v1/users/me/wallets/{address}/balance", axum::routing::get(crate::rest::get_wallet_balance))
        .route("/api/v1/users/me/analytics/stats", axum::routing::get(crate::rest::get_user_analytics_stats))
        .route("/api/v1/users/me/analytics/history", axum::routing::get(crate::rest::get_user_analytics_history))
        .route("/api/v1/users/me/transactions", axum::routing::get(crate::rest::get_user_transactions))
        // Carbon / ESG (Modernized)
        .route("/api/v1/users/me/carbon/balance", axum::routing::get(crate::rest::get_carbon_balance))
        .route("/api/v1/users/me/carbon/history", axum::routing::get(crate::rest::get_carbon_history))
        .route("/api/v1/users/me/carbon/transactions", axum::routing::get(crate::rest::get_carbon_transactions))
        .route("/api/v1/users/me/carbon/transfer", axum::routing::post(crate::rest::transfer_carbon_credits))
        .layer(TraceLayer::new_for_http())
        .with_state(state.clone());

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
    let rest_handle = axum::serve(rest_listener, app)
        .with_graceful_shutdown(async move {
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

async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({"status": "ok", "service": "trading"})))
}

async fn metrics_handler() -> impl IntoResponse {
    (StatusCode::OK, "trading_active_orders 0\n")
}
