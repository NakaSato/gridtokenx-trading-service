use axum::{Router, response::IntoResponse, http::StatusCode, Json};
use std::sync::Arc;
use connectrpc::Server;
use tracing::info;
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
        .layer(TraceLayer::new_for_http())
        .with_state(state.clone());

    // 2. Initialize gRPC Service
    let grpc_service = TradingGrpcService::new(state);
    let grpc_router = Arc::new(grpc_service).register(connectrpc::Router::new());
    let grpc_server = Server::new(grpc_router);

    // 3. Start Servers
    let rest_addr: std::net::SocketAddr = format!("0.0.0.0:{}", port).parse()?;
    let grpc_addr: std::net::SocketAddr = format!("0.0.0.0:{}", grpc_port).parse()?;

    info!("🚀 Trading REST API listening on {}", rest_addr);
    info!("🚀 Trading gRPC/ConnectRPC listening on {}", grpc_addr);

    let rest_listener = tokio::net::TcpListener::bind(&rest_addr).await?;
    let grpc_listener = tokio::net::TcpListener::bind(&grpc_addr).await?;

    tokio::select! {
        _ = axum::serve(rest_listener, app.into_make_service()) => {
            info!("REST server stopped");
        }
        _ = axum::serve(grpc_listener, grpc_server.into_axum_service().into_make_service()) => {
            info!("gRPC server stopped");
        }
        _ = token.cancelled() => {
            info!("Shutdown signal received, closing servers...");
        }
    }

    Ok(())
}

async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({"status": "ok"})))
}

async fn metrics_handler() -> impl IntoResponse {
    // Placeholder for actual metrics
    (StatusCode::OK, "trading_active_orders 0\n")
}
