mod builder;

use std::sync::Arc;
use tracing::{info, error};
use sqlx::postgres::PgPoolOptions;
use builder::ServiceBuilder;
use tokio_util::sync::CancellationToken;
use trading_api::state::AppState;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Setup telemetry
    trading_infra::init_telemetry("trading-service");
    
    // Install default crypto provider for rustls (required for rustls 0.23+)
    rustls::crypto::ring::default_provider().install_default().expect("Failed to install default crypto provider");
    
    info!("Starting GridTokenX Trading Service (Modular Monolith)");

    // 2. Load environment
    dotenvy::dotenv().ok();
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let redis_url = std::env::var("REDIS_URL").expect("REDIS_URL must be set");
    let kafka_brokers = std::env::var("KAFKA_BROKERS").expect("KAFKA_BROKERS must be set");
    let chain_bridge_url = std::env::var("CHAIN_BRIDGE_URL").expect("CHAIN_BRIDGE_URL must be set");
    let solana_rpc_url = std::env::var("SOLANA_RPC_URL").expect("SOLANA_RPC_URL must be set");
    let encryption_secret = std::env::var("ENCRYPTION_SECRET").expect("ENCRYPTION_SECRET must be set");
    
    let port: u16 = std::env::var("HTTP_PORT").unwrap_or_else(|_| "8093".to_string()).parse()?;
    let grpc_port: u16 = std::env::var("GRPC_PORT").unwrap_or_else(|_| "8092".to_string()).parse()?;

    // 3. Initialize DB
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&database_url)
        .await?;

    // 4. Build system
    let (infra, services) = ServiceBuilder::build(
        pool,
        &redis_url,
        &kafka_brokers,
        &chain_bridge_url,
        &solana_rpc_url,
        &encryption_secret,
    ).await?;

    info!("System assembled successfully");

    // 5. Graceful Shutdown Token
    let shutdown_token = CancellationToken::new();
    let main_token = shutdown_token.clone();

    // 6. Start background workers
    let matcher_worker = trading_logic::MatcherWorker::new(
        services.matcher.clone(),
        tokio::time::Duration::from_secs(1),
    );
    tokio::spawn(async move {
        matcher_worker.run().await;
    });

    let settlement_worker = trading_logic::SettlementWorker::new(
        services.settlement.clone(),
        tokio::time::Duration::from_secs(10),
        10, // batch limit
    );
    tokio::spawn(async move {
        settlement_worker.run().await;
    });

    // 7. Start API Server
    let state = AppState {
        order_repo: infra.order_repo,
        settlement_repo: infra.settlement_repo,
        events: infra.events,
        blockchain: infra.blockchain,
        audit: infra.audit,
        matcher: services.matcher,
        settlement: services.settlement,
    };

    tokio::spawn(async move {
        if let Err(e) = trading_api::startup::run(state, port, grpc_port, main_token).await {
            error!("API server error: {}", e);
        }
    });

    // 8. Wait for shutdown signal
    info!("Trading service is running...");
    tokio::signal::ctrl_c().await?;
    info!("Shutting down...");
    shutdown_token.cancel();
    
    // Give it a moment to stop
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    Ok(())
}
