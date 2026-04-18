mod builder;

use std::sync::Arc;
use tracing::{info, error};
use sqlx::postgres::PgPoolOptions;
use builder::ServiceBuilder;

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

    // 3. Initialize DB
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&database_url)
        .await?;

    // 4. Build system
    let (_infra, services) = ServiceBuilder::build(
        pool,
        &redis_url,
        &kafka_brokers,
        &chain_bridge_url,
        &solana_rpc_url,
        &encryption_secret,
    ).await?;

    info!("System assembled successfully");

    // 5. Start background workers
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

    // 6. Wait for shutdown signal
    info!("Trading service is running...");
    tokio::signal::ctrl_c().await?;
    info!("Shutting down...");

    Ok(())
}
