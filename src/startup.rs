use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;
use crate::api::trading_service::TradingServiceImpl;
use crate::trading_proto::trading_service_server::TradingServiceServer;
use crate::core::config::Config;
use crate::infra::{db, events::EventBus};
use crate::infra::blockchain::{BlockchainService, WalletService};
use crate::services::{ErcService, SettlementService};
use crate::domain::{
    energy::GridTopologyService,
    trading::engine::OrderMatchingEngine,
    trading::clearing::MarketClearingService,
};
use tracing::info;

pub struct AppState {
    pub db: db::DatabasePool,
    pub config: Arc<Config>,
    pub event_bus: EventBus,
    pub blockchain: Arc<BlockchainService>,
    pub erc_service: Arc<ErcService>,
    pub settlement_service: Arc<SettlementService>,
    pub matching_engine: Arc<OrderMatchingEngine>,
    pub grid_topology: Arc<GridTopologyService>,
    pub market_clearing: Arc<MarketClearingService>,
}

pub async fn run() -> anyhow::Result<()> {
    let config = Arc::new(Config::from_env()?);
    let addr: SocketAddr = format!("0.0.0.0:{}", 50052).parse()?; // Use a dedicated port

    // Initialize Database
    let db_pool = db::setup_database(&config.database_url).await?;
    
    // Initialize EventBus
    let event_bus = EventBus::new(&config.redis_url).await?;

    // Initialize Blockchain Service
    let blockchain = Arc::new(BlockchainService::new(
        config.solana_rpc_url.clone(),
        config.environment.clone(),
        config.solana_programs.clone(),
    )?);

    // Initialize Topology Service
    let grid_topology = Arc::new(GridTopologyService::new());

    // Initialize ERC Service
    let erc_service = Arc::new(ErcService::new(
        db_pool.clone(),
        (*blockchain).clone(),
        event_bus.clone(),
    ));

    // Initialize Settlement Service
    let settlement_service = Arc::new(SettlementService::new(
        db_pool.clone(),
        blockchain.clone(),
        event_bus.clone(),
    ));

    // Initialize Market Clearing Service
    let wallet_service = WalletService::new(&config.solana_rpc_url);
    let market_clearing = Arc::new(MarketClearingService::new(
        db_pool.clone(),
        blockchain.clone(),
        wallet_service,
        erc_service.clone(),
    ).with_settlement((*settlement_service).clone()));

    // Initialize Order Matching Engine
    let matching_engine = Arc::new(OrderMatchingEngine::new(
        db_pool.clone(),
        1, // 1 second interval
        (*grid_topology).clone(),
    )
    .with_event_bus(event_bus.clone())
    .with_blockchain((*blockchain).clone())
    .with_settlement((*settlement_service).clone())
    .with_market_clearing((*market_clearing).clone()));

    // Start Matching Engine
    matching_engine.start().await;

    let state = Arc::new(AppState {
        db: db_pool,
        config: config.clone(),
        event_bus,
        blockchain,
        erc_service,
        settlement_service,
        matching_engine,
        grid_topology,
        market_clearing,
    });
    
    let trading_service = TradingServiceImpl::new(state.clone());

    info!("✅ Trading gRPC server listening on {}", addr);

    Server::builder()
        .add_service(TradingServiceServer::new(trading_service))
        .serve(addr)
        .await?;

    Ok(())
}
