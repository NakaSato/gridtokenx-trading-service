use crate::api::trading_service::TradingServiceImpl;
use crate::core::config::Config;
use crate::domain::{
    energy::GridTopologyService,
    trading::clearing::MarketClearingService,
    trading::engine::OrderMatchingEngine,
    trading::settlement::{SettlementConfig, SettlementManager},
};
use crate::infra::blockchain::settlement::BlockchainSettlementProvider;
use crate::infra::blockchain::{BlockchainService, WalletService};
use crate::infra::{db, events::EventBus};
use crate::services::{ErcService, SettlementService};
use crate::trading_proto::TradingServiceExt;
use std::net::SocketAddr;
use std::sync::Arc;
use connectrpc::Server;
use anyhow::Context as _;
use tracing::{error, info};
use axum::{Router, routing::get, response::IntoResponse, http::StatusCode, middleware};
use std::sync::OnceLock;
use tokio_util::sync::CancellationToken;
// use tokio::signal;

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

pub async fn run(token: CancellationToken) -> anyhow::Result<()> {
    let config = Arc::new(Config::from_env().context("Failed to load configuration from environment")?);
    let addr: SocketAddr = format!("0.0.0.0:{}", 8092)
        .parse()
        .context("Failed to parse trading service address")?; // Use a dedicated port

    // Initialize Database
    let db_pool = db::setup_database(&config.database_url)
        .await
        .context("Failed to initialize database pool")?;

    // Run database migrations
    sqlx::migrate!("./migrations")
        .run(&db_pool)
        .await
        .context("Failed to run database migrations")?;
    info!("✅ Database migrations completed");

    // Initialize EventBus
    let event_bus = EventBus::new(&config.redis_url)
        .await
        .context("Failed to initialize event bus (Redis)")?;

    // Initialize Blockchain Service
    let blockchain = Arc::new(
        BlockchainService::new(
            config.solana_rpc_url.clone(),
            config.environment.clone(),
            config.solana_programs.clone(),
        )
        .context("Failed to initialize blockchain service")?,
    );

    // Initialize Topology Service
    let grid_topology = Arc::new(GridTopologyService::new());

    // Initialize ERC Service
    let erc_service = Arc::new(ErcService::new(
        db_pool.clone(),
        (*blockchain).clone(),
        event_bus.clone(),
    ));

    // Initialize Settlement Service
    let settlement_config = SettlementConfig::from_env();
    let settlement_manager = Arc::new(SettlementManager::new(db_pool.clone(), settlement_config));
    let settlement_provider = Arc::new(BlockchainSettlementProvider::new(blockchain.clone()));

    let settlement_service = Arc::new(SettlementService::new(
        settlement_manager,
        settlement_provider,
        event_bus.clone(),
        Some(erc_service.clone()),
        blockchain.clone(),
    ));

    // Initialize Market Clearing Service
    let wallet_service = WalletService::new(&config.solana_rpc_url);
    if let Err(e) = wallet_service.initialize_authority().await {
        error!("Failed to initialize authority wallet: {}", e);
        // Continue but log error - might be funded later or use env
    }
    let market_clearing = Arc::new(
        MarketClearingService::new(
            db_pool.clone(),
            config.clone(),
            blockchain.clone(),
            wallet_service,
            erc_service.clone(),
            token.clone(),
        )
        .with_settlement((*settlement_service).clone()),
    );

    // Initialize Order Matching Engine
    let matching_engine = Arc::new(
        OrderMatchingEngine::new(db_pool.clone())
            .with_interval(1)
            .with_topology(grid_topology.clone())
            .with_event_bus(event_bus.clone())
            .with_blockchain((*blockchain).clone())
            .with_settlement((*settlement_service).clone())
            .with_market_clearing((*market_clearing).clone()),
    );

    // Start Matching Engine
    matching_engine.start(token.clone()).await;

    // Start Event Consumer for reactive matching
    let event_bus_clone = event_bus.clone();
    let matching_engine_clone = matching_engine.clone();
    let consumer_token = token.clone();
    tokio::spawn(async move {
        let group_name = "trading-service-matcher";
        let consumer_name = format!("matcher-{}", uuid::Uuid::new_v4());

        // Ensure consumer group exists
        let _ = event_bus_clone.create_consumer_group(group_name).await;

        info!("🚀 Starting real-time order matching event consumer");

        tokio::select! {
            result = event_bus_clone
                .consume_events(group_name, &consumer_name, move |event| {
                    let engine = matching_engine_clone.clone();
                    async move {
                        if let crate::domain::events::Event::OrderCreated(order) = event {
                            info!("📥 Received real-time OrderCreated event: {}", order.id);
                            engine.notify_new_order(order.zone_id, Some(order)).await;
                        }
                        Ok(())
                    }
                }) => {
                    if let Err(e) = result {
                        error!("Event consumer loop failed: {}", e);
                    }
                },
            _ = consumer_token.cancelled() => {
                info!("🔄 Order matching event consumer shutting down...");
            }
        }
    });

    // Start Settlement Processor
    let settlement_service_clone = settlement_service.clone();
    let enable_settlement = std::env::var("ENABLE_SETTLEMENT_PROCESSOR")
        .map(|v| v.to_lowercase() == "true")
        .unwrap_or(true);

    if enable_settlement {
        let settlement_token = token.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = settlement_service_clone.process_pending_settlements().await {
                            error!("Settlement processor error: {}", e);
                        }
                    }
                    _ = settlement_token.cancelled() => {
                        info!("🔄 Settlement processor shutting down...");
                        break;
                    }
                }
            }
        });
        info!("✅ Settlement Service started");
    } else {
        info!("ℹ️ Settlement Service background loop is DISABLED via ENABLE_SETTLEMENT_PROCESSOR");
    }

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
    let grpc_router = Arc::new(trading_service).register(connectrpc::Router::new());
    let grpc_server = Server::new(grpc_router);

    // Start metrics HTTP server on separate port
    let metrics_port = 8093;
    let metrics_addr = format!("0.0.0.0:{}", metrics_port);
    let metrics_listener = tokio::net::TcpListener::bind(&metrics_addr)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to bind metrics to {}: {}", metrics_addr, e))?;
    
    let metrics_app = Router::new()
        .route("/metrics", get(get_metrics))
        .layer(middleware::from_fn(crate::api::middleware::otel_tracing::otel_tracing_middleware));
    
    info!("✅ Trading gRPC server listening on {}", addr);
    info!("✅ Trading metrics server listening on {}", metrics_addr);

    use futures::TryFutureExt;
    let rest_token = token.clone();
    let rest_handle = axum::serve(metrics_listener, metrics_app)
        .with_graceful_shutdown(async move {
            rest_token.cancelled().await;
        });
        
    let grpc_handle = grpc_server.serve(addr).map_err(|e| anyhow::anyhow!(e.to_string()));

    tokio::select! {
        res = rest_handle => {
            if let Err(e) = res {
                error!("Metrics server failed: {}", e);
            }
        }
        res = grpc_handle => {
            if let Err(e) = res {
                error!("gRPC server failed: {}", e);
            }
        },
    };

    Ok(())
}

/// Metrics endpoint handler - exposes Prometheus-format metrics
async fn get_metrics() -> impl IntoResponse {
    use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle, Matcher};
    static PROMETHEUS_HANDLE: OnceLock<Option<PrometheusHandle>> = OnceLock::new();

    let handle_opt = PROMETHEUS_HANDLE.get_or_init(|| {
        PrometheusBuilder::new()
            .set_buckets_for_metric(
                Matcher::Prefix("trading".to_string()),
                &[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
            )
            .ok()?
            .install_recorder()
            .ok()
    });

    match handle_opt {
        Some(handle) => (StatusCode::OK, handle.render()),
        None => (StatusCode::INTERNAL_SERVER_ERROR, "Metrics recorder failed to initialize".to_string()),
    }
}
