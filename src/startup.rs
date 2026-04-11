use crate::api::trading_service::TradingServiceImpl;
use crate::core::config::Config;
use crate::domain::{
    energy::GridTopologyService,
    trading::clearing::MarketClearingService,
    trading::engine::{OrderMatchingEngine, rehydration::StateRehydrator},
    trading::settlement::{SettlementConfig, SettlementManager},
};
use crate::infra::blockchain::settlement::BlockchainSettlementProvider;
use crate::infra::blockchain::{BlockchainService, WalletService};
use crate::infra::{db, events::{EventBus, kafka::KafkaTopics, kafka_consumer::KafkaConsumer}, logging::{AuditLogger, AuditWorker}};
use crate::domain::vpp::VppRepository;
use crate::services::{ErcService, SettlementService, P2PConfigService, TriggerEvaluator, RecurringEvaluator, MarketDataService};
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
    pub p2p_config: Arc<P2PConfigService>,
    pub audit_logger: AuditLogger,
    pub trigger_evaluator: Arc<TriggerEvaluator>,
    pub recurring_evaluator: Arc<RecurringEvaluator>,
    pub market_data_aggregator: Arc<MarketDataService>,
    pub vpp_repository: Arc<VppRepository>,
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

    // Initialize EventBus (Kafka or Redis Streams based on config)
    let event_bus = EventBus::new(
        &config.redis_url,
        config.kafka_enabled,
        Some(&config.kafka_bootstrap_servers),
        Some(&config.kafka_topic_prefix),
    )
        .await
        .context("Failed to initialize event bus")?;

    // Initialize Audit Logging
    let (audit_tx, audit_rx) = tokio::sync::mpsc::channel(1000);
    let audit_logger = AuditLogger::new(db_pool.clone(), audit_tx);
    let audit_worker = AuditWorker {
        receiver: audit_rx,
        logger: audit_logger.clone(),
    };
    
    // Start Audit Worker
    let _audit_handle = tokio::spawn(async move {
        audit_worker.run().await;
    });
    info!("✅ Audit Logging initialized and worker started");

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

    // Initialize P2P Config Service
    let p2p_config = Arc::new(P2PConfigService::new(db_pool.clone()));
    p2p_config.initialize().await.context("Failed to initialize P2P config cache")?;

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
            audit_logger.clone(),
            Some(event_bus.clone()),
            token.clone(),
        )
        .with_settlement((*settlement_service).clone())
        .with_p2p_config(p2p_config.clone()),
    );

    // Initialize Market Data Aggregator Service
    let market_data_aggregator = Arc::new(
        MarketDataService::new(db_pool.clone())
    );

    // Initialize VPP Infrastructure
    let vpp_repository = Arc::new(VppRepository::new(db_pool.clone()));
    let vpp_aggregator = Arc::new(crate::domain::vpp::aggregator::VppAggregator::new(vpp_repository.clone()));

    // Start VPP Aggregator Kafka Consumer
    if let Ok(brokers) = std::env::var("KAFKA_BOOTSTRAP_SERVERS") {
        let topic = std::env::var("KAFKA_TOPIC_METER_READINGS").unwrap_or_else(|_| "meter.readings".to_string());
        let aggregator_clone = vpp_aggregator.clone();
        let aggregator_token = token.clone();
        tokio::spawn(async move {
            if let Err(e) = aggregator_clone.run(&brokers, &topic, "trading-vpp-group", aggregator_token).await {
                error!("❌ VPP Aggregator failed: {}", e);
            }
        });
    }

    // Initialize Order Matching Engine
    let matching_engine = Arc::new(
        OrderMatchingEngine::new(db_pool.clone())
            .with_interval(1)
            .with_topology(grid_topology.clone())
            .with_event_bus(event_bus.clone())
            .with_blockchain((*blockchain).clone())
            .with_ohlc_aggregator(market_data_aggregator.clone())
            .with_settlement((*settlement_service).clone())
            .with_market_clearing((*market_clearing).clone())
            .with_p2p_config(p2p_config.clone()),
    );

    // [PHASE 4] Kafka-Driven Recovery (Rehydration)
    let mut rehydrated_state = None;
    if config.kafka_enabled {
        info!("🔄 Initiating Kafka state rehydration...");
        let topics = KafkaTopics::with_prefix(&config.kafka_topic_prefix);
        let consumer_topics = vec![topics.orders_created, topics.orders_updated];
        
        match KafkaConsumer::new(&config.kafka_bootstrap_servers, consumer_topics, None) {
            Ok(consumer) => {
                let rehydrator = StateRehydrator::new(consumer);
                match rehydrator.rehydrate().await {
                    Ok(state) => {
                        info!("✅ Rehydration successful: {} active orders recovered", state.len());
                        rehydrated_state = Some(state);
                    }
                    Err(e) => {
                        error!("❌ Rehydration failed: {}. Proceeding with clean state (DB bootstrap will follow).", e);
                    }
                }
            }
            Err(e) => {
                error!("❌ Failed to initialize rehydration consumer: {}. Skipping.", e);
            }
        }
    }

    // Start Matching Engine with rehydrated state
    matching_engine.start(token.clone(), rehydrated_state).await;

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
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Drain the queue: Process multiple batches in one tick if there's a backlog
                        let mut batches_in_tick = 0;
                        const MAX_BATCHES_PER_TICK: u32 = 10;
                        
                        while batches_in_tick < MAX_BATCHES_PER_TICK {
                            match settlement_service_clone.process_pending_settlements().await {
                                Ok(pending_count) => {
                                    if pending_count == 0 {
                                        break; // Queue is empty
                                    }
                                    batches_in_tick += 1;
                                    
                                    // If we processed a full batch (5), keep going immediately.
                                    // Otherwise, wait for the next tick.
                                    if pending_count <= 5 {
                                        break;
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Settlement processor error: {}", e);
                                    break;
                                }
                            }
                        }
                    }
                    _ = settlement_token.cancelled() => {
                        info!("🔄 Settlement processor shutting down...");
                        break;
                    }
                }
            }
        });
        info!("✅ Settlement Service started (Drain-the-Queue mode enabled)");
    } else {
        info!("ℹ️ Settlement Service background loop is DISABLED via ENABLE_SETTLEMENT_PROCESSOR");
    }

    // Initialize Trigger Evaluator Service
    let trigger_evaluator = Arc::new(
        TriggerEvaluator::new(db_pool.clone(), matching_engine.clone())
    );

    // Initialize Recurring Evaluator Service
    let recurring_evaluator = Arc::new(
        RecurringEvaluator::new(db_pool.clone(), matching_engine.clone())
    );

    // Start Trigger Evaluation Worker
    let trigger_evaluator_clone = trigger_evaluator.clone();
    let trigger_token = token.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = trigger_evaluator_clone.process_triggers().await {
                        error!("Trigger evaluator error: {}", e);
                    }
                }
                _ = trigger_token.cancelled() => {
                    info!("🔄 Trigger evaluation worker shutting down...");
                    break;
                }
            }
        }
    });
    info!("✅ Trigger Evaluation Worker started");

    // Start Recurring Evaluation Worker
    let recurring_evaluator_clone = recurring_evaluator.clone();
    let recurring_token = token.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = recurring_evaluator_clone.process_recurring_orders_with_metrics().await {
                        error!("Recurring evaluator error: {}", e);
                    }
                }
                _ = recurring_token.cancelled() => {
                    info!("🔄 Recurring evaluation worker shutting down...");
                    break;
                }
            }
        }
    });
    info!("✅ Recurring Evaluation Worker started");

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
        p2p_config,
        audit_logger,
        trigger_evaluator,
        recurring_evaluator,
        market_data_aggregator,
        vpp_repository,
    });

    let trading_service = TradingServiceImpl::new(state.clone());
    let grpc_router = Arc::new(trading_service).register(connectrpc::Router::new());
    let grpc_server = Server::new(grpc_router);

    // Start REST HTTP server (metrics + settlement endpoint)
    let metrics_port = 8093;
    let metrics_addr = format!("0.0.0.0:{}", metrics_port);
    let metrics_listener = tokio::net::TcpListener::bind(&metrics_addr)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to bind metrics to {}: {}", metrics_addr, e))?;
    
    let rest_app = Router::new()
        .route("/metrics", get(get_metrics))
        .route("/api/v1/settlement/generation-mint", axum::routing::post(settle_generation_mint_rest))
        .layer(middleware::from_fn(crate::api::middleware::otel_tracing::otel_tracing_middleware))
        .with_state(state.clone());
    
    info!("✅ Trading gRPC server listening on {}", addr);
    info!("✅ Trading REST server listening on {} (metrics + settlement)", metrics_addr);

    use futures::TryFutureExt;
    let rest_token = token.clone();
    let rest_handle = axum::serve(metrics_listener, rest_app)
        .with_graceful_shutdown(async move {
            rest_token.cancelled().await;
        });
        
    let grpc_handle = grpc_server.serve(addr).map_err(|e| anyhow::anyhow!(e.to_string()));

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
        },
    };

    Ok(())
}

// =============================================================================
// REST Settlement Handler (Oracle Bridge → Trading Service)
// =============================================================================

#[derive(serde::Deserialize)]
struct GenerationMintRequest {
    meter_id: uuid::Uuid,
    meter_serial: String,
    user_id: uuid::Uuid,
    start_time: chrono::DateTime<chrono::Utc>,
    end_time: chrono::DateTime<chrono::Utc>,
    energy_generated_kwh: rust_decimal::Decimal,
    energy_consumed_kwh: rust_decimal::Decimal,
    reading_count: u64,
    signature: String,
}

#[derive(serde::Serialize)]
struct GenerationMintResponse {
    signature: String,
    meter_serial: String,
    amount_minted: rust_decimal::Decimal,
    status: String,
}

async fn settle_generation_mint_rest(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    axum::Json(payload): axum::Json<GenerationMintRequest>,
) -> impl IntoResponse {
    info!(
        "💰 Settlement request received (REST): {} (Gen: {} kWh, Window: {} - {})",
        payload.meter_serial,
        payload.energy_generated_kwh,
        payload.start_time,
        payload.end_time
    );

    // 1. Verify Oracle signature is present
    if payload.signature.is_empty() {
        return (StatusCode::BAD_REQUEST, axum::Json(serde_json::json!({
            "error": "Missing oracle signature"
        }))).into_response();
    }

    // 2. Execute via SettlementService
    match state.settlement_service
        .execute_generation_mint(
            payload.meter_id,
            &payload.meter_serial,
            payload.energy_generated_kwh,
            payload.start_time.timestamp(),
        )
        .await
    {
        Ok(tx_signature) => {
            info!(
                "⛓️ Generation Mint Success: [Meter] {} - Minted: {} GRX - TX: {}",
                payload.meter_serial, payload.energy_generated_kwh, tx_signature
            );
            (StatusCode::OK, axum::Json(serde_json::json!(GenerationMintResponse {
                signature: tx_signature,
                meter_serial: payload.meter_serial,
                amount_minted: payload.energy_generated_kwh,
                status: "settled".to_string(),
            }))).into_response()
        }
        Err(e) => {
            error!("❌ Generation mint failed for {}: {}", payload.meter_serial, e);
            (StatusCode::INTERNAL_SERVER_ERROR, axum::Json(serde_json::json!({
                "error": format!("On-chain minting failed: {}", e)
            }))).into_response()
        }
    }
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
