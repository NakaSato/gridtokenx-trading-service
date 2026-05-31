use sqlx::PgPool;
use std::sync::Arc;
use trading_core::traits::*;
use trading_infra::audit::AuditLogger;
use trading_infra::blockchain::BlockchainService;
use trading_infra::cache::CacheService;
use trading_logic::{GridAwareTopology, MatcherService, SettlementService};
use trading_persistence::repositories::{
    PostgresAnalyticsRepository, PostgresCarbonRepository, PostgresFuturesRepository,
    PostgresOrderRepository, PostgresSettlementRepository,
};
use uuid::Uuid;

/// Container for all infrastructure components
pub struct Infrastructure {
    pub db: PgPool,
    pub order_repo: Arc<dyn OrderRepository>,
    pub settlement_repo: Arc<dyn SettlementRepository>,
    pub futures_repo: Arc<dyn FuturesRepository>,
    pub carbon_repo: Arc<dyn CarbonRepository>,
    pub analytics_repo: Arc<dyn AnalyticsRepository>,
    pub blockchain: Arc<dyn BlockchainGateway>,
    pub identity: Arc<dyn IdentityGateway>,
    pub cache: Arc<dyn CacheStore>,
    pub audit: Arc<dyn AuditLog>,
    pub events: Arc<dyn EventPublisher>,
}

/// Container for all domain services
pub struct AppServices {
    pub settlement: Arc<SettlementService>,
    pub matcher: Arc<MatcherService>,
    pub oracle_consumer: Arc<trading_logic::workers::OracleConsumer>,
}

/// Builder to assemble the system
pub struct ServiceBuilder;

impl ServiceBuilder {
    pub async fn build(
        db_pool: PgPool,
        config: Arc<trading_core::config::Config>,
    ) -> Result<(Infrastructure, AppServices), Box<dyn std::error::Error>> {
        // 1. Infrastructure
        let redis_url = &config.redis_url;
        let kafka_brokers = &config.kafka_bootstrap_servers;
        let chain_bridge_url = &config.chain_bridge_url;
        let encryption_secret = &config.encryption_secret;

        let order_repo = Arc::new(PostgresOrderRepository::new(db_pool.clone()));
        let settlement_repo = Arc::new(PostgresSettlementRepository::new(db_pool.clone()));
        let futures_repo = Arc::new(PostgresFuturesRepository::new(db_pool.clone()));
        let carbon_repo = Arc::new(PostgresCarbonRepository::new(db_pool.clone()));
        let analytics_repo = Arc::new(PostgresAnalyticsRepository::new(db_pool.clone()));

        let identity: Arc<dyn IdentityGateway> = Arc::new(
            trading_infra::identity::IamIdentityGateway::new(
                &config.iam_service_url,
                config.internal_api_key.clone(),
            )
        );

        let blockchain: Arc<dyn BlockchainGateway> = Arc::new(
            BlockchainService::new(
                chain_bridge_url.to_string(),
                config.solana_cluster.clone(),
                config.solana_programs.clone(),
                Some(db_pool.clone()),
                None,
            )
            .await?
            .with_identity_gateway(identity.clone()),
        );

        let cache: Arc<dyn CacheStore> = Arc::new(CacheService::new(redis_url).await?);

        // Initialize Audit System
        let (audit_tx, audit_rx) = tokio::sync::mpsc::channel(1000);
        let audit_logger = AuditLogger::new(db_pool.clone(), audit_tx);
        let audit_worker =
            trading_infra::audit::worker::AuditWorker::new(audit_logger.clone(), audit_rx);
        tokio::spawn(async move {
            audit_worker.run().await;
        });

        let audit: Arc<dyn AuditLog> = Arc::new(audit_logger);

        // Use the unified EventBus (supports Redis Streams for Telemetry)
        let event_bus = Arc::new(
            trading_infra::events::EventBus::new(
                redis_url,
                false, // kafka_enabled: disable for now to focus on Redis telemetry
                Some(kafka_brokers),
                Some("trading"),
            )
            .await?,
        );

        let events: Arc<dyn EventPublisher> = event_bus.clone();

        let infra = Infrastructure {
            db: db_pool,
            order_repo: order_repo.clone(),
            settlement_repo: settlement_repo.clone(),
            futures_repo,
            carbon_repo,
            analytics_repo,
            blockchain: blockchain.clone(),
            identity: identity.clone(),
            cache,
            audit: audit.clone(),
            events: events.clone(),
        };

        // 2. Services
        let settlement_service = Arc::new(SettlementService::new(
            settlement_repo.clone(),
            blockchain.clone(),
            audit.clone(),
            config.platform_user_id,
            config.oracle_feed_in_tariff,
            config.oracle_bridge_public_key.clone(),
        ));

        let matcher_service = Arc::new(MatcherService::new(
            order_repo.clone(),
            settlement_repo.clone(),
            events.clone(),
            Arc::new(GridAwareTopology::new()),
        ));

        // Initialize Oracle Consumer
        let oracle_consumer = Arc::new(trading_logic::workers::OracleConsumer::new(
            events.clone(),
            settlement_service.clone(),
            "trading_oracle_group".to_string(),
            format!("trading_oracle_{}", Uuid::new_v4()),
        ));

        let services = AppServices {
            settlement: settlement_service,
            matcher: matcher_service,
            oracle_consumer,
        };

        Ok((infra, services))
    }
}
