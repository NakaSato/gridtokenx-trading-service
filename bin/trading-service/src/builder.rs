use std::sync::Arc;
use sqlx::PgPool;
use trading_core::traits::*;
use trading_persistence::repositories::{PostgresOrderRepository, PostgresSettlementRepository};
use trading_infra::blockchain::BlockchainService;
use trading_infra::cache::CacheService;
use trading_infra::audit::AuditLogger;
use trading_infra::events::KafkaEventBus;
use trading_logic::{SettlementService, MatcherService, StaticTopology};

/// Container for all infrastructure components
pub struct Infrastructure {
    pub db: PgPool,
    pub order_repo: Arc<dyn OrderRepository>,
    pub settlement_repo: Arc<dyn SettlementRepository>,
    pub blockchain: Arc<dyn BlockchainGateway>,
    pub cache: Arc<dyn CacheStore>,
    pub audit: Arc<dyn AuditLog>,
    pub events: Arc<dyn EventPublisher>,
}

/// Container for all domain services
pub struct AppServices {
    pub settlement: Arc<SettlementService>,
    pub matcher: Arc<MatcherService>,
}

/// Builder to assemble the system
pub struct ServiceBuilder;

impl ServiceBuilder {
    pub async fn build(
        db_pool: PgPool,
        redis_url: &str,
        kafka_brokers: &str,
        chain_bridge_url: &str,
        solana_rpc_url: &str,
        _encryption_secret: &str,
    ) -> Result<(Infrastructure, AppServices), Box<dyn std::error::Error>> {
        // 1. Infrastructure
        let order_repo = Arc::new(PostgresOrderRepository::new(db_pool.clone()));
        let settlement_repo = Arc::new(PostgresSettlementRepository::new(db_pool.clone()));
        
        let blockchain: Arc<dyn BlockchainGateway> = Arc::new(BlockchainService::new(
            chain_bridge_url.to_string(),
            "devnet".to_string(),
            trading_core::config::SolanaProgramsConfig::default(),
            None,
        ).await?);

        let cache: Arc<dyn CacheStore> = Arc::new(CacheService::new(redis_url).await?);
        
        // Initialize Audit System
        let (audit_tx, audit_rx) = tokio::sync::mpsc::channel(1000);
        let audit_logger = AuditLogger::new(db_pool.clone(), audit_tx);
        let audit_worker = trading_infra::audit::worker::AuditWorker::new(audit_logger.clone(), audit_rx);
        tokio::spawn(async move {
            audit_worker.run().await;
        });
        
        let audit: Arc<dyn AuditLog> = Arc::new(audit_logger);
        let events: Arc<dyn EventPublisher> = Arc::new(KafkaEventBus::new(kafka_brokers, Some("trading")).await?);

        let infra = Infrastructure {
            db: db_pool,
            order_repo: order_repo.clone(),
            settlement_repo: settlement_repo.clone(),
            blockchain: blockchain.clone(),
            cache,
            audit: audit.clone(),
            events: events.clone(),
        };

        // 2. Services
        let settlement_service = Arc::new(SettlementService::new(
            settlement_repo.clone(),
            blockchain.clone(),
            audit.clone(),
        ));

        let matcher_service = Arc::new(MatcherService::new(
            order_repo.clone(),
            settlement_repo.clone(),
            events.clone(),
            Arc::new(StaticTopology),
        ));

        let services = AppServices {
            settlement: settlement_service,
            matcher: matcher_service,
        };

        Ok((infra, services))
    }
}
