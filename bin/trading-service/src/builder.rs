use sqlx::PgPool;
use std::sync::Arc;
use trading_core::traits::*;
use trading_infra::audit::AuditLogger;
use trading_infra::blockchain::BlockchainService;
use trading_infra::cache::CacheService;
use trading_logic::{GridAwareTopology, MatcherService, SettlementService};
use trading_persistence::repositories::{
    PostgresAnalyticsRepository, PostgresCarbonRepository, PostgresFuturesRepository,
    PostgresOrderRepository, PostgresPriceAlertRepository, PostgresRecurringOrderRepository,
    PostgresSettlementRepository,
};
use uuid::Uuid;

/// Container for all infrastructure components
pub struct Infrastructure {
    pub db: PgPool,
    pub order_repo: Arc<dyn OrderRepository>,
    pub settlement_repo: Arc<dyn SettlementRepository>,
    pub outbox_repo: Arc<dyn OutboxRepository>,
    pub futures_repo: Arc<dyn FuturesRepository>,
    pub carbon_repo: Arc<dyn CarbonRepository>,
    pub analytics_repo: Arc<dyn AnalyticsRepository>,
    pub price_alert_repo: Arc<dyn PriceAlertRepository>,
    pub recurring_repo: Arc<dyn RecurringOrderRepository>,
    pub vpp_repo: Arc<dyn VppRepository>,
    pub blockchain: Arc<dyn BlockchainGateway>,
    pub identity: Arc<dyn IdentityGateway>,
    pub cache: Arc<dyn CacheStore>,
    pub audit: Arc<dyn AuditLog>,
    pub events: Arc<dyn EventPublisher>,
    pub event_bus: Arc<dyn EventPublisher>,
}

/// Container for all domain services
pub struct AppServices {
    pub settlement: Arc<SettlementService>,
    pub matcher: Arc<MatcherService>,
    pub vpp: Arc<trading_logic::vpp::VppService>,
    pub oracle_consumer: Arc<trading_logic::workers::OracleConsumer>,
    pub recurring_evaluator: Arc<trading_logic::RecurringEvaluator>,
    pub trigger_evaluator: Arc<trading_logic::TriggerEvaluator>,
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

        let order_repo = Arc::new(PostgresOrderRepository::new(db_pool.clone()));
        let settlement_repo = Arc::new(PostgresSettlementRepository::new(db_pool.clone()));
        let outbox_repo = Arc::new(trading_persistence::repositories::PostgresOutboxRepository::new(db_pool.clone()));
        let futures_repo = Arc::new(PostgresFuturesRepository::new(db_pool.clone()));
        let carbon_repo = Arc::new(PostgresCarbonRepository::new(db_pool.clone()));
        let analytics_repo = Arc::new(PostgresAnalyticsRepository::new(db_pool.clone()));
        let price_alert_repo = Arc::new(PostgresPriceAlertRepository::new(db_pool.clone()));
        let recurring_repo = Arc::new(PostgresRecurringOrderRepository::new(db_pool.clone()));
        let vpp_repo = Arc::new(trading_persistence::repositories::PostgresVppRepository::new(db_pool.clone()));

        // IAM identity gateway. NOTE: trading does not call IAM at request time
        // — auth is header-based, injected by the APISIX gateway (see
        // trading-api `auth.rs`). This gateway exists only to satisfy the
        // BlockchainService custodial-signing seam below, and its sole method
        // (`sign_message`) is intentionally inert (IAM's SignMessage RPC was
        // removed; signing moved to Chain Bridge `chain.tx.submit`). Kept wired
        // so the seam is ready when the on-chain create_order gains a
        // non-signing owner account; remove if that path is abandoned.
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
            .with_identity_gateway(identity.clone())
            .with_oracle_mint_enabled(config.oracle_mint_enabled)
            .with_trade_settlement_enabled(config.trade_settlement_enabled),
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
                config.kafka_enabled,
                Some(kafka_brokers),
                Some(&config.kafka_topic_prefix),
            )
            .await?,
        );

        // Transactional Outbox Publisher & Worker
        let outbox_worker = trading_infra::events::outbox_worker::OutboxWorker::new(
            outbox_repo.clone(),
            event_bus.clone(),
        );
        tokio::spawn(async move {
            if let Err(e) = outbox_worker.run().await {
                tracing::error!("Outbox worker failed: {}", e);
            }
        });

        let events: Arc<dyn EventPublisher> = Arc::new(
            trading_infra::events::OutboxPublisher::new(outbox_repo.clone())
        );

        let infra = Infrastructure {
            db: db_pool,
            order_repo: order_repo.clone(),
            settlement_repo: settlement_repo.clone(),
            outbox_repo: outbox_repo.clone(),
            futures_repo,
            carbon_repo,
            analytics_repo,
            price_alert_repo: price_alert_repo.clone(),
            recurring_repo: recurring_repo.clone(),
            vpp_repo: vpp_repo.clone(),
            blockchain: blockchain.clone(),
            identity: identity.clone(),
            cache,
            audit: audit.clone(),
            events: events.clone(),
            event_bus: event_bus.clone(),
        };

        // 2. Services
        let settlement_service = Arc::new(SettlementService::new(
            settlement_repo.clone(),
            blockchain.clone(),
            audit.clone(),
            config.platform_user_id,
            config.oracle_feed_in_tariff,
        ));

        let matcher_service = Arc::new(MatcherService::new(
            order_repo.clone(),
            settlement_repo.clone(),
            Arc::new(GridAwareTopology::new()),
        ));

        let vpp_service = Arc::new(trading_logic::vpp::VppService::new(
            vpp_repo.clone(),
            audit.clone(),
            events.clone(),
        ));

        // Initialize Oracle Consumer
        let oracle_consumer = Arc::new(trading_logic::workers::OracleConsumer::new(
            event_bus.clone(),
            settlement_service.clone(),
            "trading_oracle_group".to_string(),
            format!("trading_oracle_{}", Uuid::new_v4()),
        ));

        // Recurring-order & price-alert automation (Phase 6). Both read/write
        // through the same repos + outbox as the REST layer.
        let recurring_evaluator = Arc::new(trading_logic::RecurringEvaluator::new(
            recurring_repo.clone(),
            order_repo.clone(),
        ));

        let trigger_evaluator = Arc::new(trading_logic::TriggerEvaluator::new(
            price_alert_repo.clone(),
            order_repo.clone(),
        ));

        let services = AppServices {
            settlement: settlement_service,
            matcher: matcher_service,
            vpp: vpp_service,
            oracle_consumer,
            recurring_evaluator,
            trigger_evaluator,
        };

        Ok((infra, services))
    }
}
