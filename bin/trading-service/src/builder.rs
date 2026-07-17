use sqlx::PgPool;
use std::sync::Arc;
use trading_core::traits::*;
use trading_infra::audit::AuditLogger;
use trading_infra::blockchain::BlockchainService;
use trading_infra::cache::CacheService;
use trading_logic::{ClearingService, GridAwareTopology, MatcherService, SettlementService};
use trading_persistence::repositories::{
    PostgresAnalyticsRepository, PostgresCarbonRepository, PostgresFuturesRepository,
    PostgresMeterRepository, PostgresOrderRepository, PostgresPriceAlertRepository,
    PostgresRecurringOrderRepository,
    PostgresSettlementRepository,
};

/// Container for all infrastructure components
pub struct Infrastructure {
    pub db: PgPool,
    pub order_repo: Arc<dyn OrderRepository>,
    pub meter_repo: Arc<dyn MeterRepository>,
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
    /// Read-model feed worker (DB-per-service Phase 1). `Some` only when
    /// `TRADING_READMODEL_FEED` is on; `main.rs` spawns it. Boot backfill has
    /// already run by the time this is populated.
    pub readmodel_feed_worker: Option<trading_logic::ReadModelFeedWorker>,
}

/// Container for all domain services
pub struct AppServices {
    pub settlement: Arc<SettlementService>,
    pub matcher: Arc<MatcherService>,
    pub clearing: Arc<ClearingService>,
    pub vpp: Arc<trading_logic::vpp::VppService>,
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
        let meter_repo = Arc::new(PostgresMeterRepository::new(db_pool.clone()));
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

        // Read-model feed (DB-per-service Phase 1). OFF unless TRADING_READMODEL_FEED=true;
        // when off nothing is constructed and no worker spawns (zero behavior change).
        // When on: build the two read-model repos, run the one-shot boot backfill from
        // the still-reachable source tables, then hand the worker to main.rs to spawn.
        let readmodel_feed_worker = if config.readmodel_feed_enabled {
            let wallet_rm: Arc<dyn WalletReadModelRepository> = Arc::new(
                trading_persistence::repositories::PgWalletReadModelRepository::new(db_pool.clone()),
            );
            let meter_rm: Arc<dyn MeterReadModelRepository> = Arc::new(
                trading_persistence::repositories::PgMeterReadModelRepository::new(db_pool.clone()),
            );
            match wallet_rm.backfill_wallets().await {
                Ok(n) => tracing::info!("read-model boot backfill: {n} wallet rows"),
                Err(e) => tracing::error!("read-model wallet backfill failed: {e}"),
            }
            match meter_rm.backfill_meters().await {
                Ok(n) => tracing::info!("read-model boot backfill: {n} meter rows"),
                Err(e) => tracing::error!("read-model meter backfill failed: {e}"),
            }
            Some(trading_logic::ReadModelFeedWorker::new(
                wallet_rm,
                meter_rm,
                config.kafka_bootstrap_servers.clone(),
                config.readmodel_meter_brokers.clone(),
                config.readmodel_iam_topic.clone(),
                config.readmodel_meter_topic.clone(),
            ))
        } else {
            None
        };

        let infra = Infrastructure {
            db: db_pool,
            order_repo: order_repo.clone(),
            meter_repo: meter_repo.clone(),
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
            readmodel_feed_worker,
        };

        // 2. Services
        let settlement_service = Arc::new(SettlementService::new(
            settlement_repo.clone(),
            blockchain.clone(),
            audit.clone(),
            config.platform_user_id,
        ));

        let matcher_service = Arc::new(MatcherService::new(
            order_repo.clone(),
            settlement_repo.clone(),
            Arc::new(GridAwareTopology::new()),
        ));

        // Uniform-price clearing for the Interval segment (Phase 4). Same repos +
        // topology as the matcher; the ClearingWorker drives it on each tick.
        let clearing_service = Arc::new(ClearingService::new(
            order_repo.clone(),
            settlement_repo.clone(),
            Arc::new(GridAwareTopology::new()),
        ));

        let vpp_service = Arc::new(trading_logic::vpp::VppService::new(
            vpp_repo.clone(),
            audit.clone(),
            events.clone(),
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
            clearing: clearing_service,
            vpp: vpp_service,
            recurring_evaluator,
            trigger_evaluator,
        };

        Ok((infra, services))
    }
}
