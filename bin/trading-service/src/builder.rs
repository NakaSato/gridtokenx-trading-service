use sqlx::PgPool;
use std::sync::Arc;
use trading_core::traits::{
    AnalyticsRepository, AuditLog, BlockchainGateway, CacheStore, CarbonRepository, EventPublisher,
    FuturesRepository, IdentityGateway, MeterReadModelRepository, MeterRepository, OrderRepository,
    OutboxRepository, PriceAlertRepository, RecurringOrderRepository, SettlementRepository,
    VppRepository, WalletReadModelRepository,
};
use trading_infra::audit::AuditLogger;
use trading_infra::blockchain::BlockchainService;
use trading_infra::cache::CacheService;
use trading_logic::{ClearingService, GridAwareTopology, MatcherService, SettlementService};
use trading_persistence::repositories::{
    PostgresAnalyticsRepository, PostgresCarbonRepository, PostgresFuturesRepository,
    PostgresMeterRepository, PostgresOrderRepository, PostgresPriceAlertRepository,
    PostgresRecurringOrderRepository, PostgresSettlementRepository,
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
    /// The on-chain charge rates read at boot. Shared — not re-derived — with the
    /// matcher's topology and the settlement ledger, so the submit edges gate on
    /// exactly the tariff the chain will bill.
    pub charge_rates: Arc<dyn trading_core::charges::ChargeRates>,
    /// Keeps `charge_rates` in step with the chain. `main.rs` spawns it; without
    /// it the schedule is frozen at whatever the boot read produced.
    pub charge_rates_worker: trading_logic::ChargeRatesWorker,
    /// Chain side of the reconciler's collector cross-check.
    pub collector_balances: Arc<dyn trading_core::traits::CollectorBalanceSource>,
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
    // The composition root wires every dependency in one place BY DESIGN
    // (see CLAUDE.md); its length is the inventory of the system.
    #[allow(clippy::too_many_lines)]
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
        let outbox_repo = Arc::new(
            trading_persistence::repositories::PostgresOutboxRepository::new(db_pool.clone()),
        );
        let futures_repo = Arc::new(PostgresFuturesRepository::new(db_pool.clone()));
        let carbon_repo = Arc::new(PostgresCarbonRepository::new(db_pool.clone()));
        let analytics_repo = Arc::new(PostgresAnalyticsRepository::new(db_pool.clone()));
        let price_alert_repo = Arc::new(PostgresPriceAlertRepository::new(db_pool.clone()));
        let recurring_repo = Arc::new(PostgresRecurringOrderRepository::new(db_pool.clone()));
        let vpp_repo = Arc::new(
            trading_persistence::repositories::PostgresVppRepository::new(db_pool.clone()),
        );

        // IAM identity gateway. NOTE: trading does not call IAM at request time
        // — auth is header-based, injected by the APISIX gateway (see
        // trading-api `auth.rs`). This gateway exists only to satisfy the
        // BlockchainService custodial-signing seam below, and its sole method
        // (`sign_message`) is intentionally inert (IAM's SignMessage RPC was
        // removed; signing moved to Chain Bridge `chain.tx.submit`). Kept wired
        // so the seam is ready when the on-chain create_order gains a
        // non-signing owner account; remove if that path is abandoned.
        let identity: Arc<dyn IdentityGateway> =
            Arc::new(trading_infra::identity::IamIdentityGateway::new(
                &config.iam_service_url,
                config.internal_api_key.clone(),
            ));

        // Read-model repos (DB-per-service Phase 1). Built HERE — before the
        // BlockchainService — so the wallet read-model can be injected into it,
        // giving `get_user_primary_wallet` an on-demand self-heal path when a
        // wallet event was dropped before projection (boot-window Kafka wedge).
        // OFF unless TRADING_READMODEL_FEED=true; when off both are None (zero
        // behavior change) and the lazy-reconcile fallback is simply disabled.
        // The one-shot boot backfill (snapshot of the source tables) also runs
        // here; the live feed worker is spawned later from these same repos.
        #[allow(clippy::items_after_statements)] // alias belongs beside its binding
        type ReadModelRepos = (
            Option<Arc<dyn WalletReadModelRepository>>,
            Option<Arc<dyn MeterReadModelRepository>>,
        );
        let (wallet_rm, meter_rm): ReadModelRepos = if config.readmodel_feed_enabled {
            // Source pools live on other services' DBs (via pgdog). At cold boot
            // those backends can be briefly unready (DB still loading, pgdog not up
            // yet), so a single connect attempt races and drops the backfill to the
            // local pool — which post-split lacks the source tables and just logs a
            // spurious "relation does not exist". Retry with linear backoff so the
            // snapshot backfill actually runs once the source comes up. Bounded and
            // non-fatal: on exhaustion we skip the backfill (the event feed still
            // seeds the read-model).
            let connect_source = |name: &'static str, url: Option<String>| async move {
                let Some(url) = url else {
                    tracing::warn!(
                        "read-model backfill: no {name} source DB URL configured; \
                         backfilling from the local pool (pre-cutover shared-DB mode)"
                    );
                    return None;
                };
                #[allow(clippy::items_after_statements)] // constant beside its loop
                const MAX_ATTEMPTS: u32 = 5;
                for attempt in 1..=MAX_ATTEMPTS {
                    match sqlx::postgres::PgPoolOptions::new()
                        .max_connections(2)
                        .connect(&url)
                        .await
                    {
                        Ok(pool) => return Some(pool),
                        Err(e) if attempt < MAX_ATTEMPTS => {
                            let backoff =
                                std::time::Duration::from_millis(500 * u64::from(attempt));
                            tracing::warn!(
                                "read-model backfill: {name} source DB not ready \
                                 (attempt {attempt}/{MAX_ATTEMPTS}, retrying in {backoff:?}): {e}"
                            );
                            tokio::time::sleep(backoff).await;
                        }
                        Err(e) => {
                            tracing::error!(
                                "read-model backfill: failed to connect {name} source DB after \
                                 {MAX_ATTEMPTS} attempts (backfill skipped, event feed \
                                 unaffected): {e}"
                            );
                            return None;
                        }
                    }
                }
                None
            };
            let iam_source = connect_source("IAM", config.readmodel_iam_db_url.clone()).await;
            let meter_source =
                connect_source("metering", config.readmodel_meter_db_url.clone()).await;

            let mut wallet_repo =
                trading_persistence::repositories::PgWalletReadModelRepository::new(
                    db_pool.clone(),
                );
            if let Some(pool) = iam_source {
                wallet_repo = wallet_repo.with_source_pool(pool);
            }
            let mut meter_repo_rm =
                trading_persistence::repositories::PgMeterReadModelRepository::new(db_pool.clone());
            if let Some(pool) = meter_source {
                meter_repo_rm = meter_repo_rm.with_source_pool(pool);
            }
            let wallet_rm: Arc<dyn WalletReadModelRepository> = Arc::new(wallet_repo);
            let meter_rm: Arc<dyn MeterReadModelRepository> = Arc::new(meter_repo_rm);
            match wallet_rm.backfill_wallets().await {
                Ok(n) => tracing::info!("read-model boot backfill: {n} wallet rows"),
                Err(e) => tracing::error!("read-model wallet backfill failed: {e}"),
            }
            match meter_rm.backfill_meters().await {
                Ok(n) => tracing::info!("read-model boot backfill: {n} meter rows"),
                Err(e) => tracing::error!("read-model meter backfill failed: {e}"),
            }
            (Some(wallet_rm), Some(meter_rm))
        } else {
            (None, None)
        };

        let mut blockchain_svc = BlockchainService::new(
            chain_bridge_url.clone(),
            config.solana_cluster.clone(),
            config.solana_programs.clone(),
            Some(db_pool.clone()),
            None,
        )
        .await?
        .with_identity_gateway(identity.clone())
        .with_trade_settlement_enabled(config.trade_settlement_enabled)
        .with_per_user_escrow_settlement(config.per_user_escrow_settlement);
        if let Some(wallet_rm) = &wallet_rm {
            blockchain_svc = blockchain_svc.with_wallet_read_model(wallet_rm.clone());
        }
        // Settlement charge rates, read once from chain before the concrete service
        // is erased behind the gateway trait. They decide what the `settlements`
        // ledger records as fee/wheeling/loss/net, so they must be the on-chain
        // values — `config.transaction_fee_bps` disagrees with the deployed market
        // (50 vs 25 bps) and would book double the real fee.
        //
        // On failure start from ZERO rather than a guess: zero is visibly wrong and
        // comes with this error, whereas an invented rate would be quietly wrong —
        // which is the failure mode this replaces. Unlike before, ZERO is no longer
        // terminal: `ChargeRatesWorker` re-reads on a cadence, so a failed boot read
        // heals on the first successful poll instead of persisting until restart.
        let boot_rates = match blockchain_svc.read_charge_rates().await {
            Ok(r) => {
                tracing::info!(
                    fee_bps = r.fee_bps,
                    wheeling_rate_per_kwh = r.wheeling_rate_per_kwh,
                    loss_bps = r.loss_bps,
                    "settlement charge rates loaded from chain"
                );
                r
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "could not read on-chain charge rates; the ledger will record ZERO \
                     fee/wheeling/loss and the sell-price floor is disabled until the \
                     ChargeRatesWorker completes a successful poll"
                );
                trading_core::charges::StaticChargeRates::ZERO
            }
        };

        // ONE cell, shared by every consumer — the matcher's landed cost, the
        // settlement ledger, the submit edges' sell-price floor and the quote
        // endpoint. Handing out copies here is what would let them drift apart
        // again, which is the whole failure this schedule keeps being fixed for.
        let live_rates = Arc::new(trading_core::charges::RefreshingChargeRates::new(
            boot_rates,
        ));
        let charge_rates: Arc<dyn trading_core::charges::ChargeRates> = live_rates.clone();

        let blockchain_svc = Arc::new(blockchain_svc);
        let charge_rates_worker = trading_logic::ChargeRatesWorker::new(
            blockchain_svc.clone(),
            live_rates,
            config.charge_rates_refresh_secs,
        );
        let collector_balances: Arc<dyn trading_core::traits::CollectorBalanceSource> =
            blockchain_svc.clone();
        let blockchain: Arc<dyn BlockchainGateway> = blockchain_svc;

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
            trading_infra::events::OutboxPublisher::new(outbox_repo.clone()),
        );

        // Read-model feed worker (DB-per-service Phase 1). The repos + boot
        // backfill were built above (so the wallet read-model could be injected
        // into the BlockchainService); here we just hand the live-feed worker to
        // main.rs to spawn. Present only when both repos exist
        // (TRADING_READMODEL_FEED=true) — otherwise no worker spawns, zero
        // behavior change.
        let readmodel_feed_worker = match (wallet_rm, meter_rm) {
            (Some(wallet_rm), Some(meter_rm)) => Some(trading_logic::ReadModelFeedWorker::new(
                wallet_rm,
                meter_rm,
                config.kafka_bootstrap_servers.clone(),
                config.readmodel_meter_brokers.clone(),
                config.readmodel_iam_topic.clone(),
                config.readmodel_meter_topic.clone(),
            )),
            _ => None,
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
            charge_rates: charge_rates.clone(),
            charge_rates_worker,
            collector_balances,
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
            Arc::new(GridAwareTopology::new(charge_rates.clone())),
            charge_rates.clone(),
        ));

        // Uniform-price clearing for the Interval segment (Phase 4). Same repos +
        // topology as the matcher; the ClearingWorker drives it on each tick.
        let clearing_service = Arc::new(ClearingService::new(
            order_repo.clone(),
            settlement_repo.clone(),
            Arc::new(GridAwareTopology::new(charge_rates.clone())),
            charge_rates.clone(),
        ));

        let vpp_service = Arc::new(trading_logic::vpp::VppService::new(
            vpp_repo.clone(),
            audit.clone(),
            events.clone(),
        ));

        // Recurring-order & price-alert automation (Phase 6). Both read/write
        // through the same repos + outbox as the REST layer.
        // The matcher doubles as the evaluator's `MatchTrigger`, so an order it
        // places is matched on arrival rather than on the fallback tick.
        let recurring_evaluator = Arc::new(trading_logic::RecurringEvaluator::new(
            recurring_repo.clone(),
            order_repo.clone(),
            matcher_service.clone(),
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
