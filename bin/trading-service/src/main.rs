use sqlx::postgres::PgPoolOptions;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use trading_api::state::AppState;
use trading_service::builder::ServiceBuilder;

#[tokio::main]
// Boot sequence: build once, then spawn each worker — reads as the runtime map.
#[allow(clippy::too_many_lines)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Setup telemetry. Bind the guard for the whole process so a graceful
    // shutdown flushes buffered spans; the service.name matches the gridtokenx-*
    // convention used by the other services in Tempo.
    let _telemetry = trading_infra::init_telemetry("gridtokenx-trading");

    // Install the Prometheus recorder BEFORE any worker spawns or `record_*` fires —
    // metrics emitted before the global recorder is set go to a no-op sink and are lost.
    trading_infra::metrics::install_recorder();

    // External NTP server (Cloudflare primary, Google fallback) as primary wall-clock
    // source — order/match/settlement timestamps now use agreed time, not a drifting
    // container clock. Background poller; `time::now()` is non-blocking, degrades to OS clock.
    trading_infra::time::init_default();

    // Install default crypto provider for rustls (required for rustls 0.23+)
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install default crypto provider");

    info!("Starting GridTokenX Trading Service (Modular Monolith)");

    // 2. Load Config
    let config = std::sync::Arc::new(trading_core::config::Config::from_env()?);

    let port: u16 = std::env::var("HTTP_PORT")
        .unwrap_or_else(|_| "8093".to_string())
        .parse()?;
    let grpc_port: u16 = std::env::var("GRPC_PORT")
        .unwrap_or_else(|_| "8092".to_string())
        .parse()?;

    // 3. Initialize DB
    let pool = PgPoolOptions::new()
        .max_connections(config.max_connections)
        .connect(&config.database_url)
        .await?;

    // 4. Build system
    let (infra, services) = ServiceBuilder::build(pool, config.clone()).await?;

    info!("System assembled successfully");

    // 5. Graceful Shutdown Token
    let shutdown_token = CancellationToken::new();
    let main_token = shutdown_token.clone();

    // 6. Start background workers
    // Realtime matching: the submit paths wake this worker the moment an order is
    // accepted, so a crossing pair matches within MATCHER_DEBOUNCE_MS instead of
    // waiting out the tick. MATCHER_INTERVAL_MS stays as the fallback tick (see
    // MatcherConfig); MATCHER_REALTIME=false reverts to pure polling.
    let matcher_interval = tokio::time::Duration::from_millis(config.matcher.interval_ms);
    let matcher_worker = if config.matcher.realtime_enabled {
        trading_logic::MatcherWorker::realtime(
            services.matcher.clone(),
            matcher_interval,
            tokio::time::Duration::from_millis(config.matcher.debounce_ms),
        )
    } else {
        trading_logic::MatcherWorker::new(services.matcher.clone(), matcher_interval)
    };
    tokio::spawn(async move {
        matcher_worker.run().await;
    });

    // Uniform-price clearing for the Interval segment (Phase 4). Polls every 60s;
    // clears + closes any epoch whose 15-minute window has elapsed.
    let clearing_worker = trading_logic::ClearingWorker::new(
        services.clearing.clone(),
        tokio::time::Duration::from_mins(1),
    );
    tokio::spawn(async move {
        clearing_worker.run().await;
    });

    // Expiry reaper — marks open orders past their expires_at as Expired. Its own
    // worker (not the matcher) so it runs in every role. Since the active-order
    // queries no longer filter expired rows at the DB level, this is the ONLY
    // mechanism that drops expired orders from the active set, so: (1) a tight 10s
    // cadence keeps the expired backlog the matcher re-fetches small, and (2) it
    // is supervised — if the loop ever panics it is respawned (with a capped
    // exponential backoff) instead of silently stopping all expiry.
    let reaper_repo = infra.order_repo.clone();
    tokio::spawn(async move {
        // Exponential backoff capped at 30s so a deterministic panic-on-spawn
        // degrades to one respawn per 30s instead of a 1Hz crash-loop that floods
        // logs; reset to the base delay after a run that stayed up past it.
        const BASE_BACKOFF: std::time::Duration = std::time::Duration::from_secs(1);
        const MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(30);
        // Full-jitter respawn delay so co-deployed matcher replicas don't all
        // respawn a panicking reaper on the same schedule. `attempt` climbs per
        // crash, resets after a run that stayed up past the max backoff.
        let mut attempt = 0u32;
        loop {
            let repo = reaper_repo.clone();
            let started = tokio::time::Instant::now();
            let handle = tokio::spawn(async move {
                trading_logic::ReaperWorker::new(repo, tokio::time::Duration::from_secs(10))
                    .run()
                    .await;
            });
            match handle.await {
                // run() is an infinite loop, so a clean return only happens if it
                // is ever given a graceful-shutdown path. Treat that as intended
                // shutdown and stop supervising — but log it so it is never silent.
                Ok(()) => {
                    info!("ReaperWorker run() returned; supervisor exiting (expiry stopped)");
                    break;
                }
                Err(e) => {
                    // Reset backoff if the task ran healthily for a while before dying.
                    if started.elapsed() >= MAX_BACKOFF {
                        attempt = 0;
                    }
                    let delay = gridtokenx_telemetry::backoff::full_jitter(
                        attempt,
                        BASE_BACKOFF,
                        MAX_BACKOFF,
                    );
                    error!(
                        "ReaperWorker task terminated unexpectedly ({e}); respawning in {:?}",
                        delay
                    );
                    tokio::time::sleep(delay).await;
                    attempt = attempt.saturating_add(1);
                }
            }
        }
    });

    let settlement_worker = trading_logic::SettlementWorker::new(
        services.settlement.clone(),
        tokio::time::Duration::from_secs(10),
        10, // batch limit
    );
    tokio::spawn(async move {
        settlement_worker.run().await;
    });

    let supply_sync_worker = trading_logic::SupplySyncWorker::new(
        infra.blockchain.clone(),
        config.tokenization.polling_interval_secs,
    );
    tokio::spawn(async move {
        supply_sync_worker.run().await;
    });

    // Recurring-order evaluator: place due recurring orders (Phase 6).
    let recurring_worker = trading_logic::RecurringEvaluatorWorker::new(
        services.recurring_evaluator.clone(),
        tokio::time::Duration::from_mins(1),
    );
    tokio::spawn(async move {
        recurring_worker.run().await;
    });

    // Price-alert trigger evaluator: fire alerts against market price (Phase 6).
    let trigger_worker = trading_logic::TriggerEvaluatorWorker::new(
        services.trigger_evaluator.clone(),
        tokio::time::Duration::from_secs(30),
    );
    tokio::spawn(async move {
        trigger_worker.run().await;
    });

    // Read-model feed worker (DB-per-service Phase 1). Present only when
    // TRADING_READMODEL_FEED=true; the boot backfill already ran in the builder.
    // When the flag is off this is None and nothing spawns (zero behavior change).
    if let Some(readmodel_worker) = infra.readmodel_feed_worker {
        tokio::spawn(async move {
            readmodel_worker.run().await;
        });
    }

    // 7. Start API Server
    //
    // Market-data fan-out for `GET /ws/trading`. The hub is shared between the
    // Kafka pump below and every socket; without the pump it stays empty and the
    // route simply never emits, so this is inert when Kafka is disabled.
    let ws_hub = std::sync::Arc::new(trading_api::websocket::ZoneHub::new());
    if config.kafka_enabled {
        let topics =
            trading_infra::events::kafka::KafkaTopics::with_prefix(&config.kafka_topic_prefix);
        trading_api::websocket::spawn_kafka_pump(
            config.kafka_bootstrap_servers.clone(),
            vec![
                topics.orders_created,
                topics.orders_matched,
                topics.orders_updated,
                topics.settlements,
            ],
            // Distinct group per pod: every gateway needs *all* partitions, so
            // pods must not share a group and split them between each other.
            format!("trading-ws-gateway-{}", uuid::Uuid::new_v4()),
            std::sync::Arc::clone(&ws_hub),
        );
    } else {
        info!("Market-data WebSocket pump not started (KAFKA_EVENTS_ENABLED=false)");
    }

    let state = AppState {
        config: config.clone(),
        order_repo: infra.order_repo,
        meter_repo: infra.meter_repo,
        settlement_repo: infra.settlement_repo,
        futures_repo: infra.futures_repo,
        carbon_repo: infra.carbon_repo,
        analytics_repo: infra.analytics_repo,
        price_alert_repo: infra.price_alert_repo,
        recurring_repo: infra.recurring_repo,
        events: infra.events,
        blockchain: infra.blockchain,
        identity: infra.identity,
        audit: infra.audit,
        matcher: services.matcher,
        settlement: services.settlement,
        vpp: services.vpp,
        ws_hub,
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
