use trading_service::builder::ServiceBuilder;
use sqlx::postgres::PgPoolOptions;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use trading_api::state::AppState;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Setup telemetry
    trading_infra::init_telemetry("trading-service");

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
    let matcher_worker = trading_logic::MatcherWorker::new(
        services.matcher.clone(),
        tokio::time::Duration::from_secs(1),
    );
    tokio::spawn(async move {
        matcher_worker.run().await;
    });

    // Uniform-price clearing for the Interval segment (Phase 4). Polls every 60s;
    // clears + closes any epoch whose 15-minute window has elapsed.
    let clearing_worker = trading_logic::ClearingWorker::new(
        services.clearing.clone(),
        tokio::time::Duration::from_secs(60),
    );
    tokio::spawn(async move {
        clearing_worker.run().await;
    });

    // Expiry reaper — marks open orders past their expires_at as Expired. Its own
    // worker (not the matcher) so it runs in every role. Since the active-order
    // queries no longer filter expired rows at the DB level, this is the ONLY
    // mechanism that drops expired orders from the active set, so: (1) a tight 10s
    // cadence keeps the expired backlog the matcher re-fetches small, and (2) it
    // is supervised — if the loop ever panics it is respawned (with a short
    // backoff) instead of silently stopping all expiry.
    let reaper_repo = infra.order_repo.clone();
    tokio::spawn(async move {
        // Exponential backoff capped at 30s so a deterministic panic-on-spawn
        // degrades to one respawn per 30s instead of a 1Hz crash-loop that floods
        // logs; reset to the base delay after a run that stayed up past it.
        const BASE_BACKOFF: tokio::time::Duration = tokio::time::Duration::from_secs(1);
        const MAX_BACKOFF: tokio::time::Duration = tokio::time::Duration::from_secs(30);
        let mut backoff = BASE_BACKOFF;
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
                        backoff = BASE_BACKOFF;
                    }
                    error!(
                        "ReaperWorker task terminated unexpectedly ({e}); respawning in {:?}",
                        backoff
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
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
        tokio::time::Duration::from_secs(60),
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

    // 7. Start API Server
    let state = AppState {
        config: config.clone(),
        order_repo: infra.order_repo,
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
