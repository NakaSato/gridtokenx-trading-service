use gridtokenx_trading_service::startup;
use gridtokenx_trading_service::telemetry;
use tracing::{info, error};
use tokio_util::sync::CancellationToken;
use tokio::signal;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize OpenTelemetry tracing (sets up global subscriber)
    let telemetry_guard = telemetry::init_telemetry("gridtokenx-trading");

    info!("🚀 Starting GridTokenX Trading Service...");

    // Lifecycle coordination
    let shutdown_token = CancellationToken::new();
    let service_token = shutdown_token.clone();

    // Spawn signal handler
    tokio::spawn(async move {
        let ctrl_c = async {
            signal::ctrl_c()
                .await
                .expect("failed to install Ctrl+C handler");
        };

        #[cfg(unix)]
        let terminate = async {
            signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("failed to install signal handler")
                .recv()
                .await;
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => {
                info!("🛑 SIGINT received, triggering shutdown...");
            },
            _ = terminate => {
                info!("🛑 SIGTERM received, triggering shutdown...");
            },
        }

        shutdown_token.cancel();
    });

    // Start server
    let result = startup::run(service_token).await;
    if let Err(e) = &result {
        error!("❌ Trading service startup failed: {:#}", e);
    }

    info!("👋 Shutdown complete. Cleaning up telemetry...");
    telemetry::shutdown_telemetry(&telemetry_guard);
    result
}
