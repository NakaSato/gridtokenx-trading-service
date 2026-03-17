use gridtokenx_trading_service::startup;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    info!("🚀 Starting GridTokenX Trading Service...");

    // Start server
    startup::run().await?;

    Ok(())
}
