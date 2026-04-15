//! Simplified telemetry for GridTokenX trading service (standard logging only).

use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Initialize tracing and set up the global subscriber for JSON logging.
///
/// `_service_name_default`: Kept for backward compatibility with calls.
pub fn init_telemetry(_service_name_default: &'static str) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().json())
        .init();

    tracing::info!("Tracing initialized (JSON logging enabled)");
}
