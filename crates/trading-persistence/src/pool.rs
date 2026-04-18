//! Database pool management for the GridTokenX trading service.

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use trading_core::config::Config;
use trading_core::error::{ApiError, Result};

/// Initialize a new PostgreSQL connection pool.
pub async fn create_pool(config: &Config) -> Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(config.max_connections)
        .acquire_timeout(std::time::Duration::from_secs(3))
        .connect(&config.database_url)
        .await
        .map_err(|e| ApiError::Configuration(format!("Failed to connect to database: {}", e)))
}
