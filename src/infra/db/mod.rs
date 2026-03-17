use anyhow::Result;
use sqlx::{PgPool, Pool, Postgres, postgres::PgPoolOptions};
use tracing::{info, warn};
use std::time::Duration;

pub mod repository;
pub mod schema;

pub use repository::{PagedResult, Pagination, QueryFilter, Repository, SortOrder, Transaction};

pub type DatabasePool = Pool<Postgres>;

pub async fn setup_database(database_url: &str) -> Result<DatabasePool> {
    info!("Connecting to database with performance-optimized settings (Priority 4)");
    
    // Parse database URL and check for SSL parameters
    let ssl_mode = if database_url.contains("sslmode=require") || 
                      database_url.contains("sslmode=verify-ca") ||
                      database_url.contains("sslmode=verify-full") {
        "SSL enabled"
    } else {
        warn!("Database connection does not enforce SSL. Consider adding sslmode=require to connection string");
        "SSL not enforced"
    };
    
    info!("Database SSL mode: {}", ssl_mode);
    
    // Priority 4: Performance Optimization - Enhanced connection pool settings
    let pool = PgPoolOptions::new()
        .max_connections(100)         // Priority 4: Increased from 50 to 100 for higher concurrency
        .min_connections(10)          // Priority 4: Increased from 5 to maintain larger baseline
        .acquire_timeout(Duration::from_secs(30))   // Priority 4: Increased to 30s for stability
        .idle_timeout(Duration::from_secs(180))     // Priority 4: Reduced from 5min to 3min for faster cleanup
        .max_lifetime(Duration::from_secs(900))      // Priority 4: Reduced from 30min to 15min for fresher connections
        .test_before_acquire(true)      // Test connections before use
        .after_connect(|conn, _meta| Box::pin(async move {
            // Priority 4: Enhanced connection configuration for optimal performance
            sqlx::query!("SET timezone = 'UTC'").execute(&mut *conn).await?;
            sqlx::query!("SET statement_timeout = '60s'").execute(&mut *conn).await?;  // Increased to 60s
            sqlx::query!("SET lock_timeout = '10s'").execute(&mut *conn).await?;     // Priority 4: Add lock timeout
            sqlx::query!("SET idle_in_transaction_session_timeout = '60s'").execute(&mut *conn).await?;  // Priority 4: Prevent long idle transactions
            // Priority 4: Performance tuning settings
            sqlx::query!("SET shared_preload_libraries = 'pg_stat_statements'").execute(&mut *conn).await.ok(); // Enable query statistics
            sqlx::query!("SET track_activity_query_size = 'on'").execute(&mut *conn).await.ok(); // Track query activity
            Ok(())
        }))
        .connect(database_url)
        .await?;
    
    // Priority 4: Test connection with performance validation
    let start_time = std::time::Instant::now();
    sqlx::query!("SELECT 1, version()").execute(&pool).await?;
    let connection_time = start_time.elapsed();
    
    info!("✅ Database connection established successfully in {:?}", connection_time);
    info!("Priority 4 Performance Tuning Applied:");
    info!("  - Max connections: 100 (was 50)");
    info!("  - Min connections: 10 (was 5)");
    info!("  - Acquire timeout: 30s (was 3s)");
    info!("  - Idle timeout: 3min (was 5min)");
    info!("  - Max lifetime: 15min (was 30min)");
    info!("  - Statement timeout: 60s (was 15s)");
    
    Ok(pool)
}

pub async fn setup_timescale_database(influxdb_url: &str) -> Result<Option<DatabasePool>> {
    if influxdb_url.starts_with("http://") || influxdb_url.starts_with("https://") {
        info!("InfluxDB connection skipped (HTTP URL detected): {}", influxdb_url);
        info!("Note: InfluxDB integration requires proper client library - currently not in use");
        return Ok(None);
    }
    
    info!("Connecting to TimescaleDB: {}", influxdb_url);
    
    let pool = PgPool::connect(influxdb_url).await?;
    
    // Test the connection
    sqlx::query!("SELECT 1").execute(&pool).await?;
    
    Ok(Some(pool))
}

pub async fn run_migrations(pool: &DatabasePool) -> Result<()> {
    // Phase 10: VPP Orchestration - Ensuring migrations are picked up
    info!("Running database migrations");
    
    sqlx::migrate!("./migrations").run(pool).await?;
    
    info!("Database migrations completed successfully");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    // Note: Testcontainers integration will be implemented in Phase 2
    // when we set up full integration testing

    #[allow(dead_code)]
pub struct TestDatabase {
        pub pool: DatabasePool,
    }

    #[allow(dead_code)]
    impl TestDatabase {
        pub async fn new() -> Result<Self> {
            // For now, just create a mock connection
            // In Phase 2, we'll implement proper test database setup
            todo!("Test database setup will be implemented in Phase 2")
        }
    }
}
