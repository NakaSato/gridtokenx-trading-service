//! P2P Market Configuration Service
//!
//! Provides dynamic configuration for P2P market parameters including:
//! - Wheeling charges (transmission fees)
//! - Loss factors (technical loss percentages)
//! - Base pricing parameters
//!
//! All values are stored in the database and can be updated by admin users
//! directly through the trading dashboard.

use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::Serialize;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info};
use utoipa::ToSchema;
use anyhow::Result;

/// In-memory cache for P2P configuration values
#[derive(Clone, Debug, Default)]
pub struct P2PConfigCache {
    pub values: HashMap<String, Decimal>,
}

impl P2PConfigCache {
    pub fn get(&self, key: &str) -> Option<Decimal> {
        self.values.get(key).copied()
    }

    pub fn get_f64(&self, key: &str) -> Option<f64> {
        self.values.get(key).and_then(|d| d.to_f64())
    }

    pub fn insert(&mut self, key: String, value: Decimal) {
        self.values.insert(key, value);
    }

    pub fn update_from_map(&mut self, values: HashMap<String, Decimal>) {
        self.values = values;
    }
}

/// Service for managing P2P market configuration
#[derive(Clone, Debug)]
pub struct P2PConfigService {
    pool: PgPool,
    cache: Arc<RwLock<P2PConfigCache>>,
}

impl P2PConfigService {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            cache: Arc::new(RwLock::new(P2PConfigCache::default())),
        }
    }

    /// Initialize cache by loading all config from database
    pub async fn initialize(&self) -> Result<(), sqlx::Error> {
        info!("Initializing P2P config cache from database in Trading Service...");
        let values = self.load_all_from_db().await?;
        let mut cache = self.cache.write().await;
        cache.update_from_map(values);
        info!(
            "P2P config cache initialized with {} values",
            cache.values.len()
        );
        Ok(())
    }

    /// Reload cache from database
    pub async fn reload(&self) -> Result<(), sqlx::Error> {
        let values = self.load_all_from_db().await?;
        let mut cache = self.cache.write().await;
        cache.update_from_map(values);
        info!("P2P config cache reloaded");
        Ok(())
    }

    /// Get a config value by key (from cache, fallback to DB)
    pub async fn get(&self, key: &str) -> Option<Decimal> {
        {
            let cache = self.cache.read().await;
            if let Some(value) = cache.get(key) {
                return Some(value);
            }
        }

        match self.get_from_db(key).await {
            Ok(value) => {
                let mut cache = self.cache.write().await;
                if let Some(v) = value {
                    cache.insert(key.to_string(), v);
                }
                value
            }
            Err(e) => {
                error!("Failed to get config {} from DB: {}", key, e);
                None
            }
        }
    }

    /// Get config value as f64
    pub async fn get_f64(&self, key: &str) -> Option<f64> {
        self.get(key).await.and_then(|d| d.to_f64())
    }

    /// Get all config values for a category
    pub async fn get_by_category(
        &self,
        category: &str,
    ) -> Result<HashMap<String, Decimal>, sqlx::Error> {
        let rows = sqlx::query_as::<_, (String, Decimal)>(
            r#"
            SELECT config_key, config_value
            FROM p2p_config
            WHERE category = $1 AND is_active = true
            ORDER BY config_key
            "#,
        )
        .bind(category)
        .fetch_all(&self.pool)
        .await?;

        let mut result = HashMap::new();
        for (key, value) in rows {
            result.insert(key, value);
        }
        Ok(result)
    }

    // ============ Convenience Methods for Wheeling/Loss/Pricing ============

    /// Calculate wheeling charge for zone distance
    pub async fn calculate_wheeling_charge_for_zones(
        &self,
        from_zone: Option<i32>,
        to_zone: Option<i32>,
    ) -> Decimal {
        if let (Some(from), Some(to)) = (from_zone, to_zone) {
            let key = format!("wheeling.zone_{}_{}", from, to);
            if let Some(val) = self.get(&key).await {
                return val;
            }
            let key_rev = format!("wheeling.zone_{}_{}", to, from);
            if let Some(val) = self.get(&key_rev).await {
                return val;
            }
        }

        let zone_distance = match (from_zone, to_zone) {
            (Some(a), Some(b)) => (a - b).unsigned_abs() as i32,
            _ => 2,
        };
        self.calculate_wheeling_charge(zone_distance).await
    }

    pub async fn calculate_wheeling_charge(&self, zone_distance: i32) -> Decimal {
        if zone_distance == 0 {
            if let Some(val) = self.get("wheeling.same_zone").await {
                return val;
            }
            if let Some(val) = self.get("wheeling.zone_0").await {
                return val;
            }
            Decimal::from_f64(0.50).unwrap_or_default()
        } else if zone_distance == 1 {
            if let Some(val) = self.get("wheeling.adjacent_zone").await {
                return val;
            }
            if let Some(val) = self.get("wheeling.zone_1").await {
                return val;
            }
            Decimal::from_f64(1.00).unwrap_or_default()
        } else {
            let base = self
                .get("wheeling.base_charge")
                .await
                .unwrap_or_else(|| Decimal::from_f64(1.50).unwrap_or_default());
            let rate = self
                .get("wheeling.distance_rate")
                .await
                .unwrap_or_else(|| Decimal::from_f64(0.10).unwrap_or_default());
            let distance = Decimal::from(zone_distance);
            base + (rate * distance)
        }
    }

    pub async fn calculate_loss_factor_for_zones(
        &self,
        from_zone: Option<i32>,
        to_zone: Option<i32>,
    ) -> Decimal {
        if let (Some(from), Some(to)) = (from_zone, to_zone) {
            let key = format!("loss.zone_{}_{}", from, to);
            if let Some(val) = self.get(&key).await {
                return val;
            }
            let key_rev = format!("loss.zone_{}_{}", to, from);
            if let Some(val) = self.get(&key_rev).await {
                return val;
            }
        }

        let zone_distance = match (from_zone, to_zone) {
            (Some(a), Some(b)) => (a - b).unsigned_abs() as i32,
            _ => 2,
        };
        self.calculate_loss_factor(zone_distance).await
    }

    pub async fn calculate_loss_factor(&self, zone_distance: i32) -> Decimal {
        if zone_distance == 0 {
            if let Some(val) = self.get("loss.same_zone").await {
                return val;
            }
            if let Some(val) = self.get("loss.zone_0").await {
                return val;
            }
            Decimal::from_f64(0.01).unwrap_or_default()
        } else if zone_distance == 1 {
            if let Some(val) = self.get("loss.adjacent_zone").await {
                return val;
            }
            if let Some(val) = self.get("loss.zone_1").await {
                return val;
            }
            Decimal::from_f64(0.03).unwrap_or_default()
        } else {
            let base = self
                .get("loss.base_loss")
                .await
                .unwrap_or_else(|| Decimal::from_f64(0.03).unwrap_or_default());
            let rate = self
                .get("loss.distance_rate")
                .await
                .unwrap_or_else(|| Decimal::from_f64(0.01).unwrap_or_default());
            let distance = Decimal::from(zone_distance);
            let loss = base + (rate * distance);
            let max_loss = self
                .get("loss.max_loss")
                .await
                .unwrap_or_else(|| Decimal::from_f64(0.15).unwrap_or_default());
            loss.min(max_loss)
        }
    }

    pub async fn get_market_prices(&self) -> MarketPrices {
        MarketPrices {
            base_price: self
                .get_f64("pricing.base_price_thb_kwh")
                .await
                .unwrap_or(4.0),
            grid_import_price: self
                .get_f64("pricing.grid_import_price_thb_kwh")
                .await
                .unwrap_or(4.5),
            grid_export_price: self
                .get_f64("pricing.grid_export_price_thb_kwh")
                .await
                .unwrap_or(2.2),
            transaction_fee_bps: self
                .get_f64("pricing.transaction_fee_bps")
                .await
                .unwrap_or(25.0),
            min_price: self
                .get_f64("pricing.min_price_per_kwh")
                .await
                .unwrap_or(2.2),
            max_price: self
                .get_f64("pricing.max_price_per_kwh")
                .await
                .unwrap_or(4.15),
        }
    }

    async fn load_all_from_db(&self) -> Result<HashMap<String, Decimal>, sqlx::Error> {
        let rows = sqlx::query_as::<_, (String, Decimal)>(
            r#"
            SELECT config_key, config_value
            FROM p2p_config
            WHERE is_active = true
            ORDER BY config_key
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut result = HashMap::new();
        for (key, value) in rows {
            result.insert(key, value);
        }
        Ok(result)
    }

    async fn get_from_db(&self, key: &str) -> Result<Option<Decimal>, sqlx::Error> {
        let row = sqlx::query_as::<_, (Decimal,)>(
            r#"
            SELECT config_value
            FROM p2p_config
            WHERE config_key = $1 AND is_active = true
            "#,
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| r.0))
    }

    /// Get all active configurations as a HashMap for bulk loading
    pub async fn get_all_active(&self) -> Result<HashMap<String, Decimal>, sqlx::Error> {
        self.load_all_from_db().await
    }
}

/// Market pricing configuration
#[derive(Debug, Clone)]
pub struct MarketPrices {
    pub base_price: f64,
    pub grid_import_price: f64,
    pub grid_export_price: f64,
    pub transaction_fee_bps: f64,
    pub min_price: f64,
    pub max_price: f64,
}

/// Audit entry for config changes
#[derive(Debug, Clone, sqlx::FromRow, Serialize, ToSchema)]
pub struct P2PConfigAuditEntry {
    pub id: i32,
    pub config_key: String,
    pub old_value: Option<Decimal>,
    pub new_value: Option<Decimal>,
    pub changed_at: chrono::DateTime<chrono::Utc>,
    pub changed_by: Option<i32>,
    pub change_reason: Option<String>,
}
