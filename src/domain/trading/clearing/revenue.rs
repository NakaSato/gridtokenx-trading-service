use anyhow::Result;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use utoipa::ToSchema;
use uuid::Uuid;

use super::MarketClearingService;

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct PlatformRevenueSummary {
    pub total_revenue: Decimal,
    pub platform_fees: Decimal,
    pub wheeling_charges: Decimal,
    pub loss_costs: Decimal,
    pub settlement_count: i64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, sqlx::FromRow)]
pub struct RevenueRecord {
    pub id: Uuid,
    pub settlement_id: Uuid,
    pub amount: Decimal,
    pub revenue_type: String,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl MarketClearingService {
    /// Get aggregated platform revenue statistics
    pub async fn get_platform_revenue_summary(&self) -> Result<PlatformRevenueSummary> {
        let row = sqlx::query(
            r#"
            SELECT 
                COALESCE(SUM(amount), 0) as total_revenue,
                COALESCE(SUM(amount) FILTER (WHERE revenue_type = 'platform_fee'), 0) as platform_fees,
                COALESCE(SUM(amount) FILTER (WHERE revenue_type = 'wheeling_charge'), 0) as wheeling_charges,
                COALESCE(SUM(amount) FILTER (WHERE revenue_type = 'loss_cost'), 0) as loss_costs,
                COUNT(DISTINCT settlement_id) as settlement_count
            FROM platform_revenue
            "#
        )
        .fetch_one(&self.db)
        .await?;

        Ok(PlatformRevenueSummary {
            total_revenue: row
                .get::<Option<Decimal>, _>("total_revenue")
                .unwrap_or(Decimal::ZERO),
            platform_fees: row
                .get::<Option<Decimal>, _>("platform_fees")
                .unwrap_or(Decimal::ZERO),
            wheeling_charges: row
                .get::<Option<Decimal>, _>("wheeling_charges")
                .unwrap_or(Decimal::ZERO),
            loss_costs: row
                .get::<Option<Decimal>, _>("loss_costs")
                .unwrap_or(Decimal::ZERO),
            settlement_count: row.get::<Option<i64>, _>("settlement_count").unwrap_or(0),
        })
    }

    /// Get detailed revenue records with pagination
    pub async fn get_revenue_records(&self, limit: i64, offset: i64) -> Result<Vec<RevenueRecord>> {
        let rows = sqlx::query_as::<_, RevenueRecord>(
            r#"
            SELECT id, settlement_id, amount, revenue_type, description, created_at
            FROM platform_revenue
            ORDER BY created_at DESC
            LIMIT $1 OFFSET $2
            "#,
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.db)
        .await?;

        Ok(rows)
    }
}
