//! VPP repository implementation.

use async_trait::async_trait;
use sqlx::PgPool;
use trading_core::models::{VppCluster, VppMember};
use trading_core::traits::{TraitResult, VppRepository};

pub struct PostgresVppRepository {
    pool: PgPool,
}

impl PostgresVppRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl VppRepository for PostgresVppRepository {
    async fn get_cluster_by_id(&self, cluster_id: &str) -> TraitResult<Option<VppCluster>> {
        let cluster = sqlx::query_as::<_, VppCluster>(
            r#"SELECT id, cluster_id, zone_id, total_capacity_kwh, current_stored_kwh, 
               soc_percentage, target_soc_percentage, flex_up_kw, flex_down_kw, 
               health_score, resource_count, dispatch_mode, last_update, created_at
               FROM vpp_clusters WHERE cluster_id = $1"#,
        )
        .bind(cluster_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(cluster)
    }

    async fn get_all_clusters(&self) -> TraitResult<Vec<VppCluster>> {
        let clusters = sqlx::query_as::<_, VppCluster>(
            r#"SELECT id, cluster_id, zone_id, total_capacity_kwh, current_stored_kwh, 
               soc_percentage, target_soc_percentage, flex_up_kw, flex_down_kw, 
               health_score, resource_count, dispatch_mode, last_update, created_at
               FROM vpp_clusters"#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(clusters)
    }

    async fn get_member_association(&self, meter_id: &str) -> TraitResult<Option<VppMember>> {
        // TODO(db-split): cross-domain JOIN to metering `meters`. Phase 1 keeps this SQL;
        // at cutover replace `LEFT JOIN meters met ON m.meter_id = met.serial_number` with
        // a JOIN to the Trading-owned `meter_read_model` (fed by meter NATS events + backfill).
        // See migrations/20260715000001_trading_phase1_local_models.sql + docs/db-split-phase1.md.
        let member = sqlx::query_as::<_, VppMember>(
            r#"SELECT m.id, m.cluster_id, m.meter_id, m.contribution_weight,
               m.is_active, m.joined_at, met.rated_power_kw, met.rated_capacity_kwh
               FROM vpp_cluster_members m
               LEFT JOIN meters met ON m.meter_id = met.serial_number
               WHERE m.meter_id = $1 AND m.is_active = TRUE"#,
        )
        .bind(meter_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(member)
    }

    async fn get_cluster_members(&self, cluster_id: &str) -> TraitResult<Vec<VppMember>> {
        // TODO(db-split): cross-domain JOIN to metering `meters`. Phase 1 keeps this SQL;
        // at cutover replace `LEFT JOIN meters met ON m.meter_id = met.serial_number` with
        // a JOIN to the Trading-owned `meter_read_model` (fed by meter NATS events + backfill).
        // See migrations/20260715000001_trading_phase1_local_models.sql + docs/db-split-phase1.md.
        let members = sqlx::query_as::<_, VppMember>(
            r#"SELECT m.id, m.cluster_id, m.meter_id, m.contribution_weight,
               m.is_active, m.joined_at, met.rated_power_kw, met.rated_capacity_kwh
               FROM vpp_cluster_members m
               LEFT JOIN meters met ON m.meter_id = met.serial_number
               WHERE m.cluster_id = $1 AND m.is_active = TRUE"#,
        )
        .bind(cluster_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(members)
    }

    async fn update_cluster_metrics(
        &self,
        cluster_id: &str,
        stored_kwh: f64,
        soc: f64,
        flex_up: f64,
        flex_down: f64,
    ) -> TraitResult<()> {
        sqlx::query(
            r#"UPDATE vpp_clusters 
               SET current_stored_kwh = $2,
                   soc_percentage = $3,
                   flex_up_kw = $4,
                   flex_down_kw = $5,
                   last_update = NOW()
               WHERE cluster_id = $1"#,
        )
        .bind(cluster_id)
        .bind(stored_kwh)
        .bind(soc)
        .bind(flex_up)
        .bind(flex_down)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
