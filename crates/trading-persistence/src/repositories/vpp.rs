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

    async fn get_member_association(&self, meter_id: &str) -> TraitResult<Option<VppMember>> {
        let member = sqlx::query_as::<_, VppMember>(
            r#"SELECT m.id, m.cluster_id, m.meter_id, m.contribution_weight, 
               m.is_active, m.joined_at, met.rated_power_kw, met.rated_capacity_kwh
               FROM vpp_cluster_members m
               JOIN meters met ON m.meter_id = met.serial_number
               WHERE m.meter_id = $1 AND m.is_active = TRUE"#,
        )
        .bind(meter_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(member)
    }

    async fn update_cluster_metrics(
        &self,
        cluster_id: &str,
        stored_kwh: f64,
        soc: f64,
    ) -> TraitResult<()> {
        sqlx::query(
            r#"UPDATE vpp_clusters 
               SET current_stored_kwh = $2,
                   soc_percentage = $3,
                   last_update = NOW()
               WHERE cluster_id = $1"#,
        )
        .bind(cluster_id)
        .bind(stored_kwh)
        .bind(soc)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
