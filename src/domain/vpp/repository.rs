use sqlx::PgPool;
use anyhow::Result;
use crate::domain::vpp::models::{VppCluster, VppMember};

pub struct VppRepository {
    pool: PgPool,
}

impl VppRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn get_cluster_by_id(&self, cluster_id: &str) -> Result<Option<VppCluster>> {
        let cluster = sqlx::query_as!(
            VppCluster,
            r#"SELECT id, cluster_id, zone_id, total_capacity_kwh, current_stored_kwh, 
               soc_percentage, target_soc_percentage, flex_up_kw, flex_down_kw, 
               health_score, resource_count, dispatch_mode, last_update, created_at
               FROM vpp_clusters WHERE cluster_id = $1"#,
            cluster_id
        )
        .fetch_optional(&self.pool)
        .await?;
        
        Ok(cluster)
    }

    pub async fn get_member_association(&self, meter_id: &str) -> Result<Option<VppMember>> {
        let member = sqlx::query_as!(
            VppMember,
            r#"SELECT m.id, m.cluster_id, m.meter_id, m.contribution_weight, 
               m.is_active, m.joined_at, met.rated_power_kw, met.rated_capacity_kwh
               FROM vpp_cluster_members m
               JOIN meters met ON m.meter_id = met.serial_number
               WHERE m.meter_id = $1 AND m.is_active = TRUE"#,
            meter_id
        )
        .fetch_optional(&self.pool)
        .await?;
        
        Ok(member)
    }

    pub async fn update_cluster_metrics(
        &self, 
        cluster_id: &str, 
        delta_stored: f64,
        new_stored: Option<f64>,
        new_soc: Option<f64>
    ) -> Result<()> {
        sqlx::query!(
            r#"UPDATE vpp_clusters 
               SET current_stored_kwh = COALESCE($2, current_stored_kwh + $3),
                   soc_percentage = COALESCE($4, (COALESCE($2, current_stored_kwh + $3) / NULLIF(total_capacity_kwh, 0)) * 100),
                   last_update = NOW()
               WHERE cluster_id = $1"#,
            cluster_id,
            new_stored,
            delta_stored,
            new_soc
        )
        .execute(&self.pool)
        .await?;
        
        Ok(())
    }
}
