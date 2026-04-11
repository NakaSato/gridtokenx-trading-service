use serde::{Deserialize, Serialize};
use uuid::Uuid;
use chrono::{DateTime, Utc};
use sqlx::FromRow;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct VppCluster {
    pub id: Uuid,
    pub cluster_id: String,
    pub zone_id: Option<i32>,
    pub total_capacity_kwh: f64,
    pub current_stored_kwh: f64,
    pub soc_percentage: f64,
    pub target_soc_percentage: f64,
    pub flex_up_kw: f64,
    pub flex_down_kw: f64,
    pub health_score: f64,
    pub resource_count: i32,
    pub dispatch_mode: String,
    pub last_update: Option<DateTime<Utc>>,
    pub created_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct VppMember {
    pub id: Uuid,
    pub cluster_id: String,
    pub meter_id: String,
    pub contribution_weight: Option<f64>,
    pub is_active: Option<bool>,
    pub joined_at: Option<DateTime<Utc>>,
    
    // Joined from meters table
    pub rated_power_kw: Option<f64>,
    pub rated_capacity_kwh: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VppMetricsUpdate {
    pub cluster_id: String,
    pub delta_energy_kwh: f64,
    pub current_power_kw: f64,
}
