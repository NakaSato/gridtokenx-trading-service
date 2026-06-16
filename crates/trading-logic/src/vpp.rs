use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;
use trading_core::models::VppCluster;
use trading_core::traits::{VppRepository, TraitResult, AuditLog, EventPublisher};
use trading_core::events::{Event, VppDispatchedPayload};

/// Service for VPP (Virtual Power Plant) orchestration and optimization.
pub struct VppService {
    repo: Arc<dyn VppRepository>,
    audit: Arc<dyn AuditLog>,
    events: Arc<dyn EventPublisher>,
}

impl VppService {
    pub fn new(
        repo: Arc<dyn VppRepository>,
        audit: Arc<dyn AuditLog>,
        events: Arc<dyn EventPublisher>,
    ) -> Self {
        Self { repo, audit, events }
    }

    /// Get cluster status with aggregated real-time metrics.
    pub async fn get_cluster_status(&self, cluster_id: &str) -> TraitResult<VppCluster> {
        let cluster = self.repo.get_cluster_by_id(cluster_id).await?
            .ok_or_else(|| trading_core::error::ApiError::NotFound(format!("VPP Cluster {} not found", cluster_id)))?;
        
        Ok(cluster)
    }

    /// Optimized multi-objective dispatch for a VPP cluster.
    /// Returns a map of meter_id -> dispatch_kw.
    pub async fn optimize_dispatch(
        &self,
        cluster_id: &str,
        target_kw: f64,
        nodal_prices: Option<HashMap<String, f64>>,
        carbon_intensity: Option<f64>,
    ) -> TraitResult<HashMap<String, f64>> {
        if target_kw == 0.0 {
            return Ok(HashMap::new());
        }

        let cluster = self.get_cluster_status(cluster_id).await?;
        let members = self.repo.get_cluster_members(cluster_id).await?;

        if members.is_empty() {
            return Ok(HashMap::new());
        }

        info!("Optimizing VPP dispatch for {}: target={}kW, members={}", cluster_id, target_kw, members.len());

        // Weights: SOC (30%), Price (40%), Carbon (30%)
        let mut weights = HashMap::new();
        let mut total_weight = 0.0;

        for m in &members {
            // 1. SOC Weight (Simplified: if we don't have per-member real-time SOC, use cluster avg or fallback)
            // In a real system, we'd fetch real-time SOC for each member from Redis.
            let soc_percent = cluster.soc_percentage; // Fallback to cluster average
            let soc_w = if target_kw > 0.0 {
                soc_percent / 100.0 // Prefer high SOC for discharging
            } else {
                (100.0 - soc_percent) / 100.0 // Prefer low SOC for charging
            };

            // 2. Price Weight
            let price = nodal_prices.as_ref().and_then(|p| p.get(&m.meter_id)).copied().unwrap_or(4.0);
            let price_w = if target_kw > 0.0 {
                price / 6.0 // Normalized to max 6 THB
            } else {
                1.0 - (price / 6.0)
            };

            // 3. Carbon Weight
            let intensity = carbon_intensity.unwrap_or(250.0);
            let carbon_w = if target_kw > 0.0 {
                intensity / 500.0 // Displace dirty power
            } else {
                1.0 - (intensity / 500.0) // Soak up clean power
            };

            let contribution_weight = m.contribution_weight.unwrap_or(1.0);
            let weight = (soc_w * 0.3 + price_w * 0.4 + carbon_w * 0.3) * contribution_weight;
            
            weights.insert(m.meter_id.clone(), weight);
            total_weight += weight;
        }

        let mut dispatches = HashMap::new();
        if total_weight <= 0.0 {
            let shared_target = target_kw / members.len() as f64;
            for m in members {
                dispatches.insert(m.meter_id, shared_target);
            }
            return Ok(dispatches);
        }

        for m in members {
            let w = weights.get(&m.meter_id).unwrap_or(&0.0);
            let raw_dispatch = (w / total_weight) * target_kw;
            
            // Clip to member physical limits
            let max_p = m.rated_power_kw.unwrap_or(5.0);
            let final_dispatch = if target_kw > 0.0 {
                raw_dispatch.min(max_p)
            } else {
                raw_dispatch.max(-max_p)
            };
            
            dispatches.insert(m.meter_id, final_dispatch);
        }

        self.audit.log_action(
            Uuid::nil(), // System action
            "vpp_dispatch",
            &format!("Cluster {}: Dispatched {}kW across {} members", cluster_id, target_kw, dispatches.len())
        ).await?;

        // 3. Publish Event
        let event = Event::VppDispatched(VppDispatchedPayload {
            cluster_id: cluster_id.to_string(),
            target_kw,
            members_commanded: dispatches.len(),
            timestamp: gridtokenx_telemetry::time::now(),
        });
        
        if let Err(e) = self.events.publish(event).await {
            tracing::error!("Failed to publish VppDispatched event: {}", e);
        }

        Ok(dispatches)
    }

    /// Synchronize cluster state from real-time telemetry aggregates.
    pub async fn sync_cluster_state(&self, cluster_id: &str) -> TraitResult<()> {
        let members = self.repo.get_cluster_members(cluster_id).await?;
        
        let mut total_capacity = 0.0;
        let mut total_power = 0.0;
        let mut current_stored = 0.0;
        
        for m in members {
            total_capacity += m.rated_capacity_kwh.unwrap_or(10.0);
            total_power += m.rated_power_kw.unwrap_or(5.0);
            // In a real system, we would fetch the latest SOC from Redis here.
            // For now, we simulate an update.
            current_stored += m.rated_capacity_kwh.unwrap_or(10.0) * 0.5; 
        }

        let soc = if total_capacity > 0.0 { (current_stored / total_capacity) * 100.0 } else { 0.0 };

        self.repo.update_cluster_metrics(
            cluster_id,
            current_stored,
            soc,
            total_power, // Simplified: flex_up = total_power
            total_power, // Simplified: flex_down = total_power
        ).await?;

        Ok(())
    }
}
