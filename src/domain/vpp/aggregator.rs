use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use rdkafka::{
    consumer::{StreamConsumer, Consumer},
    config::ClientConfig,
    Message,
};
use tracing::{info, warn, error, debug};
use serde::{Deserialize, Serialize};
use dashmap::DashMap;
use crate::domain::vpp::repository::VppRepository;
use crate::domain::vpp::models::VppMember;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaMeterReading {
    pub meter_id: String,
    pub timestamp: i64,
    pub energy_generated: f64,
    pub energy_consumed: f64,
    pub surplus: f64,
    pub voltage: f64,
    pub verified: bool,
}

pub struct VppAggregator {
    repository: Arc<VppRepository>,
    membership_cache: DashMap<String, VppMember>, // meter_id -> Member info
}

impl VppAggregator {
    pub fn new(repository: Arc<VppRepository>) -> Self {
        Self {
            repository,
            membership_cache: DashMap::new(),
        }
    }

    pub async fn run(
        &self,
        bootstrap_servers: &str,
        topic: &str,
        group_id: &str,
        token: CancellationToken,
    ) -> anyhow::Result<()> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("group.id", group_id)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "latest")
            .create()?;

        consumer.subscribe(&[topic])?;

        info!("🚀 VPP Aggregator started. Consuming from {} (Topic: {})", bootstrap_servers, topic);

        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    info!("🛑 VPP Aggregator shutting down...");
                    break;
                }
                msg = consumer.recv() => {
                    match msg {
                        Ok(borrowed_message) => {
                            let payload = match borrowed_message.payload_view::<str>() {
                                Some(Ok(s)) => s,
                                Some(Err(e)) => {
                                    warn!("Error while deserializing message payload: {:?}", e);
                                    continue;
                                }
                                None => continue,
                            };

                            if let Ok(reading) = serde_json::from_str::<KafkaMeterReading>(payload) {
                                if let Err(e) = self.process_reading(reading).await {
                                    error!("Failed to process VPP reading: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            error!("Kafka error: {}", e);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn process_reading(&self, reading: KafkaMeterReading) -> anyhow::Result<()> {
        // 1. Resolve membership (with cache)
        let member = if let Some(m) = self.membership_cache.get(&reading.meter_id) {
            Some(m.clone())
        } else {
            match self.repository.get_member_association(&reading.meter_id).await? {
                Some(m) => {
                    self.membership_cache.insert(reading.meter_id.clone(), m.clone());
                    Some(m)
                }
                None => None,
            }
        };

        let member = match member {
            Some(m) => m,
            None => return Ok(()), // Not a VPP member, ignore
        };

        // 2. Perform aggregation
        // We use 'surplus' (net generation) to update the virtual storage state of the cluster
        // In a real VPP, this would distinguish between battery storage and solar generation
        let delta_stored = reading.surplus;
        
        debug!("📈 Aggregating reading for VPP cluster {}: meter={}, surplus={}", 
            member.cluster_id, reading.meter_id, delta_stored);

        let start = std::time::Instant::now();
        let res = self.repository.update_cluster_metrics(
            &member.cluster_id,
            delta_stored,
            None,
            None
        ).await;

        let duration = start.elapsed().as_secs_f64() * 1000.0;
        crate::metrics::record_vpp_aggregation(&member.cluster_id, duration, res.is_ok());

        if res.is_ok() {
            // Fetch updated cluster for SOC gauge recording
            if let Ok(Some(cluster)) = self.repository.get_cluster_by_id(&member.cluster_id).await {
                crate::metrics::record_vpp_cluster_soc(&member.cluster_id, cluster.soc_percentage);
            }
        }

        res?;

        Ok(())
    }
}
