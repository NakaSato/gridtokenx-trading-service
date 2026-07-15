//! Read-model feed worker (DB-per-service Phase 1).
//!
//! Consumes IAM wallet events and metering meter events off Kafka and projects
//! them into the Trading-owned read-model tables (`iam_wallet_read_model`,
//! `meter_read_model`) via the [`WalletReadModelRepository`] /
//! [`MeterReadModelRepository`] traits. This is the live-update half of the
//! read-model; the boot backfill (snapshotting the source tables) runs once in
//! `builder.rs` before this worker is spawned.
//!
//! It mirrors `trading-infra::events::kafka_consumer::KafkaConsumer`: an rdkafka
//! `StreamConsumer` (`enable.auto.commit=true`, `auto.offset.reset=earliest`)
//! subscribed to the configured topics, an infinite `recv()` loop, and per-message
//! `serde_json` deserialization. It deliberately does NOT deserialize into
//! `trading_core::events::Event` (that enum's JSON differs from IAM's wire shape);
//! it uses a dedicated [`FeedEvent`] envelope instead. The loop never panics —
//! bad payloads and unknown event types are logged and skipped.

use std::sync::Arc;
use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use serde::Deserialize;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use trading_core::traits::{
    MeterReadModelRecord, MeterReadModelRepository, WalletReadModelRecord,
    WalletReadModelRepository,
};

/// Kafka consumer group for the read-model feed. Stable (not ephemeral) so the
/// projection resumes from its committed offset across restarts.
const CONSUMER_GROUP: &str = "trading-read-model-feed";

/// Minimal envelope matching the IAM / meter domain-event wire shape
/// `{id, event_type, timestamp, data, source}`. Only `event_type` + `data`
/// are consumed; the rest are ignored.
#[derive(Debug, Deserialize)]
struct FeedEvent {
    event_type: String,
    #[serde(default)]
    data: serde_json::Value,
}

fn default_true() -> bool {
    true
}

/// `data` payload for `UserWalletLinked` / `UserOnboarded`.
#[derive(Debug, Deserialize)]
struct WalletLinkedData {
    user_id: Uuid,
    wallet_address: String,
    #[serde(default)]
    user_account_pda: Option<String>,
    #[serde(default)]
    shard_id: Option<i16>,
    #[serde(default)]
    is_primary: bool,
    #[serde(default = "default_true")]
    blockchain_registered: bool,
}

/// `data` payload for `UserWalletPrimaryChanged`.
#[derive(Debug, Deserialize)]
struct WalletPrimaryChangedData {
    user_id: Uuid,
    wallet_address: String,
    #[serde(default)]
    is_primary: bool,
}

/// `data` payload for `MeterRegistered` / `MeterUpdated`.
#[derive(Debug, Deserialize)]
struct MeterData {
    serial_number: String,
    meter_id: Uuid,
    user_id: Uuid,
    #[serde(default)]
    zone_id: Option<i32>,
    #[serde(default)]
    status: Option<String>,
}

/// Projects IAM wallet + metering meter events into the local read-model tables.
pub struct ReadModelFeedWorker {
    wallet_repo: Arc<dyn WalletReadModelRepository>,
    meter_repo: Arc<dyn MeterReadModelRepository>,
    brokers: String,
    iam_topic: String,
    meter_topic: String,
}

impl ReadModelFeedWorker {
    pub fn new(
        wallet_repo: Arc<dyn WalletReadModelRepository>,
        meter_repo: Arc<dyn MeterReadModelRepository>,
        brokers: String,
        iam_topic: String,
        meter_topic: String,
    ) -> Self {
        Self {
            wallet_repo,
            meter_repo,
            brokers,
            iam_topic,
            meter_topic,
        }
    }

    /// Build the consumer, subscribe, and stream forever. On a fatal setup error
    /// (bad broker config / subscribe failure) it logs and returns — the process
    /// keeps running with the feed disabled rather than crashing.
    pub async fn run(self) {
        let consumer: StreamConsumer = match ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("group.id", CONSUMER_GROUP)
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest")
            .set("fetch.message.max.bytes", "10485760") // 10MB, mirrors kafka_consumer.rs
            .create()
        {
            Ok(c) => c,
            Err(e) => {
                error!("ReadModelFeedWorker: failed to create Kafka consumer: {e}");
                return;
            }
        };

        let topics = [self.iam_topic.as_str(), self.meter_topic.as_str()];
        if let Err(e) = consumer.subscribe(&topics) {
            error!("ReadModelFeedWorker: failed to subscribe to {topics:?}: {e}");
            return;
        }
        info!("📥 ReadModelFeedWorker streaming on topics: {topics:?}");

        loop {
            match consumer.recv().await {
                Ok(msg) => {
                    if let Some(payload) = msg.payload() {
                        self.handle_payload(payload).await;
                    }
                }
                Err(e) => {
                    error!("ReadModelFeedWorker: Kafka consumer error: {e}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    /// Parse one message and project it. Never panics; every failure is a
    /// log-and-continue so a single poison message cannot stall the feed.
    async fn handle_payload(&self, payload: &[u8]) {
        let event: FeedEvent = match serde_json::from_slice(payload) {
            Ok(e) => e,
            Err(e) => {
                warn!("ReadModelFeedWorker: undecodable event envelope: {e}");
                return;
            }
        };

        match event.event_type.as_str() {
            "UserWalletLinked" | "UserOnboarded" => {
                let data: WalletLinkedData = match serde_json::from_value(event.data) {
                    Ok(d) => d,
                    Err(e) => {
                        warn!(
                            "ReadModelFeedWorker: bad {} payload: {e}",
                            event.event_type
                        );
                        return;
                    }
                };
                let rec = WalletReadModelRecord {
                    user_id: data.user_id,
                    wallet_address: data.wallet_address,
                    is_primary: data.is_primary,
                    blockchain_registered: data.blockchain_registered,
                    user_account_pda: data.user_account_pda,
                    shard_id: data.shard_id,
                };
                if let Err(e) = self.wallet_repo.upsert_wallet(&rec).await {
                    error!("ReadModelFeedWorker: wallet upsert failed: {e}");
                }
            }
            "UserWalletPrimaryChanged" => {
                let data: WalletPrimaryChangedData = match serde_json::from_value(event.data) {
                    Ok(d) => d,
                    Err(e) => {
                        warn!("ReadModelFeedWorker: bad UserWalletPrimaryChanged payload: {e}");
                        return;
                    }
                };
                if let Err(e) = self
                    .wallet_repo
                    .set_wallet_primary(data.user_id, &data.wallet_address, data.is_primary)
                    .await
                {
                    error!("ReadModelFeedWorker: wallet primary update failed: {e}");
                }
            }
            "MeterRegistered" | "MeterUpdated" => {
                let data: MeterData = match serde_json::from_value(event.data) {
                    Ok(d) => d,
                    Err(e) => {
                        warn!(
                            "ReadModelFeedWorker: bad {} payload: {e}",
                            event.event_type
                        );
                        return;
                    }
                };
                let rec = MeterReadModelRecord {
                    serial_number: data.serial_number,
                    meter_id: data.meter_id,
                    user_id: data.user_id,
                    zone_id: data.zone_id,
                    status: data.status,
                };
                if let Err(e) = self.meter_repo.upsert_meter(&rec).await {
                    error!("ReadModelFeedWorker: meter upsert failed: {e}");
                }
            }
            other => {
                debug!("ReadModelFeedWorker: skipping unhandled event_type '{other}'");
            }
        }
    }
}
