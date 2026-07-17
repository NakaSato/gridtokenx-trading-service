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

/// `data` payload for `UserWalletUnlinked` (revocation propagation).
#[derive(Debug, Deserialize)]
struct WalletUnlinkedData {
    user_id: Uuid,
    wallet_address: String,
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

/// The projection decision derived from a parsed [`FeedEvent`], with all I/O
/// (the actual repo upsert) deferred to the caller. Splitting the pure
/// event-type routing + payload parsing out of the async worker keeps it
/// unit-testable without Kafka or a database.
#[derive(Debug)]
enum FeedAction {
    /// Route to `WalletReadModelRepository::upsert_wallet`.
    UpsertWallet(WalletReadModelRecord),
    /// Route to `WalletReadModelRepository::set_wallet_primary`.
    SetWalletPrimary {
        user_id: Uuid,
        wallet_address: String,
        is_primary: bool,
    },
    /// Route to `WalletReadModelRepository::delete_wallet` (revocation).
    DeleteWallet {
        user_id: Uuid,
        wallet_address: String,
    },
    /// Route to `MeterReadModelRepository::upsert_meter`.
    UpsertMeter(MeterReadModelRecord),
    /// A recognised event_type whose `data` failed to parse into the typed
    /// payload — logged and skipped by the caller (carries the original
    /// event_type + serde error text for the warning).
    BadPayload { event_type: String, error: String },
    /// An unrecognised event_type — logged at debug and skipped.
    Skip { event_type: String },
}

/// Pure event router: map a decoded [`FeedEvent`] envelope to the projection
/// [`FeedAction`] the worker should apply. No I/O, no logging — deterministic
/// on its input, so it can be exhaustively unit-tested. Consumes `event`
/// because the typed-payload parse takes ownership of `event.data`.
fn classify(event: FeedEvent) -> FeedAction {
    let FeedEvent { event_type, data } = event;
    match event_type.as_str() {
        "UserWalletLinked" | "UserOnboarded" => match serde_json::from_value::<WalletLinkedData>(data)
        {
            Ok(d) => FeedAction::UpsertWallet(WalletReadModelRecord {
                user_id: d.user_id,
                wallet_address: d.wallet_address,
                is_primary: d.is_primary,
                blockchain_registered: d.blockchain_registered,
                user_account_pda: d.user_account_pda,
                shard_id: d.shard_id,
            }),
            Err(e) => FeedAction::BadPayload {
                event_type,
                error: e.to_string(),
            },
        },
        "UserWalletPrimaryChanged" => {
            match serde_json::from_value::<WalletPrimaryChangedData>(data) {
                Ok(d) => FeedAction::SetWalletPrimary {
                    user_id: d.user_id,
                    wallet_address: d.wallet_address,
                    is_primary: d.is_primary,
                },
                Err(e) => FeedAction::BadPayload {
                    event_type,
                    error: e.to_string(),
                },
            }
        }
        "UserWalletUnlinked" => match serde_json::from_value::<WalletUnlinkedData>(data) {
            Ok(d) => FeedAction::DeleteWallet {
                user_id: d.user_id,
                wallet_address: d.wallet_address,
            },
            Err(e) => FeedAction::BadPayload {
                event_type,
                error: e.to_string(),
            },
        },
        "MeterRegistered" | "MeterUpdated" => match serde_json::from_value::<MeterData>(data) {
            Ok(d) => FeedAction::UpsertMeter(MeterReadModelRecord {
                serial_number: d.serial_number,
                meter_id: d.meter_id,
                user_id: d.user_id,
                zone_id: d.zone_id,
                status: d.status,
            }),
            Err(e) => FeedAction::BadPayload {
                event_type,
                error: e.to_string(),
            },
        },
        _ => FeedAction::Skip { event_type },
    }
}

/// Projects IAM wallet + metering meter events into the local read-model tables.
pub struct ReadModelFeedWorker {
    wallet_repo: Arc<dyn WalletReadModelRepository>,
    meter_repo: Arc<dyn MeterReadModelRepository>,
    brokers: String,
    /// Cluster carrying `meter_topic`. Equals `brokers` unless meter_events lives on
    /// a different Kafka cluster than iam.user.events (see [`run`]).
    meter_brokers: String,
    iam_topic: String,
    meter_topic: String,
}

impl ReadModelFeedWorker {
    pub fn new(
        wallet_repo: Arc<dyn WalletReadModelRepository>,
        meter_repo: Arc<dyn MeterReadModelRepository>,
        brokers: String,
        meter_brokers: String,
        iam_topic: String,
        meter_topic: String,
    ) -> Self {
        Self {
            wallet_repo,
            meter_repo,
            brokers,
            meter_brokers,
            iam_topic,
            meter_topic,
        }
    }

    /// Stream events into the read-model. IAM wallet events (`iam_topic`) and meter
    /// events (`meter_topic`) can live on DIFFERENT Kafka clusters — IAM publishes to
    /// its own broker (kafka-cmd in this deploy, shared with noti), meter-service to
    /// another (kafka-market). When `meter_brokers` names a different cluster than
    /// `brokers`, run a SECOND consumer there for the meter topic so both streams are
    /// consumed from where they actually live; otherwise a single consumer drains
    /// both (original behavior — a no-op when the two brokers match).
    pub async fn run(self) {
        let this = Arc::new(self);
        if this.meter_brokers == this.brokers {
            let topics = vec![this.iam_topic.clone(), this.meter_topic.clone()];
            this.clone()
                .drain(this.brokers.clone(), topics, CONSUMER_GROUP.to_string())
                .await;
            return;
        }
        let iam = tokio::spawn(this.clone().drain(
            this.brokers.clone(),
            vec![this.iam_topic.clone()],
            format!("{CONSUMER_GROUP}-iam"),
        ));
        let meter = tokio::spawn(this.clone().drain(
            this.meter_brokers.clone(),
            vec![this.meter_topic.clone()],
            format!("{CONSUMER_GROUP}-meter"),
        ));
        let _ = tokio::join!(iam, meter);
    }

    /// Build a consumer on `brokers`, subscribe to `topics`, and stream forever. On a
    /// fatal setup error (bad broker config / subscribe failure) it logs and returns —
    /// the process keeps running with that consumer disabled rather than crashing.
    async fn drain(self: Arc<Self>, brokers: String, topics: Vec<String>, group_id: String) {
        let consumer: StreamConsumer = match ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("group.id", &group_id)
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest")
            .set("fetch.message.max.bytes", "10485760") // 10MB, mirrors kafka_consumer.rs
            .create()
        {
            Ok(c) => c,
            Err(e) => {
                error!("ReadModelFeedWorker: failed to create Kafka consumer ({group_id}) on {brokers}: {e}");
                return;
            }
        };

        let topic_refs: Vec<&str> = topics.iter().map(String::as_str).collect();
        if let Err(e) = consumer.subscribe(&topic_refs) {
            error!("ReadModelFeedWorker: failed to subscribe to {topics:?} on {brokers}: {e}");
            return;
        }
        info!("📥 ReadModelFeedWorker streaming on {brokers} topics: {topics:?}");

        loop {
            match consumer.recv().await {
                Ok(msg) => {
                    if let Some(payload) = msg.payload() {
                        self.handle_payload(payload).await;
                    }
                }
                Err(e) => {
                    error!("ReadModelFeedWorker: Kafka consumer error on {brokers}: {e}");
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

        match classify(event) {
            FeedAction::UpsertWallet(rec) => {
                if let Err(e) = self.wallet_repo.upsert_wallet(&rec).await {
                    error!("ReadModelFeedWorker: wallet upsert failed: {e}");
                }
            }
            FeedAction::SetWalletPrimary {
                user_id,
                wallet_address,
                is_primary,
            } => {
                if let Err(e) = self
                    .wallet_repo
                    .set_wallet_primary(user_id, &wallet_address, is_primary)
                    .await
                {
                    error!("ReadModelFeedWorker: wallet primary update failed: {e}");
                }
            }
            FeedAction::DeleteWallet {
                user_id,
                wallet_address,
            } => {
                if let Err(e) = self.wallet_repo.delete_wallet(user_id, &wallet_address).await {
                    error!("ReadModelFeedWorker: wallet delete failed: {e}");
                }
            }
            FeedAction::UpsertMeter(rec) => {
                if let Err(e) = self.meter_repo.upsert_meter(&rec).await {
                    error!("ReadModelFeedWorker: meter upsert failed: {e}");
                }
            }
            FeedAction::BadPayload { event_type, error } => {
                warn!("ReadModelFeedWorker: bad {event_type} payload: {error}");
            }
            FeedAction::Skip { event_type } => {
                debug!("ReadModelFeedWorker: skipping unhandled event_type '{event_type}'");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Decode a full IAM/meter wire envelope (`{id, event_type, timestamp,
    /// data, source}`) exactly as `handle_payload` does, then classify it.
    fn classify_envelope(v: serde_json::Value) -> FeedAction {
        let event: FeedEvent =
            serde_json::from_slice(v.to_string().as_bytes()).expect("envelope must decode");
        classify(event)
    }

    // ── Envelope deserialization ─────────────────────────────────────────────

    #[test]
    fn feed_event_ignores_extra_top_level_fields() {
        // The real envelope carries id/timestamp/source that FeedEvent drops.
        let event: FeedEvent = serde_json::from_value(json!({
            "id": "8d6a529c-8d22-4eb2-83f8-cd6860f55e9b",
            "event_type": "UserOnboarded",
            "timestamp": "2026-07-15T12:00:00Z",
            "source": "iam-service",
            "data": { "user_id": Uuid::new_v4(), "wallet_address": "W1", "is_primary": true }
        }))
        .expect("envelope with extra fields must still decode");
        assert_eq!(event.event_type, "UserOnboarded");
    }

    #[test]
    fn feed_event_missing_data_defaults_to_null() {
        // `data` is #[serde(default)] — absent means serde_json::Value::Null,
        // which then fails the typed parse and becomes BadPayload (not a panic).
        let event: FeedEvent =
            serde_json::from_value(json!({ "event_type": "UserOnboarded" })).expect("decode");
        assert!(event.data.is_null());
        assert!(matches!(classify(event), FeedAction::BadPayload { .. }));
    }

    // ── Wallet routing (UserWalletLinked / UserOnboarded) ────────────────────

    #[test]
    fn user_wallet_linked_routes_to_upsert_wallet() {
        let uid = Uuid::new_v4();
        let action = classify_envelope(json!({
            "event_type": "UserWalletLinked",
            "data": {
                "user_id": uid,
                "wallet_address": "Wallet111",
                "user_account_pda": "Pda111",
                "shard_id": 3,
                "is_primary": true,
                "blockchain_registered": true
            }
        }));
        match action {
            FeedAction::UpsertWallet(rec) => {
                assert_eq!(rec.user_id, uid);
                assert_eq!(rec.wallet_address, "Wallet111");
                assert_eq!(rec.user_account_pda.as_deref(), Some("Pda111"));
                assert_eq!(rec.shard_id, Some(3));
                assert!(rec.is_primary);
                assert!(rec.blockchain_registered);
            }
            other => panic!("expected UpsertWallet, got {other:?}"),
        }
    }

    #[test]
    fn user_onboarded_routes_to_upsert_wallet() {
        // UserOnboarded shares the WalletLinkedData payload + upsert action.
        let uid = Uuid::new_v4();
        let action = classify_envelope(json!({
            "event_type": "UserOnboarded",
            "data": { "user_id": uid, "wallet_address": "W2", "is_primary": true }
        }));
        assert!(matches!(action, FeedAction::UpsertWallet(rec) if rec.user_id == uid));
    }

    #[test]
    fn wallet_linked_absent_is_primary_defaults_false() {
        // Documents ACTUAL behavior: is_primary is #[serde(default)] on a bool,
        // so an absent field yields `false` — the wallet is projected as
        // NON-primary. (See reported finding: if the aggregator's rule is that a
        // first/only wallet should be primary, this default under-promotes it.)
        let action = classify_envelope(json!({
            "event_type": "UserWalletLinked",
            "data": { "user_id": Uuid::new_v4(), "wallet_address": "W3" }
        }));
        match action {
            FeedAction::UpsertWallet(rec) => assert!(
                !rec.is_primary,
                "absent is_primary currently defaults to false, not true"
            ),
            other => panic!("expected UpsertWallet, got {other:?}"),
        }
    }

    #[test]
    fn wallet_linked_absent_blockchain_registered_defaults_true() {
        // blockchain_registered uses #[serde(default = "default_true")].
        let action = classify_envelope(json!({
            "event_type": "UserWalletLinked",
            "data": { "user_id": Uuid::new_v4(), "wallet_address": "W4", "is_primary": true }
        }));
        match action {
            FeedAction::UpsertWallet(rec) => assert!(
                rec.blockchain_registered,
                "absent blockchain_registered must default to true"
            ),
            other => panic!("expected UpsertWallet, got {other:?}"),
        }
    }

    #[test]
    fn wallet_linked_absent_optionals_are_none() {
        let action = classify_envelope(json!({
            "event_type": "UserWalletLinked",
            "data": { "user_id": Uuid::new_v4(), "wallet_address": "W5", "is_primary": false }
        }));
        match action {
            FeedAction::UpsertWallet(rec) => {
                assert!(rec.user_account_pda.is_none());
                assert!(rec.shard_id.is_none());
            }
            other => panic!("expected UpsertWallet, got {other:?}"),
        }
    }

    #[test]
    fn wallet_linked_missing_required_field_is_bad_payload_not_panic() {
        // wallet_address is required (no serde default) — omitting it must NOT
        // panic; it becomes BadPayload carrying the source event_type.
        let action = classify_envelope(json!({
            "event_type": "UserWalletLinked",
            "data": { "user_id": Uuid::new_v4() }
        }));
        assert!(matches!(
            action,
            FeedAction::BadPayload { event_type, .. } if event_type == "UserWalletLinked"
        ));
    }

    #[test]
    fn wallet_linked_malformed_uuid_is_bad_payload() {
        let action = classify_envelope(json!({
            "event_type": "UserWalletLinked",
            "data": { "user_id": "not-a-uuid", "wallet_address": "W6" }
        }));
        assert!(matches!(action, FeedAction::BadPayload { .. }));
    }

    // ── Primary-changed routing ──────────────────────────────────────────────

    #[test]
    fn user_wallet_primary_changed_routes_to_set_primary() {
        let uid = Uuid::new_v4();
        let action = classify_envelope(json!({
            "event_type": "UserWalletPrimaryChanged",
            "data": { "user_id": uid, "wallet_address": "WP", "is_primary": true }
        }));
        match action {
            FeedAction::SetWalletPrimary {
                user_id,
                wallet_address,
                is_primary,
            } => {
                assert_eq!(user_id, uid);
                assert_eq!(wallet_address, "WP");
                assert!(is_primary);
            }
            other => panic!("expected SetWalletPrimary, got {other:?}"),
        }
    }

    #[test]
    fn primary_changed_absent_is_primary_defaults_false() {
        let action = classify_envelope(json!({
            "event_type": "UserWalletPrimaryChanged",
            "data": { "user_id": Uuid::new_v4(), "wallet_address": "WP2" }
        }));
        assert!(matches!(
            action,
            FeedAction::SetWalletPrimary { is_primary: false, .. }
        ));
    }

    #[test]
    fn primary_changed_missing_wallet_is_bad_payload() {
        let action = classify_envelope(json!({
            "event_type": "UserWalletPrimaryChanged",
            "data": { "user_id": Uuid::new_v4() }
        }));
        assert!(matches!(
            action,
            FeedAction::BadPayload { event_type, .. } if event_type == "UserWalletPrimaryChanged"
        ));
    }

    // ── Unlinked / revocation routing (UserWalletUnlinked) ───────────────────

    #[test]
    fn user_wallet_unlinked_routes_to_delete_wallet() {
        let uid = Uuid::new_v4();
        let action = classify_envelope(json!({
            "event_type": "UserWalletUnlinked",
            "data": { "user_id": uid, "wallet_address": "WGone" }
        }));
        match action {
            FeedAction::DeleteWallet {
                user_id,
                wallet_address,
            } => {
                assert_eq!(user_id, uid);
                assert_eq!(wallet_address, "WGone");
            }
            other => panic!("expected DeleteWallet, got {other:?}"),
        }
    }

    #[test]
    fn unlinked_missing_wallet_is_bad_payload() {
        // wallet_address is required (no serde default) — omitting it must NOT
        // panic; it becomes BadPayload carrying the source event_type.
        let action = classify_envelope(json!({
            "event_type": "UserWalletUnlinked",
            "data": { "user_id": Uuid::new_v4() }
        }));
        assert!(matches!(
            action,
            FeedAction::BadPayload { event_type, .. } if event_type == "UserWalletUnlinked"
        ));
    }

    #[test]
    fn unlinked_malformed_uuid_is_bad_payload() {
        let action = classify_envelope(json!({
            "event_type": "UserWalletUnlinked",
            "data": { "user_id": "not-a-uuid", "wallet_address": "WGone" }
        }));
        assert!(matches!(action, FeedAction::BadPayload { .. }));
    }

    // ── Meter routing (MeterRegistered / MeterUpdated) ───────────────────────

    #[test]
    fn meter_registered_routes_to_upsert_meter() {
        let mid = Uuid::new_v4();
        let uid = Uuid::new_v4();
        let action = classify_envelope(json!({
            "event_type": "MeterRegistered",
            "data": {
                "serial_number": "SN-1",
                "meter_id": mid,
                "user_id": uid,
                "zone_id": 7,
                "status": "active"
            }
        }));
        match action {
            FeedAction::UpsertMeter(rec) => {
                assert_eq!(rec.serial_number, "SN-1");
                assert_eq!(rec.meter_id, mid);
                assert_eq!(rec.user_id, uid);
                assert_eq!(rec.zone_id, Some(7));
                assert_eq!(rec.status.as_deref(), Some("active"));
            }
            other => panic!("expected UpsertMeter, got {other:?}"),
        }
    }

    #[test]
    fn meter_updated_routes_to_upsert_meter_with_optional_defaults() {
        let action = classify_envelope(json!({
            "event_type": "MeterUpdated",
            "data": { "serial_number": "SN-2", "meter_id": Uuid::new_v4(), "user_id": Uuid::new_v4() }
        }));
        match action {
            FeedAction::UpsertMeter(rec) => {
                assert_eq!(rec.serial_number, "SN-2");
                assert!(rec.zone_id.is_none());
                assert!(rec.status.is_none());
            }
            other => panic!("expected UpsertMeter, got {other:?}"),
        }
    }

    #[test]
    fn meter_missing_serial_is_bad_payload_not_panic() {
        let action = classify_envelope(json!({
            "event_type": "MeterRegistered",
            "data": { "meter_id": Uuid::new_v4(), "user_id": Uuid::new_v4() }
        }));
        assert!(matches!(
            action,
            FeedAction::BadPayload { event_type, .. } if event_type == "MeterRegistered"
        ));
    }

    // ── Unknown / skip ───────────────────────────────────────────────────────

    #[test]
    fn unknown_event_type_is_skipped() {
        let action = classify_envelope(json!({
            "event_type": "SomethingElseHappened",
            "data": { "whatever": 1 }
        }));
        assert!(matches!(
            action,
            FeedAction::Skip { event_type } if event_type == "SomethingElseHappened"
        ));
    }

    #[test]
    fn empty_event_type_is_skipped() {
        let action = classify_envelope(json!({ "event_type": "", "data": {} }));
        assert!(matches!(action, FeedAction::Skip { .. }));
    }

    #[test]
    fn unknown_event_type_never_parses_data() {
        // An unknown type must short-circuit to Skip even if `data` is garbage
        // that no typed payload would accept — routing keys on event_type only.
        let action = classify_envelope(json!({
            "event_type": "UnrelatedEvent",
            "data": "this is not even an object"
        }));
        assert!(matches!(action, FeedAction::Skip { .. }));
    }
}
