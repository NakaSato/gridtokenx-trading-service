//! Market-data WebSocket gateway.
//!
//! Fans Kafka trading events out to browsers as a **sequenced per-zone stream**,
//! so the client can hold an order book built from `snapshot + deltas` and know
//! — rather than guess — when it has missed something.
//!
//! ## Why a hub and not a consumer per socket
//!
//! [`KafkaConsumer`] joins a consumer group. One per connection would make every
//! browser tab a group member and turn each open/close into a partition
//! rebalance. Instead a single pump task ([`spawn_kafka_pump`]) consumes the
//! trading topics once and republishes into per-zone in-process broadcast
//! channels that sockets subscribe to.
//!
//! ## Ordering
//!
//! `KafkaEventBus::route_event` keys every zone-bearing event by `zone_{id}`
//! (`trading-infra/src/events/kafka.rs`), so all events for a zone land on one
//! partition and Kafka delivers them in order. The pump is the only writer per
//! zone, so the `seq` it stamps is a gap-free total order **within that zone**.
//! Across zones there is no ordering, and none is claimed.
//!
//! Events with `zone_id: None` are dropped rather than broadcast: Kafka keys
//! those by their own UUID, scattering them over partitions, so they carry no
//! ordering guarantee and must not be spliced into a sequenced stream.

use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use dashmap::DashMap;
use jsonwebtoken::{decode, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{info, warn};

use crate::state::AppState;
use trading_core::events::Event;

/// Per-zone ring depth. A socket that falls this far behind is resynced rather
/// than buffered — see [`handle_socket`].
const ZONE_CHANNEL_CAPACITY: usize = 512;

#[derive(Deserialize)]
pub struct WsQuery {
    pub token: String,
    /// Zone to subscribe to. Maps 1:1 onto a Kafka partition key.
    ///
    /// Omit it to receive **every** zone. Market-wide views (best bid/ask, trade
    /// history) span zones, and picking one would silently drop the others'
    /// updates. Every frame carries its own `zone_id` and a sequence that is
    /// still per-zone, so an all-zones subscriber tracks one sequence per zone
    /// rather than a single global one — there is no cross-zone order to claim.
    pub zone_id: Option<i32>,
}

#[derive(Deserialize)]
struct Claims {
    sub: String,
}

/// One outbound frame. `seq` is monotonic and gap-free per `zone_id`; the client
/// applies a delta only when `seq == last_seq + 1`.
#[derive(Debug, Clone, Serialize)]
pub struct MarketFrame {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub seq: u64,
    pub zone_id: i32,
    pub data: serde_json::Value,
    pub timestamp: String,
}

/// Control frame telling the client its stream is no longer contiguous and it
/// must refetch the REST snapshot. Correctness over continuity.
#[derive(Debug, Clone, Serialize)]
struct ResyncFrame {
    #[serde(rename = "type")]
    kind: &'static str,
    /// `None` for an all-zones subscriber: the drop spans zones, so every
    /// tracked sequence is suspect, not just one.
    zone_id: Option<i32>,
    /// Sequence the stream has reached; everything before it is unreliable.
    /// `None` alongside a `None` zone, for the same reason.
    seq: Option<u64>,
    reason: &'static str,
}

struct ZoneChannel {
    tx: broadcast::Sender<Arc<str>>,
    seq: AtomicU64,
}

/// Registry of per-zone broadcast channels. Cheap to clone (`Arc` it once into
/// [`AppState`]); zones are created lazily on first publish or subscribe.
pub struct ZoneHub {
    zones: DashMap<i32, Arc<ZoneChannel>>,
    /// Every frame, regardless of zone, for market-wide subscribers. Separate
    /// from the per-zone channels rather than derived from them: zones are
    /// created lazily on first event, so a subscriber that had joined "all the
    /// zones that exist right now" would never see one that appears later.
    firehose: broadcast::Sender<Arc<str>>,
}

impl ZoneHub {
    #[must_use]
    pub fn new() -> Self {
        let (firehose, _) = broadcast::channel(ZONE_CHANNEL_CAPACITY);
        Self {
            zones: DashMap::new(),
            firehose,
        }
    }

    fn zone(&self, zone_id: i32) -> Arc<ZoneChannel> {
        if let Some(existing) = self.zones.get(&zone_id) {
            return Arc::clone(existing.value());
        }
        Arc::clone(
            self.zones
                .entry(zone_id)
                .or_insert_with(|| {
                    let (tx, _) = broadcast::channel(ZONE_CHANNEL_CAPACITY);
                    Arc::new(ZoneChannel {
                        tx,
                        seq: AtomicU64::new(0),
                    })
                })
                .value(),
        )
    }

    /// Sequence most recently stamped for `zone_id`. The REST snapshot reports
    /// this so the client knows where to splice deltas in.
    #[must_use]
    pub fn current_seq(&self, zone_id: i32) -> u64 {
        self.zones
            .get(&zone_id)
            .map_or(0, |z| z.seq.load(Ordering::Acquire))
    }

    #[must_use]
    pub fn subscribe(&self, zone_id: i32) -> (broadcast::Receiver<Arc<str>>, u64) {
        let zone = self.zone(zone_id);
        // Subscribe before reading seq: a frame published in between is
        // delivered to the receiver, so the client sees seq+1 and stays
        // contiguous. The reverse order would drop it and force a resync.
        let rx = zone.tx.subscribe();
        (rx, zone.seq.load(Ordering::Acquire))
    }

    /// Every zone's frames on one receiver, for market-wide views. Frames stay
    /// individually zone-tagged and per-zone sequenced; interleaving across
    /// zones is arbitrary and carries no meaning.
    #[must_use]
    pub fn subscribe_all(&self) -> broadcast::Receiver<Arc<str>> {
        self.firehose.subscribe()
    }

    /// Stamp and broadcast an event. No-op for events without a zone, and for
    /// zones nobody is watching (`send` fails only when there are no receivers).
    pub fn publish(&self, event: &Event) {
        let Some((zone_id, kind)) = routable(event) else {
            return;
        };
        let zone = self.zone(zone_id);

        // `Event` is adjacently tagged (`#[serde(tag = "event_type", content =
        // "data")]`), so serializing it whole yields `{event_type, data}` and the
        // frame would read `frame.data.data.id`. Lift the payload out: the
        // frame's own `type` already names the event, in the wire vocabulary
        // rather than the Rust one, so the nested tag is both redundant and
        // misleading. Clients get `frame.data.id`.
        let data = match serde_json::to_value(event) {
            Ok(mut v) => {
                if v.get("data").is_some() {
                    v["data"].take()
                } else {
                    v
                }
            }
            Err(e) => {
                warn!("🌐 WS: failed to serialize {kind} event: {e}");
                return;
            }
        };

        // fetch_add returns the previous value; first frame is seq 1, matching
        // the "snapshot at seq 0 then apply 1.." contract.
        let seq = zone.seq.fetch_add(1, Ordering::AcqRel) + 1;

        let frame = MarketFrame {
            kind,
            seq,
            zone_id,
            data,
            timestamp: chrono::Utc::now().to_rfc3339(),
        };

        match serde_json::to_string(&frame) {
            Ok(json) => {
                // One allocation shared by both fan-outs. `send` fails only when
                // nobody is listening, which is the common case for a zone with
                // no subscribers — not an error.
                let payload: Arc<str> = Arc::from(json.as_str());
                let _ = zone.tx.send(Arc::clone(&payload));
                let _ = self.firehose.send(payload);
            }
            Err(e) => warn!("🌐 WS: failed to encode frame for zone {}: {}", zone_id, e),
        }
    }
}

impl Default for ZoneHub {
    fn default() -> Self {
        Self::new()
    }
}

/// Zone and browser-facing type name for an event, or `None` when it must not
/// be broadcast — either it carries no zone (and so no ordering guarantee, see
/// the module docs) or it is not part of the market-data contract.
///
/// Zone and wire name are derived together on purpose. They are the two things
/// a frame cannot be built without, so pairing them here makes it impossible to
/// start broadcasting an event while forgetting to name it.
///
/// The names are deliberately **not** `Event::outbox_event_type()`. That tag
/// serves the outbox table and topic routing — internal concerns, free to change
/// with a refactor. These strings are a public contract with browsers, so they
/// live here and change only on purpose.
fn routable(event: &Event) -> Option<(i32, &'static str)> {
    let (zone, kind) = match event {
        Event::OrderCreated(p) => (p.zone_id, "order_created"),
        Event::OrderMatched(p) => (p.zone_id, "order_matched"),
        Event::OrderUpdate { zone_id, .. } => (*zone_id, "order_update"),
        Event::PeakPriceUpdate { zone_id, .. } => (*zone_id, "peak_price_update"),
        Event::SettlementRequested(s) => (s.buyer_zone_id, "settlement_requested"),
        _ => (None, ""),
    };
    zone.map(|z| (z, kind))
}

/// Consume the trading topics once for the whole process and republish into
/// `hub`. Spawn one of these per pod.
pub fn spawn_kafka_pump(
    bootstrap_servers: String,
    topics: Vec<String>,
    group_id: String,
    hub: Arc<ZoneHub>,
) {
    tokio::spawn(async move {
        let consumer = match trading_infra::events::kafka_consumer::KafkaConsumer::new(
            &bootstrap_servers,
            topics.clone(),
            Some(group_id),
        ) {
            Ok(c) => c,
            Err(e) => {
                warn!("🌐 WS: Kafka pump disabled — consumer init failed: {}", e);
                return;
            }
        };

        info!("🌐 WS: market-data pump consuming {:?}", topics);

        let result = consumer
            .stream(move |event| {
                let hub = Arc::clone(&hub);
                async move {
                    hub.publish(&event);
                    Ok(())
                }
            })
            .await;

        // `stream` only returns on subscribe failure; the poll loop is infinite.
        if let Err(e) = result {
            warn!("🌐 WS: market-data pump stopped: {}", e);
        }
    });
}

/// Upgrade handler for `GET /ws/trading?token=<jwt>&zone_id=<i32>`.
///
/// Verifies the JWT itself: APISIX serves this route without the shared plugin
/// config (the plugins break the upgrade handshake, see `apisix.yaml`), so the
/// usual gateway-injected `x-gridtokenx-user-id` header is absent here.
#[allow(clippy::unused_async)]
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(query): Query<WsQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    if state.config.jwt_secret.is_empty() {
        warn!("🌐 WS: rejected upgrade — JWT_SECRET is not configured");
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }

    let key = DecodingKey::from_secret(state.config.jwt_secret.as_bytes());
    let user_id = match decode::<Claims>(&query.token, &key, &Validation::default()) {
        Ok(data) => match uuid::Uuid::parse_str(&data.claims.sub) {
            Ok(id) => id,
            Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
        },
        Err(e) => {
            warn!("🌐 WS: JWT validation failed: {}", e);
            return StatusCode::UNAUTHORIZED.into_response();
        }
    };

    let zone_id = query.zone_id;
    let hub = Arc::clone(&state.ws_hub);
    ws.on_upgrade(move |socket| handle_socket(socket, hub, zone_id, user_id))
}

async fn handle_socket(
    mut socket: WebSocket,
    hub: Arc<ZoneHub>,
    zone_id: Option<i32>,
    user_id: uuid::Uuid,
) {
    let mut rx = if let Some(zone) = zone_id {
        let (rx, joined_at_seq) = hub.subscribe(zone);
        info!(
            "🌐 WS: user {} subscribed to zone {} at seq {}",
            user_id, zone, joined_at_seq
        );
        rx
    } else {
        info!("🌐 WS: user {} subscribed to all zones", user_id);
        hub.subscribe_all()
    };
    metrics::counter!("trading_ws_connections_opened").increment(1);

    loop {
        tokio::select! {
            inbound = socket.recv() => {
                match inbound {
                    // The client sends nothing but close today; subscriptions
                    // are fixed at upgrade time via the query string.
                    Some(Ok(WsMessage::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        warn!("🌐 WS: socket error for user {}: {}", user_id, e);
                        break;
                    }
                }
            }
            outbound = rx.recv() => {
                match outbound {
                    Ok(frame) => {
                        if socket.send(WsMessage::Text(frame.as_ref().into())).await.is_err() {
                            break;
                        }
                    }
                    // The slow-consumer path. Do NOT grow a buffer to catch up:
                    // tell the client its view is stale and let it refetch the
                    // snapshot, which is bounded work.
                    Err(broadcast::error::RecvError::Lagged(missed)) => {
                        metrics::counter!("trading_ws_resync_sent").increment(1);
                        warn!(
                            "🌐 WS: user {} lagged {} frames on {} — resyncing",
                            user_id,
                            missed,
                            zone_id.map_or_else(|| "all zones".to_string(), |z| format!("zone {z}"))
                        );
                        let resync = ResyncFrame {
                            kind: "resync",
                            zone_id,
                            seq: zone_id.map(|z| hub.current_seq(z)),
                            reason: "lagged",
                        };
                        match serde_json::to_string(&resync) {
                            Ok(json) => {
                                if socket.send(WsMessage::Text(json.into())).await.is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                warn!("🌐 WS: failed to encode resync frame: {}", e);
                                break;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }

    metrics::counter!("trading_ws_connections_closed").increment(1);
    info!(
        "🌐 WS: user {} left {}",
        user_id,
        zone_id.map_or_else(|| "all zones".to_string(), |z| format!("zone {z}"))
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use trading_core::events::OrderCreatedPayload;
    use uuid::Uuid;

    fn order_created(zone_id: Option<i32>) -> Event {
        Event::OrderCreated(OrderCreatedPayload {
            id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            order_type: "limit".to_string(),
            side: "buy".to_string(),
            energy_amount: Decimal::from(10),
            price_per_kwh: Decimal::from(3),
            status: "open".to_string(),
            zone_id,
            created_at: Some(chrono::Utc::now()),
        })
    }

    fn seq_of(json: &str) -> u64 {
        serde_json::from_str::<serde_json::Value>(json)
            .expect("frame is json")
            .get("seq")
            .and_then(serde_json::Value::as_u64)
            .expect("frame carries seq")
    }

    /// The core contract the client's gap detection relies on: sequences are
    /// contiguous from 1, per zone.
    #[tokio::test]
    async fn seq_is_contiguous_within_a_zone() {
        let hub = ZoneHub::new();
        let (mut rx, start) = hub.subscribe(7);
        assert_eq!(start, 0, "fresh zone starts at 0");

        for _ in 0..3 {
            hub.publish(&order_created(Some(7)));
        }

        assert_eq!(seq_of(&rx.recv().await.expect("frame delivered")), 1);
        assert_eq!(seq_of(&rx.recv().await.expect("frame delivered")), 2);
        assert_eq!(seq_of(&rx.recv().await.expect("frame delivered")), 3);
        assert_eq!(hub.current_seq(7), 3);
    }

    /// Zones are independent streams — traffic in one must not advance another's
    /// sequence, or subscribers would see phantom gaps.
    #[tokio::test]
    async fn zones_have_independent_sequences() {
        let hub = ZoneHub::new();
        let (mut rx_a, _) = hub.subscribe(1);
        let (mut rx_b, _) = hub.subscribe(2);

        hub.publish(&order_created(Some(1)));
        hub.publish(&order_created(Some(2)));
        hub.publish(&order_created(Some(1)));

        assert_eq!(seq_of(&rx_a.recv().await.expect("frame delivered")), 1);
        assert_eq!(seq_of(&rx_a.recv().await.expect("frame delivered")), 2);
        assert_eq!(
            seq_of(&rx_b.recv().await.expect("frame delivered")),
            1,
            "zone 2 unaffected"
        );
        assert_eq!(hub.current_seq(1), 2);
        assert_eq!(hub.current_seq(2), 1);
    }

    /// Zone-less events carry no Kafka ordering guarantee, so they must never
    /// enter a sequenced stream.
    #[tokio::test]
    async fn zoneless_events_are_not_broadcast() {
        let hub = ZoneHub::new();
        let (mut rx, _) = hub.subscribe(3);

        hub.publish(&order_created(None));
        hub.publish(&order_created(Some(3)));

        // Only the zoned event arrives, and it is seq 1 — the dropped one did
        // not consume a sequence number.
        assert_eq!(seq_of(&rx.recv().await.expect("frame delivered")), 1);
    }

    fn type_of_frame(json: &str) -> String {
        serde_json::from_str::<serde_json::Value>(json)
            .expect("frame is json")
            .get("type")
            .and_then(|v| v.as_str())
            .expect("frame carries type")
            .to_string()
    }

    /// The payload sits directly under `data` — not `data.data`. Clients key
    /// frames by `data.id` / `data.match_id`, so the adjacent tag `Event`
    /// serializes with must not leak into the wire shape.
    #[tokio::test]
    async fn frame_payload_is_not_double_nested() {
        let hub = ZoneHub::new();
        let mut rx = hub.subscribe_all();

        hub.publish(&order_created(Some(1)));
        let json = rx.recv().await.expect("frame delivered");
        let frame: serde_json::Value = serde_json::from_str(&json).expect("frame is json");

        let data = frame.get("data").expect("frame carries data");
        assert!(
            data.get("id").is_some(),
            "payload must be at data.id, got: {data}"
        );
        assert!(
            data.get("data").is_none() && data.get("event_type").is_none(),
            "the adjacent serde tag must not reach the wire: {data}"
        );
    }

    /// These strings are the browser-facing contract — the frontend's
    /// `WebSocketMessageType` union matches on them. Renaming one silently stops
    /// every handler firing, so pin them here rather than letting a refactor of
    /// the internal `Event` enum change them by accident.
    #[tokio::test]
    async fn wire_type_names_are_stable() {
        let hub = ZoneHub::new();
        let mut rx = hub.subscribe_all();

        hub.publish(&order_created(Some(1)));
        assert_eq!(
            type_of_frame(&rx.recv().await.expect("frame delivered")),
            "order_created",
            "must be snake_case wire name, not the Rust variant `OrderCreated`"
        );
    }

    fn zone_of_frame(json: &str) -> i64 {
        serde_json::from_str::<serde_json::Value>(json)
            .expect("frame is json")
            .get("zone_id")
            .and_then(serde_json::Value::as_i64)
            .expect("frame carries zone_id")
    }

    /// The market-wide case: one subscriber sees every zone. Picking a single
    /// zone instead would silently drop the others' updates, which is the bug
    /// this mode exists to prevent.
    #[tokio::test]
    async fn firehose_spans_every_zone() {
        let hub = ZoneHub::new();
        let mut rx = hub.subscribe_all();

        hub.publish(&order_created(Some(0)));
        hub.publish(&order_created(Some(1)));
        hub.publish(&order_created(Some(0)));

        let a = rx.recv().await.expect("frame delivered");
        let b = rx.recv().await.expect("frame delivered");
        let c = rx.recv().await.expect("frame delivered");

        assert_eq!((zone_of_frame(&a), seq_of(&a)), (0, 1));
        assert_eq!(
            (zone_of_frame(&b), seq_of(&b)),
            (1, 1),
            "zone 1 has its own seq"
        );
        assert_eq!((zone_of_frame(&c), seq_of(&c)), (0, 2));
    }

    /// A zone that first appears *after* a subscriber joined must still reach
    /// it — the reason the firehose is its own channel rather than a fan-in over
    /// the zones that happened to exist at subscribe time.
    #[tokio::test]
    async fn firehose_includes_zones_created_later() {
        let hub = ZoneHub::new();
        let mut rx = hub.subscribe_all();

        hub.publish(&order_created(Some(42))); // zone 42 did not exist yet

        let frame = rx.recv().await.expect("frame delivered");
        assert_eq!(zone_of_frame(&frame), 42);
    }

    /// Subscribing to one zone must not leak other zones onto that socket.
    #[tokio::test]
    async fn per_zone_subscription_stays_scoped() {
        let hub = ZoneHub::new();
        let (mut rx, _) = hub.subscribe(5);

        hub.publish(&order_created(Some(6)));
        hub.publish(&order_created(Some(5)));

        let frame = rx.recv().await.expect("frame delivered");
        assert_eq!(zone_of_frame(&frame), 5, "zone 6 must not appear here");
    }

    /// A consumer that overruns the ring gets `Lagged`, which is what drives the
    /// resync frame in `handle_socket`.
    #[tokio::test]
    async fn overrunning_the_ring_reports_lag() {
        let hub = ZoneHub::new();
        let (mut rx, _) = hub.subscribe(9);

        for _ in 0..(ZONE_CHANNEL_CAPACITY + 10) {
            hub.publish(&order_created(Some(9)));
        }

        assert!(
            matches!(rx.recv().await, Err(broadcast::error::RecvError::Lagged(_))),
            "slow consumer must surface Lagged rather than silently skipping"
        );
    }
}
