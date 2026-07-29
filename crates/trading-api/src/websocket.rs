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
    pub zone_id: i32,
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
    zone_id: i32,
    /// Sequence the stream has reached; everything before it is unreliable.
    seq: u64,
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
}

impl ZoneHub {
    #[must_use]
    pub fn new() -> Self {
        Self {
            zones: DashMap::new(),
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

    /// Stamp and broadcast an event. No-op for events without a zone, and for
    /// zones nobody is watching (`send` fails only when there are no receivers).
    pub fn publish(&self, event: &Event) {
        let Some(zone_id) = zone_of(event) else {
            return;
        };
        let zone = self.zone(zone_id);

        let data = match serde_json::to_value(event) {
            Ok(v) => v,
            Err(e) => {
                warn!("🌐 WS: failed to serialize {} event: {}", event.outbox_event_type(), e);
                return;
            }
        };

        // fetch_add returns the previous value; first frame is seq 1, matching
        // the "snapshot at seq 0 then apply 1.." contract.
        let seq = zone.seq.fetch_add(1, Ordering::AcqRel) + 1;

        let frame = MarketFrame {
            kind: event.outbox_event_type(),
            seq,
            zone_id,
            data,
            timestamp: chrono::Utc::now().to_rfc3339(),
        };

        match serde_json::to_string(&frame) {
            Ok(json) => {
                let _ = zone.tx.send(Arc::from(json.as_str()));
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

/// Zone a market event belongs to, or `None` when it carries no zone and
/// therefore no ordering guarantee.
fn zone_of(event: &Event) -> Option<i32> {
    match event {
        Event::OrderCreated(p) => p.zone_id,
        Event::OrderMatched(p) => p.zone_id,
        Event::OrderUpdate { zone_id, .. } | Event::PeakPriceUpdate { zone_id, .. } => *zone_id,
        Event::SettlementRequested(s) => s.buyer_zone_id,
        _ => None,
    }
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
    zone_id: i32,
    user_id: uuid::Uuid,
) {
    let (mut rx, joined_at_seq) = hub.subscribe(zone_id);
    metrics::counter!("trading_ws_connections_opened").increment(1);
    info!(
        "🌐 WS: user {} subscribed to zone {} at seq {}",
        user_id, zone_id, joined_at_seq
    );

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
                            "🌐 WS: user {} lagged {} frames on zone {} — resyncing",
                            user_id, missed, zone_id
                        );
                        let resync = ResyncFrame {
                            kind: "resync",
                            zone_id,
                            seq: hub.current_seq(zone_id),
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
    info!("🌐 WS: user {} left zone {}", user_id, zone_id);
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
        assert_eq!(seq_of(&rx_b.recv().await.expect("frame delivered")), 1, "zone 2 unaffected");
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
