use crate::domain::events::{Event, OrderMatchedPayload};
use anyhow::Result;
use redis::aio::ConnectionManager;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::AsyncCommands;
use metrics::{counter, histogram};
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

pub struct EventPersistenceWorker {
    db: PgPool,
    redis_conn: ConnectionManager,
    stream_name: String,
    consumer_group: String,
    consumer_name: String,
    running: Arc<RwLock<bool>>,
}

impl EventPersistenceWorker {
    pub async fn new(db: PgPool, redis_url: &str, running: Arc<RwLock<bool>>) -> Result<Self> {
        let client = redis::Client::open(redis_url)?;
        let redis_conn = ConnectionManager::new(client).await?;

        let stream_name = std::env::var("EVENT_STREAM_NAME")
            .unwrap_or_else(|_| "gridtokenx:events:v1".to_string());

        let consumer_group = "persistence-worker".to_string();
        let consumer_name = format!("worker-{}", uuid::Uuid::new_v4());

        Ok(Self {
            db,
            redis_conn,
            stream_name,
            consumer_group,
            consumer_name,
            running,
        })
    }

    pub async fn start(&self) -> Result<()> {
        info!(
            "Starting EventPersistenceWorker (Group: {}, Consumer: {})",
            self.consumer_group, self.consumer_name
        );

        // Ensure consumer group exists
        let mut conn = self.redis_conn.clone();
        let _: redis::RedisResult<()> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&self.stream_name)
            .arg(&self.consumer_group)
            .arg("0")
            .arg("MKSTREAM")
            .query_async(&mut conn)
            .await;

        loop {
            // Check if still running
            {
                let is_running = self.running.read().await;
                if !*is_running {
                    break;
                }
            }

            let mut conn = self.redis_conn.clone();
            let opts = StreamReadOptions::default()
                .group(&self.consumer_group, &self.consumer_name)
                .block(5000)
                .count(10);

            let read_result: redis::RedisResult<StreamReadReply> = conn
                .xread_options(&[&self.stream_name], &[">"], &opts)
                .await;

            match read_result {
                Ok(reply) => {
                    for stream in reply.keys {
                        for entry in stream.ids {
                            if let Some(event_json) = entry.map.get("event") {
                                if let Ok(event_str) = redis::from_redis_value::<String>(event_json)
                                {
                                    if let Ok(event) = serde_json::from_str::<Event>(&event_str) {
                                        let start = std::time::Instant::now();
                                        match self.handle_event(event).await {
                                            Ok(_) => {
                                                // Record latency and success
                                                histogram!("event_persistence_latency_seconds").record(start.elapsed().as_secs_f64());
                                                counter!("event_persistence_count", "status" => "success").increment(1);
                                                
                                                // ACK the message
                                                let _: redis::RedisResult<()> = conn
                                                    .xack(
                                                        &self.stream_name,
                                                        &self.consumer_group,
                                                        &[entry.id],
                                                    )
                                                    .await;
                                            }
                                            Err(e) => {
                                                counter!("event_persistence_count", "status" => "error").increment(1);
                                                error!("Error handling event {}: {}", entry.id, e);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    if !e.is_timeout() {
                        error!("Redis xread error: {}", e);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }

        info!("EventPersistenceWorker stopped");
        Ok(())
    }

    async fn handle_event(&self, event: Event) -> Result<()> {
        match event {
            Event::OrderMatched(payload) => self.handle_order_matched(payload).await,
            Event::SettlementRequested(settlement) => {
                self.handle_settlement_requested(settlement).await
            }
            Event::OrderUpdate {
                id,
                filled_amount,
                status,
            } => {
                sqlx::query(
                    "UPDATE trading_orders SET filled_amount = $1, status = $2::order_status, updated_at = NOW() WHERE id = $3"
                )
                .bind(filled_amount)
                .bind(status)
                .bind(id)
                .execute(&self.db)
                .await?;
                Ok(())
            }
            Event::PeakPriceUpdate { id, peak_price } => {
                sqlx::query("UPDATE markets SET peak_price = $1, updated_at = NOW() WHERE id = $2")
                    .bind(peak_price)
                    .bind(id)
                    .execute(&self.db)
                    .await?;
                Ok(())
            }
            Event::TriggerExecution { id, triggered_at } => {
                sqlx::query(
                    "UPDATE trigger_orders SET status = 'executed', triggered_at = $1, updated_at = NOW() WHERE id = $2"
                )
                .bind(triggered_at)
                .bind(id)
                .execute(&self.db)
                .await?;
                Ok(())
            }
            Event::OrderCreated(_) => Ok(()),
            _ => {
                debug!("Event persistence skipped for variant");
                Ok(())
            }
        }
    }

    async fn handle_order_matched(&self, payload: OrderMatchedPayload) -> Result<()> {
        debug!("Persisting OrderMatched event: {}", payload.match_id);

        // 1. Insert Match Record
        sqlx::query(
            r#"
            INSERT INTO order_matches (id, epoch_id, buy_order_id, sell_order_id, matched_amount, match_price, match_time, status, zone_id, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), NOW())
            "#
        )
        .bind(payload.match_id)
        .bind(payload.epoch_id)
        .bind(payload.buy_order_id)
        .bind(payload.sell_order_id)
        .bind(payload.amount)
        .bind(payload.price)
        .bind(payload.timestamp)
        .bind("pending")
        .bind(payload.zone_id)
        .execute(&self.db)
        .await?;

        Ok(())
    }

    async fn handle_settlement_requested(
        &self,
        s: crate::domain::trading::settlement::Settlement,
    ) -> Result<()> {
        debug!("Persisting SettlementRequested event: {}", s.id);

        sqlx::query(
            r#"
            INSERT INTO settlements (
                id, buyer_id, seller_id, buy_order_id, sell_order_id,
                energy_amount, price_per_kwh, total_amount, fee_amount, net_amount, status, created_at,
                wheeling_charge, loss_factor, loss_cost, effective_energy, buyer_zone_id, seller_zone_id, epoch_id,
                buyer_session_token, seller_session_token
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
            "#,
        )
        .bind(s.id)
        .bind(s.buyer_id)
        .bind(s.seller_id)
        .bind(s.buy_order_id)
        .bind(s.sell_order_id)
        .bind(s.energy_amount)
        .bind(s.price)
        .bind(s.total_value)
        .bind(s.fee_amount)
        .bind(s.net_amount)
        .bind(s.status.to_string())
        .bind(s.created_at)
        .bind(s.wheeling_charge)
        .bind(s.loss_factor)
        .bind(s.loss_cost)
        .bind(s.effective_energy)
        .bind(s.buyer_zone_id)
        .bind(s.seller_zone_id)
        .bind(s.epoch_id)
        .bind(&s.buyer_session_token)
        .bind(&s.seller_session_token)
        .execute(&self.db)
        .await?;

        Ok(())
    }

    async fn handle_meter_reading_created(
        &self,
        payload: crate::domain::events::MeterReadingPayload,
    ) -> Result<()> {
        debug!("Persisting MeterReadingCreated event: {}", payload.reading_id);

        let gen = payload.energy_generated.unwrap_or_default();
        let con = payload.energy_consumed.unwrap_or_default();
        let surplus = if gen > con { Some(gen - con) } else { None };
        let deficit = if con > gen { Some(con - gen) } else { None };

        sqlx::query(
            r#"
            INSERT INTO meter_readings (
                id, meter_id, wallet_address, timestamp,
                energy_generated, energy_consumed, surplus_energy, deficit_energy,
                voltage, current, battery_level, temperature, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
            "#,
        )
        .bind(payload.reading_id)
        .bind(payload.meter_id)
        .bind(payload.wallet_address)
        .bind(payload.timestamp)
        .bind(payload.energy_generated)
        .bind(payload.energy_consumed)
        .bind(surplus)
        .bind(deficit)
        .bind(payload.voltage)
        .bind(payload.current)
        .bind(payload.battery_level)
        .bind(payload.temperature)
        .execute(&self.db)
        .await?;

        Ok(())
    }
}
