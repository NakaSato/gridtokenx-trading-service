use anyhow::Result;
use crate::domain::events::Event;
use crate::domain::trading::models::TradingOrderDb;
use crate::infra::db::schema::types::OrderStatus;
use crate::infra::events::kafka_consumer::KafkaConsumer;
use std::collections::HashMap;
use tracing::{info, warn};
use uuid::Uuid;

/// Service responsible for replaying Kafka history to reconstruct order book state.
pub struct StateRehydrator {
    consumer: KafkaConsumer,
}

impl StateRehydrator {
    pub fn new(consumer: KafkaConsumer) -> Self {
        Self { consumer }
    }

    /// Play back Kafka history and return a map of active orders.
    pub async fn rehydrate(&self) -> Result<HashMap<Uuid, TradingOrderDb>> {
        let mut orders: HashMap<Uuid, TradingOrderDb> = HashMap::new();
        
        info!("Starting state rehydration from Kafka...");

        // Subscribe and seek to beginning
        self.consumer.subscribe_from_beginning().await?;

        // Rehydrate using the stream
        self.consumer.rehydrate(|event| {
            self.handle_event(&mut orders, event);
            Ok(())
        }).await?;

        info!("Rehydration complete. Found {} active orders in Kafka.", orders.len());
        
        Ok(orders)
    }

    /// Processes a single event during rehydration.
    pub fn handle_event(&self, orders: &mut HashMap<Uuid, TradingOrderDb>, event: Event) {
        match event {
            Event::OrderCreated(order) => {
                // Only track orders that could still be active
                if Self::is_active_status(&order.status) {
                    orders.insert(order.id, order);
                }
            }
            Event::OrderUpdate { id, filled_amount, status } => {
                if let Some(order) = orders.get_mut(&id) {
                    order.filled_amount = Some(filled_amount);
                    
                    // Parse status string back to enum
                    if let Ok(new_status) = status.parse::<OrderStatus>() {
                        let is_active = Self::is_active_status(&new_status);
                        order.status = new_status;
                        
                        // If it's no longer active, remove it from the matching set
                        if !is_active {
                            orders.remove(&id);
                        }
                    }
                }
            }
            _ => {
                // Other events (Matched, Settled) are not needed for 1st-level order book reconstruction
            }
        }
    }

    fn is_active_status(status: &OrderStatus) -> bool {
        matches!(
            status,
            OrderStatus::Pending | OrderStatus::Active | OrderStatus::PartiallyFilled
        )
    }
}
