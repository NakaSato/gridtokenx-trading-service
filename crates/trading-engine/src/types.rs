//! Types for the matching engine.

use rust_decimal::Decimal;
use uuid::Uuid;
use trading_core::fast_price::FastPrice;
use trading_core::types::TimeInForce;

/// Lightweight order representation for the matching engine.
/// Optimized for cache locality and fast comparison.
/// Minimal order representation for the matching hot-path.
/// Optimized for cache locality (64 bytes).
#[derive(Debug, Clone, Copy)]
pub struct FastOrder {
    pub id: Uuid,
    pub user_id: Uuid,
    pub price: FastPrice,
    pub energy_amount: Decimal,
    pub filled_amount: Decimal,
    pub zone_id: Option<i32>,
    pub created_at_ns: i64,
    pub expires_at_ns: Option<i64>,
    pub time_in_force: TimeInForce,
    pub metadata_index: usize, // Index into OrderMetadata sidecar
}

/// Metadata not required for the matching hot-path.
#[derive(Debug, Clone)]
pub struct OrderMetadata {
    pub epoch_id: Option<Uuid>,
    pub order_pda: Option<String>,
    pub session_token: Option<String>,
}

impl FastOrder {
    pub fn remaining_amount(&self) -> Decimal {
        self.energy_amount - self.filled_amount
    }

    pub fn is_expired(&self, now_ns: i64) -> bool {
        if let Some(expires_at) = self.expires_at_ns {
            expires_at <= now_ns
        } else {
            false
        }
    }
}

/// Result of a matching cycle.
#[derive(Debug, Clone)]
pub struct MatchResult {
    pub buy_order_id: Uuid,
    pub sell_order_id: Uuid,
    pub match_amount: Decimal,
    pub match_price: Decimal,
    pub total_energy_cost: Decimal,
    pub wheeling_charge: Decimal,
    pub loss_factor: Decimal,
    pub loss_cost: Decimal,
    pub buyer_zone_id: Option<i32>,
    pub seller_zone_id: Option<i32>,
    pub buyer_id: Uuid,
    pub seller_id: Uuid,
    pub epoch_id: Uuid,
    pub buyer_session_token: Option<String>,
    pub seller_session_token: Option<String>,
    pub buyer_order_pda: Option<String>,
    pub seller_order_pda: Option<String>,
}

/// Statistics about a matching cycle.
#[derive(Debug, Clone, Default)]
pub struct CycleStats {
    pub matches_created: usize,
    pub total_volume: Decimal,
}
