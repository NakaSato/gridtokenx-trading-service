//! Types for the matching engine.

use rust_decimal::Decimal;
use trading_core::fast_price::FastPrice;
use trading_core::types::TimeInForce;
use uuid::Uuid;

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

use std::sync::Arc;

/// Metadata not required for the matching hot-path.
#[derive(Debug, Clone)]
pub struct OrderMetadata {
    /// Market epoch the order trades in. Non-optional by construction: a match
    /// is written to `settlements`/`order_matches`, whose `epoch_id` is NOT NULL
    /// and FKs to `market_epochs`. Modelling this as `Option` invited the
    /// `unwrap_or_default()` that silently substituted `Uuid::nil()` — never a
    /// real epoch — so every such trade failed its FK and re-matched forever.
    /// `to_fast_orders` drops epoch-less orders before they reach a book.
    pub epoch_id: Uuid,
    pub order_pda: Option<Arc<str>>,
    pub session_token: Option<Arc<str>>,
}

impl FastOrder {
    #[must_use]
    pub fn remaining_amount(&self) -> Decimal {
        self.energy_amount - self.filled_amount
    }

    #[must_use]
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
    pub buy_metadata_index: usize,
    pub sell_metadata_index: usize,
    pub match_amount: Decimal,
    pub match_price: Decimal,
    /// The price the trade actually settles/records at — what `persist_matches`
    /// writes to the settlement, `order_matches` ledger, and `OrderMatched` event.
    /// The two markets differ, so each engine sets it explicitly:
    /// - **CDA** (`engine.rs`): the seller's **ask**. The intra-zone discount pulls
    ///   `match_price` (landed cost) below the ask, but on-chain settlement moves
    ///   `amount * ask` from escrow to the seller, so the ask is what reconciles.
    /// - **Uniform auction** (`uniform_auction.rs`): the **clearing price** `p_star`.
    ///   It is `>= ask`, so the on-chain `price >= sell_ask` guard still holds and
    ///   the seller receives the auction outcome, not just the ask.
    pub settle_price: Decimal,
    /// Seller's limit (ask) price. The intra-zone discount can pull `match_price`
    /// (the buyer's landed cost) below this; on-chain settlement executes at
    /// [`Self::settle_price`] (`>= ask`), so the on-chain `price >= sell_ask` guard
    /// holds. See `clearing_support::persist_matches`.
    pub seller_price: Decimal,
    /// Buyer's limit (bid) price. When `seller_price > buyer_price` the match only
    /// crossed because the discount rescued an above-bid local sell — no price
    /// satisfies `sell_ask <= price <= buy_bid`, so such matches are not settled
    /// on-chain.
    pub buyer_price: Decimal,
    pub total_energy_cost: Decimal,
    pub wheeling_charge: Decimal,
    pub loss_factor: Decimal,
    pub loss_cost: Decimal,
    pub buyer_zone_id: Option<i32>,
    pub seller_zone_id: Option<i32>,
    pub buyer_id: Uuid,
    pub seller_id: Uuid,
    pub epoch_id: Uuid,
}

/// Statistics about a matching cycle.
#[derive(Debug, Clone, Default)]
pub struct CycleStats {
    pub matches_created: usize,
    pub total_volume: Decimal,
    /// Ids of Immediate-or-Cancel orders whose unfilled remainder must be
    /// cancelled by the caller after this cycle. IOC fills whatever crosses in
    /// the single matching pass; anything left over never rests in the book, so
    /// the engine reports it here and the orchestrator marks it `Cancelled`.
    pub ioc_cancellations: Vec<Uuid>,
}
