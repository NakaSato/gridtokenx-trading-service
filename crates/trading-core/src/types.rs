//! Canonical enum types for the GridTokenX trading domain.
//!
//! These are the single source of truth — all other crates import from here.

use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};
use utoipa::ToSchema;

// ── Order Enums ──────────────────────────────────────────────────────────────

#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, sqlx::Type,
    PartialEq, Eq, ToSchema, Display, EnumString,
)]
#[sqlx(type_name = "order_type", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum OrderType {
    Limit,
    Market,
}

impl OrderType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Limit => "limit",
            Self::Market => "market",
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type,
    ToSchema, Display, EnumString,
)]
#[sqlx(type_name = "order_side", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum OrderSide {
    Buy,
    Sell,
}

impl OrderSide {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Buy => "buy",
            Self::Sell => "sell",
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type,
    ToSchema, Display, EnumString,
)]
#[sqlx(type_name = "order_status", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum OrderStatus {
    Pending,
    Active,
    #[sqlx(rename = "partially_filled")]
    #[serde(rename = "partially_filled")]
    PartiallyFilled,
    Filled,
    Settled,
    Cancelled,
    Expired,
}

impl OrderStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Active => "active",
            Self::PartiallyFilled => "partially_filled",
            Self::Filled => "filled",
            Self::Settled => "settled",
            Self::Cancelled => "cancelled",
            Self::Expired => "expired",
        }
    }
}

// ── Time-in-Force ────────────────────────────────────────────────────────────

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type,
    ToSchema, Display, EnumString,
)]
#[sqlx(type_name = "time_in_force", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum TimeInForce {
    /// Good-Til-Cancelled
    Gtc,
    /// Fill-or-Kill
    Fok,
    /// Immediate-or-Cancel
    Ioc,
}

impl Default for TimeInForce {
    fn default() -> Self {
        Self::Gtc
    }
}

// ── Conditional Order Enums ──────────────────────────────────────────────────

/// Type of conditional order trigger
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type,
    ToSchema, Display, EnumString,
)]
#[sqlx(type_name = "trigger_type", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum TriggerType {
    /// Sell when price drops below trigger_price (to limit losses)
    StopLoss,
    /// Sell when price rises above trigger_price (to lock in profits)
    TakeProfit,
    /// Dynamic stop that follows price movements
    TrailingStop,
}

/// Status of a conditional order trigger
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type,
    ToSchema, Display, EnumString,
)]
#[sqlx(type_name = "trigger_status", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum TriggerStatus {
    /// Waiting for trigger condition to be met
    Pending,
    /// Trigger condition met, order executed
    Triggered,
    /// Order cancelled by user
    Cancelled,
    /// Order expired before trigger
    Expired,
}

// ── Recurring Order Enums ────────────────────────────────────────────────────

/// Interval type for recurring orders
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type,
    ToSchema, Display, EnumString,
)]
#[sqlx(type_name = "interval_type", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum IntervalType {
    Hourly,
    Daily,
    Weekly,
    Monthly,
}

/// Status of a recurring order
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type,
    ToSchema, Display, EnumString,
)]
#[sqlx(type_name = "recurring_status", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum RecurringStatus {
    Active,
    Paused,
    Completed,
    Cancelled,
}

// ── Epoch Enums ──────────────────────────────────────────────────────────────

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type,
    ToSchema, Display, EnumString,
)]
#[sqlx(type_name = "epoch_status", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum EpochStatus {
    Pending,
    Active,
    Cleared,
    Settled,
}

// ── User Role ────────────────────────────────────────────────────────────────

#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, sqlx::Type, ToSchema,
    Display, EnumString,
)]
#[sqlx(type_name = "user_role", rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum UserRole {
    User,
    Admin,
    Prosumer,
    Consumer,
    Corporate,
}
