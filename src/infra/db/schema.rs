// Database schema definitions will be added here
// This module will contain SQL schema definitions and migrations

pub mod types {
    use serde::{Deserialize, Serialize};
    use utoipa::ToSchema;

    #[derive(
        Debug,
        Clone,
        Serialize,
        Deserialize,
        sqlx::Type,
        ToSchema,
        strum::Display,
        strum::EnumString,
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

    #[derive(
        Debug,
        Clone,
        Copy,
        Serialize,
        Deserialize,
        sqlx::Type,
        PartialEq,
        ToSchema,
        strum::Display,
        strum::EnumString,
    )]
    #[sqlx(type_name = "order_type", rename_all = "lowercase")]
    #[serde(rename_all = "lowercase")]
    #[strum(serialize_all = "lowercase")]
    pub enum OrderType {
        Limit,
        Market,
    }

    #[derive(
        Debug,
        Clone,
        Copy,
        PartialEq,
        Serialize,
        Deserialize,
        sqlx::Type,
        ToSchema,
        strum::Display,
        strum::EnumString,
    )]
    #[sqlx(type_name = "order_side", rename_all = "lowercase")]
    #[serde(rename_all = "lowercase")]
    #[strum(serialize_all = "lowercase")]
    pub enum OrderSide {
        Buy,
        Sell,
    }

    impl OrderType {
        pub fn as_str(&self) -> &'static str {
            match self {
                OrderType::Limit => "limit",
                OrderType::Market => "market",
            }
        }
    }

    impl OrderSide {
        pub fn as_str(&self) -> &'static str {
            match self {
                OrderSide::Buy => "buy",
                OrderSide::Sell => "sell",
            }
        }
    }

    #[derive(
        Debug,
        Clone,
        PartialEq,
        Serialize,
        Deserialize,
        sqlx::Type,
        ToSchema,
        strum::Display,
        strum::EnumString,
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
                OrderStatus::Pending => "pending",
                OrderStatus::Active => "active",
                OrderStatus::PartiallyFilled => "partially_filled",
                OrderStatus::Filled => "filled",
                OrderStatus::Settled => "settled",
                OrderStatus::Cancelled => "cancelled",
                OrderStatus::Expired => "expired",
            }
        }
    }

    #[derive(
        Debug,
        Clone,
        PartialEq,
        Serialize,
        Deserialize,
        sqlx::Type,
        ToSchema,
        strum::Display,
        strum::EnumString,
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

    #[derive(
        Debug,
        Clone,
        Copy,
        PartialEq,
        Eq,
        Serialize,
        Deserialize,
        sqlx::Type,
        ToSchema,
        strum::Display,
        strum::EnumString,
    )]
    #[sqlx(type_name = "trigger_type", rename_all = "snake_case")]
    #[serde(rename_all = "snake_case")]
    #[strum(serialize_all = "snake_case")]
    pub enum TriggerType {
        StopLoss,
        TakeProfit,
        TrailingStop,
    }

    #[derive(
        Debug,
        Clone,
        Copy,
        PartialEq,
        Eq,
        Serialize,
        Deserialize,
        sqlx::Type,
        ToSchema,
        strum::Display,
        strum::EnumString,
    )]
    #[sqlx(type_name = "trigger_status", rename_all = "snake_case")]
    #[serde(rename_all = "snake_case")]
    #[strum(serialize_all = "snake_case")]
    pub enum TriggerStatus {
        Pending,
        Triggered,
        Cancelled,
        Expired,
    }

    #[derive(
        Debug,
        Clone,
        Copy,
        PartialEq,
        Eq,
        Serialize,
        Deserialize,
        sqlx::Type,
        ToSchema,
        strum::Display,
        strum::EnumString,
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

    #[derive(
        Debug,
        Clone,
        Copy,
        PartialEq,
        Eq,
        Serialize,
        Deserialize,
        sqlx::Type,
        ToSchema,
        strum::Display,
        strum::EnumString,
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

    #[derive(
        Debug,
        Clone,
        Copy,
        PartialEq,
        Eq,
        Serialize,
        Deserialize,
        sqlx::Type,
        ToSchema,
        strum::Display,
        strum::EnumString,
    )]
    #[sqlx(type_name = "time_in_force", rename_all = "lowercase")]
    #[serde(rename_all = "lowercase")]
    #[strum(serialize_all = "lowercase")]
    pub enum TimeInForce {
        Gtc, // Good-Til-Cancelled
        Fok, // Fill-or-Kill
        Ioc, // Immediate-or-Cancel
    }

    impl Default for TimeInForce {
        fn default() -> Self {
            Self::Gtc
        }
    }
}
