// Database schema definitions will be added here
// This module will contain SQL schema definitions and migrations

pub mod types {
    use serde::{Deserialize, Serialize};
    use std::fmt;
    use utoipa::ToSchema;

    #[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type, ToSchema)]
    #[sqlx(type_name = "user_role", rename_all = "lowercase")]
    pub enum UserRole {
        User,
        Admin,
        Prosumer,
        Consumer,
        Corporate,
    }

    #[derive(Debug, Clone, Copy, Serialize, Deserialize, sqlx::Type, PartialEq, ToSchema)]
    #[sqlx(type_name = "order_type", rename_all = "lowercase")]
    #[serde(rename_all = "lowercase")]
    pub enum OrderType {
        Limit,
        Market,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, sqlx::Type, ToSchema)]
    #[sqlx(type_name = "order_side", rename_all = "lowercase")]
    #[serde(rename_all = "lowercase")]
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

    impl fmt::Display for OrderType {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.as_str())
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

    impl fmt::Display for OrderSide {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.as_str())
        }
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize, sqlx::Type, ToSchema)]
    #[sqlx(type_name = "order_status", rename_all = "snake_case")]
    #[serde(rename_all = "snake_case")]
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

    impl fmt::Display for OrderStatus {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.as_str())
        }
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize, sqlx::Type, ToSchema)]
    #[sqlx(type_name = "epoch_status", rename_all = "snake_case")]
    #[serde(rename_all = "snake_case")]
    pub enum EpochStatus {
        Pending,
        Active,
        Cleared,
        Settled,
    }

    impl fmt::Display for EpochStatus {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                EpochStatus::Pending => write!(f, "pending"),
                EpochStatus::Active => write!(f, "active"),
                EpochStatus::Cleared => write!(f, "cleared"),
                EpochStatus::Settled => write!(f, "settled"),
            }
        }
    }
}
