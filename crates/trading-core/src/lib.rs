//! `trading-core` — Domain primitives, error types, and trait definitions.
//!
//! This is the foundation crate of the GridTokenX trading workspace.
//! Every other crate depends on this one. It contains:
//!
//! - **`types`** — Canonical enum types (`OrderSide`, `OrderStatus`, `TriggerType`, etc.)
//! - **`models`** — Domain model structs (`TradingOrder`, `Settlement`, `TradeMatch`, etc.)
//! - **`error`** — `ApiError` enum and `Result` type alias
//! - **`config`** — Service configuration and tokenization config
//! - **`fast_price`** — `FastPrice` fixed-point arithmetic for hot-path matching
//! - **`events`** — Domain event definitions
//! - **`numeric`** — Safe numeric conversion utilities
//! - **`traits`** — Repository and service trait definitions for dependency injection

pub mod config;
pub mod error;
pub mod events;
pub mod fast_price;
pub mod models;
pub mod numeric;
pub mod recurring;
pub mod traits;
pub mod types;
