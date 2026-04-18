//! `trading-persistence` — Database access layer.
//!
//! SQLx repositories implementing the traits defined in `trading-core`.
//! Owns all SQL queries and migration files.

pub mod pool;
pub mod repositories;
