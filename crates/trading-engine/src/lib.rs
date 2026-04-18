//! `trading-engine` — Pure synchronous CDA matching engine.
//!
//! This crate contains the Continuous Double Auction matching algorithm
//! with **zero async, zero I/O, zero database** dependencies.
//! It is the hottest path in the system and must remain pure and sync.

pub mod engine;
pub mod types;
