//! `trading-api` — HTTP/gRPC API handlers.
//!
//! ConnectRPC trading service implementation split into focused handler modules,
//! REST endpoints, and middleware.

pub mod auth;
pub mod handlers;
pub mod middleware;
pub mod rest;
pub mod startup;
pub mod state;
