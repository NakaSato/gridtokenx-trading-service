//! `trading-api` — HTTP/gRPC API handlers.
//!
//! ConnectRPC trading service implementation split into focused handler modules,
//! REST endpoints, and middleware.

pub mod handlers;
pub mod rest;
pub mod middleware;
pub mod state;
