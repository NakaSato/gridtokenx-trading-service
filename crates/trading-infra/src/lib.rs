//! `trading-infra` — External integration adapters.
//!
//! Blockchain, Kafka, Redis, audit logging, metrics, and telemetry.

pub mod audit;
pub mod blockchain;
pub mod cache;
pub mod events;
pub mod identity;
pub mod metrics;
pub mod telemetry;

// Re-exports for convenience
pub use audit::{AuditLog, AuditLogger};
pub use blockchain::BlockchainService;
pub use cache::CacheService;
pub use events::{EventBus, KafkaEventBus};
pub use identity::IamIdentityGateway;
pub use telemetry::{init_telemetry, time};
