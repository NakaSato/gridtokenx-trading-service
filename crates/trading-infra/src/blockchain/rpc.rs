//! Blockchain services module

pub mod service;

// Re-exports
pub use gridtokenx_blockchain_core::rpc::instructions::OffchainOrderPayload;
pub use service::BlockchainService;
