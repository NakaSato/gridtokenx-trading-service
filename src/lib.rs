pub mod api;
pub mod core;
pub mod domain;
pub mod infra;
pub mod services;
pub mod startup;
pub mod metrics;
pub mod telemetry;
pub mod utils;

pub mod trading_proto {
    include!(concat!(env!("OUT_DIR"), "/_trading_include.rs"));
    pub use trading::*;
}
