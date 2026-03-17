pub mod api;
pub mod core;
pub mod domain;
pub mod infra;
pub mod services;
pub mod startup;

pub mod trading_proto {
    tonic::include_proto!("trading");
}
