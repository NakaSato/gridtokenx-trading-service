use std::sync::Arc;
use trading_core::traits::{
    AnalyticsRepository, AuditLog, BlockchainGateway, CarbonRepository, EventPublisher,
    FuturesRepository, IdentityGateway, MeterRepository, OrderRepository, PriceAlertRepository,
    RecurringOrderRepository, SettlementRepository,
};
use trading_logic::{MatcherService, SettlementService};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<trading_core::config::Config>,
    pub order_repo: Arc<dyn OrderRepository>,
    pub meter_repo: Arc<dyn MeterRepository>,
    pub settlement_repo: Arc<dyn SettlementRepository>,
    pub futures_repo: Arc<dyn FuturesRepository>,
    pub carbon_repo: Arc<dyn CarbonRepository>,
    pub analytics_repo: Arc<dyn AnalyticsRepository>,
    pub price_alert_repo: Arc<dyn PriceAlertRepository>,
    pub recurring_repo: Arc<dyn RecurringOrderRepository>,
    pub events: Arc<dyn EventPublisher>,
    pub blockchain: Arc<dyn BlockchainGateway>,
    pub identity: Arc<dyn IdentityGateway>,
    pub audit: Arc<dyn AuditLog>,
    pub matcher: Arc<MatcherService>,
    pub settlement: Arc<SettlementService>,
    pub vpp: Arc<trading_logic::vpp::VppService>,
    /// Per-zone market-data fan-out backing `GET /ws/trading`. Shared with the
    /// Kafka pump spawned in `main.rs`; inert until that pump runs.
    pub ws_hub: Arc<crate::websocket::ZoneHub>,
}
