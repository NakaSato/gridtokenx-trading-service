use std::sync::Arc;
use trading_core::traits::{
    AnalyticsRepository, AuditLog, BlockchainGateway, CarbonRepository, EventPublisher,
    FuturesRepository, IdentityGateway, OrderRepository, SettlementRepository,
};
use trading_logic::{MatcherService, SettlementService};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<trading_core::config::Config>,
    pub order_repo: Arc<dyn OrderRepository>,
    pub settlement_repo: Arc<dyn SettlementRepository>,
    pub futures_repo: Arc<dyn FuturesRepository>,
    pub carbon_repo: Arc<dyn CarbonRepository>,
    pub analytics_repo: Arc<dyn AnalyticsRepository>,
    pub events: Arc<dyn EventPublisher>,
    pub blockchain: Arc<dyn BlockchainGateway>,
    pub identity: Arc<dyn IdentityGateway>,
    pub audit: Arc<dyn AuditLog>,
    pub matcher: Arc<MatcherService>,
    pub settlement: Arc<SettlementService>,
    pub vpp: Arc<trading_logic::vpp::VppService>,
}
