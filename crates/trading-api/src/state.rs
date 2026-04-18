use std::sync::Arc;
use trading_core::traits::{OrderRepository, SettlementRepository, EventPublisher, BlockchainGateway, AuditLog};
use trading_logic::{MatcherService, SettlementService};

#[derive(Clone)]
pub struct AppState {
    pub order_repo: Arc<dyn OrderRepository>,
    pub settlement_repo: Arc<dyn SettlementRepository>,
    pub events: Arc<dyn EventPublisher>,
    pub blockchain: Arc<dyn BlockchainGateway>,
    pub audit: Arc<dyn AuditLog>,
    pub matcher: Arc<MatcherService>,
    pub settlement: Arc<SettlementService>,
}
