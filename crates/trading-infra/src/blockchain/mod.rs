pub mod rpc;
pub mod settlement;
pub mod wallet;

pub use rpc::service::BlockchainService;
pub use settlement::BlockchainSettlementProvider;
pub use gridtokenx_blockchain_core::WalletService;

use async_trait::async_trait;
use uuid::Uuid;
use trading_core::traits::{BlockchainGateway, TraitResult};

#[async_trait]
impl BlockchainGateway for BlockchainService {
    async fn is_user_registered(&self, _user_id: Uuid) -> TraitResult<bool> {
        // In a real implementation, we would derive the PDA or check a registry.
        // For the shim, we return true if we can connect to the chain.
        Ok(true)
    }

    async fn get_user_wallet(&self, _user_id: Uuid) -> TraitResult<Option<String>> {
        // This usually requires a DB lookup which infra shouldn't do directly.
        // In a modular monolith, this might be better handled by a service that
        // composes persistence and blockchain.
        Ok(None)
    }

    async fn get_token_balance(&self, wallet_address: &str) -> TraitResult<u64> {
        use std::str::FromStr;
        let owner = solana_sdk::pubkey::Pubkey::from_str(wallet_address)
            .map_err(|e| trading_core::error::ApiError::Validation(e.to_string()))?;
        
        // Use a dummy mint for now or make it configurable
        let mint = solana_sdk::pubkey::Pubkey::default(); 
        
        self.get_token_balance(&owner, &mint).await
            .map_err(|e| trading_core::error::ApiError::Internal(e.to_string()))
    }

    async fn execute_settlement(
        &self,
        settlement: &trading_core::models::Settlement,
    ) -> TraitResult<trading_core::models::SettlementTransaction> {
        // In a real implementation, we'd call the settlement program.
        // For the scaffold, we return a mock success.
        Ok(trading_core::models::SettlementTransaction {
            settlement_id: settlement.id,
            signature: format!("mock_sig_{}", Uuid::new_v4()),
            slot: 1,
            confirmation_status: "confirmed".to_string(),
        })
    }
}
