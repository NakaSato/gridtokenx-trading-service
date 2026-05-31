use anyhow::{anyhow, Result};
use solana_sdk::{instruction::Instruction, signature::Signature, transaction::Transaction};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;

use async_trait::async_trait;
use std::fmt::Debug;

use super::chain_v1::{
    chain_bridge_service_client::ChainBridgeServiceClient, GetAccountDataRequest,
    GetAccountDataResponse, GetBalanceRequest, GetBalanceResponse, GetLatestBlockhashRequest,
    GetLatestBlockhashResponse, GetRecentPrioritizationFeesRequest,
    GetRecentPrioritizationFeesResponse, GetSignatureStatusRequest, GetSignatureStatusResponse,
    GetSlotRequest, GetSlotResponse, GetTokenAccountBalanceRequest, GetTokenAccountBalanceResponse,
    GetTransactionDetailsRequest, GetTransactionDetailsResponse, RequestAirdropRequest,
    RequestAirdropResponse, SimulateTransactionRequest, SimulateTransactionResponse,
    SubmitTransactionRequest, SubmitTransactionResponse,
};

use super::metrics::BlockchainMetrics;
use super::priority_fee::{PriorityFeeService, PriorityLevel, TransactionType};
use tracing::{info, warn};

/// Trait to abstract the Chain Bridge gRPC client for testing
#[async_trait]
pub trait ChainBridgeProvider: Send + Sync + Debug {
    async fn submit_transaction(
        &self,
        request: SubmitTransactionRequest,
    ) -> Result<SubmitTransactionResponse>;
    async fn get_latest_blockhash(
        &self,
        request: GetLatestBlockhashRequest,
    ) -> Result<GetLatestBlockhashResponse>;
    async fn get_recent_prioritization_fees(
        &self,
        request: GetRecentPrioritizationFeesRequest,
    ) -> Result<GetRecentPrioritizationFeesResponse>;
    async fn get_signature_status(
        &self,
        request: GetSignatureStatusRequest,
    ) -> Result<GetSignatureStatusResponse>;
    async fn get_slot(&self, request: GetSlotRequest) -> Result<GetSlotResponse>;
    async fn get_balance(&self, request: GetBalanceRequest) -> Result<GetBalanceResponse>;
    async fn get_account_data(
        &self,
        request: GetAccountDataRequest,
    ) -> Result<GetAccountDataResponse>;
    async fn get_token_account_balance(
        &self,
        request: GetTokenAccountBalanceRequest,
    ) -> Result<GetTokenAccountBalanceResponse>;
    async fn simulate_transaction(
        &self,
        request: SimulateTransactionRequest,
    ) -> Result<SimulateTransactionResponse>;
    async fn request_airdrop(
        &self,
        request: RequestAirdropRequest,
    ) -> Result<RequestAirdropResponse>;
    async fn get_transaction_details(
        &self,
        request: GetTransactionDetailsRequest,
    ) -> Result<GetTransactionDetailsResponse>;
}

#[cfg(any(test, feature = "mocks"))]
#[derive(Debug, Default)]
pub struct MockChainBridgeProvider;

#[cfg(any(test, feature = "mocks"))]
#[async_trait]
impl ChainBridgeProvider for MockChainBridgeProvider {
    async fn submit_transaction(
        &self,
        _request: SubmitTransactionRequest,
    ) -> Result<SubmitTransactionResponse> {
        Ok(SubmitTransactionResponse {
            success: true,
            signature: solana_sdk::signature::Signature::from([0u8; 64]).to_string(),
            error_message: String::new(),
            slot: 100,
            ..Default::default()
        })
    }

    async fn get_latest_blockhash(
        &self,
        _request: GetLatestBlockhashRequest,
    ) -> Result<GetLatestBlockhashResponse> {
        Ok(GetLatestBlockhashResponse {
            blockhash: solana_sdk::hash::Hash::default().to_string(),
            last_valid_block_height: 1000,
            ..Default::default()
        })
    }

    async fn get_recent_prioritization_fees(
        &self,
        _request: GetRecentPrioritizationFeesRequest,
    ) -> Result<GetRecentPrioritizationFeesResponse> {
        Ok(GetRecentPrioritizationFeesResponse {
            fees: vec![],
            ..Default::default()
        })
    }

    async fn get_signature_status(
        &self,
        _request: GetSignatureStatusRequest,
    ) -> Result<GetSignatureStatusResponse> {
        Ok(GetSignatureStatusResponse {
            confirmed: true,
            status: "Finalized".to_string(),
            error: "".to_string(),
            ..Default::default()
        })
    }

    async fn get_slot(&self, _request: GetSlotRequest) -> Result<GetSlotResponse> {
        Ok(GetSlotResponse {
            slot: 100,
            ..Default::default()
        })
    }

    async fn get_balance(&self, _request: GetBalanceRequest) -> Result<GetBalanceResponse> {
        Ok(GetBalanceResponse {
            lamports: 1_000_000_000,
            ..Default::default()
        })
    }

    async fn get_account_data(
        &self,
        _request: GetAccountDataRequest,
    ) -> Result<GetAccountDataResponse> {
        Ok(GetAccountDataResponse {
            exists: true,
            data: vec![1, 2, 3],
            ..Default::default()
        })
    }

    async fn get_token_account_balance(
        &self,
        _request: GetTokenAccountBalanceRequest,
    ) -> Result<GetTokenAccountBalanceResponse> {
        Ok(GetTokenAccountBalanceResponse {
            amount: "100".to_string(),
            decimals: 9,
            ui_amount: 100.0,
            ..Default::default()
        })
    }

    async fn simulate_transaction(
        &self,
        _request: SimulateTransactionRequest,
    ) -> Result<SimulateTransactionResponse> {
        Ok(SimulateTransactionResponse {
            success: true,
            logs: vec![],
            ..Default::default()
        })
    }

    async fn request_airdrop(
        &self,
        _request: RequestAirdropRequest,
    ) -> Result<RequestAirdropResponse> {
        Ok(RequestAirdropResponse {
            signature: solana_sdk::signature::Signature::from([0u8; 64]).to_string(),
            ..Default::default()
        })
    }

    async fn get_transaction_details(
        &self,
        _request: GetTransactionDetailsRequest,
    ) -> Result<GetTransactionDetailsResponse> {
        Ok(GetTransactionDetailsResponse {
            slot: 100,
            ..Default::default()
        })
    }
}

/// Production implementation using the actual gRPC client
#[derive(Debug, Clone)]
pub struct RealChainBridgeProvider {
    client: ChainBridgeServiceClient<tonic::transport::Channel>,
}

impl RealChainBridgeProvider {
    pub fn new(client: ChainBridgeServiceClient<tonic::transport::Channel>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ChainBridgeProvider for RealChainBridgeProvider {
    async fn submit_transaction(
        &self,
        request: SubmitTransactionRequest,
    ) -> Result<SubmitTransactionResponse> {
        let mut client = self.client.clone();
        let resp = client
            .submit_transaction(tonic::Request::new(request))
            .await?;
        Ok(resp.into_inner())
    }

    async fn get_latest_blockhash(
        &self,
        request: GetLatestBlockhashRequest,
    ) -> Result<GetLatestBlockhashResponse> {
        let mut client = self.client.clone();
        let resp = client
            .get_latest_blockhash(tonic::Request::new(request))
            .await?;
        Ok(resp.into_inner())
    }

    async fn get_recent_prioritization_fees(
        &self,
        request: GetRecentPrioritizationFeesRequest,
    ) -> Result<GetRecentPrioritizationFeesResponse> {
        let mut client = self.client.clone();
        let resp = client
            .get_recent_prioritization_fees(tonic::Request::new(request))
            .await?;
        Ok(resp.into_inner())
    }

    async fn get_signature_status(
        &self,
        request: GetSignatureStatusRequest,
    ) -> Result<GetSignatureStatusResponse> {
        let mut client = self.client.clone();
        let resp = client
            .get_signature_status(tonic::Request::new(request))
            .await?;
        Ok(resp.into_inner())
    }

    async fn get_slot(&self, request: GetSlotRequest) -> Result<GetSlotResponse> {
        let mut client = self.client.clone();
        let resp = client.get_slot(tonic::Request::new(request)).await?;
        Ok(resp.into_inner())
    }

    async fn get_balance(&self, request: GetBalanceRequest) -> Result<GetBalanceResponse> {
        let mut client = self.client.clone();
        let resp = client.get_balance(tonic::Request::new(request)).await?;
        Ok(resp.into_inner())
    }

    async fn get_account_data(
        &self,
        request: GetAccountDataRequest,
    ) -> Result<GetAccountDataResponse> {
        let mut client = self.client.clone();
        let resp = client
            .get_account_data(tonic::Request::new(request))
            .await?;
        Ok(resp.into_inner())
    }

    async fn get_token_account_balance(
        &self,
        request: GetTokenAccountBalanceRequest,
    ) -> Result<GetTokenAccountBalanceResponse> {
        let mut client = self.client.clone();
        let resp = client
            .get_token_account_balance(tonic::Request::new(request))
            .await?;
        Ok(resp.into_inner())
    }

    async fn simulate_transaction(
        &self,
        request: SimulateTransactionRequest,
    ) -> Result<SimulateTransactionResponse> {
        let mut client = self.client.clone();
        let resp = client
            .simulate_transaction(tonic::Request::new(request))
            .await?;
        Ok(resp.into_inner())
    }

    async fn request_airdrop(
        &self,
        request: RequestAirdropRequest,
    ) -> Result<RequestAirdropResponse> {
        let mut client = self.client.clone();
        let resp = client.request_airdrop(tonic::Request::new(request)).await?;
        Ok(resp.into_inner())
    }

    async fn get_transaction_details(
        &self,
        request: GetTransactionDetailsRequest,
    ) -> Result<GetTransactionDetailsResponse> {
        let mut client = self.client.clone();
        let resp = client
            .get_transaction_details(tonic::Request::new(request))
            .await?;
        Ok(resp.into_inner())
    }
}

/// TransactionHandler handles the submission and confirmation of transactions
#[derive(Clone)]
pub struct TransactionHandler {
    provider: Arc<dyn ChainBridgeProvider>,
    metrics: Arc<dyn BlockchainMetrics>,
    recent_blockhash: Arc<RwLock<Option<CachedBlockhash>>>,
}

struct CachedBlockhash {
    hash: solana_sdk::hash::Hash,
    fetched_at: std::time::Instant,
}

const BLOCKHASH_TTL: std::time::Duration = std::time::Duration::from_secs(30);

impl TransactionHandler {
    pub fn new(
        provider: Arc<dyn ChainBridgeProvider>,
        metrics: Arc<dyn BlockchainMetrics>,
    ) -> Self {
        Self {
            provider,
            metrics,
            recent_blockhash: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn submit_transaction(&self, mut transaction: Transaction) -> Result<Signature> {
        let start_time = std::time::Instant::now();
        let recent_blockhash = self.get_latest_blockhash().await?;
        transaction.message.recent_blockhash = recent_blockhash;

        let serialized_transaction = bincode::serialize(&transaction)?;
        let request = SubmitTransactionRequest {
            serialized_transaction,
            skip_preflight: false,
            retry_count: 3,
            key_id: "platform_admin".to_string(),
            ..Default::default()
        };

        let response = self.provider.submit_transaction(request).await?;

        if !response.success {
            return Err(anyhow!("Transaction failed: {}", response.error_message));
        }

        let sig = Signature::from_str(&response.signature)?;

        self.metrics.track_operation(
            "submit_transaction",
            start_time.elapsed().as_millis() as f64,
            true,
        );
        Ok(sig)
    }

    pub async fn get_latest_blockhash(&self) -> Result<solana_sdk::hash::Hash> {
        {
            let cache = self.recent_blockhash.read().await;
            if let Some(ref c) = *cache {
                if c.fetched_at.elapsed() < BLOCKHASH_TTL {
                    return Ok(c.hash);
                }
            }
        }

        // Cache miss or expired — fetch fresh
        let response = self
            .provider
            .get_latest_blockhash(GetLatestBlockhashRequest {})
            .await?;

        let hash = solana_sdk::hash::Hash::from_str(&response.blockhash)?;
        *self.recent_blockhash.write().await = Some(CachedBlockhash {
            hash,
            fetched_at: std::time::Instant::now(),
        });
        Ok(hash)
    }

    pub async fn add_priority_fee_to_instructions(
        &self,
        instructions: &mut Vec<Instruction>,
        tx_type_str: &'static str,
        priority: Option<PriorityLevel>,
    ) -> Result<()> {
        let tx_type = match tx_type_str {
            "token_minting" | "minting" => TransactionType::TokenMinting,
            "settlement" => TransactionType::Settlement,
            "order_creation" | "create_order" => TransactionType::OrderCreation,
            _ => TransactionType::TokenTransfer,
        };

        let level =
            priority.unwrap_or_else(|| PriorityFeeService::recommend_priority_level(tx_type));

        let account_keys: Vec<String> = instructions
            .iter()
            .flat_map(|i| i.accounts.iter().map(|a| a.pubkey.to_string()))
            .collect();

        let response = self
            .provider
            .get_recent_prioritization_fees(GetRecentPrioritizationFeesRequest {
                account_keys,
                ..Default::default()
            })
            .await?;

        let recent_fees: Vec<super::priority_fee::PrioritizationFeeResult> = response
            .fees
            .into_iter()
            .map(|f| super::priority_fee::PrioritizationFeeResult {
                slot: f.slot,
                prioritization_fee: f.prioritization_fee,
            })
            .collect();
        let fee = PriorityFeeService::calculate_adaptive_fee(&recent_fees, level, tx_type);

        PriorityFeeService::add_priority_fee_raw(
            instructions,
            fee,
            Some(PriorityFeeService::recommend_compute_limit(tx_type)),
            Some(level.description()),
        )
    }

    pub async fn send_and_confirm_transaction(
        &self,
        transaction: &Transaction,
    ) -> Result<Signature> {
        let signature = self.submit_transaction(transaction.clone()).await?;
        info!(
            "⌛ Transaction submitted: {}. Waiting for confirmation...",
            signature
        );

        let start_time = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(60);
        let poll_interval = std::time::Duration::from_secs(1);

        while start_time.elapsed() < timeout {
            match self.get_signature_status(&signature).await {
                Ok(Some(confirmed)) => {
                    if confirmed {
                        info!("✅ Transaction confirmed: {}", signature);
                        return Ok(signature);
                    }
                }
                Ok(None) => {
                    // Not found yet or not confirmed, continue polling
                }
                Err(e) => {
                    warn!(
                        "⚠️ Error checking status for {}: {}. Retrying...",
                        signature, e
                    );
                }
            }
            tokio::time::sleep(poll_interval).await;
        }

        Err(anyhow!(
            "Transaction confirmation timed out after {:?} for {}",
            timeout,
            signature
        ))
    }

    pub async fn confirm_transaction(&self, signature: &str) -> Result<bool> {
        // Placeholder: in the proxy architecture, the bridge handles confirmation.
        // We can query the bridge for status later if needed.
        let response = self
            .provider
            .get_signature_status(super::chain_v1::GetSignatureStatusRequest {
                signature: signature.to_string(),
            })
            .await?;

        Ok(response.confirmed)
    }

    pub async fn get_signature_status(&self, signature: &Signature) -> Result<Option<bool>> {
        let confirmed = self.confirm_transaction(&signature.to_string()).await?;
        Ok(Some(confirmed))
    }

    pub async fn get_slot(&self) -> Result<u64> {
        let response = self
            .provider
            .get_slot(super::chain_v1::GetSlotRequest {})
            .await?;
        Ok(response.slot)
    }

    pub async fn get_balance(&self, request: GetBalanceRequest) -> Result<GetBalanceResponse> {
        self.provider.get_balance(request).await
    }

    pub async fn get_account_data(
        &self,
        request: GetAccountDataRequest,
    ) -> Result<GetAccountDataResponse> {
        self.provider.get_account_data(request).await
    }

    pub async fn get_token_account_balance(
        &self,
        request: GetTokenAccountBalanceRequest,
    ) -> Result<GetTokenAccountBalanceResponse> {
        self.provider.get_token_account_balance(request).await
    }

    pub async fn request_airdrop(
        &self,
        pubkey: &solana_sdk::pubkey::Pubkey,
        lamports: u64,
    ) -> Result<Signature> {
        let request = RequestAirdropRequest {
            pubkey: pubkey.to_string(),
            lamports,
        };
        let response = self.provider.request_airdrop(request).await?;
        if !response.success {
            return Err(anyhow!("Airdrop failed: {}", response.error_message));
        }
        Signature::from_str(&response.signature).map_err(|e| anyhow!("Invalid signature: {}", e))
    }

    pub async fn get_transaction_details(
        &self,
        signature: &str,
    ) -> Result<GetTransactionDetailsResponse> {
        self.provider
            .get_transaction_details(GetTransactionDetailsRequest {
                signature: signature.to_string(),
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::metrics::NoopMetrics;
    use solana_sdk::hash::Hash;
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::signature::{Keypair, Signer};
    use solana_sdk::system_instruction;

    #[tokio::test]
    async fn test_submit_transaction() {
        let provider = Arc::new(MockChainBridgeProvider);
        let metrics = Arc::new(NoopMetrics {});
        let handler = TransactionHandler::new(provider, metrics);

        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let ix = system_instruction::transfer(&from.pubkey(), &to, 1000);
        let tx = Transaction::new_with_payer(&[ix], Some(&from.pubkey()));

        let sig = handler.submit_transaction(tx).await.unwrap();
        assert_eq!(sig, Signature::from([0u8; 64]));
    }

    #[derive(Debug)]
    struct MultiPollMockChainBridgeProvider {
        polls_before_confirm: Arc<tokio::sync::Mutex<u32>>,
    }

    #[async_trait]
    impl ChainBridgeProvider for MultiPollMockChainBridgeProvider {
        async fn submit_transaction(
            &self,
            _request: SubmitTransactionRequest,
        ) -> Result<SubmitTransactionResponse> {
            Ok(SubmitTransactionResponse {
                success: true,
                signature: Signature::from([1u8; 64]).to_string(),
                ..Default::default()
            })
        }
        async fn get_latest_blockhash(
            &self,
            _request: GetLatestBlockhashRequest,
        ) -> Result<GetLatestBlockhashResponse> {
            Ok(GetLatestBlockhashResponse {
                blockhash: Hash::default().to_string(),
                ..Default::default()
            })
        }
        async fn get_recent_prioritization_fees(
            &self,
            _request: GetRecentPrioritizationFeesRequest,
        ) -> Result<GetRecentPrioritizationFeesResponse> {
            Ok(GetRecentPrioritizationFeesResponse::default())
        }
        async fn get_signature_status(
            &self,
            _request: GetSignatureStatusRequest,
        ) -> Result<GetSignatureStatusResponse> {
            let mut polls = self.polls_before_confirm.lock().await;
            if *polls > 0 {
                *polls -= 1;
                Ok(GetSignatureStatusResponse {
                    confirmed: false,
                    ..Default::default()
                })
            } else {
                Ok(GetSignatureStatusResponse {
                    confirmed: true,
                    status: "Finalized".to_string(),
                    ..Default::default()
                })
            }
        }
        async fn get_slot(&self, _request: GetSlotRequest) -> Result<GetSlotResponse> {
            Ok(GetSlotResponse::default())
        }
        async fn get_balance(&self, _request: GetBalanceRequest) -> Result<GetBalanceResponse> {
            Ok(GetBalanceResponse::default())
        }
        async fn get_account_data(
            &self,
            _request: GetAccountDataRequest,
        ) -> Result<GetAccountDataResponse> {
            Ok(GetAccountDataResponse::default())
        }
        async fn get_token_account_balance(
            &self,
            _request: GetTokenAccountBalanceRequest,
        ) -> Result<GetTokenAccountBalanceResponse> {
            Ok(GetTokenAccountBalanceResponse::default())
        }
        async fn simulate_transaction(
            &self,
            _request: SimulateTransactionRequest,
        ) -> Result<SimulateTransactionResponse> {
            Ok(SimulateTransactionResponse {
                success: true,
                ..Default::default()
            })
        }
        async fn request_airdrop(
            &self,
            _request: RequestAirdropRequest,
        ) -> Result<RequestAirdropResponse> {
            Ok(RequestAirdropResponse {
                success: true,
                signature: Signature::from([2u8; 64]).to_string(),
                ..Default::default()
            })
        }
        async fn get_transaction_details(
            &self,
            _request: GetTransactionDetailsRequest,
        ) -> Result<GetTransactionDetailsResponse> {
            Ok(GetTransactionDetailsResponse {
                found: true,
                ..Default::default()
            })
        }
    }

    #[tokio::test]
    async fn test_send_and_confirm_transaction_polling() {
        let provider = Arc::new(MultiPollMockChainBridgeProvider {
            polls_before_confirm: Arc::new(tokio::sync::Mutex::new(2)),
        });
        let metrics = Arc::new(NoopMetrics {});
        let handler = TransactionHandler::new(provider, metrics);

        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let ix = system_instruction::transfer(&from.pubkey(), &to, 1000);
        let tx = Transaction::new_with_payer(&[ix], Some(&from.pubkey()));

        let start = std::time::Instant::now();
        let sig = handler.send_and_confirm_transaction(&tx).await.unwrap();

        assert_eq!(sig, Signature::from([1u8; 64]));
        // Should have taken at least 2 seconds (2 polls)
        assert!(start.elapsed().as_secs() >= 2);
    }

    #[tokio::test]
    async fn test_get_latest_blockhash_caching() {
        let provider = Arc::new(MockChainBridgeProvider);
        let metrics = Arc::new(NoopMetrics {});
        let handler = TransactionHandler::new(provider, metrics);

        // First call should fetch from provider
        let bh1 = handler.get_latest_blockhash().await.unwrap();
        assert_eq!(bh1, Hash::default());

        // Subsequent call should use cache
        let bh2 = handler.get_latest_blockhash().await.unwrap();
        assert_eq!(bh2, Hash::default());
    }
}
