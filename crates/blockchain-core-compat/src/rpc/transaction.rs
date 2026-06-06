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

/// Tri-state outcome of a signature status query against the bridge.
///
/// The bridge proto returns `confirmed: bool` plus a `status` string and an
/// `error` string. Folding that into `Option<bool>` lost the failure case —
/// a failed tx was indistinguishable from "not confirmed yet", so callers
/// polled out the full timeout instead of failing fast. This enum keeps the
/// three states distinct.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignatureState {
    /// No confirmation yet — unknown to the bridge or below the confirmed
    /// commitment level (e.g. "processed"). Keep polling.
    Pending,
    /// Confirmed or finalized on chain.
    Confirmed,
    /// The bridge reported a transaction error. Terminal — stop polling.
    Failed(String),
}

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
        })
    }

    async fn get_latest_blockhash(
        &self,
        _request: GetLatestBlockhashRequest,
    ) -> Result<GetLatestBlockhashResponse> {
        Ok(GetLatestBlockhashResponse {
            blockhash: solana_sdk::hash::Hash::default().to_string(),
            last_valid_block_height: 1000,
        })
    }

    async fn get_recent_prioritization_fees(
        &self,
        _request: GetRecentPrioritizationFeesRequest,
    ) -> Result<GetRecentPrioritizationFeesResponse> {
        Ok(GetRecentPrioritizationFeesResponse { fees: vec![] })
    }

    async fn get_signature_status(
        &self,
        _request: GetSignatureStatusRequest,
    ) -> Result<GetSignatureStatusResponse> {
        Ok(GetSignatureStatusResponse {
            confirmed: true,
            status: "Finalized".to_string(),
            error: "".to_string(),
        })
    }

    async fn get_slot(&self, _request: GetSlotRequest) -> Result<GetSlotResponse> {
        Ok(GetSlotResponse { slot: 100 })
    }

    async fn get_balance(&self, _request: GetBalanceRequest) -> Result<GetBalanceResponse> {
        Ok(GetBalanceResponse {
            lamports: 1_000_000_000,
        })
    }

    async fn get_account_data(
        &self,
        _request: GetAccountDataRequest,
    ) -> Result<GetAccountDataResponse> {
        Ok(GetAccountDataResponse {
            exists: true,
            data: vec![1, 2, 3],
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
    /// Serializes blockhash fetches so a cold/expired cache triggers exactly one
    /// provider RPC instead of one per concurrent caller (single-flight).
    blockhash_fetch_lock: Arc<tokio::sync::Mutex<()>>,
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
            blockhash_fetch_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub async fn submit_transaction(&self, mut transaction: Transaction) -> Result<Signature> {
        let start_time = std::time::Instant::now();
        // Only stamp a blockhash when the caller left it unset. Overwriting an
        // already-set blockhash here invalidated locally-signed transactions
        // whenever the cached hash rotated between sign and submit (signature
        // was computed over the old blockhash). The bridge-signing path passes a
        // default blockhash and still gets one filled in.
        if transaction.message.recent_blockhash == solana_sdk::hash::Hash::default() {
            transaction.message.recent_blockhash = self.get_latest_blockhash().await?;
        }

        let serialized_transaction = bincode::serialize(&transaction)?;
        let request = SubmitTransactionRequest {
            serialized_transaction,
            skip_preflight: false,
            retry_count: 3,
            key_id: "platform_admin".to_string(),
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
        if let Some(hash) = self.cached_blockhash().await {
            return Ok(hash);
        }

        // Cache miss or expired. Single-flight: hold the fetch lock so concurrent
        // callers don't each hit the provider. Re-check the cache after acquiring
        // — a prior holder may have just refreshed it.
        let _guard = self.blockhash_fetch_lock.lock().await;
        if let Some(hash) = self.cached_blockhash().await {
            return Ok(hash);
        }

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

    /// Returns the cached blockhash if present and within TTL.
    async fn cached_blockhash(&self) -> Option<solana_sdk::hash::Hash> {
        let cache = self.recent_blockhash.read().await;
        cache.as_ref().and_then(|c| {
            (c.fetched_at.elapsed() < BLOCKHASH_TTL).then_some(c.hash)
        })
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
            .get_recent_prioritization_fees(GetRecentPrioritizationFeesRequest { account_keys })
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
            match self.signature_state(&signature).await {
                Ok(SignatureState::Confirmed) => {
                    info!("✅ Transaction confirmed: {}", signature);
                    return Ok(signature);
                }
                Ok(SignatureState::Failed(err)) => {
                    // Terminal: the bridge reported an on-chain error. Stop
                    // polling instead of waiting out the full timeout.
                    return Err(anyhow!(
                        "Transaction {} failed on chain: {}",
                        signature,
                        err
                    ));
                }
                Ok(SignatureState::Pending) => {
                    // Not found yet or below confirmed level — keep polling.
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

        // Surface a bridge-reported failure instead of dropping it — an unconfirmed
        // tx with a populated error is a failure, not a "keep polling" state.
        if !response.error.is_empty() {
            warn!(
                "Signature {} reported error by bridge: {}",
                signature, response.error
            );
        }

        Ok(response.confirmed)
    }

    /// Tri-state signature status from the bridge. Distinguishes "not confirmed
    /// yet" (keep polling) from "failed on chain" (terminal) — the proto
    /// response carries both `confirmed` and a populated `error`, which the old
    /// `Result<Option<bool>>` shape collapsed (a failure looked like "not yet").
    pub async fn signature_state(&self, signature: &Signature) -> Result<SignatureState> {
        let response = self
            .provider
            .get_signature_status(super::chain_v1::GetSignatureStatusRequest {
                signature: signature.to_string(),
            })
            .await?;

        // Error takes precedence: a populated error is terminal even if some
        // bridge also flips `confirmed`.
        if !response.error.is_empty() {
            return Ok(SignatureState::Failed(response.error));
        }
        if response.confirmed {
            return Ok(SignatureState::Confirmed);
        }
        Ok(SignatureState::Pending)
    }

    /// Back-compat shim over [`signature_state`]. Prefer `signature_state` — this
    /// lossy mapping cannot tell `Pending` from `Failed` cleanly. `Some(true)` =
    /// confirmed, `None` = pending, `Some(false)` = failed.
    pub async fn get_signature_status(&self, signature: &Signature) -> Result<Option<bool>> {
        Ok(match self.signature_state(signature).await? {
            SignatureState::Confirmed => Some(true),
            SignatureState::Pending => None,
            SignatureState::Failed(_) => Some(false),
        })
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
    use solana_system_interface::instruction as system_instruction;

    #[tokio::test]
    async fn test_submit_transaction() {
        let provider = Arc::new(MockChainBridgeProvider);
        let metrics = Arc::new(NoopMetrics {});
        let handler = TransactionHandler::new(provider, metrics);

        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let ix = system_instruction::transfer(&from.pubkey(), &to, 1000);
        let tx = Transaction::new_with_payer(&[ix], Some(&from.pubkey()));

        let sig = handler
            .submit_transaction(tx)
            .await
            .expect("transaction submission should succeed");
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
        let sig = handler
            .send_and_confirm_transaction(&tx)
            .await
            .expect("transaction confirmation should succeed");

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
        let bh1 = handler
            .get_latest_blockhash()
            .await
            .expect("first blockhash fetch should succeed");
        assert_eq!(bh1, Hash::default());

        // Subsequent call should use cache
        let bh2 = handler
            .get_latest_blockhash()
            .await
            .expect("cached blockhash fetch should succeed");
        assert_eq!(bh2, Hash::default());
    }

    /// Records the blockhash of every submitted transaction so a test can assert
    /// `submit_transaction` does not clobber a caller-set (signed) blockhash.
    #[derive(Debug, Default)]
    struct CapturingProvider {
        submitted_blockhash: Arc<tokio::sync::Mutex<Option<Hash>>>,
        /// Hash returned by get_latest_blockhash — distinct so an unwanted
        /// overwrite is detectable.
        provider_blockhash: Hash,
        /// Counts provider-level blockhash fetches (for single-flight test).
        blockhash_calls: Arc<std::sync::atomic::AtomicUsize>,
        /// If set, get_signature_status reports this as a terminal error.
        status_error: Option<String>,
        /// If set, request_airdrop reports failure (success=false) carrying this
        /// message; if None, it succeeds with a valid signature.
        airdrop_error: Option<String>,
    }

    #[async_trait]
    impl ChainBridgeProvider for CapturingProvider {
        async fn submit_transaction(
            &self,
            request: SubmitTransactionRequest,
        ) -> Result<SubmitTransactionResponse> {
            let tx: Transaction = bincode::deserialize(&request.serialized_transaction)?;
            *self.submitted_blockhash.lock().await = Some(tx.message.recent_blockhash);
            Ok(SubmitTransactionResponse {
                success: true,
                signature: Signature::from([3u8; 64]).to_string(),
                ..Default::default()
            })
        }
        async fn get_latest_blockhash(
            &self,
            _request: GetLatestBlockhashRequest,
        ) -> Result<GetLatestBlockhashResponse> {
            self.blockhash_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            // Small delay so concurrent callers overlap and would each fetch
            // without single-flight.
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            Ok(GetLatestBlockhashResponse {
                blockhash: self.provider_blockhash.to_string(),
                ..Default::default()
            })
        }
        async fn get_recent_prioritization_fees(
            &self,
            _r: GetRecentPrioritizationFeesRequest,
        ) -> Result<GetRecentPrioritizationFeesResponse> {
            Ok(GetRecentPrioritizationFeesResponse::default())
        }
        async fn get_signature_status(
            &self,
            _r: GetSignatureStatusRequest,
        ) -> Result<GetSignatureStatusResponse> {
            Ok(GetSignatureStatusResponse {
                error: self.status_error.clone().unwrap_or_default(),
                ..Default::default()
            })
        }
        async fn get_slot(&self, _r: GetSlotRequest) -> Result<GetSlotResponse> {
            Ok(GetSlotResponse::default())
        }
        async fn get_balance(&self, _r: GetBalanceRequest) -> Result<GetBalanceResponse> {
            Ok(GetBalanceResponse::default())
        }
        async fn get_account_data(
            &self,
            _r: GetAccountDataRequest,
        ) -> Result<GetAccountDataResponse> {
            Ok(GetAccountDataResponse::default())
        }
        async fn get_token_account_balance(
            &self,
            _r: GetTokenAccountBalanceRequest,
        ) -> Result<GetTokenAccountBalanceResponse> {
            Ok(GetTokenAccountBalanceResponse::default())
        }
        async fn simulate_transaction(
            &self,
            _r: SimulateTransactionRequest,
        ) -> Result<SimulateTransactionResponse> {
            Ok(SimulateTransactionResponse::default())
        }
        async fn request_airdrop(
            &self,
            _r: RequestAirdropRequest,
        ) -> Result<RequestAirdropResponse> {
            match &self.airdrop_error {
                Some(msg) => Ok(RequestAirdropResponse {
                    success: false,
                    error_message: msg.clone(),
                    ..Default::default()
                }),
                None => Ok(RequestAirdropResponse {
                    success: true,
                    signature: Signature::from([4u8; 64]).to_string(),
                    ..Default::default()
                }),
            }
        }
        async fn get_transaction_details(
            &self,
            _r: GetTransactionDetailsRequest,
        ) -> Result<GetTransactionDetailsResponse> {
            Ok(GetTransactionDetailsResponse::default())
        }
    }

    #[tokio::test]
    async fn test_submit_preserves_caller_blockhash() {
        let captured = Arc::new(tokio::sync::Mutex::new(None));
        let caller_bh = Hash::new_from_array([7u8; 32]);
        let provider_bh = Hash::new_from_array([9u8; 32]);
        let provider = Arc::new(CapturingProvider {
            submitted_blockhash: captured.clone(),
            provider_blockhash: provider_bh,
            ..Default::default()
        });
        let handler = TransactionHandler::new(provider, Arc::new(NoopMetrics {}));

        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let ix = system_instruction::transfer(&from.pubkey(), &to, 1000);
        let mut tx = Transaction::new_with_payer(&[ix], Some(&from.pubkey()));
        // Caller signs over its own blockhash (as OnChainManager does).
        tx.sign(&[&from], caller_bh);

        handler
            .submit_transaction(tx)
            .await
            .expect("submit should succeed");

        let submitted = captured.lock().await.expect("a tx should have been submitted");
        // Must be the signed blockhash, not the provider's fresh one.
        assert_eq!(submitted, caller_bh);
        assert_ne!(submitted, provider_bh);
    }

    #[tokio::test]
    async fn test_submit_stamps_blockhash_when_unset() {
        let captured = Arc::new(tokio::sync::Mutex::new(None));
        let provider_bh = Hash::new_from_array([9u8; 32]);
        let provider = Arc::new(CapturingProvider {
            submitted_blockhash: captured.clone(),
            provider_blockhash: provider_bh,
            ..Default::default()
        });
        let handler = TransactionHandler::new(provider, Arc::new(NoopMetrics {}));

        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let ix = system_instruction::transfer(&from.pubkey(), &to, 1000);
        // Unsigned, default blockhash (bridge-signing path) — handler must fill it.
        let tx = Transaction::new_with_payer(&[ix], Some(&from.pubkey()));
        assert_eq!(tx.message.recent_blockhash, Hash::default());

        handler
            .submit_transaction(tx)
            .await
            .expect("submit should succeed");

        let submitted = captured.lock().await.expect("a tx should have been submitted");
        assert_eq!(submitted, provider_bh);
    }

    #[tokio::test]
    async fn test_blockhash_single_flight() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let provider = Arc::new(CapturingProvider {
            provider_blockhash: Hash::new_from_array([5u8; 32]),
            blockhash_calls: calls.clone(),
            ..Default::default()
        });
        let handler = TransactionHandler::new(provider, Arc::new(NoopMetrics {}));

        // 16 concurrent cold-cache fetches must collapse to a single provider RPC.
        let mut set = Vec::new();
        for _ in 0..16 {
            let h = handler.clone();
            set.push(tokio::spawn(async move { h.get_latest_blockhash().await }));
        }
        for j in set {
            j.await.expect("join").expect("blockhash fetch should succeed");
        }

        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_send_and_confirm_fails_fast_on_error() {
        // A bridge-reported error must short-circuit the poll loop — return Err
        // immediately, not poll out the 60s timeout.
        let provider = Arc::new(CapturingProvider {
            status_error: Some("insufficient funds for rent".to_string()),
            ..Default::default()
        });
        let handler = TransactionHandler::new(provider, Arc::new(NoopMetrics {}));

        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let ix = system_instruction::transfer(&from.pubkey(), &to, 1000);
        let tx = Transaction::new_with_payer(&[ix], Some(&from.pubkey()));

        let start = std::time::Instant::now();
        let err = handler
            .send_and_confirm_transaction(&tx)
            .await
            .expect_err("a bridge-reported error must fail the confirmation");

        assert!(
            err.to_string().contains("insufficient funds for rent"),
            "error should surface the bridge message, got: {err}"
        );
        // Must not have polled out the 60s timeout.
        assert!(
            start.elapsed().as_secs() < 5,
            "should fail fast, took {:?}",
            start.elapsed()
        );
    }

    #[tokio::test]
    async fn test_signature_state_maps_proto_fields() {
        // Pending: confirmed=false, no error.
        let pending = Arc::new(CapturingProvider::default());
        let h = TransactionHandler::new(pending, Arc::new(NoopMetrics {}));
        assert_eq!(
            h.signature_state(&Signature::from([1u8; 64]))
                .await
                .expect("status query should succeed"),
            SignatureState::Pending
        );
        // Back-compat shim: Pending -> None.
        assert_eq!(
            h.get_signature_status(&Signature::from([1u8; 64]))
                .await
                .expect("shim should succeed"),
            None
        );

        // Failed: error populated.
        let failed = Arc::new(CapturingProvider {
            status_error: Some("blockhash expired".to_string()),
            ..Default::default()
        });
        let h = TransactionHandler::new(failed, Arc::new(NoopMetrics {}));
        assert_eq!(
            h.signature_state(&Signature::from([1u8; 64]))
                .await
                .expect("status query should succeed"),
            SignatureState::Failed("blockhash expired".to_string())
        );
        // Back-compat shim: Failed -> Some(false).
        assert_eq!(
            h.get_signature_status(&Signature::from([1u8; 64]))
                .await
                .expect("shim should succeed"),
            Some(false)
        );
    }

    #[tokio::test]
    async fn test_signature_state_confirmed() {
        // MultiPollMock with 0 remaining polls confirms immediately.
        let provider = Arc::new(MultiPollMockChainBridgeProvider {
            polls_before_confirm: Arc::new(tokio::sync::Mutex::new(0)),
        });
        let h = TransactionHandler::new(provider, Arc::new(NoopMetrics {}));
        assert_eq!(
            h.signature_state(&Signature::from([1u8; 64]))
                .await
                .expect("status query should succeed"),
            SignatureState::Confirmed
        );
        assert_eq!(
            h.get_signature_status(&Signature::from([1u8; 64]))
                .await
                .expect("shim should succeed"),
            Some(true)
        );
    }

    // P3: request_airdrop must route through the bridge and surface a failure as
    // an error, not the previous silent no-op. Outside dev the bridge rejects
    // the airdrop (success=false) — the caller must see that.
    #[tokio::test]
    async fn test_request_airdrop_failure_surfaces_error() {
        let provider = Arc::new(CapturingProvider {
            airdrop_error: Some("airdrop disabled outside dev".to_string()),
            ..Default::default()
        });
        let h = TransactionHandler::new(provider, Arc::new(NoopMetrics {}));

        let err = h
            .request_airdrop(&Pubkey::new_unique(), 1_000_000_000)
            .await
            .expect_err("a bridge-rejected airdrop must error");
        assert!(
            err.to_string().contains("airdrop disabled outside dev"),
            "error should surface the bridge message, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_request_airdrop_success_returns_signature() {
        let provider = Arc::new(CapturingProvider::default()); // airdrop_error: None
        let h = TransactionHandler::new(provider, Arc::new(NoopMetrics {}));

        let sig = h
            .request_airdrop(&Pubkey::new_unique(), 1_000_000_000)
            .await
            .expect("a successful airdrop should return its signature");
        assert_eq!(sig, Signature::from([4u8; 64]));
    }
}
