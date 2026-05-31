use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;

use crate::rpc::chain_v1::*;
use crate::rpc::metrics::BlockchainMetrics;
use crate::rpc::nats_schema::{
    TxResultMessage, TxSimulateMessage, TxSimulateResultMessage, TxSubmitMessage,
};
use crate::rpc::transaction::{ChainBridgeProvider, RealChainBridgeProvider};

/// Hybrid provider: writes go through NATS JetStream, reads stay on gRPC.
pub struct NatsChainBridgeProvider {
    nats: async_nats::Client,
    /// SPIFFE URI of this service — included in every published message
    service_identity: String,
    /// Delegates all read RPCs to the existing gRPC provider
    grpc: Arc<RealChainBridgeProvider>,
    /// Metrics collector
    metrics: Arc<dyn BlockchainMetrics>,
    /// How long to wait for a reply before returning an error
    tx_timeout: Duration,
}

impl NatsChainBridgeProvider {
    pub fn new(
        nats: async_nats::Client,
        service_identity: String,
        grpc: Arc<RealChainBridgeProvider>,
        metrics: Arc<dyn BlockchainMetrics>,
        tx_timeout: Duration,
    ) -> Self {
        Self {
            nats,
            service_identity,
            grpc,
            metrics,
            tx_timeout,
        }
    }
}

impl std::fmt::Debug for NatsChainBridgeProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsChainBridgeProvider")
            .field("service_identity", &self.service_identity)
            .field("tx_timeout", &self.tx_timeout)
            .finish()
    }
}

#[async_trait]
impl ChainBridgeProvider for NatsChainBridgeProvider {
    // --- WRITE: goes through NATS ---

    async fn submit_transaction(
        &self,
        request: SubmitTransactionRequest,
    ) -> Result<SubmitTransactionResponse> {
        let correlation_id = uuid::Uuid::new_v4().to_string();
        let reply_subject = format!("chain.tx.result.{}", correlation_id);

        let mut reply_sub = self
            .nats
            .subscribe(reply_subject.clone())
            .await
            .map_err(|e| anyhow!("Failed to subscribe to reply subject: {}", e))?;

        let envelope = TxSubmitMessage {
            correlation_id: correlation_id.clone(),
            reply_subject,
            serialized_tx: request.serialized_transaction,
            key_id: request.key_id,
            skip_preflight: request.skip_preflight,
            retry_count: request.retry_count,
            service_identity: self.service_identity.clone(),
            created_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };

        self.nats
            .publish("chain.tx.submit", serde_json::to_vec(&envelope)?.into())
            .await
            .map_err(|e| anyhow!("Failed to publish to NATS: {}", e))?;

        let start_time = std::time::Instant::now();
        // Await result with timeout
        match tokio::time::timeout(self.tx_timeout, reply_sub.next()).await {
            Ok(Some(msg)) => {
                let result: TxResultMessage = serde_json::from_slice(&msg.payload)?;
                self.metrics.track_operation(
                    "nats_submit_transaction",
                    start_time.elapsed().as_millis() as f64,
                    result.success,
                );
                Ok(SubmitTransactionResponse {
                    success: result.success,
                    signature: result.signature.unwrap_or_default(),
                    error_message: result.error.unwrap_or_default(),
                    slot: result.slot,
                    ..Default::default()
                })
            }
            Ok(None) => {
                self.metrics.track_operation(
                    "nats_submit_transaction",
                    start_time.elapsed().as_millis() as f64,
                    false,
                );
                Err(anyhow!("NATS reply channel closed unexpectedly"))
            }
            Err(_) => {
                self.metrics.track_operation(
                    "nats_submit_transaction",
                    start_time.elapsed().as_millis() as f64,
                    false,
                );
                Err(anyhow!(
                    "submit_transaction timed out after {:?}",
                    self.tx_timeout
                ))
            }
        }
    }

    // --- READS: delegate to gRPC ---

    async fn get_latest_blockhash(
        &self,
        r: GetLatestBlockhashRequest,
    ) -> Result<GetLatestBlockhashResponse> {
        self.grpc.get_latest_blockhash(r).await
    }

    async fn get_balance(&self, r: GetBalanceRequest) -> Result<GetBalanceResponse> {
        self.grpc.get_balance(r).await
    }

    async fn get_account_data(&self, r: GetAccountDataRequest) -> Result<GetAccountDataResponse> {
        self.grpc.get_account_data(r).await
    }

    async fn get_recent_prioritization_fees(
        &self,
        r: GetRecentPrioritizationFeesRequest,
    ) -> Result<GetRecentPrioritizationFeesResponse> {
        self.grpc.get_recent_prioritization_fees(r).await
    }

    async fn get_token_account_balance(
        &self,
        r: GetTokenAccountBalanceRequest,
    ) -> Result<GetTokenAccountBalanceResponse> {
        self.grpc.get_token_account_balance(r).await
    }

    async fn get_signature_status(
        &self,
        r: GetSignatureStatusRequest,
    ) -> Result<GetSignatureStatusResponse> {
        self.grpc.get_signature_status(r).await
    }

    async fn get_slot(&self, r: GetSlotRequest) -> Result<GetSlotResponse> {
        self.grpc.get_slot(r).await
    }

    async fn simulate_transaction(
        &self,
        request: SimulateTransactionRequest,
    ) -> Result<SimulateTransactionResponse> {
        let correlation_id = uuid::Uuid::new_v4().to_string();
        let reply_subject = format!("chain.tx.simulate.result.{}", correlation_id);

        let mut reply_sub = self
            .nats
            .subscribe(reply_subject.clone())
            .await
            .map_err(|e| anyhow!("Failed to subscribe to simulate reply subject: {}", e))?;

        let envelope = TxSimulateMessage {
            correlation_id: correlation_id.clone(),
            reply_subject,
            serialized_tx: request.serialized_transaction,
            key_id: request.key_id,
            service_identity: self.service_identity.clone(),
            created_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };

        self.nats
            .publish("chain.tx.simulate", serde_json::to_vec(&envelope)?.into())
            .await
            .map_err(|e| anyhow!("Failed to publish simulate to NATS: {}", e))?;

        let start_time = std::time::Instant::now();
        // Await result with timeout
        match tokio::time::timeout(self.tx_timeout, reply_sub.next()).await {
            Ok(Some(msg)) => {
                let result: TxSimulateResultMessage = serde_json::from_slice(&msg.payload)?;
                self.metrics.track_operation(
                    "nats_simulate_transaction",
                    start_time.elapsed().as_millis() as f64,
                    result.success,
                );
                Ok(SimulateTransactionResponse {
                    success: result.success,
                    compute_units_consumed: result.compute_units_consumed,
                    error_message: result.error_message,
                    logs: result.logs,
                })
            }
            Ok(None) => {
                self.metrics.track_operation(
                    "nats_simulate_transaction",
                    start_time.elapsed().as_millis() as f64,
                    false,
                );
                Err(anyhow!("NATS simulate reply channel closed unexpectedly"))
            }
            Err(_) => {
                self.metrics.track_operation(
                    "nats_simulate_transaction",
                    start_time.elapsed().as_millis() as f64,
                    false,
                );
                Err(anyhow!(
                    "simulate_transaction timed out after {:?}",
                    self.tx_timeout
                ))
            }
        }
    }

    async fn request_airdrop(
        &self,
        request: RequestAirdropRequest,
    ) -> Result<RequestAirdropResponse> {
        self.grpc.request_airdrop(request).await
    }

    async fn get_transaction_details(
        &self,
        request: GetTransactionDetailsRequest,
    ) -> Result<GetTransactionDetailsResponse> {
        self.grpc.get_transaction_details(request).await
    }
}
