#[derive(Debug, Clone, Copy)]
pub enum TransactionType {
    TokenMinting,
    Settlement,
    OrderCreation,
    ERCIssuance,
    MeterReading,
    TokenTransfer,
}

impl TransactionType {
    pub fn should_use_priority_fees(&self) -> bool {
        true
    }
}

pub struct PriorityFeeService;
impl PriorityFeeService {
    pub fn recommend_priority_level(t: TransactionType) -> u64 {
        match t {
            TransactionType::Settlement => 5_000, // Higher priority for settlements
            _ => 1_000,                          // Default
        }
    }
    pub fn recommend_compute_limit(t: TransactionType) -> u32 {
        match t {
            TransactionType::Settlement => 600_000, // Higher limit for batched settlements
            _ => 200_000,
        }
    }
    pub fn add_priority_fee(
        instructions: &mut Vec<Instruction>,
        priority_fee: u64,
        compute_limit: Option<u32>,
    ) -> anyhow::Result<()> {
        if let Some(limit) = compute_limit {
            instructions.insert(
                0,
                solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_limit(limit),
            );
        }

        if priority_fee > 0 {
            instructions.insert(
                0,
                solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_price(
                    priority_fee,
                ),
            );
        }

        Ok(())
    }
}
use anyhow::{anyhow, Result};
use bs58;
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    instruction::Instruction,
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
    transaction::Transaction,
};
use spl_token_2022;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
// use crate::api::middleware::metrics::{track_blockchain_operation, track_transaction_retry, track_rpc_error, track_priority_fee_recommended};
fn track_blockchain_operation(_op: &str, _duration: f64, _success: bool) {}
fn track_transaction_retry(_op: &str, _retry_count: u32) {}
fn track_rpc_error(_op: &str, _error: &str) {}
fn track_priority_fee_recommended(_op: &str, _fee: u64) {}
use tracing::{debug, error, info, warn};

pub struct BlockchainUtils;
impl BlockchainUtils {
    pub fn load_keypair_from_file(path: &str) -> anyhow::Result<solana_sdk::signature::Keypair> {
        let bytes = std::fs::read_to_string(path)?;
        let key_vec: Vec<u8> = serde_json::from_str(&bytes)?;
        solana_sdk::signature::Keypair::try_from(key_vec.as_slice()).map_err(|e| anyhow::anyhow!(e))
    }
}

/// Transaction handling for Solana blockchain operations with enhanced performance and security
#[derive(Clone)]
pub struct TransactionHandler {
    rpc_client: Arc<RpcClient>,
    /// Cached recent blockhash for performance
    recent_blockhash: Arc<RwLock<Option<solana_sdk::hash::Hash>>>,
    /// Connection pool for better performance
    connection_pool: Arc<RwLock<Vec<Arc<RpcClient>>>>,
}

impl std::fmt::Debug for TransactionHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionHandler")
            .field("rpc_url", &self.rpc_client.url())
            .finish()
    }
}

impl TransactionHandler {
    /// Create a new transaction handler with connection pooling
    pub fn new(rpc_client: Arc<RpcClient>) -> Self {
        info!("Initializing transaction handler with connection pooling");
        Self {
            rpc_client,
            recent_blockhash: Arc::new(RwLock::new(None)),
            connection_pool: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Get or create a connection from the pool
    async fn get_connection(&self) -> Arc<RpcClient> {
        let mut pool = self.connection_pool.write().await;

        // Return existing connection if available
        if let Some(conn) = pool.pop() {
            debug!("Reusing existing connection from pool");
            return conn;
        }

        // Create new connection if pool is empty
        if pool.is_empty() {
            let new_conn = Arc::new(RpcClient::new(self.rpc_client.url()));
            pool.push(new_conn.clone());
            info!("Created new RPC connection (pool size: {})", pool.len());
            return new_conn;
        }

        // Create new connection and add to pool
        let new_conn = Arc::new(RpcClient::new(self.rpc_client.url()));
        pool.push(new_conn.clone());
        info!("Created new RPC connection (pool size: {})", pool.len());
        new_conn
    }

    /// Return connection to pool after use
    async fn return_connection(&self, conn: Arc<RpcClient>) {
        let mut pool = self.connection_pool.write().await;
        pool.push(conn);
        debug!("Returned connection to pool (pool size: {})", pool.len());
    }

    /// Submit transaction with simulation and priority fees
    pub async fn submit_transaction(&self, mut transaction: Transaction) -> Result<Signature> {
        let start_time = std::time::Instant::now();

        // Get recent blockhash for transaction
        let recent_blockhash = self.get_recent_blockhash().await?;
        transaction.message.recent_blockhash = recent_blockhash;

        // 1. Simulate transaction first to validate
        let sim_start = std::time::Instant::now();
        let sim_res = self.simulate_transaction(&transaction).await;
        track_blockchain_operation(
            "simulate_transaction",
            sim_start.elapsed().as_millis() as f64,
            sim_res.is_ok(),
        );

        if let Err(e) = sim_res {
            error!("Transaction simulation failed: {}", e);
            return Err(anyhow!("Transaction simulation failed: {}", e));
        }

        // NOTE: Priority fees are best added BEFORE transaction compilation.
        // For raw transactions submitted here, we assume they either have fees
        // or we log that we cannot easily add them to a compiled message.
        debug!("Submitting pre-compiled transaction (skipping auto-priority-fee)");

        // 3. Sign transaction with secure key management
        let signature = self.sign_transaction(&mut transaction).await?;

        // 4. Submit to network with retry logic
        let signature = self.submit_with_retry(transaction, signature).await?;

        let duration = start_time.elapsed();
        track_blockchain_operation(
            "submit_transaction_total",
            duration.as_millis() as f64,
            true,
        );
        info!("Transaction submitted successfully in {:?}", duration);

        Ok(signature)
    }

    /// Simulate transaction with enhanced validation
    pub async fn simulate_transaction(&self, transaction: &Transaction) -> Result<()> {
        debug!(
            "Simulating transaction with {} instructions",
            transaction.message.instructions.len()
        );

        let conn = self.get_connection().await;

        // Use RpcSimulateTransactionConfig with better validation
        let config = solana_client::rpc_config::RpcSimulateTransactionConfig {
            sig_verify: false,
            replace_recent_blockhash: true,
            ..Default::default()
        };

        let simulation = conn
            .simulate_transaction_with_config(transaction, config)
            .map_err(|e| anyhow!("Transaction simulation failed: {}", e))?;

        self.return_connection(conn).await;

        if let Some(err) = simulation.value.err {
            warn!("Transaction simulation errors: {:?}", err);
            return Err(anyhow!(
                "Transaction simulation validation failed: {:?}",
                err
            ));
        }

        if let Some(logs) = &simulation.value.logs {
            if !logs.is_empty() {
                for log in logs {
                    debug!("Simulation log: {}", log);
                }
            }
        }

        debug!("Transaction simulation completed successfully");
        Ok(())
    }

    /// Add priority fees to instruction list based on type
    fn add_priority_fees(
        &self,
        instructions: &mut Vec<Instruction>,
        tx_type_str: &'static str,
    ) -> Result<()> {
        debug!("Adding priority fees for transaction type: {}", tx_type_str);

        // Map string type to TransactionType enum
        let tx_type = match tx_type_str {
            "token_minting" | "minting" => TransactionType::TokenMinting,
            "settlement" => TransactionType::Settlement,
            "order_creation" | "create_order" => TransactionType::OrderCreation,
            "erc_issuance" => TransactionType::ERCIssuance,
            "meter_reading" => TransactionType::MeterReading,
            _ => TransactionType::TokenTransfer, // Default
        };

        if !tx_type.should_use_priority_fees() {
            return Ok(());
        }

        let priority_level = PriorityFeeService::recommend_priority_level(tx_type);
        let compute_limit = PriorityFeeService::recommend_compute_limit(tx_type);

        PriorityFeeService::add_priority_fee(instructions, priority_level, Some(compute_limit))?;

        Ok(())
    }

    /// Sign transaction with secure key management
    async fn sign_transaction(&self, transaction: &mut Transaction) -> Result<Signature> {
        // Get recent blockhash
        let recent_blockhash = self.get_recent_blockhash().await?;
        transaction.message.recent_blockhash = recent_blockhash;

        // Get payer keypair from secure storage
        let payer_keypair = self.get_payer_keypair().await?;

        // Validate transaction before signing
        self.validate_transaction(transaction).await?;

        // Sign with proper fee payer
        transaction
            .try_sign(&[&payer_keypair], recent_blockhash)
            .map_err(|e| anyhow!("Failed to sign transaction: {}", e))?;

        debug!("Transaction signed successfully");
        // Return the signature
        Ok(transaction.signatures[0])
    }

    /// Get payer keypair with proper fallbacks
    async fn get_payer_keypair(&self) -> Result<Keypair> {
        // Try loading from secure storage first
        if let Ok(keypair) = self.load_payer_keypair().await {
            return Ok(keypair);
        }

        // Fallback to environment variable
        if let Ok(private_key) = std::env::var("PAYER_PRIVATE_KEY") {
            if let Ok(key_bytes) = bs58::decode(&private_key).into_vec() {
                // Solana keypair can be 64 bytes (full keypair) or 32 bytes (secret key)
                if key_bytes.len() == 64 {
                    // Full keypair format - extract the secret key (first 32 bytes)
                    let mut secret_key = [0u8; 32];
                    secret_key.copy_from_slice(&key_bytes[..32]);
                    return Ok(Keypair::new_from_array(secret_key));
                } else if key_bytes.len() == 32 {
                    // Just the secret key
                    let mut secret_key = [0u8; 32];
                    secret_key.copy_from_slice(&key_bytes);
                    return Ok(Keypair::new_from_array(secret_key));
                }
            }
        }

        // Fallback to development keypair
        warn!("Using fallback keypair - set PAYER_PRIVATE_KEY for production");
        Ok(Keypair::new())
    }

    /// Load payer keypair from secure storage
    async fn load_payer_keypair(&self) -> Result<Keypair> {
        // Try loading from multiple secure locations
        let key_paths = vec![
            "/run/secrets/payer.json",
            "/app/payer.json",
            "/etc/gridtokenx/payer.json",
        ];

        for path in key_paths {
            if let Ok(keypair) = BlockchainUtils::load_keypair_from_file(path) {
                info!("Loaded payer keypair from: {}", path);
                return Ok(keypair);
            }
        }

        Err(anyhow!("Payer keypair not found in secure storage"))
    }

    /// Validate transaction before submission
    async fn validate_transaction(&self, transaction: &Transaction) -> Result<()> {
        // Check instruction count
        if transaction.message.instructions.is_empty() {
            return Err(anyhow!("Transaction cannot be empty"));
        }

        // Check for duplicate instructions
        let instruction_count = transaction.message.instructions.len();
        if instruction_count > 10 {
            warn!(
                "Transaction has {} instructions - consider batch optimization",
                instruction_count
            );
        }

        // Validate each instruction
        for (i, instruction) in transaction.message.instructions.iter().enumerate() {
            if instruction.data.is_empty() {
                return Err(anyhow!("Instruction {} cannot be empty", i));
            }
        }

        debug!("Transaction validation passed");
        Ok(())
    }

    /// Submit transaction with retry logic and enhanced error handling
    pub async fn submit_with_retry(
        &self,
        mut transaction: Transaction,
        _initial_signature: Signature,
    ) -> Result<Signature> {
        let mut attempts: u32 = 0;
        let max_retries = 5;
        let base_delay = Duration::from_millis(500);
        let max_delay = Duration::from_secs(10);

        loop {
            attempts += 1;

            if attempts > 1 {
                warn!("Transaction retry attempt {}/{}", attempts, max_retries);

                // Exponential backoff with jitter
                // delay = base_delay * 2^(attempts-2) + jitter
                // For attempt 2: 500ms * 2^0 = 500ms
                // For attempt 3: 500ms * 2^1 = 1000ms
                // For attempt 4: 500ms * 2^2 = 2000ms
                let backoff_power = 2u32.saturating_pow(attempts.saturating_sub(2));
                let mut delay = base_delay.saturating_mul(backoff_power);

                // Add simple jitter (up to 25% of the delay)
                let jitter_ms = (backoff_power as u64 * 125) % 500; // Very simple pseudo-jitter
                delay += Duration::from_millis(jitter_ms);

                if delay > max_delay {
                    delay = max_delay;
                }

                debug!("Backing off for {:?} before retry", delay);
                tokio::time::sleep(delay).await;

                // Update transaction with new blockhash for retry
                let recent_blockhash = self.get_recent_blockhash().await?;
                transaction.message.recent_blockhash = recent_blockhash;

                // Resign transaction
                transaction
                    .try_sign(&[&self.get_payer_keypair().await?], recent_blockhash)
                    .map_err(|e| anyhow!("Failed to sign transaction during retry: {}", e))?;
            }

            let conn = self.get_connection().await;

            match conn.send_and_confirm_transaction(&transaction) {
                Ok(sig) => {
                    info!("Transaction submitted successfully on attempt {}", attempts);
                    track_transaction_retry("submit_transaction", attempts);
                    return Ok(sig);
                }
                Err(e) => {
                    error!(
                        "Transaction submission failed on attempt {}: {:?}",
                        attempts, e
                    );

                    // Track specific RPC errors if available
                    if let solana_client::client_error::ClientErrorKind::RpcError(
                        solana_client::rpc_request::RpcError::RpcResponseError {
                            code,
                            ref message,
                            ref data,
                            ..
                        },
                    ) = e.kind()
                    {
                        track_rpc_error("submit_transaction", &code.to_string());
                        debug!(
                            "RPC error details: code={}, message={}, data={:?}",
                            code, message, data
                        );

                        // Handle non-retryable errors
                        // -32002: Transaction preflight check failed (often permanent if logic/funds)
                        // But some preflight errors are transient (e.g., account in use -429? No, that's rate limit)
                        if *code == -32002 && message.contains("insufficient funds") {
                            track_transaction_retry("submit_transaction", attempts);
                            return Err(anyhow!(
                                "Permanent error: insufficient funds for transaction"
                            ));
                        }
                    }

                    // If we've reached max retries, return error
                    if attempts >= max_retries {
                        track_transaction_retry("submit_transaction", attempts);
                        return Err(anyhow!(
                            "Transaction failed after {} retries: {}",
                            max_retries,
                            e
                        ));
                    }

                    // Specific handling for common errors to adjust strategy
                    match e.kind() {
                        solana_client::client_error::ClientErrorKind::RpcError(
                            solana_client::rpc_request::RpcError::RpcResponseError {
                                code,
                                message,
                                ..
                            },
                        ) => {
                            if *code == -32005 {
                                // Node is behind
                                warn!(
                                    "RPC Node is behind: {}. Retrying with extra delay.",
                                    message
                                );
                                tokio::time::sleep(Duration::from_secs(2)).await;
                            }
                        }
                        solana_client::client_error::ClientErrorKind::Reqwest(re)
                            if re.is_timeout() =>
                        {
                            warn!("RPC request timeout. Retrying...");
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    /// Get recent blockhash with caching
    async fn get_recent_blockhash(&self) -> Result<solana_sdk::hash::Hash> {
        let start = std::time::Instant::now();
        // Check cache first
        {
            let cache = self.recent_blockhash.read().await;
            if let Some(blockhash) = *cache {
                debug!("Using cached blockhash");
                return Ok(blockhash);
            }
        }

        // Fetch from network if not cached
        let conn = self.get_connection().await;
        let blockhash = conn
            .get_latest_blockhash()
            .map_err(|e| anyhow!("Failed to get latest blockhash: {}", e))?;

        // Update cache
        {
            let mut cache = self.recent_blockhash.write().await;
            *cache = Some(blockhash);
            debug!("Updated cached blockhash: {}", blockhash);
        }

        self.return_connection(conn).await;
        track_blockchain_operation(
            "get_recent_blockhash",
            start.elapsed().as_millis() as f64,
            true,
        );
        Ok(blockhash)
    }

    /// Enhanced account balance queries with caching
    pub async fn get_balance(&self, pubkey: &Pubkey, force_refresh: bool) -> Result<u64> {
        let cache_key = format!("balance:{}", pubkey);
        
        // Only check cache if NOT forcing a refresh
        if !force_refresh {
            // Check cache first
            if let Some(cached_balance) = self.get_cached_balance(&cache_key).await {
                debug!("Using cached balance for {}: {}", pubkey, cached_balance);
                return Ok(cached_balance);
            }
        }

        // Fetch from network
        let conn = self.get_connection().await;
        let balance = conn
            .get_balance(pubkey)
            .map_err(|e| anyhow!("Failed to get balance: {}", e))?;

        // Update cache with short TTL
        // If force_refresh is true and balance is 0, we might want to skip caching 
        // to allow immediate retry (common during startup race conditions)
        if !force_refresh || balance > 0 {
            self.update_balance_cache(&cache_key, balance, 60).await;
        }

        self.return_connection(conn).await;
        Ok(balance)
    }

    /// Get token account balance
    pub async fn get_token_account_balance(&self, token_account: &Pubkey) -> Result<u64> {
        let conn = self.get_connection().await;

        let balance_result = conn
            .get_token_account_balance(token_account)
            .map_err(|e| anyhow!("Failed to get token account balance: {}", e))?;

        self.return_connection(conn).await;

        // Parse amount as u64 (lamports/raw units)
        balance_result
            .amount
            .parse::<u64>()
            .map_err(|e| anyhow!("Failed to parse token amount: {}", e))
    }

    /// Simple in-memory balance cache
    async fn get_cached_balance(&self, _key: &str) -> Option<u64> {
        // This is a simple implementation - in production, use Redis
        None
    }

    async fn update_balance_cache(&self, _key: &str, _balance: u64, _ttl: u64) {
        // This is a simple implementation - in production, use Redis
        // For now, no-op
    }

    /// Get the RPC client
    pub fn client(&self) -> &RpcClient {
        &self.rpc_client
    }

    /// Get the current size of the connection pool
    pub async fn get_pool_size(&self) -> u64 {
        let pool = self.connection_pool.read().await;
        pool.len() as u64
    }

    /// Add priority fee to instruction list manually
    pub fn add_priority_fee_to_instructions(
        &self,
        instructions: &mut Vec<Instruction>,
        tx_type: &'static str,
    ) -> Result<()> {
        self.add_priority_fees(instructions, tx_type)
    }

    /// Confirm transaction status
    pub async fn confirm_transaction(&self, signature: &str) -> Result<bool> {
        let sig =
            Signature::from_str(signature).map_err(|e| anyhow!("Invalid signature: {}", e))?;

        let status = self
            .rpc_client
            .get_signature_status(&sig)
            .map_err(|e| anyhow!("Failed to get signature status: {}", e))?;

        Ok(status.is_some())
    }

    // Get trade record from blockchain - DISABLED
    // pub async fn get_trade_record(
    //     &self,
    //     _signature: &str,
    // ) -> Result<crate::domain::trading::models::TradeRecord> {
    //     Err(anyhow!("Trade record fetching not implemented"))
    // }

    /// Check if the service is healthy
    pub async fn health_check(&self) -> Result<bool> {
        match self.rpc_client.get_health() {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    /// Request airdrop (devnet/localnet only)
    pub async fn request_airdrop(&self, pubkey: &Pubkey, lamports: u64) -> Result<Signature> {
        self.rpc_client
            .request_airdrop(pubkey, lamports)
            .map_err(|e| anyhow!("Failed to request airdrop: {}", e))
    }

    /// Get account balance in SOL
    pub async fn get_balance_sol(&self, pubkey: &Pubkey, force_refresh: bool) -> Result<f64> {
        let lamports = self.get_balance(pubkey, force_refresh).await?;
        Ok(lamports as f64 / 1_000_000_000.0)
    }

    /// Send and confirm a transaction
    pub async fn send_and_confirm_transaction(
        &self,
        transaction: &Transaction,
    ) -> Result<Signature> {
        self.rpc_client
            .send_and_confirm_transaction(transaction)
            .map_err(|e| anyhow!("Failed to send and confirm transaction: {}", e))
    }

    /// Get transaction status
    pub async fn get_signature_status(&self, signature: &Signature) -> Result<Option<bool>> {
        let status = self
            .rpc_client
            .get_signature_status(signature)
            .map_err(|e| anyhow!("Failed to get signature status: {}", e))?;

        Ok(status.map(|s| s.is_ok()))
    }

    /// Get recent blockhash
    pub async fn get_latest_blockhash(&self) -> Result<solana_sdk::hash::Hash> {
        self.rpc_client
            .get_latest_blockhash()
            .map_err(|e| anyhow!("Failed to get latest blockhash: {}", e))
    }

    /// Get slot height
    pub async fn get_slot(&self) -> Result<u64> {
        self.rpc_client
            .get_slot()
            .map_err(|e| anyhow!("Failed to get slot: {}", e))
    }

    /// Get account info
    pub async fn get_account(&self, pubkey: &Pubkey) -> Result<solana_sdk::account::Account> {
        let conn = self.get_connection().await;
        let account = conn
            .get_account(pubkey)
            .map_err(|e| anyhow!("Failed to get account: {}", e))?;
        self.return_connection(conn).await;
        Ok(account)
    }

    /// Get account data
    pub async fn get_account_data(&self, pubkey: &Pubkey) -> Result<Vec<u8>> {
        let account = self
            .rpc_client
            .get_account(pubkey)
            .map_err(|e| anyhow!("Failed to get account: {}", e))?;

        Ok(account.data)
    }

    /// Check if an account exists
    pub async fn account_exists(&self, pubkey: &Pubkey) -> Result<bool> {
        match self.rpc_client.get_account(pubkey) {
            Ok(_) => {
                debug!("Account {} exists", pubkey);
                Ok(true)
            }
            Err(e) => {
                warn!("Account {} check failed/not found: {}", pubkey, e);
                Ok(false)
            }
        }
    }

    /// Build, sign, and send a transaction (ASYNC - no confirmation wait)
    ///
    /// **Performance**: Returns immediately after sending (~100-300ms)
    /// Does NOT wait for confirmation. Use for background tasks where
    /// eventual consistency is acceptable.
    pub async fn build_and_send_transaction(
        &self,
        instructions: Vec<solana_sdk::instruction::Instruction>,
        signers: &[&Keypair],
    ) -> Result<Signature> {
        let recent_blockhash = self
            .rpc_client
            .get_latest_blockhash()
            .map_err(|e| anyhow!("Failed to get blockhash: {}", e))?;

        let mut transaction =
            Transaction::new_with_payer(&instructions, Some(&signers[0].pubkey()));
        transaction.sign(signers, recent_blockhash);

        // Use send_transaction instead of send_and_confirm_transaction
        // This returns immediately without waiting for confirmation
        self.rpc_client
            .send_transaction(&transaction)
            .map_err(|e| anyhow!("Failed to send transaction: {}", e))
    }

    /// Build, sign, and send a transaction with priority (ASYNC - no confirmation wait)
    ///
    /// **Performance**: Returns immediately after sending (~100-300ms)
    /// Does NOT wait for confirmation. Use for background tasks where
    /// eventual consistency is acceptable.
    pub async fn build_and_send_transaction_with_priority(
        &self,
        mut instructions: Vec<solana_sdk::instruction::Instruction>,
        signers: &[&Keypair],
        transaction_type: &'static str,
    ) -> Result<Signature> {
        // Add priority fees BEFORE compilation
        self.add_priority_fees(&mut instructions, transaction_type)?;

        let recent_blockhash = self.get_latest_blockhash().await?;
        let mut transaction =
            Transaction::new_with_payer(&instructions, Some(&signers[0].pubkey()));

        transaction.sign(signers, recent_blockhash);

        // Use send_transaction instead of send_and_confirm_transaction
        // This returns immediately without waiting for confirmation
        self.rpc_client
            .send_transaction(&transaction)
            .map_err(|e| anyhow!("Failed to send transaction: {}", e))
    }

    /// Wait for transaction confirmation
    pub async fn wait_for_confirmation(
        &self,
        signature: &Signature,
        timeout_secs: u64,
    ) -> Result<bool> {
        let start = std::time::Instant::now();

        loop {
            if start.elapsed().as_secs() >= timeout_secs {
                return Ok(false);
            }

            match self.rpc_client.get_signature_status(signature) {
                Ok(Some(_)) => return Ok(true),
                Ok(None) => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
            }
        }
    }

    /// Wait for transaction to reach target confirmations
    pub async fn wait_for_confirmations(
        &self,
        signature: &Signature,
        target_confirmations: u64,
        timeout_secs: u64,
    ) -> Result<TransactionStatus> {
        let start = std::time::Instant::now();
        info!(
            "Waiting for {} confirmations on signature: {}",
            target_confirmations, signature
        );

        loop {
            if start.elapsed().as_secs() >= timeout_secs {
                warn!("Transaction confirmation timeout after {}s", timeout_secs);
                return Ok(TransactionStatus::Pending);
            }

            match self.get_transaction_status(signature).await? {
                TransactionStatus::Finalized => {
                    info!("Transaction {} finalized", signature);
                    return Ok(TransactionStatus::Finalized);
                }
                TransactionStatus::Confirmed(count) if count >= target_confirmations => {
                    info!("Transaction {} reached {} confirmations", signature, count);
                    return Ok(TransactionStatus::Confirmed(count));
                }
                TransactionStatus::Failed(err) => {
                    error!("Transaction {} failed: {}", signature, err);
                    return Ok(TransactionStatus::Failed(err));
                }
                status => {
                    debug!("Transaction {} status: {:?}, waiting...", signature, status);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }

    /// Get detailed transaction status
    pub async fn get_transaction_status(&self, signature: &Signature) -> Result<TransactionStatus> {
        use solana_client::rpc_config::RpcTransactionConfig;
        use solana_transaction_status::UiTransactionEncoding;

        // First check signature status
        let status = self
            .rpc_client
            .get_signature_status(signature)
            .map_err(|e| anyhow!("Failed to get signature status: {}", e))?;

        match status {
            None => Ok(TransactionStatus::Pending),
            Some(result) => match result {
                Ok(_) => {
                    // Transaction succeeded, check confirmation level
                    let config = RpcTransactionConfig {
                        encoding: Some(UiTransactionEncoding::Json),
                        commitment: Some(
                            solana_sdk::commitment_config::CommitmentConfig::finalized(),
                        ),
                        max_supported_transaction_version: Some(0),
                    };

                    match self
                        .rpc_client
                        .get_transaction_with_config(signature, config)
                    {
                        Ok(tx) => {
                            if tx.slot > 0 {
                                // Get current slot to calculate confirmations
                                let current_slot = self.rpc_client.get_slot().unwrap_or(0);
                                let confirmations = current_slot.saturating_sub(tx.slot);

                                // Solana considers 32+ confirmations as finalized
                                if confirmations >= 32 {
                                    Ok(TransactionStatus::Finalized)
                                } else {
                                    Ok(TransactionStatus::Confirmed(confirmations))
                                }
                            } else {
                                Ok(TransactionStatus::Processed)
                            }
                        }
                        Err(_) => {
                            // Transaction exists but can't get details - it's at least processed
                            Ok(TransactionStatus::Processed)
                        }
                    }
                }
                Err(err) => Ok(TransactionStatus::Failed(format!("{:?}", err))),
            },
        }
    }

    /// Get the number of confirmations for a transaction
    pub async fn get_confirmation_count(&self, signature: &Signature) -> Result<u64> {
        match self.get_transaction_status(signature).await? {
            TransactionStatus::Finalized => Ok(32), // Finalized = 32+ confirmations
            TransactionStatus::Confirmed(count) => Ok(count),
            TransactionStatus::Processed => Ok(1),
            TransactionStatus::Pending => Ok(0),
            TransactionStatus::Failed(_) => Ok(0),
        }
    }

    /// Estimate transaction fee before sending
    pub async fn estimate_transaction_fee(&self, transaction: &Transaction) -> Result<FeeEstimate> {
        // Get fee for message
        let fee = self
            .rpc_client
            .get_fee_for_message(&transaction.message)
            .map_err(|e| anyhow!("Failed to estimate fee: {}", e))?;

        // Get priority fee estimate (simplified - actual implementation would query recent fees)
        let priority_fee = self.get_priority_fee_estimate().await?;

        Ok(FeeEstimate {
            base_fee: fee,
            priority_fee,
            total_fee: fee + priority_fee,
        })
    }

    /// Get priority fee estimate based on recent transactions
    async fn get_priority_fee_estimate(&self) -> Result<u64> {
        // Query recent priority fees from the network
        // For now, use a simple heuristic based on recent blocks
        // Default priority fee: 0.00001 SOL = 10,000 lamports
        let default_priority_fee = 10_000u64;

        // Try to get recent prioritization fees
        match self.rpc_client.get_recent_prioritization_fees(&[]) {
            Ok(fees) => {
                if fees.is_empty() {
                    Ok(default_priority_fee)
                } else {
                    // Calculate median priority fee
                    let mut fee_values: Vec<u64> =
                        fees.iter().map(|f| f.prioritization_fee).collect();
                    fee_values.sort();
                    let median = fee_values[fee_values.len() / 2];
                    // Add 20% buffer for reliability
                    let recommended = median.saturating_mul(120) / 100;
                    track_priority_fee_recommended("recent", recommended);
                    Ok(recommended)
                }
            }
            Err(_) => {
                track_priority_fee_recommended("default", default_priority_fee);
                Ok(default_priority_fee)
            }
        }
    }

    /// Check if account has sufficient SOL for transaction fees
    pub async fn check_sufficient_sol(
        &self,
        pubkey: &Pubkey,
        required_fee: u64,
    ) -> Result<SolBalanceCheck> {
        let balance = self.get_balance(pubkey, true).await?;
        let rent_exempt_minimum = 890_880u64; // Approximate rent-exempt minimum for an account

        let required_total = required_fee + rent_exempt_minimum;
        let sufficient = balance >= required_total;

        Ok(SolBalanceCheck {
            balance,
            required_fee,
            rent_exempt_minimum,
            sufficient,
            deficit: if sufficient {
                0
            } else {
                required_total - balance
            },
        })
    }

    /// Send transaction with retry
    pub async fn send_transaction_with_retry(
        &self,
        instructions: Vec<solana_sdk::instruction::Instruction>,
        signers: &[&Keypair],
        max_retries: u32,
    ) -> Result<Signature> {
        let mut attempts = 0;

        loop {
            attempts += 1;

            match self
                .build_and_send_transaction(instructions.clone(), signers)
                .await
            {
                Ok(sig) => return Ok(sig),
                Err(e) if attempts >= max_retries => {
                    return Err(anyhow!("Failed after {} retries: {}", max_retries, e));
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            }
        }
    }

    /// Build a transaction without sending
    pub async fn build_transaction(
        &self,
        instructions: Vec<solana_sdk::instruction::Instruction>,
        payer: &Pubkey,
    ) -> Result<Transaction> {
        let _recent_blockhash = self
            .rpc_client
            .get_latest_blockhash()
            .map_err(|e| anyhow!("Failed to get blockhash: {}", e))?;

        Ok(Transaction::new_with_payer(&instructions, Some(payer)))
    }

    // ============ ESCROW METHODS ============

    /// Derive escrow PDA for an order
    pub fn derive_escrow_pda(order_id: &[u8; 32], program_id: &Pubkey) -> (Pubkey, u8) {
        Pubkey::find_program_address(&[b"escrow", order_id.as_ref()], program_id)
    }

    /// Lock tokens to escrow for a buy order
    pub async fn lock_tokens_to_escrow(
        &self,
        buyer_authority: &Keypair,
        buyer_ata: &Pubkey,
        escrow_ata: &Pubkey,
        token_mint: &Pubkey,
        amount: u64,
        decimals: u8,
    ) -> Result<Signature> {
        info!(
            "🔒 Locking {} tokens to escrow: {} -> {}",
            amount, buyer_ata, escrow_ata
        );

        let token_program = self.get_token_program_for_mint(token_mint).await?;

        // Check if Token-2022
        let token_2022_program = Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")?;
        let is_token_2022 = token_program == token_2022_program;

        let transfer_ix = if is_token_2022 {
            spl_token_2022::instruction::transfer_checked(
                &token_program,
                buyer_ata,
                token_mint,
                escrow_ata,
                &buyer_authority.pubkey(),
                &[],
                amount,
                decimals,
            )?
        } else {
            spl_token::instruction::transfer_checked(
                &token_program,
                buyer_ata,
                token_mint,
                escrow_ata,
                &buyer_authority.pubkey(),
                &[],
                amount,
                decimals,
            )?
        };

        let payer: Keypair = self.get_payer_keypair().await?;
        let recent_blockhash = self.get_recent_blockhash().await?;
        let transaction = Transaction::new_signed_with_payer(
            &[transfer_ix],
            Some(&payer.pubkey()),
            &[&payer, buyer_authority],
            recent_blockhash,
        );

        let signature = self.submit_transaction(transaction).await?;
        info!("🔒 Escrow lock complete: {}", signature);
        Ok(signature)
    }

    /// Release escrow tokens to seller after settlement
    pub async fn release_escrow_to_seller(
        &self,
        escrow_authority: &Keypair,
        escrow_ata: &Pubkey,
        seller_ata: &Pubkey,
        token_mint: &Pubkey,
        amount: u64,
        decimals: u8,
    ) -> Result<Signature> {
        info!(
            "✅ Releasing {} tokens from escrow: {} -> {}",
            amount, escrow_ata, seller_ata
        );

        let token_program = self.get_token_program_for_mint(token_mint).await?;

        // Check if Token-2022
        let token_2022_program = Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")?;
        let is_token_2022 = token_program == token_2022_program;

        let transfer_ix = if is_token_2022 {
            spl_token_2022::instruction::transfer_checked(
                &token_program,
                escrow_ata,
                token_mint,
                seller_ata,
                &escrow_authority.pubkey(),
                &[],
                amount,
                decimals,
            )?
        } else {
            spl_token::instruction::transfer_checked(
                &token_program,
                escrow_ata,
                token_mint,
                seller_ata,
                &escrow_authority.pubkey(),
                &[],
                amount,
                decimals,
            )?
        };

        let payer: Keypair = self.get_payer_keypair().await?;
        let recent_blockhash = self.get_recent_blockhash().await?;
        let transaction = Transaction::new_signed_with_payer(
            &[transfer_ix],
            Some(&payer.pubkey()),
            &[&payer, escrow_authority],
            recent_blockhash,
        );

        let signature = self.submit_transaction(transaction).await?;
        info!("✅ Escrow release complete: {}", signature);
        Ok(signature)
    }

    /// Refund escrow tokens to buyer on order cancel
    pub async fn refund_escrow_to_buyer(
        &self,
        escrow_authority: &Keypair,
        escrow_ata: &Pubkey,
        buyer_ata: &Pubkey,
        token_mint: &Pubkey,
        amount: u64,
        decimals: u8,
    ) -> Result<Signature> {
        info!(
            "↩️ Refunding {} tokens from escrow: {} -> {}",
            amount, escrow_ata, buyer_ata
        );

        let token_program = self.get_token_program_for_mint(token_mint).await?;

        // Check if Token-2022
        let token_2022_program = Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")?;
        let is_token_2022 = token_program == token_2022_program;

        let transfer_ix = if is_token_2022 {
            spl_token_2022::instruction::transfer_checked(
                &token_program,
                escrow_ata,
                token_mint,
                buyer_ata,
                &escrow_authority.pubkey(),
                &[],
                amount,
                decimals,
            )?
        } else {
            spl_token::instruction::transfer_checked(
                &token_program,
                escrow_ata,
                token_mint,
                buyer_ata,
                &escrow_authority.pubkey(),
                &[],
                amount,
                decimals,
            )?
        };

        let payer: Keypair = self.get_payer_keypair().await?;
        let recent_blockhash = self.get_recent_blockhash().await?;
        let transaction = Transaction::new_signed_with_payer(
            &[transfer_ix],
            Some(&payer.pubkey()),
            &[&payer, escrow_authority],
            recent_blockhash,
        );

        let signature = self.submit_transaction(transaction).await?;
        info!("↩️ Escrow refund complete: {}", signature);
        Ok(signature)
    }

    /// Get the token program ID for a given mint (Token vs Token-2022)
    pub async fn get_token_program_for_mint(&self, mint: &Pubkey) -> Result<Pubkey> {
        let conn = self.get_connection().await;
        let account = conn
            .get_account(mint)
            .map_err(|e| anyhow!("Failed to get mint account {}: {}", mint, e))?;
        self.return_connection(conn).await;

        Ok(account.owner)
    }
}

/// Transaction status for detailed tracking
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionStatus {
    /// Transaction not yet submitted or not found
    Pending,
    /// Transaction included in a block (1 confirmation)
    Processed,
    /// Transaction confirmed with N confirmations
    Confirmed(u64),
    /// Transaction finalized (32+ confirmations, irreversible)
    Finalized,
    /// Transaction failed with error message
    Failed(String),
}

/// Fee estimation result
#[derive(Debug, Clone)]
pub struct FeeEstimate {
    /// Base transaction fee in lamports
    pub base_fee: u64,
    /// Recommended priority fee in lamports
    pub priority_fee: u64,
    /// Total estimated fee (base + priority)
    pub total_fee: u64,
}

/// SOL balance check result
#[derive(Debug, Clone)]
pub struct SolBalanceCheck {
    /// Current balance in lamports
    pub balance: u64,
    /// Required fee for the transaction
    pub required_fee: u64,
    /// Minimum balance to keep for rent exemption
    pub rent_exempt_minimum: u64,
    /// Whether balance is sufficient
    pub sufficient: bool,
    /// Deficit amount if insufficient (0 if sufficient)
    pub deficit: u64,
}

/// Enhanced utilities for transaction operations
pub mod utils {
    use super::*;
    use anyhow::Result;
    use solana_sdk::{instruction::Instruction, pubkey::Pubkey};
    use std::sync::Arc;
    use tracing::debug;

    /// Create a transfer instruction with proper validation
    /// Uses spl_token::instruction::transfer_checked for Token-2022 compatibility
    pub fn create_transfer_instruction(
        from_ata: &Pubkey,
        to_ata: &Pubkey,
        mint_pubkey: &Pubkey,
        owner: &Pubkey,
        amount: u64,
        decimals: u8,
    ) -> Result<Instruction> {
        // Validate inputs
        if amount == 0 {
            return Err(anyhow!("Transfer amount cannot be zero"));
        }

        if !is_valid_pubkey(from_ata) || !is_valid_pubkey(to_ata) || !is_valid_pubkey(mint_pubkey) {
            return Err(anyhow!("Invalid public key in transfer instruction"));
        }

        debug!(
            "Creating transfer_checked instruction: {} tokens from {} to {}",
            amount, from_ata, to_ata
        );

        // Use transfer_checked for Token-2022 compatibility
        let instruction = spl_token::instruction::transfer_checked(
            &spl_token::ID, // Use standard token program; caller can override for Token-2022
            from_ata,
            mint_pubkey,
            to_ata,
            owner,
            &[], // No multisig signers
            amount,
            decimals,
        )?;

        Ok(instruction)
    }

    /// Create a transfer instruction for Token-2022
    pub fn create_transfer_instruction_2022(
        from_ata: &Pubkey,
        to_ata: &Pubkey,
        mint_pubkey: &Pubkey,
        owner: &Pubkey,
        amount: u64,
        decimals: u8,
    ) -> Result<Instruction> {
        if amount == 0 {
            return Err(anyhow!("Transfer amount cannot be zero"));
        }

        debug!(
            "Creating Token-2022 transfer_checked instruction: {} tokens from {} to {}",
            amount, from_ata, to_ata
        );

        // Token-2022 program ID
        let token_2022_program = Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")?;

        let instruction = spl_token::instruction::transfer_checked(
            &token_2022_program,
            from_ata,
            mint_pubkey,
            to_ata,
            owner,
            &[],
            amount,
            decimals,
        )?;

        Ok(instruction)
    }

    /// Validate a Solana public key
    pub fn is_valid_pubkey(pubkey: &Pubkey) -> bool {
        // Just check it's not all zeros
        pubkey.to_bytes() != [0u8; 32]
    }

    /// Get the associated token account address for an owner and mint
    pub fn get_ata_address(owner: &Pubkey, mint: &Pubkey) -> Pubkey {
        spl_associated_token_account::get_associated_token_address(owner, mint)
    }

    /// Get the associated token account address for Token-2022
    pub fn get_ata_address_2022(owner: &Pubkey, mint: &Pubkey) -> Pubkey {
        let token_2022_program = Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
            .expect("Invalid Token-2022 program ID");
        spl_associated_token_account::get_associated_token_address_with_program_id(
            owner,
            mint,
            &token_2022_program,
        )
    }

    /// Get or create an associated token account
    /// Returns the ATA address and optionally an instruction to create it
    pub async fn get_or_create_ata(
        rpc_client: &Arc<RpcClient>,
        owner: &Pubkey,
        mint: &Pubkey,
        payer: &Pubkey,
    ) -> Result<(Pubkey, Option<Instruction>)> {
        let ata = get_ata_address(owner, mint);

        // Check if the ATA already exists
        match rpc_client.get_account(&ata) {
            Ok(account) => {
                if !account.data.is_empty() {
                    debug!("ATA {} already exists for owner {}", ata, owner);
                    return Ok((ata, None));
                }
            }
            Err(_) => {
                // Account doesn't exist, need to create it
            }
        }

        debug!("Creating ATA instruction for owner {} mint {}", owner, mint);

        // Create instruction to create the ATA
        let create_ata_ix =
            spl_associated_token_account::instruction::create_associated_token_account(
                payer,
                owner,
                mint,
                &spl_token::ID,
            );

        Ok((ata, Some(create_ata_ix)))
    }

    /// Get or create an associated token account for Token-2022
    pub async fn get_or_create_ata_2022(
        rpc_client: &Arc<RpcClient>,
        owner: &Pubkey,
        mint: &Pubkey,
        payer: &Pubkey,
    ) -> Result<(Pubkey, Option<Instruction>)> {
        let token_2022_program = Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")?;
        let ata = spl_associated_token_account::get_associated_token_address_with_program_id(
            owner,
            mint,
            &token_2022_program,
        );

        // Check if the ATA already exists
        match rpc_client.get_account(&ata) {
            Ok(account) => {
                if !account.data.is_empty() {
                    debug!("Token-2022 ATA {} already exists for owner {}", ata, owner);
                    return Ok((ata, None));
                }
            }
            Err(_) => {
                // Account doesn't exist
            }
        }

        debug!(
            "Creating Token-2022 ATA instruction for owner {} mint {}",
            owner, mint
        );

        let create_ata_ix =
            spl_associated_token_account::instruction::create_associated_token_account(
                payer,
                owner,
                mint,
                &token_2022_program,
            );

        Ok((ata, Some(create_ata_ix)))
    }

    /// Determine if a mint uses Token-2022
    pub async fn is_token_2022(rpc_client: &Arc<RpcClient>, mint: &Pubkey) -> Result<bool> {
        let token_2022_program = Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")?;

        match rpc_client.get_account(mint) {
            Ok(account) => Ok(account.owner == token_2022_program),
            Err(e) => Err(anyhow!("Failed to get mint account: {}", e)),
        }
    }
}
