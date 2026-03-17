use anyhow::Result;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use sqlx::Row;
use uuid::Uuid;
use tracing::{error, info};

use crate::infra::db::schema::types::OrderSide;
use crate::infra::blockchain::WalletService;
use crate::infra::blockchain::rpc::OffchainOrderPayload;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signer;
use super::MarketClearingService;
use std::str::FromStr;

use std::collections::HashMap;

impl MarketClearingService {
    /// Batch pre-fetch and decrypt wallets for a list of user IDs to avoid O(N) PBKDF2 bottleneck
    pub async fn fetch_and_decrypt_wallets_batch(
        &self,
        user_ids: Vec<Uuid>,
    ) -> Result<HashMap<Uuid, solana_sdk::signature::Keypair>> {
        if user_ids.is_empty() { return Ok(HashMap::new()); }

        info!("🔐 Batch decrypting {} user wallets for matching engine", user_ids.len());
        
        // 1. Fetch all encrypted keys in one DB query
        let users_data = sqlx::query!(
            "SELECT id, wallet_address, encrypted_private_key, wallet_salt, encryption_iv FROM users WHERE id = ANY($1)",
            &user_ids
        )
        .fetch_all(&self.db)
        .await?;

        let master_secret = self.config.encryption_secret.clone();
        let mut tasks = Vec::new();

        // 2. Parallel Decryption (CPU-intensive PBKDF2)
        for user in users_data {
            let secret = master_secret.clone();
            if let (Some(enc_key), Some(iv), Some(salt)) = (
                user.encrypted_private_key,
                user.encryption_iv,
                user.wallet_salt,
            ) {
                tasks.push(tokio::task::spawn_blocking(move || {
                    use base64::{engine::general_purpose, Engine as _};
                    use solana_sdk::signature::Keypair;

                    let enc_key_b64 = general_purpose::STANDARD.encode(enc_key);
                    let iv_b64 = general_purpose::STANDARD.encode(iv);
                    let salt_b64 = general_purpose::STANDARD.encode(salt);

                    // Note: In matching engine, we use master_secret as password for background decryption
                    // Production: This would use a specific matching-authority key.
                    match WalletService::decrypt_private_key(
                        &secret,
                        &secret, 
                        &enc_key_b64,
                        &salt_b64,
                        &iv_b64,
                    ) {
                        Ok(pk_bytes) => {
                            if let Ok(kp) = Keypair::try_from(pk_bytes.as_slice()) {
                                Ok((user.id, kp))
                            } else {
                                Err(anyhow::anyhow!("Invalid key for user {}", user.id))
                            }
                        }
                        Err(e) => Err(anyhow::anyhow!("Decryption failed for {}: {}", user.id, e)),
                    }
                }));
            }
        }

        let mut decrypted_wallets = HashMap::new();
        let results = futures::future::join_all(tasks).await;

        for res in results {
            match res {
                Ok(Ok((id, kp))) => { decrypted_wallets.insert(id, kp); }
                Ok(Err(e)) => error!("❌ Batch decryption error: {}", e),
                Err(e) => error!("❌ Task join error: {}", e),
            }
        }

        Ok(decrypted_wallets)
    }

    pub(super) async fn execute_on_chain_order_creation(
        &self,
        user_id: Uuid,
        order_id: Uuid,
        side: OrderSide,
        energy_amount: Decimal,
        price_per_kwh: Decimal,
        session_token: Option<&str>,
        zone_id: Option<i32>,
    ) -> Result<()> {
        if !self.config.tokenization.enable_real_blockchain {
             info!("Blockchain processing is disabled. Skipping on-chain order creation for order {}", order_id);
             return Ok(());
        }

        use base64::{engine::general_purpose, Engine as _};
        use solana_sdk::signature::{Keypair, Signer};

        // Session cache disabled (Secure Passcode removed)
        let private_key_bytes: Option<Vec<u8>> = None;

        let keypair = if let Some(key_bytes) = private_key_bytes {
            Keypair::try_from(key_bytes.as_slice())
                .map_err(|e| anyhow::anyhow!("Invalid session key: {}", e))?
        } else {
            // Fetch user keys and decrypt with master fallback (Legacy)
            let db_user = sqlx::query!(
                "SELECT wallet_address, encrypted_private_key, wallet_salt, encryption_iv FROM users WHERE id = $1",
                user_id
            )
            .fetch_optional(&self.db)
            .await?
            .ok_or_else(|| anyhow::anyhow!("User not found"))?;

            if let (Some(enc_key), Some(iv), Some(salt)) = (
                db_user.encrypted_private_key,
                db_user.encryption_iv,
                db_user.wallet_salt,
            ) {
                let master_secret = &self.config.encryption_secret;
                let enc_key_b64 = general_purpose::STANDARD.encode(enc_key);
                let iv_b64 = general_purpose::STANDARD.encode(iv);
                let salt_b64 = general_purpose::STANDARD.encode(salt);

                let pk_bytes = WalletService::decrypt_private_key(
                    master_secret,
                    master_secret,
                    &enc_key_b64,
                    &salt_b64,
                    &iv_b64,
                )?;
                Keypair::try_from(pk_bytes.as_slice())
                    .map_err(|e| anyhow::anyhow!("Invalid decrypted key: {}", e))?
            } else {
                // Lazy wallet generation (only if session and master decryption fail)
                info!("User {} missing keys, generating new wallet...", user_id);
                let master_secret = &self.config.encryption_secret;
                let new_keypair = Keypair::new();
                let pubkey = new_keypair.pubkey().to_string();

                let (enc_key_b64, salt_b64, iv_b64) =
                    WalletService::encrypt_private_key(master_secret, master_secret, &new_keypair.to_bytes())?;

                let enc_key_bytes = general_purpose::STANDARD.decode(&enc_key_b64)?;
                let salt_bytes = general_purpose::STANDARD.decode(&salt_b64)?;
                let iv_bytes = general_purpose::STANDARD.decode(&iv_b64)?;

                sqlx::query!(
                    "UPDATE users SET wallet_address=$1, encrypted_private_key=$2, wallet_salt=$3, encryption_iv=$4 WHERE id=$5",
                )
                .bind(pubkey)
                .bind(enc_key_bytes)
                .bind(salt_bytes)
                .bind(iv_bytes)
                .bind(user_id)
                .execute(&self.db)
                .await?;
                
                new_keypair
            }
        };

        // On-chain tx
        let (signature, order_pda, order_index) = if self.config.tokenization.enable_real_blockchain {
            let trading_program_id = self.blockchain_service.trading_program_id()?;
            let (market_pda, _) = Pubkey::find_program_address(&[b"market"], &trading_program_id);

            let multiplier = Decimal::from(1_000_000_000);
            let amount_u64 = (energy_amount * multiplier).to_u64().unwrap_or(0);
            let price_u64 = (price_per_kwh * multiplier).to_u64().unwrap_or(0);

            info!("Creating order on-chain with Payer: {}", keypair.pubkey());
            info!("Market PDA: {}", market_pda);
            
            // Check balance
            if let Ok(bal) = self.blockchain_service.account_manager.get_balance(&keypair.pubkey()).await {
                info!("Payer Balance: {} lamports", bal);
            }
            
            let (sig, pda_str, index) = self.blockchain_service.execute_create_order(
                &keypair,
                &market_pda.to_string(),
                amount_u64,
                price_u64,
                match side {
                    OrderSide::Buy => "buy",
                    OrderSide::Sell => "sell",
                },
                None,
                zone_id.unwrap_or(1) as u32,
            ).await?;
            
            let pda_opt = if pda_str.is_empty() { None } else { Some(pda_str) };
            (sig.to_string(), pda_opt, Some(index as i64))
        } else {
            return Err(anyhow::anyhow!("Blockchain processing is disabled. Cannot create on-chain order."));
        };

        // Update DB with signature, PDA, and index
        sqlx::query!(
            "UPDATE trading_orders SET blockchain_tx_signature = $1, order_pda = $2, order_index = $3 WHERE id = $4",
        )
        .bind(signature)
        .bind(order_pda)
        .bind(order_index)
        .bind(order_id)
        .execute(&self.db)
        .await?;

        // 2. Execute Escrow Lock
        // If Buy: lock Currency (total cost).
        // If Sell: lock Energy (amount).
        let (asset_type, lock_amount) = match side {
            OrderSide::Buy => ("currency", price_per_kwh * energy_amount),
            OrderSide::Sell => ("energy", energy_amount),
        };

        // Only lock if amount > 0
        if lock_amount > Decimal::ZERO {
            match self.execute_escrow_lock(user_id, order_id, lock_amount, asset_type, session_token).await {
                Ok(sig) => {
                    info!("On-chain escrow lock executed for order {}: {}", order_id, sig);
                }
                Err(e) => {
                    error!("Failed to execute escrow lock for order {}: {}", order_id, e);
                }
            }
        } else {
            info!("Skipping on-chain escrow lock for order {} as amount is 0", order_id);
        }

        Ok(())
    }

    /// Execute on-chain escrow lock (transfer from user to API Authority Escrow)
    pub(super) async fn execute_escrow_lock(
        &self,
        user_id: Uuid,
        _order_id: Uuid,
        amount: Decimal,
        asset_type: &str, // "currency" or "energy"
        _session_token: Option<&str>,
    ) -> Result<String> {
        if !self.config.tokenization.enable_real_blockchain {
             return Err(anyhow::anyhow!("Blockchain processing is disabled. Cannot execute escrow lock."));
        }

        let api_authority = self.blockchain_service.account_manager.get_authority_keypair().await?;

        use base64::{engine::general_purpose, Engine as _};
        use solana_sdk::signature::{Keypair, Signer};
        use std::str::FromStr;

        // Session cache disabled (Secure Passcode removed)
        let private_key_bytes: Option<Vec<u8>> = None;

        let keypair = if let Some(key_bytes) = private_key_bytes {
            Keypair::try_from(key_bytes.as_slice())
                .map_err(|e| anyhow::anyhow!("Invalid session key for escrow: {}", e))?
        } else {
            // 1. Fetch user keys
            let db_user = sqlx::query!(
                "SELECT wallet_address, encrypted_private_key, wallet_salt, encryption_iv FROM users WHERE id = $1",
                user_id
            )
            .fetch_optional(&self.db)
            .await?
            .ok_or_else(|| anyhow::anyhow!("User not found"))?;

            if let (Some(enc_key), Some(iv), Some(salt)) = (
                db_user.encrypted_private_key,
                db_user.encryption_iv,
                db_user.wallet_salt,
            ) {
                let master_secret = &self.config.encryption_secret;
                let enc_key_b64 = general_purpose::STANDARD.encode(enc_key);
                let iv_b64 = general_purpose::STANDARD.encode(iv);
                let salt_b64 = general_purpose::STANDARD.encode(salt);

                let pk_bytes = WalletService::decrypt_private_key(
                    master_secret, 
                    master_secret,
                    &enc_key_b64,
                    &salt_b64,
                    &iv_b64,
                )?;
                Keypair::try_from(pk_bytes.as_slice())
                    .map_err(|e| anyhow::anyhow!("Invalid decrypted key for escrow: {}", e))?
            } else {
                return Err(anyhow::anyhow!("User has no wallet keys"));
            }
        };

        // 2. Select Mint based on asset_type
        let mint_str = if asset_type == "energy" {
            std::env::var("ENERGY_TOKEN_MINT")
             .unwrap_or_else(|_| "2XLTgMue7MHSjZ7A25zmV9xF6ZeBz2LouZt6Y92AtN2H".to_string())
        } else {
            // Default to Currency (USDC/THB)
            std::env::var("CURRENCY_TOKEN_MINT")
             .unwrap_or_else(|_| "4zMMC9srt5Ri5X14GAgXhaHii3GnPAEERYPJgZJDncDU".to_string())
        };
        let mint = Pubkey::from_str(&mint_str)?;

        // 3. User ATA
        let user_ata = self.blockchain_service.ensure_token_account_exists(
            &api_authority,
            &keypair.pubkey(),
            &mint
        ).await?;

        // 4. Escrow Owner (API Authority)
        let escrow_owner = api_authority.pubkey();

        // 5. Ensure Escrow ATA exists
        let escrow_ata = self.blockchain_service.ensure_token_account_exists(
            &api_authority,
            &escrow_owner,
            &mint
        ).await?;

        // 6. Lock Tokens
        let decimals = if asset_type == "energy" { 9 } else { 6 };
        let multiplier = Decimal::from(10_u64.pow(decimals as u32));
        let amount_u64 = (amount * multiplier).to_u64().unwrap_or(0);

        info!("Locking {} {} tokens ({} raw) from {} to API escrow {}", amount, asset_type, amount_u64, keypair.pubkey(), escrow_owner);

        let signature = self.blockchain_service.lock_tokens_to_escrow(
            &keypair,
            &user_ata,
            &escrow_ata,
            &mint,
            amount_u64,
            decimals
        ).await?;

        Ok(signature.to_string())
    }

    /// Execute on-chain escrow release (transfer from API Authority Escrow to Seller)
    pub(super) async fn execute_escrow_release(
        &self,
        target_user_id: Uuid,
        amount: Decimal,
        asset_type: &str, // "currency" or "energy"
    ) -> Result<String> {
        if !self.config.tokenization.enable_real_blockchain {
             return Err(anyhow::anyhow!("Blockchain processing is disabled. Cannot execute escrow release."));
        }

        use solana_sdk::signature::{Signer};
        use std::str::FromStr;

        let db_user = sqlx::query!(
            "SELECT wallet_address FROM users WHERE id = $1",
        )
        .bind(target_user_id)
        .fetch_optional(&self.db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Receiver not found"))?;

        let receiver_wallet_addr: Option<String> = db_user.get("wallet_address");
        let receiver_wallet = if let Some(addr) = receiver_wallet_addr.as_deref() {
            Pubkey::from_str(addr)?
        } else {
             return Err(anyhow::anyhow!("Receiver has no wallet address"));
        };

        // 2. Select Mint based on asset_type
        let mint_str = if asset_type == "energy" {
            std::env::var("ENERGY_TOKEN_MINT")
             .unwrap_or_else(|_| "2XLTgMue7MHSjZ7A25zmV9xF6ZeBz2LouZt6Y92AtN2H".to_string())
        } else {
            // Default to Currency (USDC/THB)
            std::env::var("CURRENCY_TOKEN_MINT")
             .unwrap_or_else(|_| "4zMMC9srt5Ri5X14GAgXhaHii3GnPAEERYPJgZJDncDU".to_string())
        };
        let mint = Pubkey::from_str(&mint_str)?;

        // 3. API Authority (Escrow Owner)
        let api_authority = self.blockchain_service.get_authority_keypair().await?;
        let escrow_owner = api_authority.pubkey();
        
        let escrow_ata = self.blockchain_service.calculate_ata_address(&escrow_owner, &mint)?;

        // 4. Ensure Receiver ATA exists
        let receiver_ata = self.blockchain_service.ensure_token_account_exists(
            &api_authority,
            &receiver_wallet,
            &mint
        ).await?;

        // 5. Release Tokens
        let decimals = if asset_type == "energy" { 9 } else { 6 };
        let multiplier = Decimal::from(10_u64.pow(decimals as u32));
        let amount_u64 = (amount * multiplier).to_u64().unwrap_or(0);

        info!("Releasing {} {} tokens from API escrow to receiver {}", amount, asset_type, receiver_wallet);

        let signature = self.blockchain_service.release_escrow_to_seller(
            &api_authority,
            &escrow_ata,
            &receiver_ata,
            &mint,
            amount_u64,
            decimals
        ).await?;

        Ok(signature.to_string())
    }

    /// Execute on-chain escrow refund (transfer from API Authority Escrow back to Buyer)
    pub(super) async fn execute_escrow_refund(
        &self,
        buyer_id: Uuid,
        amount: Decimal,
        asset_type: &str, // "currency" or "energy"
    ) -> Result<String> {
        if !self.config.tokenization.enable_real_blockchain {
             return Err(anyhow::anyhow!("Blockchain processing is disabled. Cannot execute escrow refund."));
        }

        use solana_sdk::signature::{Signer};
        use std::str::FromStr;

        // 1. Fetch User Wallet (Buyer)
        let db_user = sqlx::query!(
            "SELECT wallet_address FROM users WHERE id = $1",
        )
        .bind(buyer_id)
        .fetch_optional(&self.db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("User (Buyer) not found"))?;

        let user_wallet_addr: Option<String> = db_user.get("wallet_address");
        let user_wallet = if let Some(addr) = user_wallet_addr.as_deref() {
             Pubkey::from_str(addr)?
        } else {
             return Err(anyhow::anyhow!("User has no wallet address"));
        };

        // 2. Select Mint based on asset_type
        let mint_str = if asset_type == "energy" {
            std::env::var("ENERGY_TOKEN_MINT")
             .unwrap_or_else(|_| "2XLTgMue7MHSjZ7A25zmV9xF6ZeBz2LouZt6Y92AtN2H".to_string())
        } else {
            // Default to Currency (USDC/THB)
            std::env::var("CURRENCY_TOKEN_MINT")
             .unwrap_or_else(|_| "4zMMC9srt5Ri5X14GAgXhaHii3GnPAEERYPJgZJDncDU".to_string())
        };
        let mint = Pubkey::from_str(&mint_str)?;

        // 3. API Authority (Escrow Owner)
        let api_authority = self.blockchain_service.get_authority_keypair().await?;
        let escrow_owner = api_authority.pubkey();
        
        let escrow_ata = self.blockchain_service.calculate_ata_address(&escrow_owner, &mint)?;

        // 4. Ensure User ATA exists
        let user_ata = self.blockchain_service.ensure_token_account_exists(
             &api_authority,
             &user_wallet,
             &mint
        ).await?;

        // 5. Refund Tokens
        let decimals = if asset_type == "energy" { 9 } else { 6 };
        let multiplier = Decimal::from(10_u64.pow(decimals as u32));
        let amount_u64 = (amount * multiplier).to_u64().unwrap_or(0);

        info!("Refunding {} {} tokens from API escrow to user {}", amount, asset_type, user_wallet);

        let signature = self.blockchain_service.refund_escrow_to_buyer(
            &api_authority,
            &escrow_ata,
            &user_ata,
            &mint,
            amount_u64,
            decimals
        ).await?;

        Ok(signature.to_string())
    }

    /// Execute on-chain off-chain settlement
    pub(super) async fn execute_offchain_settlement(
        &self,
        market_pubkey: &Pubkey,
        _buyer_user_id: Uuid,
        _seller_user_id: Uuid,
        _buyer_signature: &str,
        buyer_payload_bytes: &[u8],
        _seller_signature: &str,
        seller_payload_bytes: &[u8],
        match_amount: Decimal,
        match_price: Decimal,
        wheeling_charge: Decimal,
        loss_cost: Decimal,
    ) -> Result<String> {
        if !self.config.tokenization.enable_real_blockchain {
            return Err(anyhow::anyhow!("Blockchain processing is disabled. Cannot execute off-chain settlement."));
        }

        // 1. Deserialise payloads
        let buyer_payload: OffchainOrderPayload = serde_json::from_slice(buyer_payload_bytes)?;
        let seller_payload: OffchainOrderPayload = serde_json::from_slice(seller_payload_bytes)?;

        // 2. Fetch mints
        let currency_mint_str = std::env::var("CURRENCY_TOKEN_MINT")
            .unwrap_or_else(|_| "4zMMC9srt5Ri5X14GAgXhaHii3GnPAEERYPJgZJDncDU".to_string());
        let currency_mint = Pubkey::from_str(&currency_mint_str)?;

        let energy_mint_str = std::env::var("ENERGY_TOKEN_MINT")
            .unwrap_or_else(|_| "2XLTgMue7MHSjZ7A25zmV9xF6ZeBz2LouZt6Y92AtN2H".to_string());
        let energy_mint = Pubkey::from_str(&energy_mint_str)?;

        // 3. Derive ATAs for users
        let buyer_currency_ata = self.blockchain_service.calculate_ata_address(&buyer_payload.user, &currency_mint)?;
        let seller_currency_ata = self.blockchain_service.calculate_ata_address(&seller_payload.user, &currency_mint)?;
        let seller_energy_ata = self.blockchain_service.calculate_ata_address(&seller_payload.user, &energy_mint)?;
        let buyer_energy_ata = self.blockchain_service.calculate_ata_address(&buyer_payload.user, &energy_mint)?;

        // 4. API Authority (Escrow Owner / Collector)
        let api_authority = self.blockchain_service.get_authority_keypair().await?;
        let collector_owner = api_authority.pubkey();

        // 5. Ensure Collector ATAs exist
        let fee_collector_ata = self.blockchain_service.ensure_token_account_exists(
            &api_authority,
            &collector_owner,
            &currency_mint
        ).await?;
        
        let wheeling_collector_ata = self.blockchain_service.ensure_token_account_exists(
            &api_authority,
            &collector_owner,
            &currency_mint
        ).await?;
        
        let loss_collector_ata = self.blockchain_service.ensure_token_account_exists(
            &api_authority,
            &collector_owner,
            &currency_mint
        ).await?;

        // 6. Convert amounts to atomic units
        let currency_decimals = 6;
        let energy_decimals = 9;
        
        let currency_multiplier = Decimal::from(10_u64.pow(currency_decimals));
        let energy_multiplier = Decimal::from(10_u64.pow(energy_decimals));

        let match_amount_u64 = (match_amount * energy_multiplier).to_u64().unwrap_or(0);
        let match_price_u64 = (match_price * currency_multiplier).to_u64().unwrap_or(0);
        let wheeling_charge_u64 = (wheeling_charge * currency_multiplier).to_u64().unwrap_or(0);
        let loss_cost_u64 = (loss_cost * currency_multiplier).to_u64().unwrap_or(0);

        // 7. Execute settlement on-chain
        info!("Executing atomic off-chain settlement on-chain for matching engine...");
        let signature = self.blockchain_service.execute_settle_offchain_match(
            market_pubkey,
            &buyer_payload,
            &seller_payload,
            match_amount_u64,
            match_price_u64,
            wheeling_charge_u64,
            loss_cost_u64,
            &buyer_currency_ata,
            &seller_currency_ata,
            &seller_energy_ata,
            &buyer_energy_ata,
            &fee_collector_ata,
            &wheeling_collector_ata,
            &loss_collector_ata,
            &currency_mint,
            &energy_mint,
        ).await?;

        Ok(signature.to_string())
    }
}
