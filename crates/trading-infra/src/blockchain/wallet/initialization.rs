//! Wallet Initialization Service
//!
//! This service handles:
//! - Generating new wallets for users without encrypted keys
//! - Re-encrypting wallets with old/incompatible encryption format (16-byte IV → 12-byte nonce)
//! - Validating existing wallet encryption

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use solana_sdk::signature::Signer;
use sqlx::{PgPool, Row};
use std::time::Instant;
use tracing::{info, warn};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::blockchain::rpc::BlockchainService;
use crate::blockchain::WalletService;

/// Standard nonce size for AES-GCM (12 bytes)
const STANDARD_NONCE_LEN: usize = 12;
/// Legacy IV size that some old records might have (16 bytes)
const LEGACY_IV_LEN: usize = 16;

#[derive(sqlx::FromRow)]
struct UserRow {
    id: Uuid,
    username: String,
    email: String,
    wallet_address: Option<String>,
    encrypted_private_key: Option<Vec<u8>>,
    wallet_salt: Option<Vec<u8>>,
    encryption_iv: Option<Vec<u8>>,
    _role: Option<String>,
}

/// Service for initializing and fixing user wallets
pub struct WalletInitializationService {
    db: PgPool,
    encryption_secret: String,
    blockchain_service: BlockchainService,
}

/// Status of a user's wallet
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub enum WalletStatus {
    /// No wallet data at all
    NoWallet,
    /// Has wallet address but no encrypted keys
    AddressOnlyNoKeys,
    /// Has encrypted keys with correct format (12-byte nonce)
    ValidEncryption,
    /// Has encrypted keys with legacy format (16-byte IV)
    LegacyEncryption,
    /// Encryption data is corrupted or invalid
    CorruptedEncryption,
}

/// Result of wallet diagnosis for a single user
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct WalletDiagnosis {
    pub user_id: Uuid,
    pub username: String,
    pub email: String,
    pub wallet_address: Option<String>,
    pub status: WalletStatus,
    pub nonce_size: Option<usize>,
    pub can_decrypt: bool,
    pub needs_action: bool,
    pub recommended_action: String,
}

/// Result of wallet fix operation
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct WalletFixResult {
    pub user_id: Uuid,
    pub success: bool,
    pub action_taken: String,
    pub new_wallet_address: Option<String>,
    pub error: Option<String>,
}

/// Report of batch wallet initialization/fix
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct WalletInitializationReport {
    pub total_users: usize,
    pub users_without_wallet: usize,
    pub users_with_legacy_encryption: usize,
    pub users_with_valid_encryption: usize,
    pub users_with_corrupted_encryption: usize,
    pub wallets_created: usize,
    pub wallets_re_encrypted: usize,
    pub errors: Vec<String>,
    pub duration_seconds: f64,
}

impl WalletInitializationService {
    pub fn new(
        db: PgPool,
        encryption_secret: String,
        blockchain_service: BlockchainService,
    ) -> Self {
        Self {
            db,
            encryption_secret,
            blockchain_service,
        }
    }

    /// Diagnose wallet status for a single user
    pub async fn diagnose_user_wallet(&self, user_id: Uuid) -> Result<WalletDiagnosis> {
        let wallet_data = sqlx::query_as::<_, UserRow>(
            "SELECT id, username, email, wallet_address, encrypted_private_key, wallet_salt, encryption_iv FROM users WHERE id = $1"
        )
        .bind(user_id)
        .fetch_optional(&self.db)
        .await?
        .ok_or_else(|| anyhow!("User not found"))?;

        self.diagnose_wallet_data(
            wallet_data.id,
            &wallet_data.username,
            &wallet_data.email,
            wallet_data.wallet_address.as_deref(),
            wallet_data.encrypted_private_key.as_deref(),
            wallet_data.wallet_salt.as_deref(),
            wallet_data.encryption_iv.as_deref(),
        )
        .await
    }

    /// Diagnose wallet status for all users
    pub async fn diagnose_all_users(&self) -> Result<Vec<WalletDiagnosis>> {
        let users = sqlx::query_as::<_, UserRow>(
            "SELECT id, username, email, wallet_address, encrypted_private_key, wallet_salt, encryption_iv FROM users ORDER BY created_at DESC"
        )
        .fetch_all(&self.db)
        .await?;

        let mut diagnoses = Vec::new();
        for user in users {
            let diagnosis = self
                .diagnose_wallet_data(
                    user.id,
                    &user.username,
                    &user.email,
                    user.wallet_address.as_deref(),
                    user.encrypted_private_key.as_deref(),
                    user.wallet_salt.as_deref(),
                    user.encryption_iv.as_deref(),
                )
                .await?;
            diagnoses.push(diagnosis);
        }

        Ok(diagnoses)
    }

    /// Helper to diagnose wallet data
    async fn diagnose_wallet_data(
        &self,
        user_id: Uuid,
        username: &str,
        email: &str,
        wallet_address: Option<&str>,
        encrypted_key: Option<&[u8]>,
        salt: Option<&[u8]>,
        iv: Option<&[u8]>,
    ) -> Result<WalletDiagnosis> {
        let (status, nonce_size, can_decrypt) = match (encrypted_key, salt, iv) {
            (Some(key), Some(s), Some(i)) => {
                let nonce_len = i.len();

                // Try to decrypt to verify
                let decrypt_result =
                    WalletService::decrypt_bytes(key, s, i, &self.encryption_secret);

                let can_decrypt = decrypt_result.is_ok();

                let status = if nonce_len == STANDARD_NONCE_LEN {
                    if can_decrypt {
                        WalletStatus::ValidEncryption
                    } else {
                        WalletStatus::CorruptedEncryption
                    }
                } else if nonce_len == LEGACY_IV_LEN {
                    WalletStatus::LegacyEncryption
                } else {
                    WalletStatus::CorruptedEncryption
                };

                (status, Some(nonce_len), can_decrypt)
            }
            (None, None, None) => {
                if wallet_address.is_some() {
                    (WalletStatus::AddressOnlyNoKeys, None, false)
                } else {
                    (WalletStatus::NoWallet, None, false)
                }
            }
            _ => (WalletStatus::CorruptedEncryption, None, false),
        };

        let needs_action = matches!(
            status,
            WalletStatus::NoWallet
                | WalletStatus::AddressOnlyNoKeys
                | WalletStatus::LegacyEncryption
                | WalletStatus::CorruptedEncryption
        );

        let recommended_action = match &status {
            WalletStatus::NoWallet => "Generate new wallet".to_string(),
            WalletStatus::AddressOnlyNoKeys => {
                "Generate new wallet (old address will be replaced)".to_string()
            }
            WalletStatus::ValidEncryption => "No action needed".to_string(),
            WalletStatus::LegacyEncryption => "Re-encrypt with standard 12-byte nonce".to_string(),
            WalletStatus::CorruptedEncryption => {
                "Generate new wallet (data is corrupted)".to_string()
            }
        };

        Ok(WalletDiagnosis {
            user_id,
            username: username.to_string(),
            email: email.to_string(),
            wallet_address: wallet_address.map(|s| s.to_string()),
            status,
            nonce_size,
            can_decrypt,
            needs_action,
            recommended_action,
        })
    }

    /// Fix a single user's wallet
    pub async fn fix_user_wallet(
        &self,
        user_id: Uuid,
        force_regenerate: bool,
    ) -> Result<WalletFixResult> {
        let diagnosis = self.diagnose_user_wallet(user_id).await?;

        if !diagnosis.needs_action && !force_regenerate {
            return Ok(WalletFixResult {
                user_id,
                success: true,
                action_taken: "No action needed - wallet is valid".to_string(),
                new_wallet_address: diagnosis.wallet_address,
                error: None,
            });
        }

        match diagnosis.status {
            WalletStatus::NoWallet
            | WalletStatus::AddressOnlyNoKeys
            | WalletStatus::CorruptedEncryption => {
                // Generate new wallet
                self.generate_wallet_for_user(user_id).await
            }
            WalletStatus::LegacyEncryption => {
                // Re-encrypt with proper nonce size
                self.re_encrypt_wallet(user_id).await
            }
            WalletStatus::ValidEncryption => {
                if force_regenerate {
                    self.generate_wallet_for_user(user_id).await
                } else {
                    Ok(WalletFixResult {
                        user_id,
                        success: true,
                        action_taken: "No action needed".to_string(),
                        new_wallet_address: diagnosis.wallet_address,
                        error: None,
                    })
                }
            }
        }
    }

    /// Generate a new wallet for a user
    async fn generate_wallet_for_user(&self, user_id: Uuid) -> Result<WalletFixResult> {
        info!("Generating new wallet for user: {}", user_id);

        // Get user's role for on-chain registration
        let user = sqlx::query("SELECT role::text as role FROM users WHERE id = $1")
            .bind(user_id)
            .fetch_one(&self.db)
            .await?;

        let wallet_service = &self.blockchain_service.core.wallet_service;

        // Check if Solana Bridge is available
        if wallet_service.health_check().await.is_err() {
            return Err(anyhow!(
                "Solana Bridge not available. Please ensure Chain Bridge is running."
            ));
        }

        // Create new keypair
        let keypair = WalletService::create_keypair();
        let pubkey = keypair.pubkey();
        let wallet_address = pubkey.to_string();

        // Airdrop some SOL for development
        if let Err(e) = wallet_service.request_airdrop(&pubkey, 1.0).await {
            warn!("Airdrop failed (non-blocking): {}", e);
        }

        // Wait for airdrop confirmation
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        // Encrypt private key for storage
        let (encrypted_key, salt, iv) =
            WalletService::encrypt_to_bytes(&keypair.to_bytes(), &self.encryption_secret)?;

        // Update database
        sqlx::query(
            "UPDATE users SET wallet_address = $1, encrypted_private_key = $2, wallet_salt = $3, encryption_iv = $4, updated_at = NOW() WHERE id = $5"
        )
        .bind(&wallet_address)
        .bind(&encrypted_key)
        .bind(&salt)
        .bind(&iv)
        .bind(user_id)
        .execute(&self.db)
        .await?;

        // Register user on-chain
        let user_type: u8 = match user.get::<Option<String>, _>("role").as_deref() {
            Some("prosumer") => 0,
            Some("consumer") => 1,
            _ => 1, // Default to consumer
        };

        match self
            .blockchain_service
            .register_user_on_chain(&keypair, user_type, 0, 0, 0, 0)
            .await
        {
            Ok(sig) => {
                info!(
                    "User {} registered on-chain successfully. Signature: {}",
                    user_id, sig
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            }
            Err(e) => {
                warn!(
                    "Failed to register user {} on-chain (non-blocking): {}",
                    user_id, e
                );
            }
        }

        info!(
            "Successfully generated wallet {} for user {}",
            wallet_address, user_id
        );

        Ok(WalletFixResult {
            user_id,
            success: true,
            action_taken: "Generated new wallet with proper encryption".to_string(),
            new_wallet_address: Some(wallet_address),
            error: None,
        })
    }

    /// Re-encrypt a wallet with legacy 16-byte IV to standard 12-byte nonce
    async fn re_encrypt_wallet(&self, user_id: Uuid) -> Result<WalletFixResult> {
        info!("Re-encrypting wallet for user: {}", user_id);

        // Fetch current encrypted data
        let wallet_data = sqlx::query_as::<_, UserRow>(
            "SELECT id, username, email, encrypted_private_key, wallet_salt, encryption_iv, wallet_address, role::text as role FROM users WHERE id = $1"
        )
        .bind(user_id)
        .fetch_one(&self.db)
        .await?;

        let encrypted_key = wallet_data
            .encrypted_private_key
            .ok_or_else(|| anyhow!("Missing encryption key for user {}", user_id))?;
        let salt = wallet_data
            .wallet_salt
            .ok_or_else(|| anyhow!("Missing wallet salt for user {}", user_id))?;
        let iv = wallet_data
            .encryption_iv
            .ok_or_else(|| anyhow!("Missing encryption IV for user {}", user_id))?;

        // Decrypt with old format (crypto module handles legacy IV)
        let decrypted_bytes =
            WalletService::decrypt_bytes(&encrypted_key, &salt, &iv, &self.encryption_secret)?;

        // Re-encrypt with new standard format
        let (new_encrypted, new_salt, new_iv) =
            WalletService::encrypt_to_bytes(&decrypted_bytes, &self.encryption_secret)?;

        // Verify new nonce is correct size
        if new_iv.len() != STANDARD_NONCE_LEN {
            return Err(anyhow!(
                "New encryption produced wrong nonce size: {}",
                new_iv.len()
            ));
        }

        // Update database
        sqlx::query("UPDATE users SET encrypted_private_key = $1, wallet_salt = $2, encryption_iv = $3, updated_at = NOW() WHERE id = $4")
            .bind(&new_encrypted[..])
            .bind(&new_salt[..])
            .bind(&new_iv[..])
            .bind(user_id)
            .execute(&self.db)
            .await?;

        info!(
            "Successfully re-encrypted wallet for user {} (IV: {} -> {} bytes)",
            user_id,
            iv.len(),
            new_iv.len()
        );

        Ok(WalletFixResult {
            user_id,
            success: true,
            action_taken: format!(
                "Re-encrypted wallet with standard {}-byte nonce",
                STANDARD_NONCE_LEN
            ),
            new_wallet_address: wallet_data.wallet_address,
            error: None,
        })
    }

    /// Fix all users with wallet issues
    pub async fn fix_all_users(&self) -> Result<WalletInitializationReport> {
        let start_time = Instant::now();
        info!("Starting wallet initialization for all users");

        let diagnoses = self.diagnose_all_users().await?;

        let total_users = diagnoses.len();
        let mut users_without_wallet = 0;
        let mut users_with_legacy_encryption = 0;
        let mut users_with_valid_encryption = 0;
        let mut users_with_corrupted_encryption = 0;
        let mut wallets_created = 0;
        let mut wallets_re_encrypted = 0;
        let mut errors = Vec::new();

        for diagnosis in &diagnoses {
            match diagnosis.status {
                WalletStatus::NoWallet | WalletStatus::AddressOnlyNoKeys => {
                    users_without_wallet += 1;
                }
                WalletStatus::LegacyEncryption => {
                    users_with_legacy_encryption += 1;
                }
                WalletStatus::ValidEncryption => {
                    users_with_valid_encryption += 1;
                }
                WalletStatus::CorruptedEncryption => {
                    users_with_corrupted_encryption += 1;
                }
            }
        }

        // Fix users that need it
        for diagnosis in diagnoses {
            if !diagnosis.needs_action {
                continue;
            }

            match self.fix_user_wallet(diagnosis.user_id, false).await {
                Ok(result) => {
                    if result.success {
                        if result.action_taken.contains("Generated") {
                            wallets_created += 1;
                        } else if result.action_taken.contains("Re-encrypted") {
                            wallets_re_encrypted += 1;
                        }
                    } else if let Some(err) = result.error {
                        errors.push(format!("User {}: {}", diagnosis.user_id, err));
                    }
                }
                Err(e) => {
                    let error_msg = format!("User {}: {}", diagnosis.user_id, e);
                    warn!("{}", error_msg);
                    errors.push(error_msg);
                }
            }
        }

        let duration = start_time.elapsed().as_secs_f64();
        info!(
            "Wallet initialization completed: {} created, {} re-encrypted, {} errors in {:.2}s",
            wallets_created,
            wallets_re_encrypted,
            errors.len(),
            duration
        );

        Ok(WalletInitializationReport {
            total_users,
            users_without_wallet,
            users_with_legacy_encryption,
            users_with_valid_encryption,
            users_with_corrupted_encryption,
            wallets_created,
            wallets_re_encrypted,
            errors,
            duration_seconds: duration,
        })
    }

    /// Initialize wallets for specific test users
    pub async fn initialize_test_users(&self, emails: &[&str]) -> Result<Vec<WalletFixResult>> {
        let mut results = Vec::new();

        for email in emails {
            let user = sqlx::query("SELECT id FROM users WHERE email = $1")
                .bind(*email)
                .fetch_optional(&self.db)
                .await?;

            match user {
                Some(u) => {
                    let user_id: Uuid = u.get("id");
                    let result = self.fix_user_wallet(user_id, false).await?;
                    results.push(result);
                }
                None => {
                    results.push(WalletFixResult {
                        user_id: Uuid::nil(),
                        success: false,
                        action_taken: "User not found".to_string(),
                        new_wallet_address: None,
                        error: Some(format!("No user found with email: {}", email)),
                    });
                }
            }
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nonce_size_constants() {
        // AES-GCM standard nonce is 12 bytes
        assert_eq!(STANDARD_NONCE_LEN, 12);
        // Legacy IV was 16 bytes
        assert_eq!(LEGACY_IV_LEN, 16);
    }
}
