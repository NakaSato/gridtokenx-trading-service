use crate::rpc::TransactionHandler;
use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose, Engine as _};
use hmac::Hmac;
use pbkdf2::pbkdf2;
use rand::{rngs::OsRng, RngCore};
use sha2::Sha256;
use solana_sdk::{
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
};
use std::fs;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info};

/// Service for managing Solana wallets across GridTokenX microservices
#[derive(Clone)]
pub struct WalletService {
    transaction_handler: TransactionHandler,
    /// The authority keypair (cached in memory)
    authority_keypair: Arc<RwLock<Option<Arc<Keypair>>>>,
    /// Path to wallet file (if loading from file)
    wallet_path: Option<String>,
}

impl std::fmt::Debug for WalletService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalletService")
            .field("wallet_path", &self.wallet_path)
            .finish_non_exhaustive()
    }
}

impl WalletService {
    /// Create a new WalletService instance
    pub fn new(transaction_handler: TransactionHandler) -> Self {
        info!("Initializing WalletService with Chain Bridge transaction handler");
        Self {
            transaction_handler,
            authority_keypair: Arc::new(RwLock::new(None)),
            wallet_path: None,
        }
    }

    /// Create wallet service with a specific wallet file path
    pub fn with_path<S: Into<String>>(
        transaction_handler: TransactionHandler,
        wallet_path: S,
    ) -> Self {
        let path = wallet_path.into();
        info!(
            "Initializing WalletService with Chain Bridge and wallet path: {}",
            path
        );
        Self {
            transaction_handler,
            authority_keypair: Arc::new(RwLock::new(None)),
            wallet_path: Some(path),
        }
    }

    /// Create a new Solana keypair
    pub fn create_keypair() -> Keypair {
        let keypair = Keypair::new();
        info!("Created new keypair with pubkey: {}", keypair.pubkey());
        keypair
    }

    /// Get wallet balance in lamports
    pub async fn get_balance(&self, pubkey: &Pubkey, _force_refresh: bool) -> Result<u64> {
        let request = crate::rpc::chain_v1::GetBalanceRequest {
            pubkey: pubkey.to_string(),
            force_refresh: _force_refresh,
        };
        match self.transaction_handler.get_balance(request).await {
            Ok(response) => {
                info!(
                    "Retrieved balance for {}: {} lamports",
                    pubkey, response.lamports
                );
                Ok(response.lamports)
            }
            Err(e) => {
                error!("Failed to get balance for {}: {}", pubkey, e);
                Err(e.into())
            }
        }
    }

    /// Request airdrop (localnet/devnet only)
    pub async fn request_airdrop(&self, pubkey: &Pubkey, amount_sol: f64) -> Result<Signature> {
        let lamports = (amount_sol * 1_000_000_000.0) as u64;

        info!(
            "Requesting airdrop of {} SOL ({} lamports) for {}",
            amount_sol, lamports, pubkey
        );

        self.transaction_handler
            .request_airdrop(pubkey, lamports)
            .await
    }

    /// Confirm transaction
    pub async fn confirm_transaction(&self, signature: &Signature) -> Result<bool> {
        self.transaction_handler
            .confirm_transaction(&signature.to_string())
            .await
    }

    /// Validate Solana address format
    pub fn is_valid_address(address: &str) -> bool {
        Pubkey::from_str(address).is_ok()
    }

    /// Get recent blockhash
    pub async fn get_recent_blockhash(&self) -> Result<solana_sdk::hash::Hash> {
        self.transaction_handler.get_latest_blockhash().await
    }

    /// Check if connection is healthy
    pub async fn health_check(&self) -> Result<bool> {
        self.transaction_handler
            .get_latest_blockhash()
            .await
            .map(|_| true)
    }

    /// Load authority keypair from file
    pub async fn load_from_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let path_ref = path.as_ref();
        info!("Loading authority keypair from: {:?}", path_ref);

        let contents = fs::read_to_string(path_ref)
            .with_context(|| format!("Failed to read wallet file: {:?}", path_ref))?;

        let keypair_bytes: Vec<u8> = serde_json::from_str(&contents)
            .with_context(|| "Failed to parse wallet file as JSON array")?;

        if keypair_bytes.len() != 64 {
            return Err(anyhow!(
                "Invalid keypair file: expected 64 bytes, got {}",
                keypair_bytes.len()
            ));
        }

        let secret_key: [u8; 32] = keypair_bytes[..32]
            .try_into()
            .map_err(|_| anyhow!("Failed to extract secret key"))?;
        let keypair = Keypair::new_from_array(secret_key);

        let mut lock = self.authority_keypair.write().await;
        *lock = Some(Arc::new(keypair));
        Ok(())
    }

    /// Load authority keypair from environment variable
    pub async fn load_from_env(&self) -> Result<()> {
        let private_key_str = std::env::var("AUTHORITY_WALLET_PRIVATE_KEY")
            .with_context(|| "AUTHORITY_WALLET_PRIVATE_KEY environment variable not set")?;

        let keypair_bytes = bs58::decode(&private_key_str)
            .into_vec()
            .with_context(|| "Failed to decode base58 private key")?;

        if keypair_bytes.len() != 64 {
            return Err(anyhow!("Invalid private key size"));
        }

        let secret_key: [u8; 32] = keypair_bytes[..32]
            .try_into()
            .map_err(|_| anyhow!("Failed extract secret"))?;
        let keypair = Keypair::new_from_array(secret_key);

        let mut lock = self.authority_keypair.write().await;
        *lock = Some(Arc::new(keypair));
        Ok(())
    }

    /// Initialize authority wallet
    pub async fn initialize_authority(&self) -> Result<()> {
        let loaded = if let Some(ref path) = self.wallet_path {
            if Path::new(path).exists() {
                self.load_from_file(path).await.is_ok()
            } else {
                false
            }
        } else {
            let default_paths = vec!["./dev-wallet.json", "../dev-wallet.json"];
            let mut success = false;
            for path in default_paths {
                if Path::new(path).exists() && self.load_from_file(path).await.is_ok() {
                    success = true;
                    break;
                }
            }
            success
        };

        if !loaded && std::env::var("AUTHORITY_WALLET_PRIVATE_KEY").is_ok() {
            self.load_from_env().await?;
        }

        Ok(())
    }

    pub async fn get_authority_keypair(&self) -> Result<Arc<Keypair>> {
        let lock = self.authority_keypair.read().await;
        lock.as_ref()
            .cloned()
            .ok_or_else(|| anyhow!("Authority keypair not loaded"))
    }

    /// Encrypt a private key using a password and a master secret
    pub fn encrypt_private_key(
        password: &str,
        master_secret: &str,
        private_key: &[u8],
    ) -> Result<(String, String, String)> {
        let mut salt = [0u8; 16];
        OsRng.fill_bytes(&mut salt);

        use hmac::Mac;
        let mut mac =
            <Hmac<Sha256> as hmac::digest::KeyInit>::new_from_slice(master_secret.as_bytes())?;
        mac.update(password.as_bytes());
        let sharded_key_material = mac.finalize().into_bytes();

        let mut derived_key = [0u8; 32];
        pbkdf2::<Hmac<Sha256>>(&sharded_key_material, &salt, 100_000, &mut derived_key)
            .map_err(|e| anyhow!("{:?}", e))?;

        let mut iv = [0u8; 12];
        OsRng.fill_bytes(&mut iv);
        let nonce = Nonce::from_slice(&iv);

        let cipher = Aes256Gcm::new(&derived_key.into());
        let encrypted_data = cipher
            .encrypt(nonce, private_key)
            .map_err(|e| anyhow!("{}", e))?;

        Ok((
            general_purpose::STANDARD.encode(encrypted_data),
            general_purpose::STANDARD.encode(salt),
            general_purpose::STANDARD.encode(iv),
        ))
    }

    /// Decrypt a private key using a password and a master secret
    pub fn decrypt_private_key(
        password: &str,
        master_secret: &str,
        encrypted_data_b64: &str,
        salt_b64: &str,
        iv_b64: &str,
    ) -> Result<Vec<u8>> {
        let encrypted_data = general_purpose::STANDARD.decode(encrypted_data_b64)?;
        let salt = general_purpose::STANDARD.decode(salt_b64)?;
        let iv = general_purpose::STANDARD.decode(iv_b64)?;

        use hmac::Mac;
        let mut mac =
            <Hmac<Sha256> as hmac::digest::KeyInit>::new_from_slice(master_secret.as_bytes())?;
        mac.update(password.as_bytes());
        let sharded_key_material = mac.finalize().into_bytes();

        let mut derived_key = [0u8; 32];
        pbkdf2::<Hmac<Sha256>>(&sharded_key_material, &salt, 100_000, &mut derived_key)
            .map_err(|e| anyhow!("{:?}", e))?;

        let cipher = Aes256Gcm::new(&derived_key.into());
        let nonce = Nonce::from_slice(&iv);
        let plaintext = cipher
            .decrypt(nonce, encrypted_data.as_ref())
            .map_err(|_| anyhow!("Decryption failed"))?;

        Ok(plaintext)
    }

    /// Encrypt bytes using a secret (standard 12-byte nonce)
    pub fn encrypt_to_bytes(data: &[u8], secret: &str) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        // Use a dummy password since this is for direct encryption
        let (enc_b64, salt_b64, iv_b64) = Self::encrypt_private_key("direct", secret, data)?;

        let enc_data = general_purpose::STANDARD.decode(enc_b64)?;
        let salt = general_purpose::STANDARD.decode(salt_b64)?;
        let iv = general_purpose::STANDARD.decode(iv_b64)?;

        Ok((enc_data, salt, iv))
    }

    /// Decrypt bytes using a secret
    pub fn decrypt_bytes(
        encrypted_data: &[u8],
        salt: &[u8],
        iv: &[u8],
        secret: &str,
    ) -> Result<Vec<u8>> {
        let enc_b64 = general_purpose::STANDARD.encode(encrypted_data);
        let salt_b64 = general_purpose::STANDARD.encode(salt);
        let iv_b64 = general_purpose::STANDARD.encode(iv);

        Self::decrypt_private_key("direct", secret, &enc_b64, &salt_b64, &iv_b64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_valid_address() {
        // Valid pubkey
        let pubkey = solana_sdk::pubkey::Pubkey::new_unique();
        assert!(WalletService::is_valid_address(&pubkey.to_string()));

        // Invalid pubkey (too short)
        assert!(!WalletService::is_valid_address("Short"));
        // Invalid characters
        assert!(!WalletService::is_valid_address(
            "6VERv8NMv79y6YisJ1L8hS5pM9gN6Z45326YtW9b0"
        )); // Base58 doesn't have 0
    }

    #[test]
    fn test_encryption_roundtrip() {
        let password = "test-password";
        let master_secret = "master-secret-12345678901234567890";
        let private_key = b"this-is-a-secret-private-key-data-32b";

        // Encrypt
        let (encrypted, salt, iv) =
            WalletService::encrypt_private_key(password, master_secret, private_key)
                .expect("Encryption failed");

        assert!(!encrypted.is_empty());
        assert!(!salt.is_empty());
        assert!(!iv.is_empty());

        // Decrypt
        let decrypted =
            WalletService::decrypt_private_key(password, master_secret, &encrypted, &salt, &iv)
                .expect("Decryption failed");

        assert_eq!(decrypted, private_key);
    }

    #[test]
    fn test_decryption_failure() {
        let password = "test-password";
        let master_secret = "master-secret";
        let private_key = b"secret-data";

        let (encrypted, salt, iv) =
            WalletService::encrypt_private_key(password, master_secret, private_key).unwrap();

        // Wrong password
        let result = WalletService::decrypt_private_key(
            "wrong-password",
            master_secret,
            &encrypted,
            &salt,
            &iv,
        );
        assert!(result.is_err());
    }
}
