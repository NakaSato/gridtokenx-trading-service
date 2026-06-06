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

// --- KDF versioning (PBKDF2-HMAC-SHA256 iteration count) ---
//
// Custodial keys encrypted by this module are stored by IAM. The iteration
// count is migration-sensitive: changing it in place makes existing records
// undecryptable (see SKILL.md invariant 6 + WALLET_KDF_MIGRATION_DESIGN.md).
// So the count is selected by an explicit version, never hardcoded at the call
// site. v1 = legacy 100k; v2 = OWASP-2023 600k. Only the iteration count
// differs between versions — same HMAC pre-step, salt(16), iv(12), AES-256-GCM.
const PBKDF2_ITERS_V1: u32 = 100_000;
const PBKDF2_ITERS_V2: u32 = 600_000;

/// Version written for **new** custodial-key encryption via the versioned API.
/// The legacy 3-arg [`WalletService::encrypt_private_key`] intentionally stays
/// at v1 for byte-for-byte back-compat; callers that want v2 must pass a version
/// explicitly (and store it) so decrypt can pick the matching iteration count.
pub const CURRENT_KDF_VERSION: u8 = 2;

/// Single source of truth mapping a stored KDF version to its iteration count.
/// An unknown version is an error rather than a silent wrong-iters decrypt.
fn pbkdf2_iters(version: u8) -> Result<u32> {
    match version {
        1 => Ok(PBKDF2_ITERS_V1),
        2 => Ok(PBKDF2_ITERS_V2),
        v => Err(anyhow!("unsupported KDF version {v}")),
    }
}

/// True if a record stored under `version` should be re-encrypted to
/// [`CURRENT_KDF_VERSION`]. IAM calls this after a successful decrypt and, when
/// true, re-wraps the key (lazy migration — the password is only present at
/// auth time, so there is no offline batch upgrade).
pub fn needs_rewrap(version: u8) -> bool {
    version < CURRENT_KDF_VERSION
}

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
                Err(e)
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

    /// Derive the 32-byte AES key for a given KDF `version`. Shared by encrypt
    /// and decrypt so the HMAC pre-step + iteration count can never disagree.
    fn derive_key(
        version: u8,
        password: &str,
        master_secret: &str,
        salt: &[u8],
    ) -> Result<[u8; 32]> {
        use hmac::Mac;
        let mut mac =
            <Hmac<Sha256> as hmac::digest::KeyInit>::new_from_slice(master_secret.as_bytes())?;
        mac.update(password.as_bytes());
        let sharded_key_material = mac.finalize().into_bytes();

        let iters = pbkdf2_iters(version)?;
        let mut derived_key = [0u8; 32];
        pbkdf2::<Hmac<Sha256>>(&sharded_key_material, salt, iters, &mut derived_key)
            .map_err(|e| anyhow!("{:?}", e))?;
        Ok(derived_key)
    }

    /// Encrypt a private key under an explicit KDF `version`. The caller MUST
    /// persist `version` alongside the returned `(ciphertext, salt, iv)` so the
    /// matching iteration count is used on decrypt. New custodial records should
    /// pass [`CURRENT_KDF_VERSION`].
    pub fn encrypt_private_key_versioned(
        version: u8,
        password: &str,
        master_secret: &str,
        private_key: &[u8],
    ) -> Result<(String, String, String)> {
        let mut salt = [0u8; 16];
        OsRng.fill_bytes(&mut salt);

        let derived_key = Self::derive_key(version, password, master_secret, &salt)?;

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

    /// Decrypt a private key stored under an explicit KDF `version`. Pass the
    /// version that was persisted with the record (absent/NULL in storage = v1).
    pub fn decrypt_private_key_versioned(
        version: u8,
        password: &str,
        master_secret: &str,
        encrypted_data_b64: &str,
        salt_b64: &str,
        iv_b64: &str,
    ) -> Result<Vec<u8>> {
        let encrypted_data = general_purpose::STANDARD.decode(encrypted_data_b64)?;
        let salt = general_purpose::STANDARD.decode(salt_b64)?;
        let iv = general_purpose::STANDARD.decode(iv_b64)?;

        let derived_key = Self::derive_key(version, password, master_secret, &salt)?;

        let cipher = Aes256Gcm::new(&derived_key.into());
        let nonce = Nonce::from_slice(&iv);
        let plaintext = cipher
            .decrypt(nonce, encrypted_data.as_ref())
            .map_err(|_| anyhow!("Decryption failed"))?;

        Ok(plaintext)
    }

    /// Encrypt a private key (legacy 3-arg API). Pinned to **v1 (100k iters)**
    /// for byte-for-byte back-compat with existing records and with the
    /// `encrypt_to_bytes`/`decrypt_bytes` pair. To write the stronger v2, use
    /// [`Self::encrypt_private_key_versioned`] with [`CURRENT_KDF_VERSION`] and
    /// store the version.
    pub fn encrypt_private_key(
        password: &str,
        master_secret: &str,
        private_key: &[u8],
    ) -> Result<(String, String, String)> {
        Self::encrypt_private_key_versioned(1, password, master_secret, private_key)
    }

    /// Decrypt a private key (legacy 3-arg API). Assumes **v1 (100k iters)** —
    /// the version of every record written before KDF versioning. For v2 records
    /// use [`Self::decrypt_private_key_versioned`] with the stored version.
    pub fn decrypt_private_key(
        password: &str,
        master_secret: &str,
        encrypted_data_b64: &str,
        salt_b64: &str,
        iv_b64: &str,
    ) -> Result<Vec<u8>> {
        Self::decrypt_private_key_versioned(
            1,
            password,
            master_secret,
            encrypted_data_b64,
            salt_b64,
            iv_b64,
        )
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
            WalletService::encrypt_private_key(password, master_secret, private_key)
                .expect("encryption should succeed");

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

    #[test]
    fn test_v1_roundtrip_matches_legacy_api() {
        // The versioned-v1 path and the legacy 3-arg path must be interchangeable
        // (legacy is pinned to v1). Encrypt via legacy, decrypt via versioned v1.
        let (password, master_secret, key) = ("pw", "master-secret-xyz", b"priv-key-bytes-v1");
        let (enc, salt, iv) =
            WalletService::encrypt_private_key(password, master_secret, key).unwrap();
        let out = WalletService::decrypt_private_key_versioned(
            1,
            password,
            master_secret,
            &enc,
            &salt,
            &iv,
        )
        .unwrap();
        assert_eq!(out, key);
    }

    #[test]
    fn test_v2_roundtrip() {
        let (password, master_secret, key) = ("pw", "master-secret-xyz", b"priv-key-bytes-v2");
        let (enc, salt, iv) = WalletService::encrypt_private_key_versioned(
            CURRENT_KDF_VERSION,
            password,
            master_secret,
            key,
        )
        .unwrap();
        let out = WalletService::decrypt_private_key_versioned(
            CURRENT_KDF_VERSION,
            password,
            master_secret,
            &enc,
            &salt,
            &iv,
        )
        .unwrap();
        assert_eq!(out, key);
    }

    #[test]
    fn test_cross_version_decrypt_fails() {
        // A v2 blob decrypted as v1 (wrong iteration count -> wrong key) must
        // FAIL the AES-GCM tag check, not silently return garbage.
        let (password, master_secret, key) = ("pw", "master-secret-xyz", b"priv-key-bytes");
        let (enc, salt, iv) =
            WalletService::encrypt_private_key_versioned(2, password, master_secret, key).unwrap();
        let wrong =
            WalletService::decrypt_private_key_versioned(1, password, master_secret, &enc, &salt, &iv);
        assert!(wrong.is_err(), "v2 blob must not decrypt as v1");
    }

    #[test]
    fn test_needs_rewrap_and_unknown_version() {
        assert!(needs_rewrap(1), "v1 below current -> rewrap");
        assert!(!needs_rewrap(CURRENT_KDF_VERSION), "current -> no rewrap");
        // Unknown version is a hard error, not a silent wrong-iters decrypt.
        let err = WalletService::decrypt_private_key_versioned(
            99, "pw", "ms", "AA==", "AA==", "AA==",
        );
        assert!(err.is_err());
        assert!(WalletService::encrypt_private_key_versioned(99, "pw", "ms", b"k").is_err());
    }

    #[test]
    fn test_v2_wrong_password_fails() {
        let (password, master_secret, key) = ("pw", "ms-123", b"priv");
        let (enc, salt, iv) =
            WalletService::encrypt_private_key_versioned(2, password, master_secret, key).unwrap();
        let bad = WalletService::decrypt_private_key_versioned(
            2, "nope", master_secret, &enc, &salt, &iv,
        );
        assert!(bad.is_err());
    }
}
