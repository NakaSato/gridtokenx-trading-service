use anyhow::Result;
use tracing::info;
use crate::domain::events::UserRegisteredPayload;
use crate::infra::db::DatabasePool;

/// Service responsible for managing trading-specific user data and synchronization.
pub struct ParticipantService {
    db: DatabasePool,
}

impl ParticipantService {
    pub fn new(db: DatabasePool) -> Self {
        Self { db }
    }

    /// Handles a new user registration by initializing necessary trading state.
    /// 
    /// In the current architecture, IAM and Trading share the 'users' table,
    /// so the user already exists. This service performs additional 
    /// setup like default preferences, zone assignments, or pre-warming 
    /// matching engine caches.
    pub async fn initialize_participant(&self, payload: UserRegisteredPayload) -> Result<()> {
        info!(
            user_id = %payload.user_id,
            username = %payload.username,
            "🔄 Synchronizing new market participant in Trading service"
        );

        // UPSERT user into the local database to support distributed architecture
        // Note: Using placeholders for fields managed by IAM (password, role, etc.)
        sqlx::query(
            r#"
            INSERT INTO users (id, username, email, password_hash, role, is_active, created_at, updated_at)
            VALUES ($1, $2, $3, 'external_idp_managed', 'user', true, NOW(), NOW())
            ON CONFLICT (id) DO UPDATE SET
                username = EXCLUDED.username,
                email = EXCLUDED.email,
                updated_at = NOW()
            "#
        )
            .bind(payload.user_id)
            .bind(&payload.username)
            .bind(&payload.email)
            .execute(&self.db)
            .await?;

        info!(
            user_id = %payload.user_id,
            "✅ Market participant sync completed"
        );

        Ok(())
    }

    /// Processes an on-chain onboarding event.
    pub async fn process_onboarding(&self, payload: crate::domain::events::UserOnboardedPayload) -> Result<()> {
        info!(
            user_id = %payload.user_id,
            wallet = %payload.wallet_address,
            pda = %payload.user_account_pda,
            "🔄 Processing on-chain onboarding success in Trading service"
        );

        // 1. Update user-level blockchain status
        sqlx::query(
            "UPDATE users SET blockchain_registered = true, user_account_pda = $2 WHERE id = $1"
        )
        .bind(payload.user_id)
        .bind(&payload.user_account_pda)
        .execute(&self.db)
        .await?;

        // 2. Update wallet-level blockchain status
        sqlx::query(
            r#"
            INSERT INTO user_wallets (user_id, wallet_address, blockchain_registered, user_account_pda, shard_id, blockchain_tx_signature, created_at)
            VALUES ($1, $2, true, $3, $4, $5, NOW())
            ON CONFLICT (wallet_address) DO UPDATE SET
                blockchain_registered = true,
                user_account_pda = EXCLUDED.user_account_pda,
                shard_id = EXCLUDED.shard_id,
                blockchain_tx_signature = EXCLUDED.blockchain_tx_signature
            "#
        )
        .bind(payload.user_id)
        .bind(&payload.wallet_address)
        .bind(&payload.user_account_pda)
        .bind(payload.shard_id as i16)
        .bind(&payload.transaction_signature)
        .execute(&self.db)
        .await?;

        info!(user_id = %payload.user_id, "✅ Database state synchronized for on-chain registration");

        Ok(())
    }

    /// Syncs multi-wallet registration metadata from IAM to Trading.
    pub async fn sync_wallet_metadata(&self, payload: crate::domain::events::UserWalletLinkedPayload) -> Result<()> {
        info!(
            user_id = %payload.user_id,
            wallet = %payload.wallet_address,
            "🔄 Synchronizing multi-wallet registration data in Trading service"
        );

        // UPSERT wallet metadata into user_wallets table
        sqlx::query(
            r#"
            INSERT INTO user_wallets (user_id, wallet_address, blockchain_registered, user_account_pda, shard_id, blockchain_tx_signature, created_at)
            VALUES ($1, $2, true, $3, $4, $5, NOW())
            ON CONFLICT (wallet_address) DO UPDATE SET
                blockchain_registered = true,
                user_account_pda = EXCLUDED.user_account_pda,
                shard_id = EXCLUDED.shard_id,
                blockchain_tx_signature = EXCLUDED.blockchain_tx_signature
            "#
        )
        .bind(payload.user_id)
        .bind(&payload.wallet_address)
        .bind(&payload.user_account_pda)
        .bind(payload.shard_id as i16)
        .bind(&payload.transaction_signature)
        .execute(&self.db)
        .await?;

        Ok(())
    }
}
