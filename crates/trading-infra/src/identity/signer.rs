use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Signature, Signer};
use std::sync::Arc;
use trading_core::traits::IdentityGateway;
use uuid::Uuid;

/// A Solana Signer implementation that delegates signing to the IAM Identity Service.
/// Note: Since the Signer trait is synchronous but gRPC is asynchronous, 
/// we use a block_on or specialized handling for the synchronous methods.
pub struct CustodialSigner {
    user_id: Uuid,
    pubkey: Pubkey,
    gateway: Arc<dyn IdentityGateway>,
}

impl CustodialSigner {
    pub fn new(user_id: Uuid, pubkey: Pubkey, gateway: Arc<dyn IdentityGateway>) -> Self {
        Self {
            user_id,
            pubkey,
            gateway,
        }
    }
}

impl Signer for CustodialSigner {
    fn pubkey(&self) -> Pubkey {
        self.pubkey
    }

    fn try_pubkey(&self) -> Result<Pubkey, solana_sdk::signer::SignerError> {
        Ok(self.pubkey)
    }

    fn sign_message(&self, message: &[u8]) -> Signature {
        self.try_sign_message(message).expect("Signing failed")
    }

    fn try_sign_message(
        &self,
        message: &[u8],
    ) -> Result<Signature, solana_sdk::signer::SignerError> {
        let handle = tokio::runtime::Handle::current();
        let user_id = self.user_id;
        let wallet_address = Some(self.pubkey.to_string());
        let gateway = self.gateway.clone();
        let message_vec = message.to_vec();

        let sig_bytes = tokio::task::block_in_place(|| {
            handle.block_on(async move {
                gateway.sign_message(user_id, wallet_address, message_vec).await
            })
        }).map_err(|e| {
            solana_sdk::signer::SignerError::Custom(format!("IAM signing failed: {}", e))
        })?;

        Signature::try_from(sig_bytes.as_slice()).map_err(|e| {
            solana_sdk::signer::SignerError::Custom(format!("Invalid signature received: {}", e))
        })
    }

    fn is_interactive(&self) -> bool {
        false
    }
}
