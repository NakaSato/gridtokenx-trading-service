//! Verification of the wallet Ed25519 signature that authorises an order for
//! per-user-escrow settlement.
//!
//! # Why the server re-derives the message
//!
//! The browser signs the canonical payload (see
//! [`trading_core::offchain_payload`]) and sends the signature plus the order
//! terms. If we stored the client's bytes verbatim and replayed them into the
//! Ed25519 verify instruction, a client could sign one price and submit another:
//! the signature would verify against its own bytes while the order book carried
//! different terms, and settlement would move funds on terms the user never
//! agreed to. So we rebuild the message from the values we are about to persist
//! and verify the signature against *that*. A mismatch is rejected at placement.
//!
//! Note this is distinct from `CreateOrderRequest::signature`, which is an
//! HMAC-SHA256 of the request for API anti-tamper — a different mechanism for a
//! different purpose. Do not conflate them.

use ed25519_dalek::{Signature, VerifyingKey};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum OrderSignatureError {
    #[error("wallet address is not valid Base58: {0}")]
    BadWallet(String),
    #[error("wallet address is not a 32-byte Ed25519 public key")]
    WalletNotEd25519,
    #[error("signature is not valid Base58: {0}")]
    BadSignatureEncoding(String),
    #[error("signature is not 64 bytes")]
    SignatureWrongLength,
    #[error("signature does not match the order terms for this wallet")]
    VerificationFailed,
}

/// Verify `signature_base58` is `wallet_base58`'s signature over `message`.
///
/// Uses `verify_strict` to reject small-order / torsion-component public keys,
/// which `verify` would accept — those admit signatures that validate under more
/// than one key, and this signature is what authorises moving a user's escrow.
pub fn verify_order_signature(
    wallet_base58: &str,
    signature_base58: &str,
    message: &[u8],
) -> Result<(), OrderSignatureError> {
    let wallet_bytes = bs58::decode(wallet_base58)
        .into_vec()
        .map_err(|e| OrderSignatureError::BadWallet(e.to_string()))?;
    let wallet: [u8; 32] = wallet_bytes
        .try_into()
        .map_err(|_| OrderSignatureError::WalletNotEd25519)?;
    let verifying_key =
        VerifyingKey::from_bytes(&wallet).map_err(|_| OrderSignatureError::WalletNotEd25519)?;

    let sig_bytes = bs58::decode(signature_base58)
        .into_vec()
        .map_err(|e| OrderSignatureError::BadSignatureEncoding(e.to_string()))?;
    let sig_bytes: [u8; 64] = sig_bytes
        .try_into()
        .map_err(|_| OrderSignatureError::SignatureWrongLength)?;
    let signature = Signature::from_bytes(&sig_bytes);

    verifying_key
        .verify_strict(message, &signature)
        .map_err(|_| OrderSignatureError::VerificationFailed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};
    use trading_core::offchain_payload::{message_for, SIDE_BUY, SIDE_SELL};

    fn key() -> SigningKey {
        SigningKey::from_bytes(&[7u8; 32])
    }

    fn msg(side: u8, price: u64) -> Vec<u8> {
        message_for(&[1u8; 16], &key().verifying_key().to_bytes(), 5_000_000_000, price, side, 1, 1_800_000_000)
    }

    fn signed(message: &[u8]) -> (String, String) {
        let sk = key();
        let wallet = bs58::encode(sk.verifying_key().to_bytes()).into_string();
        let sig = bs58::encode(sk.sign(message).to_bytes()).into_string();
        (wallet, sig)
    }

    #[test]
    fn accepts_a_genuine_signature() {
        let m = msg(SIDE_SELL, 2_500_000);
        let (wallet, sig) = signed(&m);
        assert_eq!(verify_order_signature(&wallet, &sig, &m), Ok(()));
    }

    /// The attack this exists to stop: sign a cheap price, submit an expensive one.
    #[test]
    fn rejects_a_signature_over_different_terms() {
        let signed_msg = msg(SIDE_SELL, 2_500_000);
        let (wallet, sig) = signed(&signed_msg);

        let submitted = msg(SIDE_SELL, 9_900_000); // different price
        assert_eq!(
            verify_order_signature(&wallet, &sig, &submitted),
            Err(OrderSignatureError::VerificationFailed)
        );
    }

    #[test]
    fn rejects_a_flipped_side() {
        let signed_msg = msg(SIDE_SELL, 2_500_000);
        let (wallet, sig) = signed(&signed_msg);
        assert_eq!(
            verify_order_signature(&wallet, &sig, &msg(SIDE_BUY, 2_500_000)),
            Err(OrderSignatureError::VerificationFailed)
        );
    }

    #[test]
    fn rejects_another_wallets_signature() {
        let m = msg(SIDE_SELL, 2_500_000);
        let (_, sig) = signed(&m);
        let other = bs58::encode(SigningKey::from_bytes(&[9u8; 32]).verifying_key().to_bytes())
            .into_string();
        assert_eq!(
            verify_order_signature(&other, &sig, &m),
            Err(OrderSignatureError::VerificationFailed)
        );
    }

    #[test]
    fn rejects_malformed_inputs() {
        let m = msg(SIDE_SELL, 2_500_000);
        let (wallet, sig) = signed(&m);

        assert!(matches!(
            verify_order_signature("not-base58-0OIl", &sig, &m),
            Err(OrderSignatureError::BadWallet(_))
        ));
        assert_eq!(
            verify_order_signature(&bs58::encode([1u8; 16]).into_string(), &sig, &m),
            Err(OrderSignatureError::WalletNotEd25519)
        );
        assert_eq!(
            verify_order_signature(&wallet, &bs58::encode([1u8; 8]).into_string(), &m),
            Err(OrderSignatureError::SignatureWrongLength)
        );
    }
}
