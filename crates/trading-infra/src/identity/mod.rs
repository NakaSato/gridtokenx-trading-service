use async_trait::async_trait;
use connectrpc::client::{HttpClient, ClientConfig};
use http::Uri;
use iam_protocol::identity::IdentityServiceClient;
use std::sync::Arc;
use trading_core::error::ApiError;
use trading_core::traits::{IdentityGateway, TraitResult};
use uuid::Uuid;

pub mod signer;

pub use signer::CustodialSigner;

/// Client for interacting with the IAM Identity Service.
///
/// NOTE: per-user message signing was removed from IAM's `IdentityService`
/// (the `SignMessage` RPC no longer exists). Custodial signing now happens on
/// the Chain Bridge side: a transaction is built, serialized, and published to
/// `chain.tx.submit` where the bridge signs it with its `platform_admin` Vault
/// Transit key. There is no per-user sign RPC to delegate to, so `sign_message`
/// below is intentionally inert until the trading on-chain `create_order`
/// instruction gains a non-signing owner account (mirroring the registry
/// `RegisterUser` 6-account "owner non-signing, bridge pays" layout from
/// commit 49728d9 / anchor 0f41280).
#[derive(Clone)]
pub struct IamIdentityGateway {
    #[allow(dead_code)]
    client: Arc<IdentityServiceClient<HttpClient>>,
    #[allow(dead_code)]
    api_key: String,
}

impl IamIdentityGateway {
    /// Create a new IAM identity gateway.
    pub fn new(iam_url: &str, api_key: String) -> Self {
        let uri = iam_url.parse::<Uri>().expect("Invalid IAM URL");
        let transport = HttpClient::plaintext();
        let client = IdentityServiceClient::new(transport, ClientConfig::new(uri));
        Self {
            client: Arc::new(client),
            api_key,
        }
    }
}

#[async_trait]
impl IdentityGateway for IamIdentityGateway {
    async fn sign_message(
        &self,
        _user_id: Uuid,
        _wallet_address: Option<String>,
        _message: Vec<u8>,
    ) -> TraitResult<Vec<u8>> {
        // IAM's SignMessage RPC was removed in favour of Chain Bridge custodial
        // signing. Per-user signing has no transport here; order creation must
        // submit an unsigned tx via `chain.tx.submit` for the bridge to sign,
        // which in turn requires the create_order instruction to carry a
        // non-signing owner account. Until that program-level change lands,
        // fail loudly rather than produce an invalid signature.
        Err(ApiError::Internal(
            "custodial sign_message unavailable: IAM SignMessage RPC removed; \
             order signing must route through Chain Bridge (chain.tx.submit) \
             once create_order supports a non-signing owner account"
                .to_string(),
        ))
    }
}
