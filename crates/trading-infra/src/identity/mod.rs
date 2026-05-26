use async_trait::async_trait;
use connectrpc::client::{HttpClient, ClientConfig, CallOptions};
use http::{HeaderValue, Uri};
use iam_protocol::identity::{IdentityServiceClient, SignMessageRequest};
use std::sync::Arc;
use trading_core::error::ApiError;
use trading_core::traits::{IdentityGateway, TraitResult};
use uuid::Uuid;

pub mod signer;

pub use signer::CustodialSigner;

/// Client for interacting with the IAM Identity Service.
#[derive(Clone)]
pub struct IamIdentityGateway {
    client: Arc<IdentityServiceClient<HttpClient>>,
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
        user_id: Uuid,
        wallet_address: Option<String>,
        message: Vec<u8>,
    ) -> TraitResult<Vec<u8>> {
        let request = SignMessageRequest {
            user_id: user_id.to_string(),
            wallet_address: wallet_address.unwrap_or_default(),
            message: message.into(),
            ..Default::default()
        };

        let api_key_header = HeaderValue::from_str(&self.api_key)
            .map_err(|e| ApiError::Internal(format!("Invalid API key header: {}", e)))?;
        let role_header = HeaderValue::from_static("trading-api");

        let options = CallOptions::default()
            .with_header("x-api-key", api_key_header)
            .with_header("x-gridtokenx-role", role_header);

        match self.client.sign_message_with_options(request, options).await {
            Ok(res) => {
                let msg = res.view();
                if !msg.error_message.is_empty() {
                    return Err(ApiError::Internal(format!("IAM signing error: {}", msg.error_message)));
                }
                Ok(msg.signature.to_vec())
            }
            Err(e) => Err(ApiError::Internal(format!("gRPC call to IAM failed: {}", e))),
        }
    }
}
