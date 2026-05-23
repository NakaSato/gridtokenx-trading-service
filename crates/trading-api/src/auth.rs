use axum::{
    extract::FromRequestParts,
    http::{request::Parts, StatusCode},
};
use uuid::Uuid;

pub const USER_ID_HEADER: &str = "x-gridtokenx-user-id";

#[derive(Debug, Clone, Copy)]
pub struct UserContext {
    pub user_id: Uuid,
}

impl<S> FromRequestParts<S> for UserContext
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, String);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // Debug logging for headers
        tracing::debug!("Incoming headers: {:?}", parts.headers);
        for (name, value) in parts.headers.iter() {
            if name.as_str().starts_with("x-gridtokenx-") {
                tracing::info!("Found GridTokenX header: {} = {:?}", name, value);
            }
        }

        if let Some(user_id_str) = parts
            .headers
            .get(USER_ID_HEADER)
            .and_then(|v| v.to_str().ok())
        {
            if let Ok(user_id) = Uuid::parse_str(user_id_str) {
                return Ok(UserContext { user_id });
            }
        }

        // For development/local testing, if the header is missing, we might want to return an error
        // or a default ID. In a "real app", we return 401.
        Err((
            StatusCode::UNAUTHORIZED,
            "Missing or invalid User ID header".to_string(),
        ))
    }
}
