use axum::{
    extract::FromRequestParts,
    http::{request::Parts, HeaderMap, HeaderValue, StatusCode},
};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

pub const INTERNAL_ROLE_HEADER: &str = "x-gridtokenx-role";
pub const GATEWAY_SECRET_HEADER: &str = "x-gridtokenx-gateway-secret";

/// Carries the extracted SPIFFE identity of the caller.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpiffeIdentity(pub String);

/// Predefined service roles based on SPIFFE identity.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ServiceRole {
    /// Ingress Gateway (APISIX/Envoy) - represents public API access
    ApiGateway,
    /// IAM Service - manages identities and registration
    IamService,
    /// Trading Service API surface
    TradingApi,
    /// Trading Matcher engine
    TradingMatcher,
    /// Oracle Bridge - handles edge metric validation
    OracleBridge,
    /// Settlement Service - manages on-chain settlement
    SettlementService,
    /// Reporting or analytics service
    ReportingService,
    /// Administrative access (super-user)
    Admin,
    /// Unknown or unauthorized identity
    Unknown,
}

impl fmt::Display for ServiceRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::ApiGateway => "api-gateway",
            Self::IamService => "iam-service",
            Self::TradingApi => "trading-api",
            Self::TradingMatcher => "trading-matcher",
            Self::OracleBridge => "oracle-bridge",
            Self::SettlementService => "settlement-service",
            Self::ReportingService => "reporting-service",
            Self::Admin => "admin",
            Self::Unknown => "unknown",
        };
        write!(f, "{}", s)
    }
}

impl FromStr for ServiceRole {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "api-gateway" => Ok(Self::ApiGateway),
            "iam-service" => Ok(Self::IamService),
            "trading-api" => Ok(Self::TradingApi),
            "trading-matcher" => Ok(Self::TradingMatcher),
            "oracle-bridge" => Ok(Self::OracleBridge),
            "settlement-service" => Ok(Self::SettlementService),
            "reporting-service" => Ok(Self::ReportingService),
            "admin" => Ok(Self::Admin),
            "unknown" => Ok(Self::Unknown),
            _ => Ok(Self::Unknown),
        }
    }
}

impl From<&SpiffeIdentity> for ServiceRole {
    fn from(identity: &SpiffeIdentity) -> Self {
        let uri = &identity.0;
        if uri.starts_with("spiffe://gridtokenx.th/prod/apisix") {
            Self::ApiGateway
        } else if uri.starts_with("spiffe://gridtokenx.th/prod/iam-service") {
            Self::IamService
        } else if uri.starts_with("spiffe://gridtokenx.th/prod/trading-service/api") {
            Self::TradingApi
        } else if uri.starts_with("spiffe://gridtokenx.th/prod/trading-service/matcher") {
            Self::TradingMatcher
        } else if uri.starts_with("spiffe://gridtokenx.th/prod/oracle-bridge") {
            Self::OracleBridge
        } else if uri.starts_with("spiffe://gridtokenx.th/prod/settlement-service") {
            Self::SettlementService
        } else if uri.starts_with("spiffe://gridtokenx.th/prod/reporting-service") {
            Self::ReportingService
        } else if uri.starts_with("spiffe://gridtokenx.th/prod/admin") {
            Self::Admin
        } else {
            Self::Unknown
        }
    }
}

#[axum::async_trait]
impl<S> FromRequestParts<S> for ServiceRole
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // 1. Look for SpiffeIdentity in request extensions (inserted by otel_tracing middleware)
        if let Some(identity) = parts.extensions.get::<SpiffeIdentity>() {
            let role = ServiceRole::from(identity);
            if role == ServiceRole::Unknown {
                tracing::warn!("Request from unknown SPIFFE identity: {}", identity.0);
            }
            return Ok(role);
        }

        // 2. Fallback: Check for the synthetic internal role header (useful for local dev/testing)
        let role = ServiceRole::from_headers(&parts.headers);
        if role != ServiceRole::Unknown {
            return Ok(role);
        }

        // Default to Unknown
        Ok(ServiceRole::Unknown)
    }
}

impl ServiceRole {
    /// Extract role from HTTP headers (used by gRPC and other contexts without extensions)
    pub fn from_headers(headers: &HeaderMap) -> Self {
        // 1. Check for the synthetic internal role header first (injected by middleware)
        if let Some(role) = headers
            .get(INTERNAL_ROLE_HEADER)
            .and_then(|v: &HeaderValue| v.to_str().ok())
            .and_then(|s: &str| s.parse::<ServiceRole>().ok())
        {
            // If the role is ApiGateway, it MUST be accompanied by the gateway secret
            if role == ServiceRole::ApiGateway {
                let expected_secret = std::env::var("GATEWAY_SECRET")
                    .unwrap_or_else(|_| "gridtokenx-gateway-secret-2025".to_string());
                let provided_secret = headers
                    .get(GATEWAY_SECRET_HEADER)
                    .and_then(|v| v.to_str().ok());

                if provided_secret != Some(&expected_secret) {
                    tracing::warn!(
                        "RBAC Verification Failed: API Gateway secret mismatch or missing"
                    );
                    return ServiceRole::Unknown;
                }
            }
            return role;
        }

        ServiceRole::Unknown
    }

    /// Helper to enforce that the caller has a specific role.
    pub fn require(self, required: ServiceRole) -> Result<(), (StatusCode, &'static str)> {
        if self == required || self == ServiceRole::Admin {
            Ok(())
        } else {
            tracing::error!("RBAC Denial: Expected {}, got {}", required, self);
            Err((StatusCode::FORBIDDEN, "Insufficient permissions"))
        }
    }

    /// Helper to enforce that the caller has one of many roles.
    pub fn require_any(self, required: &[ServiceRole]) -> Result<(), (StatusCode, &'static str)> {
        if required.contains(&self) || self == ServiceRole::Admin {
            Ok(())
        } else {
            tracing::error!("RBAC Denial: Expected one of {:?}, got {}", required, self);
            Err((StatusCode::FORBIDDEN, "Insufficient permissions"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn test_role_display_and_from_str() {
        let role = ServiceRole::IamService;
        assert_eq!(role.to_string(), "iam-service");
        assert_eq!(ServiceRole::from_str("iam-service").unwrap(), role);
        assert_eq!(
            ServiceRole::from_str("unknown-role").unwrap(),
            ServiceRole::Unknown
        );
    }

    #[test]
    fn test_role_from_spiffe_identity() {
        let identities = vec![
            (
                "spiffe://gridtokenx.th/prod/apisix",
                ServiceRole::ApiGateway,
            ),
            (
                "spiffe://gridtokenx.th/prod/iam-service",
                ServiceRole::IamService,
            ),
            (
                "spiffe://gridtokenx.th/prod/trading-service/api",
                ServiceRole::TradingApi,
            ),
            (
                "spiffe://gridtokenx.th/prod/trading-service/matcher",
                ServiceRole::TradingMatcher,
            ),
            (
                "spiffe://gridtokenx.th/prod/oracle-bridge",
                ServiceRole::OracleBridge,
            ),
            (
                "spiffe://gridtokenx.th/prod/settlement-service",
                ServiceRole::SettlementService,
            ),
            (
                "spiffe://gridtokenx.th/prod/reporting-service",
                ServiceRole::ReportingService,
            ),
            ("spiffe://gridtokenx.th/prod/admin", ServiceRole::Admin),
            ("spiffe://gridtokenx.th/prod/unknown", ServiceRole::Unknown),
        ];

        for (uri, expected_role) in identities {
            let identity = SpiffeIdentity(uri.to_string());
            assert_eq!(ServiceRole::from(&identity), expected_role);
        }
    }

    #[test]
    fn test_role_from_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(
            INTERNAL_ROLE_HEADER,
            HeaderValue::from_static("iam-service"),
        );

        let role = ServiceRole::from_headers(&headers);
        assert_eq!(role, ServiceRole::IamService);

        // Test fallback
        let role_fallback = ServiceRole::from_headers(&HeaderMap::new());
        assert_eq!(role_fallback, ServiceRole::Unknown);
    }

    #[test]
    fn test_rbac_require() {
        let role = ServiceRole::IamService;

        // Success cases
        assert!(role.require(ServiceRole::IamService).is_ok());
        assert!(ServiceRole::Admin.require(ServiceRole::IamService).is_ok());

        // Failure case
        let result = role.require(ServiceRole::TradingApi);
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().0, StatusCode::FORBIDDEN);
    }

    #[test]
    fn test_rbac_require_any() {
        let role = ServiceRole::TradingApi;
        let allowed = vec![ServiceRole::IamService, ServiceRole::TradingApi];

        // Success cases
        assert!(role.require_any(&allowed).is_ok());
        assert!(ServiceRole::Admin.require_any(&allowed).is_ok());

        // Failure case
        let result = ServiceRole::OracleBridge.require_any(&allowed);
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().0, StatusCode::FORBIDDEN);
    }
}
