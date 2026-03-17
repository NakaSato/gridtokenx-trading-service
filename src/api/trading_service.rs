use tonic::{Request, Response, Status};
use std::str::FromStr;
use rust_decimal::prelude::ToPrimitive;
use crate::trading_proto::trading_service_server::TradingService;
use crate::trading_proto::*;
use crate::startup::AppState;
use crate::services::erc::IssueErcRequest as DomainIssueErcRequest;
use crate::infra::db::schema::types::{OrderSide, OrderType, OrderStatus};
use crate::domain::trading::models::TradingOrderDb;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use uuid::Uuid;
use std::sync::Arc;
use tracing::{info, error};

pub struct TradingServiceImpl {
    pub state: Arc<AppState>,
}

impl TradingServiceImpl {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl TradingService for TradingServiceImpl {
    async fn submit_order(&self, request: Request<SubmitOrderRequest>) -> Result<Response<TradingResponse>, Status> {
        let req = request.into_inner();
        let user_id = Uuid::parse_str(&req.user_id).map_err(|_| Status::invalid_argument("Invalid user_id"))?;
        
        let side = OrderSide::from_str(&req.side).map_err(|_| Status::invalid_argument("Invalid order side"))?;
        let order_type = OrderType::from_str(&req.order_type).map_err(|_| Status::invalid_argument("Invalid order type"))?;
        let amount = Decimal::from_f64(req.energy_amount).ok_or_else(|| Status::invalid_argument("Invalid energy_amount"))?;
        let price = Decimal::from_f64(req.price_per_kwh).ok_or_else(|| Status::invalid_argument("Invalid price_per_kwh"))?;

        let order_id = Uuid::new_v4();

        // 1. Persist to DB
        // In this architecture, the MatchingEngine usually fetches from DB or we notify it.
        // For simplicity and alignment with the ported code, we'll insert into DB and then notify.
        let order = sqlx::query_as::<_, TradingOrderDb>(
            r#"
            INSERT INTO trading_orders (
                id, user_id, side, order_type, energy_amount, price_per_kwh, 
                filled_amount, status, created_at, session_token, meter_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), $9, $10)
            RETURNING *
            "#
        )
        .bind(order_id)
        .bind(user_id)
        .bind(side)
        .bind(order_type)
        .bind(amount)
        .bind(price)
        .bind(Decimal::ZERO)
        .bind(OrderStatus::Active)
        .bind(req.session_token)
        .bind(req.meter_id)
        .fetch_one(&self.state.db)
        .await
        .map_err(|e| {
            error!("Failed to persist order: {}", e);
            Status::internal("Internal database error")
        })?;

        // 2. Notify Matching Engine
        self.state.matching_engine.notify_new_order(order.zone_id, Some(order)).await;

        Ok(Response::new(TradingResponse {
            success: true,
            message: "Order submitted successfully".to_string(),
            id: Some(order_id.to_string()),
        }))
    }

    async fn cancel_order(&self, request: Request<CancelOrderRequest>) -> Result<Response<TradingResponse>, Status> {
        let req = request.into_inner();
        let order_id = Uuid::parse_str(&req.order_id).map_err(|_| Status::invalid_argument("Invalid order_id"))?;
        let user_id = Uuid::parse_str(&req.user_id).map_err(|_| Status::invalid_argument("Invalid user_id"))?;

        // 1. Update DB
        sqlx::query!(
            "UPDATE trading_orders SET status = 'cancelled', updated_at = NOW() WHERE id = $1 AND user_id = $2"
        )
        .bind(order_id)
        .bind(user_id)
        .execute(&self.state.db)
        .await
        .map_err(|e| {
            error!("Failed to cancel order: {}", e);
            Status::internal("Database error")
        })?;

        // 2. The matching engine will periodically clean up or we can notify it
        // For now, it will be removed in the next matching cycle if not found in DB
        // or we could add a specific cleanup mechanism.

        Ok(Response::new(TradingResponse {
            success: true,
            message: "Order cancelled successfully".to_string(),
            id: Some(order_id.to_string()),
        }))
    }

    async fn get_order(&self, request: Request<GetOrderRequest>) -> Result<Response<OrderResponse>, Status> {
        let req = request.into_inner();
        let order_id = Uuid::parse_str(&req.order_id).map_err(|_| Status::invalid_argument("Invalid order_id"))?;

        let order = sqlx::query_as::<_, TradingOrderDb>(
            r#"
            SELECT 
                id, user_id, side, order_type, energy_amount, price_per_kwh, 
                filled_amount, status, expires_at, created_at, filled_at, epoch_id, zone_id, 
                meter_id, refund_tx_signature, order_pda, order_index, session_token,
                trigger_price, trigger_type, trigger_status, trailing_offset, triggered_at, last_peak_price
            FROM trading_orders 
            WHERE id = $1
            "#
        )
        .bind(order_id)
        .fetch_one(&self.state.db)
        .await
        .map_err(|e| {
            error!("Failed to fetch order: {}", e);
            Status::not_found("Order not found")
        })?;

        Ok(Response::new(OrderResponse {
            id: order.id.to_string(),
            user_id: order.user_id.to_string(),
            energy_amount: order.energy_amount.to_f64().unwrap_or(0.0),
            price_per_kwh: order.price_per_kwh.to_f64().unwrap_or(0.0),
            filled_amount: order.filled_amount.unwrap_or(Decimal::ZERO).to_f64().unwrap_or(0.0),
            side: order.side.as_str().to_string(),
            status: order.status.as_str().to_string(),
            created_at: order.created_at.to_rfc3339(),
        }))
    }

    async fn list_orders(&self, request: Request<ListOrdersRequest>) -> Result<Response<ListOrdersResponse>, Status> {
        let req = request.into_inner();
        let user_id = Uuid::parse_str(&req.user_id).map_err(|_| Status::invalid_argument("Invalid user_id"))?;
        
        let orders = sqlx::query_as::<_, TradingOrderDb>(
            r#"
            SELECT 
                id, user_id, side, order_type, energy_amount, price_per_kwh, 
                filled_amount, status, expires_at, created_at, filled_at, epoch_id, zone_id, 
                meter_id, refund_tx_signature, order_pda, order_index, session_token,
                trigger_price, trigger_type, trigger_status, trailing_offset, triggered_at, last_peak_price
            FROM trading_orders 
            WHERE user_id = $1 
            ORDER BY created_at DESC 
            LIMIT 100
            "#
        )
        .bind(user_id)
        .fetch_all(&self.state.db)
        .await
        .map_err(|e| {
            error!("Failed to list orders: {}", e);
            Status::internal("Database error")
        })?;

        let response_orders = orders.into_iter().map(|o| OrderResponse {
            id: o.id.to_string(),
            user_id: o.user_id.to_string(),
            energy_amount: o.energy_amount.to_f64().unwrap_or(0.0),
            price_per_kwh: o.price_per_kwh.to_f64().unwrap_or(0.0),
            filled_amount: o.filled_amount.unwrap_or(Decimal::ZERO).to_f64().unwrap_or(0.0),
            side: o.side.as_str().to_string(),
            status: o.status.as_str().to_string(),
            created_at: o.created_at.to_rfc3339(),
        }).collect();

        Ok(Response::new(ListOrdersResponse { orders: response_orders }))
    }

    async fn issue_erc(&self, request: Request<IssueErcRequest>) -> Result<Response<TradingResponse>, Status> {
        let req = request.into_inner();
        let user_id = Uuid::parse_str(&req.user_id).map_err(|_| Status::invalid_argument("Invalid user_id"))?;
        let amount = Decimal::from_f64(req.energy_amount).ok_or_else(|| Status::invalid_argument("Invalid energy_amount"))?;

        // Fetch user's wallet address from DB (assuming identity domain is shared or we have a table)
        // For now, let's assume we can find it in a 'wallets' or 'users' table.
        let wallet: (String,) = sqlx::query_as!("SELECT wallet_address FROM user_identity WHERE id = $1")
            .bind(user_id)
            .fetch_one(&self.state.db)
            .await
            .map_err(|e| {
                error!("Failed to fetch wallet for user {}: {}", user_id, e);
                Status::not_found("User wallet not found")
            })?;

        let domain_req = DomainIssueErcRequest {
            wallet_address: wallet.0,
            meter_id: Some(req.meter_id),
            kwh_amount: amount,
            expiry_date: None,
            metadata: None,
        };

        // Issuer wallet from config or state
        let issuer_wallet = &self.state.config.solana_programs.registry_program_id; // Placeholder

        match self.state.erc_service.issue_certificate(user_id, issuer_wallet, domain_req, None).await {
            Ok(cert) => {
                Ok(Response::new(TradingResponse {
                    success: true,
                    message: "ERC issuance initiated".to_string(),
                    id: Some(cert.certificate_id),
                }))
            }
            Err(e) => {
                error!("ERC issuance failed: {}", e);
                Err(Status::internal(format!("ERC issuance failed: {}", e)))
            }
        }
    }

    async fn transfer_erc(&self, request: Request<TransferErcRequest>) -> Result<Response<TradingResponse>, Status> {
        let req = request.into_inner();
        let from_user_id = Uuid::parse_str(&req.from_user_id).map_err(|_| Status::invalid_argument("Invalid from_user_id"))?;
        let to_user_id = Uuid::parse_str(&req.to_user_id).map_err(|_| Status::invalid_argument("Invalid to_user_id"))?;
        let amount = Decimal::from_f64(req.amount).ok_or_else(|| Status::invalid_argument("Invalid amount"))?;

        // 1. Find a certificate to transfer
        let certs = self.state.erc_service.find_settlement_certificates(from_user_id, amount).await
            .map_err(|e| Status::internal(format!("Failed to find suitable certificates: {}", e)))?;
        
        let cert = certs.first().ok_or_else(|| Status::not_found("No certificates found with sufficient amount"))?;

        // 2. Fetch wallets (again, assuming shared DB for now)
        let from_wallet: (String,) = sqlx::query_as("SELECT wallet_address FROM user_identity WHERE id = $1")
            .bind(from_user_id)
            .fetch_one(&self.state.db)
            .await
            .map_err(|_| Status::not_found("Sender wallet not found"))?;

        let to_wallet: (String,) = sqlx::query_as("SELECT wallet_address FROM user_identity WHERE id = $1")
            .bind(to_user_id)
            .fetch_one(&self.state.db)
            .await
            .map_err(|_| Status::not_found("Recipient wallet not found"))?;

        // 3. Perform transfer
        // Note: tx_signature here might be placeholder or managed by ErcService background
        self.state.erc_service.transfer_certificate(cert.id, &from_wallet.0, &to_wallet.0, to_user_id, "OFFCHAIN_P2P").await
            .map_err(|e| Status::internal(format!("Transfer failed: {}", e)))?;

        Ok(Response::new(TradingResponse {
            success: true,
            message: "ERC transfer successful".to_string(),
            id: Some(cert.certificate_id.clone()),
        }))
    }

    async fn retire_erc(&self, request: Request<RetireErcRequest>) -> Result<Response<TradingResponse>, Status> {
        let req = request.into_inner();
        let user_id = Uuid::parse_str(&req.user_id).map_err(|_| Status::invalid_argument("Invalid user_id"))?;
        let amount = Decimal::from_f64(req.amount).ok_or_else(|| Status::invalid_argument("Invalid amount"))?;

        // Similar to transfer, find a certificate
        let certs = self.state.erc_service.find_settlement_certificates(user_id, amount).await
            .map_err(|e| Status::internal(format!("Failed to find suitable certificates: {}", e)))?;
        
        let cert = certs.first().ok_or_else(|| Status::not_found("No certificates found with sufficient amount"))?;

        self.state.erc_service.retire_certificate(cert.id).await
            .map_err(|e| Status::internal(format!("Retirement failed: {}", e)))?;

        Ok(Response::new(TradingResponse {
            success: true,
            message: "ERC retired successfully".to_string(),
            id: Some(cert.certificate_id.clone()),
        }))
    }

    async fn get_erc_balance(&self, request: Request<GetErcBalanceRequest>) -> Result<Response<ErcBalanceResponse>, Status> {
        let req = request.into_inner();
        let user_id = Uuid::parse_str(&req.user_id).map_err(|_| Status::invalid_argument("Invalid user_id"))?;

        let stats = self.state.erc_service.get_user_stats(user_id).await
            .map_err(|e| Status::internal(format!("Failed to fetch ERC stats: {}", e)))?;

        Ok(Response::new(ErcBalanceResponse {
            balance: stats.active_kwh.to_f64().unwrap_or(0.0),
            asset_type: "KWH_CERT".to_string(),
        }))
    }
}
