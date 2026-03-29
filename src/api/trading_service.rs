use crate::domain::trading::models::TradingOrderDb;
use crate::infra::db::schema::types::{OrderSide, OrderStatus, OrderType};
use crate::services::erc::IssueErcRequest as DomainIssueErcRequest;
use crate::startup::AppState;
use crate::trading_proto::*;
use crate::metrics;
use chrono::Utc;
use connectrpc::{Context, ConnectError};
use buffa::view::OwnedView;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tracing::{error, info};
use uuid::Uuid;

pub struct TradingServiceImpl {
    pub state: Arc<AppState>,
}

impl TradingServiceImpl {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}

impl TradingService for TradingServiceImpl {
    async fn submit_order(
        &self,
        ctx: Context,
        request: OwnedView<SubmitOrderRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        let _timer = metrics::GrpcMetricsTimer::new("submit_order");
        let start = Instant::now();
        
        let user_id = Uuid::parse_str(request.user_id)
            .map_err(|_| ConnectError::invalid_argument("Invalid user_id"))?;

        let side = OrderSide::from_str(request.side)
            .map_err(|_| ConnectError::invalid_argument("Invalid order side"))?;
        let order_type = OrderType::from_str(request.order_type)
            .map_err(|_| ConnectError::invalid_argument("Invalid order type"))?;
        let amount = Decimal::from_f64(request.energy_amount)
            .ok_or_else(|| ConnectError::invalid_argument("Invalid energy_amount"))?;
        let price = Decimal::from_f64(request.price_per_kwh)
            .ok_or_else(|| ConnectError::invalid_argument("Invalid price_per_kwh"))?;

        let order_id = Uuid::new_v4();

        let order = sqlx::query_as::<_, TradingOrderDb>(
            r#"
            INSERT INTO trading_orders (
                id, user_id, side, order_type, energy_amount, price_per_kwh,
                filled_amount, status, created_at, session_token, meter_id, zone_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), $9, $10, $11)
            RETURNING *
            "#,
        )
        .bind(order_id)
        .bind(user_id)
        .bind(side)
        .bind(order_type)
        .bind(amount)
        .bind(price)
        .bind(Decimal::ZERO)
        .bind(OrderStatus::Active)
        .bind(request.session_token)
        .bind(Uuid::parse_str(request.meter_id).ok())
        .bind(request.zone_id)
        .fetch_one(&self.state.db)
        .await
        .map_err(|e| {
            error!("Failed to persist order: {}", e);
            ConnectError::internal("Internal database error")
        })?;

        self.state
            .matching_engine
            .notify_new_order(order.zone_id, Some(order))
            .await;
        
        let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
        metrics::record_order_submission(
            &order_type.to_string(),
            &side.to_string(),
            true,
            duration_ms
        );

        Ok((TradingResponse {
            success: true,
            message: "Order submitted successfully".to_string(),
            id: Some(order_id.to_string()),
            ..Default::default()
        }, ctx))
    }

    async fn cancel_order(
        &self,
        ctx: Context,
        request: OwnedView<CancelOrderRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        let _timer = metrics::GrpcMetricsTimer::new("cancel_order");
        let start = Instant::now();
        
        let order_id = Uuid::parse_str(request.order_id)
            .map_err(|_| ConnectError::invalid_argument("Invalid order_id"))?;
        let user_id = Uuid::parse_str(request.user_id)
            .map_err(|_| ConnectError::invalid_argument("Invalid user_id"))?;

        sqlx::query(
            "UPDATE trading_orders SET status = 'cancelled', updated_at = NOW() WHERE id = $1 AND user_id = $2"
        )
        .bind(order_id)
        .bind(user_id)
        .execute(&self.state.db)
        .await
        .map_err(|e| {
            error!("Failed to cancel order: {}", e);
            ConnectError::internal("Database error")
        })?;
        
        let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
        metrics::record_order_cancellation(true, duration_ms);

        Ok((TradingResponse {
            success: true,
            message: "Order cancelled successfully".to_string(),
            id: Some(order_id.to_string()),
            ..Default::default()
        }, ctx))
    }

    async fn get_order(
        &self,
        ctx: Context,
        request: OwnedView<GetOrderRequestView<'static>>,
    ) -> Result<(OrderResponse, Context), ConnectError> {
        let order_id = Uuid::parse_str(request.order_id)
            .map_err(|_| ConnectError::invalid_argument("Invalid order_id"))?;

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
            ConnectError::not_found("Order not found")
        })?;

        Ok((OrderResponse {
            id: order.id.to_string(),
            user_id: order.user_id.to_string(),
            energy_amount: order.energy_amount.to_f64().unwrap_or(0.0),
            price_per_kwh: order.price_per_kwh.to_f64().unwrap_or(0.0),
            filled_amount: order
                .filled_amount
                .unwrap_or(Decimal::ZERO)
                .to_f64()
                .unwrap_or(0.0),
            side: order.side.to_string(),
            status: order.status.to_string(),
            created_at: order.created_at.unwrap_or(Utc::now()).to_rfc3339(),
            ..Default::default()
        }, ctx))
    }

    async fn list_orders(
        &self,
        ctx: Context,
        request: OwnedView<ListOrdersRequestView<'static>>,
    ) -> Result<(ListOrdersResponse, Context), ConnectError> {
        let user_id = Uuid::parse_str(request.user_id)
            .map_err(|_| ConnectError::invalid_argument("Invalid user_id"))?;

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
            ConnectError::internal("Database error")
        })?;

        let response_orders = orders
            .into_iter()
            .map(|o| OrderResponse {
                id: o.id.to_string(),
                user_id: o.user_id.to_string(),
                energy_amount: o.energy_amount.to_f64().unwrap_or(0.0),
                price_per_kwh: o.price_per_kwh.to_f64().unwrap_or(0.0),
                filled_amount: o
                    .filled_amount
                    .unwrap_or(Decimal::ZERO)
                    .to_f64()
                    .unwrap_or(0.0),
                side: o.side.to_string(),
                status: o.status.to_string(),
                created_at: o.created_at.unwrap_or(Utc::now()).to_rfc3339(),
                ..Default::default()
            })
            .collect();

        Ok((ListOrdersResponse {
            orders: response_orders,
            ..Default::default()
        }, ctx))
    }

    async fn notify_order(
        &self,
        ctx: Context,
        request: OwnedView<NotifyOrderRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        let order_id = Uuid::parse_str(request.order_id)
            .map_err(|_| ConnectError::invalid_argument("Invalid order_id"))?;

        let order = sqlx::query_as::<_, TradingOrderDb>(
            r#"
            SELECT 
                id, user_id, side, order_type, energy_amount, price_per_kwh, 
                filled_amount, status, expires_at, created_at, filled_at, epoch_id, zone_id, 
                meter_id, refund_tx_signature, order_pda, order_index, session_token,
                trigger_price, trigger_type, trigger_status, trailing_offset, triggered_at, last_peak_price
            FROM trading_orders 
            WHERE id = $1
            "#,
        )
        .bind(order_id)
        .fetch_one(&self.state.db)
        .await
        .map_err(|e| {
            error!("Failed to fetch order for notification {}: {}", order_id, e);
            ConnectError::not_found("Order not found")
        })?;

        self.state
            .matching_engine
            .notify_new_order(order.zone_id, Some(order))
            .await;

        Ok((TradingResponse {
            success: true,
            message: "Matching engine notified successfully".to_string(),
            id: Some(order_id.to_string()),
            ..Default::default()
        }, ctx))
    }

    async fn issue_erc(
        &self,
        ctx: Context,
        request: OwnedView<IssueERCRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        let _timer = metrics::GrpcMetricsTimer::new("issue_erc");
        let start = Instant::now();
        
        let user_id = Uuid::parse_str(request.user_id)
            .map_err(|_| ConnectError::invalid_argument("Invalid user_id"))?;
        let amount = Decimal::from_f64(request.energy_amount)
            .ok_or_else(|| ConnectError::invalid_argument("Invalid energy_amount"))?;

        let wallet: String =
            sqlx::query_scalar("SELECT wallet_address FROM user_identity WHERE id = $1")
                .bind(user_id)
                .fetch_one(&self.state.db)
                .await
                .map_err(|e| {
                    error!("Failed to fetch wallet for user {}: {}", user_id, e);
                    ConnectError::not_found("User wallet not found")
                })?;

        let domain_req = DomainIssueErcRequest {
            wallet_address: wallet,
            meter_id: Some(request.meter_id.to_string()),
            kwh_amount: amount,
            expiry_date: None,
            metadata: None,
        };

        let issuer_wallet = &self.state.config.solana_programs.registry_program_id;

        let result = self
            .state
            .erc_service
            .issue_certificate(user_id, issuer_wallet, domain_req, None)
            .await;
        
        let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
        let success = result.is_ok();
        metrics::record_erc_operation("issue", success, duration_ms);
        
        if success {
            metrics::record_erc_issuance(amount.to_f64().unwrap_or(0.0), true, duration_ms);
        }

        match result {
            Ok(cert) => Ok((TradingResponse {
                success: true,
                message: "ERC issuance initiated".to_string(),
                id: Some(cert.certificate_id),
                ..Default::default()
            }, ctx)),
            Err(e) => {
                error!("ERC issuance failed: {}", e);
                metrics::record_erc_issuance(amount.to_f64().unwrap_or(0.0), false, duration_ms);
                Err(ConnectError::internal(format!("ERC issuance failed: {}", e)))
            }
        }
    }

    async fn transfer_erc(
        &self,
        ctx: Context,
        request: OwnedView<TransferERCRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        let _timer = metrics::GrpcMetricsTimer::new("transfer_erc");
        let start = Instant::now();
        
        let from_user_id = Uuid::parse_str(request.from_user_id)
            .map_err(|_| ConnectError::invalid_argument("Invalid from_user_id"))?;
        let to_user_id = Uuid::parse_str(request.to_user_id)
            .map_err(|_| ConnectError::invalid_argument("Invalid to_user_id"))?;
        let amount = Decimal::from_f64(request.amount)
            .ok_or_else(|| ConnectError::invalid_argument("Invalid amount"))?;

        let certs = self
            .state
            .erc_service
            .find_settlement_certificates(from_user_id, amount)
            .await
            .map_err(|e| {
                ConnectError::internal(format!("Failed to find suitable certificates: {}", e))
            })?;

        let cert = certs
            .first()
            .ok_or_else(|| ConnectError::not_found("No certificates found with sufficient amount"))?;

        let from_wallet: String =
            sqlx::query_scalar("SELECT wallet_address FROM user_identity WHERE id = $1")
                .bind(from_user_id)
                .fetch_one(&self.state.db)
                .await
                .map_err(|_| ConnectError::not_found("Sender wallet not found"))?;

        let to_wallet: String =
            sqlx::query_scalar("SELECT wallet_address FROM user_identity WHERE id = $1")
                .bind(to_user_id)
                .fetch_one(&self.state.db)
                .await
                .map_err(|_| ConnectError::not_found("Recipient wallet not found"))?;

        let result = self
            .state
            .erc_service
            .transfer_certificate(
                cert.id,
                &from_wallet,
                &to_wallet,
                to_user_id,
                "OFFCHAIN_P2P",
            )
            .await;
        
        let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
        let success = result.is_ok();
        metrics::record_erc_operation("transfer", success, duration_ms);
        
        if success {
            metrics::record_erc_transfer(amount.to_f64().unwrap_or(0.0), true, duration_ms);
        }

        result
            .map_err(|e| ConnectError::internal(format!("Transfer failed: {}", e)))?;

        Ok((TradingResponse {
            success: true,
            message: "ERC transfer successful".to_string(),
            id: Some(cert.certificate_id.clone()),
            ..Default::default()
        }, ctx))
    }

    async fn retire_erc(
        &self,
        ctx: Context,
        request: OwnedView<RetireERCRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        let _timer = metrics::GrpcMetricsTimer::new("retire_erc");
        let start = Instant::now();
        
        let user_id = Uuid::parse_str(request.user_id)
            .map_err(|_| ConnectError::invalid_argument("Invalid user_id"))?;
        let amount = Decimal::from_f64(request.amount)
            .ok_or_else(|| ConnectError::invalid_argument("Invalid amount"))?;

        let certs = self
            .state
            .erc_service
            .find_settlement_certificates(user_id, amount)
            .await
            .map_err(|e| {
                ConnectError::internal(format!("Failed to find suitable certificates: {}", e))
            })?;

        let cert = certs
            .first()
            .ok_or_else(|| ConnectError::not_found("No certificates found with sufficient amount"))?;

        let result = self
            .state
            .erc_service
            .retire_certificate(cert.id)
            .await;
        
        let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
        let success = result.is_ok();
        metrics::record_erc_operation("retire", success, duration_ms);
        
        if success {
            metrics::record_erc_retirement(amount.to_f64().unwrap_or(0.0), true, duration_ms);
        }

        result
            .map_err(|e| ConnectError::internal(format!("Retirement failed: {}", e)))?;

        Ok((TradingResponse {
            success: true,
            message: "ERC retired successfully".to_string(),
            id: Some(cert.certificate_id.clone()),
            ..Default::default()
        }, ctx))
    }

    async fn get_erc_balance(
        &self,
        ctx: Context,
        request: OwnedView<GetERCBalanceRequestView<'static>>,
    ) -> Result<(ERCBalanceResponse, Context), ConnectError> {
        let user_id = Uuid::parse_str(request.user_id)
            .map_err(|_| ConnectError::invalid_argument("Invalid user_id"))?;

        let stats = self
            .state
            .erc_service
            .get_user_stats(user_id)
            .await
            .map_err(|e| ConnectError::internal(format!("Failed to fetch ERC stats: {}", e)))?;

        Ok((ERCBalanceResponse {
            balance: stats.active_kwh.to_f64().unwrap_or(0.0),
            asset_type: "KWH_CERT".to_string(),
            ..Default::default()
        }, ctx))
    }
}
