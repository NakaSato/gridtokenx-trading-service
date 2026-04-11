use crate::domain::trading::models::TradingOrderDb;
use crate::infra::db::schema::types::{OrderSide, OrderStatus, OrderType, IntervalType, RecurringStatus, TriggerType, TriggerStatus};
use crate::services::erc::IssueErcRequest as DomainIssueErcRequest;
use crate::startup::AppState;
use crate::trading_proto::*;
use crate::metrics;
use chrono::{DateTime, Utc, FixedOffset};
use connectrpc::{Context, ConnectError};
use buffa::view::OwnedView;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tracing::{error, info, warn};
use uuid::Uuid;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;

#[derive(Debug, sqlx::FromRow)]
struct TradeDataRecord {
    id: Uuid,
    quantity: Decimal,
    price: Decimal,
    total_value: Option<Decimal>,
    role: String,
    counterparty_id: Uuid,
    executed_at: chrono::DateTime<Utc>,
    status: String,
    wheeling_charge: Option<Decimal>,
    loss_cost: Option<Decimal>,
    effective_energy: Option<Decimal>,
    buyer_zone_id: Option<i32>,
    seller_zone_id: Option<i32>,
}

pub struct TradingServiceImpl {
    pub state: Arc<AppState>,
}

impl TradingServiceImpl {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }

    fn calculate_next_execution(
        interval_type: IntervalType,
        interval_value: i32,
    ) -> chrono::DateTime<Utc> {
        let now = Utc::now();
        match interval_type {
            IntervalType::Hourly => now + chrono::Duration::hours(interval_value as i64),
            IntervalType::Daily => now + chrono::Duration::days(interval_value as i64),
            IntervalType::Weekly => now + chrono::Duration::weeks(interval_value as i64),
            IntervalType::Monthly => now + chrono::Duration::days(30 * interval_value as i64),
        }
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

    async fn update_order(
        &self,
        ctx: Context,
        request: OwnedView<UpdateOrderRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        let _timer = metrics::GrpcMetricsTimer::new("update_order");
        
        let order_id = Uuid::parse_str(request.order_id)
            .map_err(|_| ConnectError::invalid_argument("Invalid order_id"))?;
        let user_id = Uuid::parse_str(request.user_id)
            .map_err(|_| ConnectError::invalid_argument("Invalid user_id"))?;

        let mut query_builder = sqlx::QueryBuilder::new("UPDATE trading_orders SET updated_at = NOW()");
        
        if let Some(amount) = request.energy_amount {
            query_builder.push(", energy_amount = ");
            query_builder.push_bind(Decimal::from_f64(amount).unwrap_or(Decimal::ZERO));
        }
        
        if let Some(price) = request.price_per_kwh {
            query_builder.push(", price_per_kwh = ");
            query_builder.push_bind(Decimal::from_f64(price).unwrap_or(Decimal::ZERO));
        }
        
        query_builder.push(" WHERE id = ");
        query_builder.push_bind(order_id);
        query_builder.push(" AND user_id = ");
        query_builder.push_bind(user_id);
        query_builder.push(" AND status = 'pending'");

        let result = query_builder.build().execute(&self.state.db).await.map_err(|e| {
            error!("Failed to update order: {}", e);
            ConnectError::internal("Database error")
        })?;

        if result.rows_affected() == 0 {
            return Err(ConnectError::not_found("Order not found or not in pending status"));
        }

        Ok((TradingResponse {
            success: true,
            message: "Order updated successfully".to_string(),
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

    async fn get_order_book(
        &self,
        ctx: Context,
        request: OwnedView<GetOrderBookRequestView<'static>>,
    ) -> Result<(ListOrdersResponse, Context), ConnectError> {
        let mut query = String::from(
            r#"
            SELECT 
                id, user_id, side, order_type, energy_amount, price_per_kwh, 
                filled_amount, status, expires_at, created_at, filled_at, epoch_id, zone_id, 
                meter_id, refund_tx_signature, order_pda, order_index, session_token,
                trigger_price, trigger_type, trigger_status, trailing_offset, triggered_at, last_peak_price
            FROM trading_orders 
            WHERE (status = 'pending' OR status = 'active')
            "#
        );

        if let Some(zone_id) = request.zone_id {
            query.push_str(&format!(" AND zone_id = {}", zone_id));
        }

        if let Some(side) = request.side {
            query.push_str(&format!(" AND side = '{}'", side));
        }

        query.push_str(" ORDER BY price_per_kwh DESC LIMIT 200");

        let orders = sqlx::query_as::<_, TradingOrderDb>(&query)
            .fetch_all(&self.state.db)
            .await
            .map_err(|e| {
                error!("Failed to fetch order book: {}", e);
                ConnectError::internal("Database error")
            })?;

        let response_orders = orders
            .into_iter()
            .map(|o| OrderResponse {
                id: o.id.to_string(),
                user_id: o.user_id.to_string(),
                energy_amount: o.energy_amount.to_f64().unwrap_or(0.0),
                price_per_kwh: o.price_per_kwh.to_f64().unwrap_or(0.0),
                filled_amount: o.filled_amount.unwrap_or(Decimal::ZERO).to_f64().unwrap_or(0.0),
                side: o.side.to_string(),
                status: o.status.to_string(),
                created_at: o.created_at.unwrap_or(Utc::now()).to_rfc3339(),
                zone_id: o.zone_id,
                ..Default::default()
            })
            .collect();

        Ok((ListOrdersResponse {
            orders: response_orders,
            ..Default::default()
        }, ctx))
    }

    async fn list_trades(
        &self,
        ctx: Context,
        request: OwnedView<ListTradesRequestView<'static>>,
    ) -> Result<(ListTradesResponse, Context), ConnectError> {
        let user_id = Uuid::parse_str(request.user_id)
            .map_err(|_| ConnectError::invalid_argument("Invalid user_id"))?;
        let limit = request.limit.unwrap_or(50);

        let trades_result = sqlx::query_as::<_, TradeDataRecord>(
            r#"
            SELECT 
                om.id,
                om.matched_amount as quantity,
                om.match_price as price,
                (om.matched_amount * om.match_price) as total_value,
                CASE 
                    WHEN b.user_id = $1 THEN 'buyer'
                    ELSE 'seller'
                END as role,
                CASE 
                    WHEN b.user_id = $1 THEN s.user_id
                    ELSE b.user_id
                END as counterparty_id,
                om.match_time as executed_at,
                om.status,
                settle.wheeling_charge,
                settle.loss_cost,
                settle.effective_energy,
                COALESCE(settle.buyer_zone_id, om.zone_id) as buyer_zone_id,
                COALESCE(settle.seller_zone_id, om.zone_id) as seller_zone_id
            FROM order_matches om
            JOIN trading_orders b ON om.buy_order_id = b.id
            JOIN trading_orders s ON om.sell_order_id = s.id
            LEFT JOIN settlements settle ON om.settlement_id = settle.id
            WHERE (b.user_id = $1 OR s.user_id = $1)
            ORDER BY om.match_time DESC
            LIMIT $2
            "#
        )
        .bind(user_id)
        .bind(limit as i64)
        .fetch_all(&self.state.db)
        .await;

        let trades = trades_result.map_err(|e| {
            error!("Failed to fetch trades: {}", e);
            ConnectError::internal("Database error")
        })?;

        let response_trades = trades
            .into_iter()
            .map(|t| TradeResponse {
                id: t.id.to_string(),
                quantity: t.quantity.to_f64().unwrap_or(0.0),
                price: t.price.to_f64().unwrap_or(0.0),
                total_value: t.total_value.unwrap_or(Decimal::ZERO).to_f64().unwrap_or(0.0),
                role: t.role,
                counterparty_id: t.counterparty_id.to_string(),
                executed_at: t.executed_at.to_rfc3339(),
                status: t.status,
                wheeling_charge: t.wheeling_charge.map(|d: Decimal| d.to_f64().unwrap_or(0.0)),
                loss_cost: t.loss_cost.map(|d: Decimal| d.to_f64().unwrap_or(0.0)),
                effective_energy: t.effective_energy.map(|d: Decimal| d.to_f64().unwrap_or(0.0)),
                buyer_zone_id: t.buyer_zone_id,
                seller_zone_id: t.seller_zone_id,
                ..Default::default()
            })
            .collect();

        Ok((ListTradesResponse {
            trades: response_trades,
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

        let result: anyhow::Result<crate::domain::trading::models::ErcCertificate> = self
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

        let certs: Vec<crate::domain::trading::models::ErcCertificate> = self
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

        let result: anyhow::Result<(crate::domain::trading::models::ErcCertificate, crate::domain::trading::models::CertificateTransfer)> = self
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

        let certs: Vec<crate::domain::trading::models::ErcCertificate> = self
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

        let result: anyhow::Result<crate::domain::trading::models::ErcCertificate> = self
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

        let stats: crate::domain::trading::models::CertificateStats = self
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

    // =============================================================================
    // P2P / Market Engine RPCs
    // =============================================================================

    async fn calculate_p2p_cost(
        &self,
        ctx: Context,
        request: OwnedView<CalculateP2PCostRequestView<'static>>,
    ) -> Result<(P2PTransactionCost, Context), ConnectError> {
        let _timer = metrics::GrpcMetricsTimer::new("calculate_p2p_cost");

        let buyer_zone = request.buyer_zone_id;
        let seller_zone = request.seller_zone_id;
        let energy_amount = request.energy_amount;
        let zone_distance = (buyer_zone - seller_zone).abs() as i32;

        // Use P2PConfigService for calculations
        let p2p = &self.state.p2p_config;
        
        let wheeling_charge_dec = p2p.calculate_wheeling_charge(zone_distance).await;
        let wheeling_rate: f64 = wheeling_charge_dec.to_f64().unwrap_or(0.02);
        
        let loss_factor_dec = p2p.calculate_loss_factor(zone_distance).await;
        let loss_factor: f64 = loss_factor_dec.to_f64().unwrap_or(0.01);

        let base_price = p2p.get_f64("pricing.base_price_thb_kwh").await.unwrap_or(4.0);
        let agreed_price = request.agreed_price.unwrap_or(base_price);

        let energy_cost = agreed_price * energy_amount;
        let wheeling_charge = wheeling_rate * energy_amount;
        let loss_cost = energy_cost * loss_factor;
        let total_cost = energy_cost + wheeling_charge + loss_cost;
        let effective_energy = energy_amount * (1.0 - loss_factor);

        Ok((P2PTransactionCost {
            energy_cost,
            wheeling_charge,
            loss_cost,
            total_cost,
            effective_energy,
            loss_factor,
            loss_allocation: "Split (50/50)".to_string(),
            zone_distance_km: (zone_distance as f64) * 5.0,
            buyer_zone,
            seller_zone,
            is_grid_compliant: true,
            grid_violation_reason: None,
            ..Default::default()
        }, ctx))
    }

    async fn get_market_prices(
        &self,
        ctx: Context,
        _request: OwnedView<::buffa_types::google::protobuf::EmptyView<'static>>,
    ) -> Result<(P2PMarketPrices, Context), ConnectError> {
        let _timer = metrics::GrpcMetricsTimer::new("get_market_prices");
        
        let p2p = &self.state.p2p_config;
        let prices = p2p.get_market_prices().await;

        // Get wheeling configs
        let wheeling_config = p2p.get_by_category("wheeling").await.unwrap_or_default();
        let mut wheeling_charges = HashMap::new();
        for (key, value) in wheeling_config {
            if let Some(zone_str) = key.strip_prefix("wheeling.zone_") {
                if let Some(val_f64) = value.to_f64() {
                    wheeling_charges.insert(zone_str.to_string(), val_f64);
                }
            }
        }

        // Get loss configs
        let loss_config = p2p.get_by_category("loss").await.unwrap_or_default();
        let mut loss_factors = HashMap::new();
        for (key, value) in loss_config {
            if let Some(zone_str) = key.strip_prefix("loss.zone_") {
                if let Some(val_f64) = value.to_f64() {
                    loss_factors.insert(zone_str.to_string(), val_f64);
                }
            }
        }

        Ok((P2PMarketPrices {
            base_price_thb_kwh: prices.base_price,
            grid_import_price_thb_kwh: prices.grid_import_price,
            grid_export_price_thb_kwh: prices.grid_export_price,
            loss_allocation_model: "Socialized".to_string(),
            wheeling_charges,
            loss_factors,
            ..Default::default()
        }, ctx))
    }

    async fn relay_order(
        &self,
        ctx: Context,
        request: OwnedView<RelayOrderRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        let _timer = metrics::GrpcMetricsTimer::new("relay_order");
        let start = Instant::now();

        // 1. Signature Verification
        // Reconstruct message: order_id(16) + pubkey(32) + energy(8) + price(8) + side(1) + zone(4) + expires(8)
        let order_id = Uuid::parse_str(request.order_id)
            .map_err(|_| ConnectError::invalid_argument("Invalid order_id hex"))?;
        let pubkey = Pubkey::from_str(request.user_pubkey)
            .map_err(|_| ConnectError::invalid_argument("Invalid user_pubkey"))?;
        let signature = Signature::try_from(request.signature)
            .map_err(|_| ConnectError::invalid_argument("Invalid signature bytes"))?;

        let mut message = Vec::with_capacity(77);
        message.extend_from_slice(order_id.as_bytes());
        message.extend_from_slice(&pubkey.to_bytes());
        message.extend_from_slice(&request.energy_amount.to_le_bytes());
        message.extend_from_slice(&request.price_per_kwh.to_le_bytes());
        message.push(request.side as u8);
        message.extend_from_slice(&(request.zone_id as u32).to_le_bytes());
        message.extend_from_slice(&request.expires_at.to_le_bytes());

        if !signature.verify(&pubkey.to_bytes(), &message) {
            warn!("RelayOrder: Invalid signature for order {}", order_id);
            return Err(ConnectError::unauthenticated("Invalid signature"));
        }

        // 2. Lookup user_id from wallet address
        let user_id: Uuid = sqlx::query_scalar("SELECT id FROM users WHERE wallet_address = $1")
            .bind(request.user_pubkey)
            .fetch_one(&self.state.db)
            .await
            .map_err(|e| {
                error!("User not found for pubkey {}: {}", request.user_pubkey, e);
                ConnectError::not_found("User identity not found for provided pubkey")
            })?;

        // 3. Convert types for MarketClearingService
        let side = if request.side == 0 { OrderSide::Buy } else { OrderSide::Sell };
        let energy_amount = Decimal::from_u64(request.energy_amount).unwrap_or_default();
        let price_per_kwh = Decimal::from_u64(request.price_per_kwh).unwrap_or_default();

        // 4. Call MarketClearingService
        self.state.market_clearing.relay_order(
            user_id,
            order_id,
            side,
            energy_amount,
            price_per_kwh,
            request.zone_id,
            bs58::encode(request.signature).into_string(),
            message
        ).await.map_err(|e| {
            error!("MarketClearingService failed to relay order {}: {}", order_id, e);
            ConnectError::internal(format!("Failed to relay order: {}", e))
        })?;

        let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
        metrics::record_order_submission("relayed", &side.to_string(), true, duration_ms);

        Ok((TradingResponse {
            success: true,
            message: "Order relayed and processed successfully".to_string(),
            id: Some(order_id.to_string()),
            ..Default::default()
        }, ctx))
    }

    // Helper functions moved to impl TradingServiceImpl

    // =============================================================================
    // Market & Status Handlers
    // =============================================================================

    async fn get_blockchain_market_data(
        &self,
        ctx: Context,
        _request: OwnedView<::buffa_types::google::protobuf::EmptyView<'static>>,
    ) -> Result<(BlockchainMarketDataResponse, Context), ConnectError> {
        let _timer = metrics::GrpcMetricsTimer::new("get_blockchain_market_data");

        // Get the Trading program ID from config
        let trading_program_id = self.state.blockchain.trading_program_id().map_err(|e| {
            error!("Failed to parse trading program ID: {}", e);
            ConnectError::internal(format!("Invalid program ID: {}", e))
        })?;

        // Derive the market PDA
        let (market_pda, _bump) = Pubkey::find_program_address(&[b"market"], &trading_program_id);

        info!("Market PDA: {}", market_pda);

        // Check if account exists and get data (using existing blockchain service)
        let account_exists = self.state.blockchain.account_exists(&market_pda).await.map_err(|e| {
            error!("Failed to check if market account exists: {}", e);
            ConnectError::internal(format!("Blockchain communication error: {}", e))
        })?;

        if !account_exists {
            return Ok((BlockchainMarketDataResponse {
                success: false,
                message: "Trading market account not found on blockchain".to_string(),
                ..Default::default()
            }, ctx));
        }

        let account_data = self.state.blockchain.get_account_data(&market_pda).await.map_err(|e| {
            error!("Failed to fetch market account data: {}", e);
            ConnectError::internal("Failed to fetch market account data from Solana")
        })?;

        // Deserialize account data (skip 8-byte discriminator)
        if account_data.len() < 8 {
            return Err(ConnectError::internal("Invalid market account data (too short)"));
        }

        let data = &account_data[8..];
        if data.len() < 72 {
             return Err(ConnectError::internal("Market data payload too short"));
        }

        // Move the parsing logic here
        let authority = Pubkey::try_from(&data[0..32])
            .map_err(|e| ConnectError::internal(format!("Invalid authority pubkey: {}", e)))?;
        let active_orders = u64::from_le_bytes(data[32..40].try_into().unwrap());
        let total_volume = u64::from_le_bytes(data[40..48].try_into().unwrap());
        let total_trades = u64::from_le_bytes(data[48..56].try_into().unwrap());
        let created_at = i64::from_le_bytes(data[56..64].try_into().unwrap());
        let clearing_enabled = data[64] != 0;
        let market_fee_bps = u16::from_le_bytes(data[66..68].try_into().unwrap());

        Ok((BlockchainMarketDataResponse {
            success: true,
            message: "Market data fetched from Solana".to_string(),
            authority: authority.to_string(),
            active_orders,
            total_volume,
            total_trades,
            market_fee_bps: market_fee_bps as u32,
            clearing_enabled,
            created_at,
            ..Default::default()
        }, ctx))
    }

    async fn get_market_stats(
        &self,
        ctx: Context,
        _request: OwnedView<::buffa_types::google::protobuf::EmptyView<'static>>,
    ) -> Result<(MarketStatsResponse, Context), ConnectError> {
        let _timer = metrics::GrpcMetricsTimer::new("get_market_stats");

        // Get average price and volume from recent matches (24h)
        let stats_row = sqlx::query(
            r#"
            SELECT
                COALESCE(AVG(match_price), 0) as avg_price,
                COALESCE(SUM(matched_amount), 0) as total_volume,
                COUNT(*) as completed_matches
            FROM order_matches
            WHERE match_time > NOW() - INTERVAL '24 hours'
            "#,
        )
        .fetch_one(&self.state.db)
        .await
        .map_err(|e| {
            error!("get_market_stats: Failed to fetch recent stats: {}", e);
            ConnectError::internal("Database error")
        })?;

        use sqlx::Row;
        let avg_price: Decimal = stats_row.get("avg_price");
        let total_volume: Decimal = stats_row.get("total_volume");
        let completed_matches: i64 = stats_row.get("completed_matches");

        // Get active orders count
        let active_orders: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM trading_orders WHERE status = 'active'")
            .fetch_one(&self.state.db)
            .await
            .unwrap_or(0);

        // Get pending orders count
        let pending_orders: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM trading_orders WHERE status = 'pending'")
            .fetch_one(&self.state.db)
            .await
            .unwrap_or(0);

        Ok((MarketStatsResponse {
            average_price: avg_price.to_f64().unwrap_or(0.0),
            total_volume: total_volume.to_f64().unwrap_or(0.0),
            active_orders,
            pending_orders,
            completed_matches,
            ..Default::default()
        }, ctx))
    }

    async fn get_matching_status(
        &self,
        ctx: Context,
        _request: OwnedView<::buffa_types::google::protobuf::EmptyView<'static>>,
    ) -> Result<(MatchingStatusResponse, Context), ConnectError> {
        let _timer = metrics::GrpcMetricsTimer::new("get_matching_status");

        // Get pending order counts and price ranges grouped by side
        let order_counts = sqlx::query(
            r#"
            SELECT 
                side::text,
                COUNT(*) as count,
                MIN(price_per_kwh)::float8 as min_price,
                MAX(price_per_kwh)::float8 as max_price
            FROM trading_orders 
            WHERE status IN ('active', 'pending')
            GROUP BY side
            "#,
        )
        .fetch_all(&self.state.db)
        .await
        .map_err(|e| {
            error!("get_matching_status: Failed to fetch order counts: {}", e);
            ConnectError::internal("Database error")
        })?;

        let mut pending_buy_orders: i64 = 0;
        let mut pending_sell_orders: i64 = 0;
        let (mut buy_min, mut buy_max) = (0.0, 0.0);
        let (mut sell_min, mut sell_max) = (0.0, 0.0);

        use sqlx::Row;
        for row in order_counts {
            let side: String = row.get("side");
            let count: i64 = row.get("count");
            let min: f64 = row.get("min_price");
            let max: f64 = row.get("max_price");

            if side == "buy" {
                pending_buy_orders = count;
                buy_min = min;
                buy_max = max;
            } else if side == "sell" {
                pending_sell_orders = count;
                sell_min = min;
                sell_max = max;
            }
        }

        // Get pending matches count
        let pending_matches: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM order_matches WHERE status = 'pending'")
            .fetch_one(&self.state.db)
            .await
            .unwrap_or(0);

        // Simple matching possibility check (highest buy >= lowest sell)
        let can_match = buy_max > 0.0 && sell_min > 0.0 && buy_max >= sell_min;
        let match_reason = if can_match {
            format!("Matching possible: buy max {:.2} >= sell min {:.2}", buy_max, sell_min)
        } else if buy_max > 0.0 && sell_min > 0.0 {
            format!("Price gap: buy max {:.2} < sell min {:.2}", buy_max, sell_min)
        } else {
            "Insufficient orders for matching".to_string()
        };

        Ok((MatchingStatusResponse {
            pending_buy_orders,
            pending_sell_orders,
            pending_matches,
            buy_min_price: buy_min,
            buy_max_price: buy_max,
            sell_min_price: sell_min,
            sell_max_price: sell_max,
            can_match,
            match_reason,
            ..Default::default()
        }, ctx))
    }

    async fn get_settlement_stats(
        &self,
        ctx: Context,
        _request: OwnedView<::buffa_types::google::protobuf::EmptyView<'static>>,
    ) -> Result<(SettlementStatsResponse, Context), ConnectError> {
        let _timer = metrics::GrpcMetricsTimer::new("get_settlement_stats");

        let stats = sqlx::query(
            r#"
            SELECT 
                COUNT(*) FILTER (WHERE status = 'pending') as pending_count,
                COUNT(*) FILTER (WHERE status = 'processing') as processing_count,
                COUNT(*) FILTER (WHERE status = 'completed') as confirmed_count,
                COUNT(*) FILTER (WHERE status = 'failed') as failed_count,
                COALESCE(SUM(CASE WHEN status = 'completed' THEN total_amount ELSE 0 END), 0) as total_settled_value
            FROM settlements
            "#
        )
        .fetch_one(&self.state.db)
        .await
        .map_err(|e| {
            error!("get_settlement_stats: Database error: {}", e);
            ConnectError::internal("Database error")
        })?;

        use sqlx::Row;
        let total_settled_val: Decimal = stats.get("total_settled_value");

        // Recent settlements
        let recent_rows = sqlx::query(
            r#"
            SELECT id, status, energy_amount, total_amount, created_at
            FROM settlements
            ORDER BY created_at DESC
            LIMIT 5
            "#
        )
        .fetch_all(&self.state.db)
        .await
        .unwrap_or_default();

        let recent_settlements = recent_rows.into_iter().map(|r| {
            RecentSettlementResponse {
                id: r.get::<Uuid, _>("id").to_string(),
                status: r.get("status"),
                energy_amount: r.get::<Decimal, _>("energy_amount").to_f64().unwrap_or(0.0),
                total_amount: r.get::<Decimal, _>("total_amount").to_f64().unwrap_or(0.0),
                created_at: r.get::<chrono::DateTime<Utc>, _>("created_at").to_rfc3339(),
                ..Default::default()
            }
        }).collect();

        Ok((SettlementStatsResponse {
            pending_count: stats.get("pending_count"),
            processing_count: stats.get("processing_count"),
            confirmed_count: stats.get("confirmed_count"),
            failed_count: stats.get("failed_count"),
            total_settled_value: total_settled_val.to_f64().unwrap_or(0.0),
            recent_settlements,
            ..Default::default()
        }, ctx))
    }

    async fn get_token_balance(
        &self,
        ctx: Context,
        request: OwnedView<GetTokenBalanceRequestView<'static>>,
    ) -> Result<(TokenBalanceResponse, Context), ConnectError> {
        let _timer = metrics::GrpcMetricsTimer::new("get_token_balance");

        let wallet_address = request.wallet_address;
        let mint_address = request.mint_address;

        let wallet_pubkey = Pubkey::from_str(wallet_address)
            .map_err(|_| ConnectError::invalid_argument("Invalid wallet address"))?;
        let mint_pubkey = Pubkey::from_str(mint_address)
            .map_err(|_| ConnectError::invalid_argument("Invalid mint address"))?;

        // Using the correct field name 'blockchain' instead of 'blockchain_service'
        let raw_balance = self.state
            .blockchain
            .get_token_balance(&wallet_pubkey, &mint_pubkey)
            .await
            .map_err(|e| ConnectError::internal(format!("Blockchain error: {}", e)))?;

        Ok((TokenBalanceResponse {
            wallet_address: wallet_address.to_string(),
            token_balance: raw_balance.to_f64().unwrap_or(0.0),
            raw_balance: raw_balance.to_u64().unwrap_or(0),
            mint: mint_address.to_string(),
            ..Default::default()
        }, ctx))
    }

    async fn create_conditional_order(
        &self,
        ctx: Context,
        request: OwnedView<CreateConditionalOrderRequestView<'_>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        let _timer = metrics::GrpcMetricsTimer::new("create_conditional_order");

        let user_id = Uuid::parse_str(request.user_id)
            .map_err(|_| ConnectError::invalid_argument("Invalid user_id"))?;
        let side = OrderSide::from_str(request.side)
            .map_err(|_| ConnectError::invalid_argument("Invalid side"))?;
        let amount = Decimal::from_f64(request.energy_amount)
            .ok_or_else(|| ConnectError::invalid_argument("Invalid energy_amount"))?;
        let trigger_price = Decimal::from_f64(request.trigger_price)
            .ok_or_else(|| ConnectError::invalid_argument("Invalid trigger_price"))?;
        let trigger_type = TriggerType::from_str(request.trigger_type)
            .map_err(|_| ConnectError::invalid_argument("Invalid trigger_type"))?;
        
        let limit_price = request.limit_price.and_then(Decimal::from_f64);
        let trailing_offset = request.trailing_offset.and_then(Decimal::from_f64);
        
        let now = Utc::now();
        let expires_at = request.expiry_time
            .as_ref()
            .and_then(|t| {
                DateTime::parse_from_rfc3339(t)
                    .map(|dt| dt.with_timezone(&Utc))
                    .ok()
            })
            .unwrap_or_else(|| now + chrono::Duration::days(7));

        let session_token = request.session_token.clone();

        sqlx::query(
            r#"
            INSERT INTO conditional_orders (
                id, user_id, side, energy_amount, trigger_price, trigger_type, 
                limit_price, trailing_offset, expires_at, status, session_token, created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW())
            "#
        )
        .bind(Uuid::new_v4())
        .bind(user_id)
        .bind(side)
        .bind(amount)
        .bind(trigger_price)
        .bind(trigger_type)
        .bind(limit_price)
        .bind(trailing_offset)
        .bind(expires_at)
        .bind(TriggerStatus::Pending)
        .bind(session_token)
        .execute(&self.state.db)
        .await
        .map_err(|e| ConnectError::internal(format!("Database error: {}", e)))?;

        Ok((TradingResponse {
            success: true,
            message: "Conditional order created successfully".to_string(),
            ..Default::default()
        }, ctx))
    }

    async fn list_conditional_orders(
        &self,
        ctx: Context,
        request: OwnedView<ListConditionalOrdersRequestView<'_>>,
    ) -> Result<(ListConditionalOrdersResponse, Context), ConnectError> {
        let _timer = metrics::GrpcMetricsTimer::new("list_conditional_orders");

        let user_id = request.user_id;

        let rows = sqlx::query(
            r#"
            SELECT * FROM conditional_orders WHERE user_id = $1 ORDER BY created_at DESC
            "#
        )
        .bind(user_id)
        .fetch_all(&self.state.db)
        .await
        .map_err(|e| ConnectError::internal(format!("Database error: {}", e)))?;

        use sqlx::Row;
        let orders = rows.into_iter().map(|row| {
            ConditionalOrderData {
                id: row.get::<Uuid, _>("id").to_string(),
                user_id: row.get::<Uuid, _>("user_id").to_string(),
                side: row.get::<OrderSide, _>("side").to_string(),
                energy_amount: row.get::<Decimal, _>("energy_amount").to_f64().unwrap_or(0.0),
                trigger_price: row.get::<Decimal, _>("trigger_price").to_f64().unwrap_or(0.0),
                trigger_type: row.get::<TriggerType, _>("trigger_type").to_string(),
                trigger_status: row.get::<TriggerStatus, _>("status").to_string(),
                limit_price: row.get::<Option<Decimal>, _>("limit_price").map(|d| d.to_f64().unwrap_or(0.0)),
                trailing_offset: row.get::<Option<Decimal>, _>("trailing_offset").map(|d| d.to_f64().unwrap_or(0.0)),
                expires_at: row.get::<Option<chrono::DateTime<Utc>>, _>("expires_at").map(|t| t.to_rfc3339()),
                created_at: row.get::<chrono::DateTime<Utc>, _>("created_at").to_rfc3339(),
                triggered_at: row.get::<Option<chrono::DateTime<Utc>>, _>("triggered_at").map(|t: DateTime<Utc>| t.to_rfc3339()),
                last_peak_price: row.get::<Option<Decimal>, _>("last_peak_price").map(|d| d.to_f64().unwrap_or(0.0)),
                ..Default::default()
            }
        }).collect();

        Ok((ListConditionalOrdersResponse {
            orders,
            ..Default::default()
        }, ctx))
    }

    async fn cancel_conditional_order(
        &self,
        ctx: Context,
        request: OwnedView<CancelConditionalOrderRequestView<'_>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        let _timer = metrics::GrpcMetricsTimer::new("cancel_conditional_order");

        let order_id = Uuid::parse_str(request.order_id)
            .map_err(|_| ConnectError::invalid_argument("Invalid order_id"))?;
        let user_id = Uuid::parse_str(request.user_id)
            .map_err(|_| ConnectError::invalid_argument("Invalid user_id"))?;

        let result = sqlx::query(
            r#"
            UPDATE conditional_orders SET status = 'cancelled' 
            WHERE id = $1 AND user_id = $2 AND status = 'pending'
            "#
        )
        .bind(order_id)
        .bind(user_id)
        .execute(&self.state.db)
        .await
        .map_err(|e| ConnectError::internal(format!("Database error: {}", e)))?;

        Ok((TradingResponse {
            success: result.rows_affected() > 0,
            message: if result.rows_affected() > 0 { "Order cancelled".to_string() } else { "Order not found or not in pending state".to_string() },
            ..Default::default()
        }, ctx))
    }

    async fn get_conditional_order(
        &self,
        ctx: Context,
        request: OwnedView<GetConditionalOrderRequestView<'_>>,
    ) -> Result<(ConditionalOrderData, Context), ConnectError> {
        let _timer = metrics::GrpcMetricsTimer::new("get_conditional_order");

        let order_id = Uuid::parse_str(request.order_id)
            .map_err(|_| ConnectError::invalid_argument("Invalid order_id"))?;
        let user_id = Uuid::parse_str(request.user_id)
            .map_err(|_| ConnectError::invalid_argument("Invalid user_id"))?;

        let row = sqlx::query(
            r#"
            SELECT * FROM conditional_orders WHERE id = $1 AND user_id = $2
            "#
        )
        .bind(order_id)
        .bind(user_id)
        .fetch_one(&self.state.db)
        .await
        .map_err(|_| ConnectError::not_found("Conditional order not found"))?;

        use sqlx::Row;
        Ok((ConditionalOrderData {
            id: row.get::<Uuid, _>("id").to_string(),
            user_id: row.get::<Uuid, _>("user_id").to_string(),
            side: row.get::<OrderSide, _>("side").to_string(),
            energy_amount: row.get::<Decimal, _>("energy_amount").to_f64().unwrap_or(0.0),
            trigger_price: row.get::<Decimal, _>("trigger_price").to_f64().unwrap_or(0.0),
            trigger_type: row.get::<TriggerType, _>("trigger_type").to_string(),
            trigger_status: row.get::<TriggerStatus, _>("status").to_string(),
            limit_price: row.get::<Option<Decimal>, _>("limit_price").map(|d| d.to_f64().unwrap_or(0.0)),
            trailing_offset: row.get::<Option<Decimal>, _>("trailing_offset").map(|d| d.to_f64().unwrap_or(0.0)),
            expires_at: row.get::<Option<chrono::DateTime<Utc>>, _>("expires_at").map(|t| t.to_rfc3339()),
            created_at: row.get::<chrono::DateTime<Utc>, _>("created_at").to_rfc3339(),
            triggered_at: row.get::<Option<chrono::DateTime<Utc>>, _>("triggered_at").map(|t: DateTime<Utc>| t.to_rfc3339()),
            last_peak_price: row.get::<Option<Decimal>, _>("last_peak_price").map(|d| d.to_f64().unwrap_or(0.0)),
            ..Default::default()
        }, ctx))
    }

    async fn create_recurring_order(
        &self,
        ctx: Context,
        request: OwnedView<CreateRecurringOrderRequestView<'_>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        let _timer = metrics::GrpcMetricsTimer::new("create_recurring_order");

        let user_id = Uuid::parse_str(request.user_id)
            .map_err(|_| ConnectError::invalid_argument("Invalid user_id"))?;
        let side = OrderSide::from_str(request.side)
            .map_err(|_| ConnectError::invalid_argument("Invalid side"))?;
        let amount = Decimal::from_f64(request.energy_amount)
            .ok_or_else(|| ConnectError::invalid_argument("Invalid energy_amount"))?;
        let max_price = Decimal::from_f64(request.max_price_per_kwh).filter(|d| d.is_sign_positive());
        let min_price = Decimal::from_f64(request.min_price_per_kwh).filter(|d| d.is_sign_positive());
        let interval_type = IntervalType::from_str(request.interval_type)
            .map_err(|_| ConnectError::invalid_argument("Invalid interval_type"))?;
        
        let interval_value = request.interval_value;
        let max_executions = request.max_executions;
        let name = request.name.to_string();
        let description = request.description.to_string();
        let session_token = request.session_token.clone();

        let order_id = Uuid::new_v4();
        let next_execution_at = Self::calculate_next_execution(interval_type, interval_value.unwrap_or(1));

        sqlx::query(
            r#"
            INSERT INTO recurring_orders (
                id, user_id, side, energy_amount, max_price_per_kwh, min_price_per_kwh,
                interval_type, interval_value, next_execution_at, status, total_executions,
                max_executions, name, description, session_token, created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 0, $11, $12, $13, $14, NOW(), NOW())
            "#
        )
        .bind(order_id)
        .bind(user_id)
        .bind(side)
        .bind(amount)
        .bind(max_price)
        .bind(min_price)
        .bind(interval_type)
        .bind(interval_value)
        .bind(next_execution_at)
        .bind(RecurringStatus::Active)
        .bind(max_executions)
        .bind(name)
        .bind(description)
        .bind(session_token)
        .execute(&self.state.db)
        .await
        .map_err(|e| ConnectError::internal(format!("Database error: {}", e)))?;

        Ok((TradingResponse {
            success: true,
            message: "Recurring order created".to_string(),
            id: Some(order_id.to_string()),
            ..Default::default()
        }, ctx))
    }

    async fn list_recurring_orders(
        &self,
        ctx: Context,
        request: OwnedView<ListRecurringOrdersRequestView<'_>>,
    ) -> Result<(ListRecurringOrdersResponse, Context), ConnectError> {
        let _timer = metrics::GrpcMetricsTimer::new("list_recurring_orders");

        let user_id = request.user_id;

        let rows = sqlx::query(
            r#"
            SELECT * FROM recurring_orders WHERE user_id = $1 ORDER BY created_at DESC
            "#
        )
        .bind(user_id)
        .fetch_all(&self.state.db)
        .await
        .map_err(|e| ConnectError::internal(format!("Database error: {}", e)))?;

        use sqlx::Row;
        let orders = rows.into_iter().map(|r| {
            RecurringOrderData {
                id: r.get::<Uuid, _>("id").to_string(),
                user_id: r.get::<Uuid, _>("user_id").to_string(),
                side: r.get::<OrderSide, _>("side").to_string(),
                energy_amount: r.get::<Decimal, _>("energy_amount").to_f64().unwrap_or(0.0),
                max_price_per_kwh: r.get::<Option<Decimal>, _>("max_price_per_kwh").map(|d| d.to_f64().unwrap_or(0.0)).unwrap_or(0.0),
                min_price_per_kwh: r.get::<Option<Decimal>, _>("min_price_per_kwh").map(|d| d.to_f64().unwrap_or(0.0)).unwrap_or(0.0),
                interval_type: r.get::<IntervalType, _>("interval_type").to_string(),
                interval_value: r.get("interval_value"),
                next_execution_at: r.get::<DateTime<Utc>, _>("next_execution_at").to_rfc3339(),
                last_executed_at: r.get::<Option<DateTime<Utc>>, _>("last_executed_at").map(|dt: DateTime<Utc>| dt.to_rfc3339()),
                status: r.get::<RecurringStatus, _>("status").to_string(),
                total_executions: r.get("total_executions"),
                max_executions: r.get("max_executions"),
                name: r.get::<Option<String>, _>("name").unwrap_or_default(),
                description: r.get::<Option<String>, _>("description").unwrap_or_default(),
                created_at: r.get::<DateTime<Utc>, _>("created_at").to_rfc3339(),
                updated_at: r.get::<DateTime<Utc>, _>("updated_at").to_rfc3339(),
                ..Default::default()
            }
        }).collect();

        Ok((ListRecurringOrdersResponse {
            orders,
            ..Default::default()
        }, ctx))
    }

    async fn get_recurring_order(
        &self,
        ctx: Context,
        request: OwnedView<GetRecurringOrderRequestView<'_>>,
    ) -> Result<(RecurringOrderResponse, Context), ConnectError> {
        let _timer = metrics::GrpcMetricsTimer::new("get_recurring_order");

        let order_id = Uuid::parse_str(request.order_id)
            .map_err(|_| ConnectError::invalid_argument("Invalid order_id"))?;
        let user_id = Uuid::parse_str(request.user_id)
            .map_err(|_| ConnectError::invalid_argument("Invalid user_id"))?;

        let row = sqlx::query(
            r#"
            SELECT * FROM recurring_orders WHERE id = $1 AND user_id = $2
            "#
        )
        .bind(order_id)
        .bind(user_id)
        .fetch_one(&self.state.db)
        .await
        .map_err(|_| ConnectError::not_found("Recurring order not found"))?;

        use sqlx::Row;
        Ok((RecurringOrderResponse {
            id: row.get::<Uuid, _>("id").to_string(),
            status: row.get::<RecurringStatus, _>("status").to_string(),
            next_execution_at: row.get::<DateTime<Utc>, _>("next_execution_at").to_rfc3339(),
            created_at: row.get::<DateTime<Utc>, _>("created_at").to_rfc3339(),
            message: "Success".to_string(),
            ..Default::default()
        }, ctx))
    }

    async fn cancel_recurring_order(
        &self,
        ctx: Context,
        request: OwnedView<CancelRecurringOrderRequestView<'_>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        let _timer = metrics::GrpcMetricsTimer::new("cancel_recurring_order");

        let order_id = Uuid::parse_str(request.order_id)
            .map_err(|_| ConnectError::invalid_argument("Invalid order_id"))?;
        let user_id = Uuid::parse_str(request.user_id)
            .map_err(|_| ConnectError::invalid_argument("Invalid user_id"))?;

        sqlx::query(
            r#"
            UPDATE recurring_orders SET status = 'cancelled', updated_at = NOW() 
            WHERE id = $1 AND user_id = $2
            "#
        )
        .bind(order_id)
        .bind(user_id)
        .execute(&self.state.db)
        .await
        .map_err(|e| ConnectError::internal(format!("Database error: {}", e)))?;

        Ok((TradingResponse {
            success: true,
            message: "Recurring order cancelled".to_string(),
            ..Default::default()
        }, ctx))
    }

    async fn pause_recurring_order(
        &self,
        ctx: Context,
        request: OwnedView<PauseRecurringOrderRequestView<'_>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        let _timer = metrics::GrpcMetricsTimer::new("pause_recurring_order");

        let order_id = Uuid::parse_str(request.order_id)
            .map_err(|_| ConnectError::invalid_argument("Invalid order_id"))?;
        let user_id = Uuid::parse_str(request.user_id)
            .map_err(|_| ConnectError::invalid_argument("Invalid user_id"))?;

        sqlx::query(
            r#"
            UPDATE recurring_orders SET status = 'paused', updated_at = NOW() 
            WHERE id = $1 AND user_id = $2 AND status = 'active'
            "#
        )
        .bind(order_id)
        .bind(user_id)
        .execute(&self.state.db)
        .await
        .map_err(|e| ConnectError::internal(format!("Database error: {}", e)))?;

        Ok((TradingResponse {
            success: true,
            message: "Recurring order paused".to_string(),
            ..Default::default()
        }, ctx))
    }

    async fn resume_recurring_order(
        &self,
        ctx: Context,
        request: OwnedView<ResumeRecurringOrderRequestView<'_>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        let _timer = metrics::GrpcMetricsTimer::new("resume_recurring_order");

        let order_id = Uuid::parse_str(request.order_id)
            .map_err(|_| ConnectError::invalid_argument("Invalid order_id"))?;
        let user_id = Uuid::parse_str(request.user_id)
            .map_err(|_| ConnectError::invalid_argument("Invalid user_id"))?;

        // Fetch current order to recalculate next execution time
        let order_row = sqlx::query("SELECT interval_type, interval_value FROM recurring_orders WHERE id = $1 AND user_id = $2")
            .bind(order_id)
            .bind(user_id)
            .fetch_one(&self.state.db)
            .await
            .map_err(|_| ConnectError::not_found("Order not found"))?;

        use sqlx::Row;
        let interval_type: IntervalType = order_row.get("interval_type");
        let interval_value: i32 = order_row.get("interval_value");
        let next_execution = Self::calculate_next_execution(interval_type, interval_value);

        let result = sqlx::query("UPDATE recurring_orders SET status = 'active', next_execution_at = $3, updated_at = NOW() WHERE id = $1 AND user_id = $2 AND status = 'paused'")
            .bind(order_id)
            .bind(user_id)
            .bind(next_execution)
            .execute(&self.state.db)
            .await
            .map_err(|e| {
                error!("Failed to resume recurring order: {}", e);
                ConnectError::internal(format!("Database error: {}", e))
            })?;

        if result.rows_affected() == 0 {
            return Err(ConnectError::not_found("Order not found or not in paused state"));
        }

        Ok((TradingResponse {
            success: true,
            message: "Recurring order resumed and rescheduled".to_string(),
            id: Some(order_id.to_string()),
            ..Default::default()
        }, ctx))
    }

    // =============================================================================
    // Oracle Bridge Settlement (Generation Mint)
    // =============================================================================

    async fn settle_generation_mint(
        &self,
        ctx: Context,
        request: OwnedView<SettleGenerationMintRequestView<'static>>,
    ) -> Result<(SettleGenerationMintResponse, Context), ConnectError> {
        let _timer = metrics::GrpcMetricsTimer::new("settle_generation_mint");

        info!(
            "💰 Settlement request received via gRPC: {} (Gen: {} kWh, Window: {} - {})",
            request.meter_serial,
            request.energy_generated_kwh,
            request.start_time,
            request.end_time
        );

        // 1. Verify Oracle signature is present
        if request.signature.is_empty() {
            return Err(ConnectError::invalid_argument("Missing oracle signature"));
        }

        // 2. Parse meter_id
        let meter_id = Uuid::parse_str(request.meter_id)
            .map_err(|_| ConnectError::invalid_argument("Invalid meter_id"))?;

        // 3. Convert kWh to Decimal
        let amount_kwh = Decimal::from_f64(request.energy_generated_kwh)
            .ok_or_else(|| ConnectError::invalid_argument("Invalid energy_generated_kwh"))?;

        // 4. Parse start_time for timestamp
        let timestamp = chrono::DateTime::parse_from_rfc3339(request.start_time)
            .map(|dt| dt.timestamp())
            .unwrap_or(0);

        // 5. Execute via SettlementService
        let tx_signature = self.state.settlement_service
            .execute_generation_mint(meter_id, request.meter_serial, amount_kwh, timestamp)
            .await
            .map_err(|e| {
                error!("❌ Generation mint failed for {}: {}", request.meter_serial, e);
                ConnectError::internal(format!("On-chain minting failed: {}", e))
            })?;

        info!(
            "⛓️ Generation Mint Success: [Meter] {} - Minted: {} GRX - TX: {}",
            request.meter_serial, amount_kwh, tx_signature
        );

        Ok((SettleGenerationMintResponse {
            tx_signature,
            meter_serial: request.meter_serial.to_string(),
            amount_minted: request.energy_generated_kwh,
            status: "settled".to_string(),
            ..Default::default()
        }, ctx))
    }

    // =========================================================================
    // VPP Orchestration Handlers
    // =========================================================================

    async fn get_vpp_cluster(
        &self,
        ctx: Context,
        request: OwnedView<GetVppClusterRequestView<'static>>,
    ) -> Result<(VppClusterResponse, Context), ConnectError> {
        let cluster = self.state.vpp_repository.get_cluster_by_id(request.cluster_id)
            .await
            .map_err(|e| ConnectError::internal(format!("Failed to fetch VPP cluster: {}", e)))?
            .ok_or_else(|| ConnectError::not_found("VPP cluster not found"))?;

        Ok((VppClusterResponse {
            cluster_id: cluster.cluster_id,
            zone_id: cluster.zone_id,
            total_capacity_kwh: cluster.total_capacity_kwh,
            current_stored_kwh: cluster.current_stored_kwh,
            soc_percentage: cluster.soc_percentage,
            target_soc_percentage: cluster.target_soc_percentage,
            flex_up_kw: cluster.flex_up_kw,
            flex_down_kw: cluster.flex_down_kw,
            health_score: cluster.health_score,
            resource_count: cluster.resource_count,
            dispatch_mode: cluster.dispatch_mode,
            last_update: cluster.last_update.map(|dt| dt.to_rfc3339()).unwrap_or_default(),
        }, ctx))
    }

    async fn list_vpp_clusters(
        &self,
        ctx: Context,
        request: OwnedView<ListVppClustersRequestView<'static>>,
    ) -> Result<(ListVppClustersResponse, Context), ConnectError> {
        let zone_id = request.zone_id;
        
        // Using raw query for listing as we don't have a specific repo method for it yet
        let clusters = sqlx::query_as!(
            crate::domain::vpp::models::VppCluster,
            r#"SELECT id, cluster_id, zone_id, total_capacity_kwh, current_stored_kwh, 
               soc_percentage, target_soc_percentage, flex_up_kw, flex_down_kw, 
               health_score, resource_count, dispatch_mode, last_update, created_at
               FROM vpp_clusters WHERE ($1::INT IS NULL OR zone_id = $1)"#,
            zone_id
        )
        .fetch_all(&self.state.db)
        .await
        .map_err(|e| ConnectError::internal(format!("Failed to list VPP clusters: {}", e)))?;

        let clusters_proto = clusters.into_iter().map(|c| VppClusterResponse {
            cluster_id: c.cluster_id,
            zone_id: c.zone_id,
            total_capacity_kwh: c.total_capacity_kwh,
            current_stored_kwh: c.current_stored_kwh,
            soc_percentage: c.soc_percentage,
            target_soc_percentage: c.target_soc_percentage,
            flex_up_kw: c.flex_up_kw,
            flex_down_kw: c.flex_down_kw,
            health_score: c.health_score,
            resource_count: c.resource_count,
            dispatch_mode: c.dispatch_mode,
            last_update: c.last_update.map(|dt| dt.to_rfc3339()).unwrap_or_default(),
        }).collect();

        Ok((ListVppClustersResponse { clusters: clusters_proto }, ctx))
    }

    async fn dispatch_vpp(
        &self,
        ctx: Context,
        request: OwnedView<DispatchVppRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        sqlx::query!(
            "UPDATE vpp_clusters SET dispatch_mode = $2, target_soc_percentage = $3, last_update = NOW() WHERE cluster_id = $1",
            request.cluster_id,
            request.dispatch_mode,
            request.target_soc
        )
        .execute(&self.state.db)
        .await
        .map_err(|e| ConnectError::internal(format!("Failed to dispatch VPP: {}", e)))?;

        info!("🎮 VPP Dispatch issued: cluster={}, mode={}, target_soc={}%", 
            request.cluster_id, request.dispatch_mode, request.target_soc);

        Ok((TradingResponse {
            success: true,
            message: format!("VPP dispatch command '{}' accepted", request.dispatch_mode),
            ..Default::default()
        }, ctx))
    }
}
