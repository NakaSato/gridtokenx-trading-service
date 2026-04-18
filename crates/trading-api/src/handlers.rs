use connectrpc::{Context, ConnectError, ErrorCode};
use buffa::view::OwnedView;
use tracing::{info, error};
use uuid::Uuid;
use rust_decimal::Decimal;
use chrono::Utc;

use trading_core::models::TradingOrder;
use trading_core::types::{OrderSide, OrderStatus, OrderType, TimeInForce};
use trading_protocol::trading_proto::*;
use crate::state::AppState;

pub struct TradingGrpcService {
    state: AppState,
}

impl TradingGrpcService {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }
}

impl TradingService for TradingGrpcService {
    async fn submit_order(
        &self,
        _ctx: Context,
        request: OwnedView<SubmitOrderRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        info!("📝 SubmitOrder: userId={}, side={}", request.user_id, request.side);

        let user_id = Uuid::parse_str(request.user_id)
            .map_err(|e| ConnectError::new(ErrorCode::InvalidArgument, format!("Invalid user_id: {}", e)))?;

        let side = match request.side.to_lowercase().as_str() {
            "buy" => OrderSide::Buy,
            "sell" => OrderSide::Sell,
            _ => return Err(ConnectError::new(ErrorCode::InvalidArgument, "Invalid side")),
        };

        let order_type = match request.order_type.to_lowercase().as_str() {
            "limit" => OrderType::Limit,
            "market" => OrderType::Market,
            _ => OrderType::Limit,
        };

        let amount = Decimal::from_f64_retain(request.energy_amount)
            .ok_or_else(|| ConnectError::new(ErrorCode::InvalidArgument, "Invalid energy_amount"))?;
        
        let price = Decimal::from_f64_retain(request.price_per_kwh)
            .ok_or_else(|| ConnectError::new(ErrorCode::InvalidArgument, "Invalid price_per_kwh"))?;

        let order = TradingOrder {
            id: Uuid::new_v4(),
            user_id,
            order_type,
            side,
            energy_amount: amount,
            price_per_kwh: price,
            filled_amount: Decimal::ZERO,
            status: OrderStatus::Pending,
            expires_at: Some(Utc::now() + chrono::Duration::hours(24)),
            created_at: Some(Utc::now()),
            filled_at: None,
            epoch_id: None,
            zone_id: request.zone_id,
            meter_id: if request.meter_id.is_empty() { None } else { Uuid::parse_str(request.meter_id).ok() },
            refund_tx_signature: None,
            order_pda: None,
            order_index: None,
            session_token: if request.session_token.is_empty() { None } else { Some(request.session_token.to_string()) },
            blockchain_status: None,
            blockchain_tx_hash: None,
            blockchain_error: None,
            retry_count: 0,
            time_in_force: TimeInForce::Gtc,
        };

        self.state.order_repo.insert_order(&order).await
            .map_err(|e| {
                error!("Database error: {}", e);
                ConnectError::new(ErrorCode::Internal, "Failed to insert order")
            })?;

        Ok((TradingResponse {
            success: true,
            message: "Order submitted successfully".to_string(),
            id: Some(order.id.to_string()),
        }, _ctx))
    }

    async fn get_order(
        &self,
        _ctx: Context,
        request: OwnedView<GetOrderRequestView<'static>>,
    ) -> Result<(OrderResponse, Context), ConnectError> {
        let order_id = Uuid::parse_str(request.order_id)
            .map_err(|e| ConnectError::new(ErrorCode::InvalidArgument, format!("Invalid order_id: {}", e)))?;

        let order = self.state.order_repo.get_order(order_id).await
            .map_err(|_| ConnectError::new(ErrorCode::Internal, "Database error"))?
            .ok_or_else(|| ConnectError::new(ErrorCode::NotFound, "Order not found"))?;

        Ok((OrderResponse {
            id: order.id.to_string(),
            user_id: order.user_id.to_string(),
            energy_amount: order.energy_amount.to_f64().unwrap_or_default(),
            price_per_kwh: order.price_per_kwh.to_f64().unwrap_or_default(),
            filled_amount: order.filled_amount.to_f64().unwrap_or_default(),
            side: order.side.to_string().to_lowercase(),
            status: order.status.to_string().to_lowercase(),
            created_at: order.created_at.map(|t| t.to_rfc3339()).unwrap_or_default(),
            zone_id: order.zone_id,
        }, _ctx))
    }

    async fn cancel_order(
        &self,
        _ctx: Context,
        request: OwnedView<CancelOrderRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        let order_id = Uuid::parse_str(request.order_id)
            .map_err(|e| ConnectError::new(ErrorCode::InvalidArgument, format!("Invalid order_id: {}", e)))?;
        let user_id = Uuid::parse_str(request.user_id)
            .map_err(|e| ConnectError::new(ErrorCode::InvalidArgument, format!("Invalid user_id: {}", e)))?;

        let success = self.state.order_repo.cancel_order(order_id, user_id).await
            .map_err(|_| ConnectError::new(ErrorCode::Internal, "Database error"))?;

        Ok((TradingResponse {
            success,
            message: if success { "Order cancelled" } else { "Order not found or already closed" }.to_string(),
            id: Some(order_id.to_string()),
        }, _ctx))
    }

    async fn list_orders(
        &self,
        _ctx: Context,
        request: OwnedView<ListOrdersRequestView<'static>>,
    ) -> Result<(ListOrdersResponse, Context), ConnectError> {
        let user_id = Uuid::parse_str(request.user_id)
            .map_err(|e| ConnectError::new(ErrorCode::InvalidArgument, format!("Invalid user_id: {}", e)))?;

        let orders = self.state.order_repo.get_orders_by_user(user_id, 50, 0).await
            .map_err(|_| ConnectError::new(ErrorCode::Internal, "Database error"))?;

        let order_responses = orders.into_iter().map(|o| OrderResponse {
            id: o.id.to_string(),
            user_id: o.user_id.to_string(),
            energy_amount: o.energy_amount.to_f64().unwrap_or_default(),
            price_per_kwh: o.price_per_kwh.to_f64().unwrap_or_default(),
            filled_amount: o.filled_amount.to_f64().unwrap_or_default(),
            side: o.side.to_string().to_lowercase(),
            status: o.status.to_string().to_lowercase(),
            created_at: o.created_at.map(|t| t.to_rfc3339()).unwrap_or_default(),
            zone_id: o.zone_id,
        }).collect();

        Ok((ListOrdersResponse { orders: order_responses }, _ctx))
    }

    // Add empty implementations for other methods to satisfy the trait
    async fn update_order(&self, _ctx: Context, _req: OwnedView<UpdateOrderRequestView<'static>>) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse { success: true, message: "Stub".to_string(), id: None }, _ctx))
    }
    async fn notify_order(&self, _ctx: Context, _req: OwnedView<NotifyOrderRequestView<'static>>) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse { success: true, message: "Stub".to_string(), id: None }, _ctx))
    }
    async fn get_order_book(&self, _ctx: Context, _req: OwnedView<GetOrderBookRequestView<'static>>) -> Result<(ListOrdersResponse, Context), ConnectError> {
        Ok((ListOrdersResponse { orders: vec![] }, _ctx))
    }
    async fn list_trades(&self, _ctx: Context, _req: OwnedView<ListTradesRequestView<'static>>) -> Result<(ListTradesResponse, Context), ConnectError> {
        Ok((ListTradesResponse { trades: vec![] }, _ctx))
    }
    async fn execute_settlement(&self, _ctx: Context, _req: OwnedView<ExecuteSettlementRequestView<'static>>) -> Result<(SettlementResponse, Context), ConnectError> {
        Ok((SettlementResponse { success: true, message: "Stub".to_string(), signature: "".to_string(), slot: 0 }, _ctx))
    }
    async fn batch_execute_settlements(&self, _ctx: Context, _req: OwnedView<BatchExecuteSettlementsRequestView<'static>>) -> Result<(BatchSettlementResponse, Context), ConnectError> {
        Ok((BatchSettlementResponse { success: true, message: "Stub".to_string(), signature: "".to_string(), slot: 0, settlement_ids: vec![] }, _ctx))
    }
    async fn issue_erc(&self, _ctx: Context, _req: OwnedView<IssueERCRequestView<'static>>) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse { success: true, message: "Stub".to_string(), id: None }, _ctx))
    }
    async fn transfer_erc(&self, _ctx: Context, _req: OwnedView<TransferERCRequestView<'static>>) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse { success: true, message: "Stub".to_string(), id: None }, _ctx))
    }
    async fn retire_erc(&self, _ctx: Context, _req: OwnedView<RetireERCRequestView<'static>>) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse { success: true, message: "Stub".to_string(), id: None }, _ctx))
    }
    async fn get_erc_balance(&self, _ctx: Context, _req: OwnedView<GetERCBalanceRequestView<'static>>) -> Result<(ERCBalanceResponse, Context), ConnectError> {
        Ok((ERCBalanceResponse { balance: 0.0, asset_type: "GRX".to_string() }, _ctx))
    }
    async fn calculate_p2_p_cost(&self, _ctx: Context, _req: OwnedView<CalculateP2PCostRequestView<'static>>) -> Result<(P2PTransactionCost, Context), ConnectError> {
        Ok((P2PTransactionCost { energy_cost: 0.0, wheeling_charge: 0.0, loss_cost: 0.0, total_cost: 0.0, effective_energy: 0.0, loss_factor: 0.0, loss_allocation: "".to_string(), zone_distance_km: 0.0, buyer_zone: 0, seller_zone: 0, is_grid_compliant: true, grid_violation_reason: None }, _ctx))
    }
    async fn get_market_prices(&self, _ctx: Context, _req: OwnedView<::buffa_types::google::protobuf::EmptyView<'static>>) -> Result<(P2PMarketPrices, Context), ConnectError> {
        Ok((P2PMarketPrices { base_price_thb_kwh: 0.0, grid_import_price_thb_kwh: 0.0, grid_export_price_thb_kwh: 0.0, loss_allocation_model: "".to_string(), wheeling_charges: std::collections::HashMap::new(), loss_factors: std::collections::HashMap::new() }, _ctx))
    }
    async fn relay_order(&self, _ctx: Context, _req: OwnedView<RelayOrderRequestView<'static>>) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse { success: true, message: "Stub".to_string(), id: None }, _ctx))
    }
    async fn get_blockchain_market_data(&self, _ctx: Context, _req: OwnedView<::buffa_types::google::protobuf::EmptyView<'static>>) -> Result<(BlockchainMarketDataResponse, Context), ConnectError> {
        Ok((BlockchainMarketDataResponse { success: true, message: "".to_string(), authority: "".to_string(), active_orders: 0, total_volume: 0, total_trades: 0, market_fee_bps: 0, clearing_enabled: true, created_at: 0 }, _ctx))
    }
    async fn get_market_stats(&self, _ctx: Context, _req: OwnedView<::buffa_types::google::protobuf::EmptyView<'static>>) -> Result<(MarketStatsResponse, Context), ConnectError> {
        Ok((MarketStatsResponse { average_price: 0.0, total_volume: 0.0, active_orders: 0, pending_orders: 0, completed_matches: 0 }, _ctx))
    }
    async fn get_matching_status(&self, _ctx: Context, _req: OwnedView<::buffa_types::google::protobuf::EmptyView<'static>>) -> Result<(MatchingStatusResponse, Context), ConnectError> {
        Ok((MatchingStatusResponse { pending_buy_orders: 0, pending_sell_orders: 0, pending_matches: 0, buy_min_price: 0.0, buy_max_price: 0.0, sell_min_price: 0.0, sell_max_price: 0.0, can_match: true, match_reason: "".to_string() }, _ctx))
    }
    async fn get_settlement_stats(&self, _ctx: Context, _req: OwnedView<::buffa_types::google::protobuf::EmptyView<'static>>) -> Result<(SettlementStatsResponse, Context), ConnectError> {
        Ok((SettlementStatsResponse { pending_count: 0, processing_count: 0, confirmed_count: 0, failed_count: 0, total_settled_value: 0.0, recent_settlements: vec![] }, _ctx))
    }
    async fn get_token_balance(&self, _ctx: Context, _req: OwnedView<GetTokenBalanceRequestView<'static>>) -> Result<(TokenBalanceResponse, Context), ConnectError> {
        Ok((TokenBalanceResponse { wallet_address: "".to_string(), token_balance: 0.0, raw_balance: 0, mint: "".to_string() }, _ctx))
    }
    async fn settle_generation_mint(&self, _ctx: Context, _req: OwnedView<SettleGenerationMintRequestView<'static>>) -> Result<(SettleGenerationMintResponse, Context), ConnectError> {
        Ok((SettleGenerationMintResponse { tx_signature: "".to_string(), meter_serial: "".to_string(), amount_minted: 0.0, status: "ok".to_string() }, _ctx))
    }
    async fn create_conditional_order(&self, _ctx: Context, _req: OwnedView<CreateConditionalOrderRequestView<'static>>) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse { success: true, message: "Stub".to_string(), id: None }, _ctx))
    }
    async fn list_conditional_orders(&self, _ctx: Context, _req: OwnedView<ListConditionalOrdersRequestView<'static>>) -> Result<(ListConditionalOrdersResponse, Context), ConnectError> {
        Ok((ListConditionalOrdersResponse { orders: vec![] }, _ctx))
    }
    async fn cancel_conditional_order(&self, _ctx: Context, _req: OwnedView<CancelConditionalOrderRequestView<'static>>) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse { success: true, message: "Stub".to_string(), id: None }, _ctx))
    }
    async fn get_conditional_order(&self, _ctx: Context, _req: OwnedView<GetConditionalOrderRequestView<'static>>) -> Result<(ConditionalOrderData, Context), ConnectError> {
        Ok((ConditionalOrderData { id: "".to_string(), user_id: "".to_string(), side: "".to_string(), energy_amount: 0.0, trigger_price: 0.0, trigger_type: "".to_string(), trigger_status: "".to_string(), limit_price: None, trailing_offset: None, expires_at: None, created_at: "".to_string(), triggered_at: None, last_peak_price: None }, _ctx))
    }
    async fn create_recurring_order(&self, _ctx: Context, _req: OwnedView<CreateRecurringOrderRequestView<'static>>) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse { success: true, message: "Stub".to_string(), id: None }, _ctx))
    }
    async fn list_recurring_orders(&self, _ctx: Context, _req: OwnedView<ListRecurringOrdersRequestView<'static>>) -> Result<(ListRecurringOrdersResponse, Context), ConnectError> {
        Ok((ListRecurringOrdersResponse { orders: vec![] }, _ctx))
    }
    async fn get_recurring_order(&self, _ctx: Context, _req: OwnedView<GetRecurringOrderRequestView<'static>>) -> Result<(RecurringOrderResponse, Context), ConnectError> {
        Ok((RecurringOrderResponse { id: "".to_string(), status: "".to_string(), next_execution_at: "".to_string(), created_at: "".to_string(), message: "".to_string() }, _ctx))
    }
    async fn cancel_recurring_order(&self, _ctx: Context, _req: OwnedView<CancelRecurringOrderRequestView<'static>>) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse { success: true, message: "Stub".to_string(), id: None }, _ctx))
    }
    async fn pause_recurring_order(&self, _ctx: Context, _req: OwnedView<PauseRecurringOrderRequestView<'static>>) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse { success: true, message: "Stub".to_string(), id: None }, _ctx))
    }
    async fn resume_recurring_order(&self, _ctx: Context, _req: OwnedView<ResumeRecurringOrderRequestView<'static>>) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse { success: true, message: "Stub".to_string(), id: None }, _ctx))
    }
    async fn get_vpp_cluster(&self, _ctx: Context, _req: OwnedView<GetVppClusterRequestView<'static>>) -> Result<(VppClusterResponse, Context), ConnectError> {
        Ok((VppClusterResponse { cluster_id: "".to_string(), zone_id: None, total_capacity_kwh: 0.0, current_stored_kwh: 0.0, soc_percentage: 0.0, target_soc_percentage: 0.0, flex_up_kw: 0.0, flex_down_kw: 0.0, health_score: 0.0, resource_count: 0, dispatch_mode: "".to_string(), last_update: "".to_string() }, _ctx))
    }
    async fn list_vpp_clusters(&self, _ctx: Context, _req: OwnedView<ListVppClustersRequestView<'static>>) -> Result<(ListVppClustersResponse, Context), ConnectError> {
        Ok((ListVppClustersResponse { clusters: vec![] }, _ctx))
    }
    async fn dispatch_vpp(&self, _ctx: Context, _req: OwnedView<DispatchVppRequestView<'static>>) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse { success: true, message: "Stub".to_string(), id: None }, _ctx))
    }
}
