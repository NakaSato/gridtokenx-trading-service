use buffa::view::OwnedView;
use chrono::Utc;
use connectrpc::{ConnectError, Context, ErrorCode};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use tracing::{error, info};
use uuid::Uuid;

use crate::state::AppState;
use trading_core::models::TradingOrder;
use trading_core::types::{OrderSide, OrderStatus, OrderType, TimeInForce};
use trading_protocol::trading_proto::*;

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
        info!(
            "📝 SubmitOrder: userId={}, side={}",
            request.user_id, request.side
        );

        let user_id = Uuid::parse_str(request.user_id).map_err(|e| {
            ConnectError::new(
                ErrorCode::InvalidArgument,
                format!("Invalid user_id: {}", e),
            )
        })?;

        let side = match request.side.to_lowercase().as_str() {
            "buy" => OrderSide::Buy,
            "sell" => OrderSide::Sell,
            _ => {
                return Err(ConnectError::new(
                    ErrorCode::InvalidArgument,
                    "Invalid side",
                ))
            }
        };

        let order_type = match request.order_type.to_lowercase().as_str() {
            "limit" => OrderType::Limit,
            "market" => OrderType::Market,
            _ => OrderType::Limit,
        };

        let amount = Decimal::from_f64_retain(request.energy_amount).ok_or_else(|| {
            ConnectError::new(ErrorCode::InvalidArgument, "Invalid energy_amount")
        })?;

        let price = Decimal::from_f64_retain(request.price_per_kwh).ok_or_else(|| {
            ConnectError::new(ErrorCode::InvalidArgument, "Invalid price_per_kwh")
        })?;

        // Resolve the active market epoch so the matcher's settlement and
        // order_matches inserts satisfy their NOT NULL FK to market_epochs.
        let epoch_id = self
            .state
            .order_repo
            .get_or_create_active_epoch()
            .await
            .map_err(|e| {
                error!("Failed to resolve active epoch: {}", e);
                ConnectError::new(ErrorCode::Internal, "Failed to resolve active epoch")
            })?;

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
            epoch_id: Some(epoch_id),
            zone_id: request.zone_id,
            meter_id: if request.meter_id.is_empty() {
                None
            } else {
                Uuid::parse_str(request.meter_id).ok()
            },
            refund_tx_signature: None,
            order_pda: None,
            order_index: None,
            session_token: if request.session_token.is_empty() {
                None
            } else {
                Some(request.session_token.to_string())
            },
            blockchain_status: None,
            blockchain_tx_hash: None,
            blockchain_error: None,
            retry_count: 0,
            time_in_force: TimeInForce::Gtc,
        };

        self.state
            .order_repo
            .insert_order(&order)
            .await
            .map_err(|e| {
                error!("Database error: {}", e);
                ConnectError::new(ErrorCode::Internal, "Failed to insert order")
            })?;

        // 3. Publish Event for Event Sourcing
        let event = trading_core::events::Event::OrderCreated(trading_core::events::OrderCreatedPayload {
            id: order.id,
            user_id: order.user_id,
            order_type: order.order_type.to_string(),
            side: order.side.to_string(),
            energy_amount: order.energy_amount,
            price_per_kwh: order.price_per_kwh,
            status: order.status.to_string(),
            zone_id: order.zone_id,
            created_at: order.created_at,
        });

        if let Err(e) = self.state.events.publish(event).await {
            error!("Failed to publish OrderCreated event: {}", e);
            // We don't fail the request if event publishing fails, but we log it.
            // In a strict event-sourcing system, we might want to fail or use a transactional outbox.
        }

        let mut res = TradingResponse::default();
        res.success = true;
        res.message = "Order submitted successfully".to_string();
        res.id = Some(order.id.to_string());

        Ok((res, _ctx))
    }

    async fn get_order(
        &self,
        _ctx: Context,
        request: OwnedView<GetOrderRequestView<'static>>,
    ) -> Result<(OrderResponse, Context), ConnectError> {
        let order_id = Uuid::parse_str(request.order_id).map_err(|e| {
            ConnectError::new(
                ErrorCode::InvalidArgument,
                format!("Invalid order_id: {}", e),
            )
        })?;

        let order = self
            .state
            .order_repo
            .get_order(order_id)
            .await
            .map_err(|_| ConnectError::new(ErrorCode::Internal, "Database error"))?
            .ok_or_else(|| ConnectError::new(ErrorCode::NotFound, "Order not found"))?;

        let mut res = OrderResponse::default();
        res.id = order.id.to_string();
        res.user_id = order.user_id.to_string();
        res.energy_amount = order.energy_amount.to_f64().unwrap_or_default();
        res.price_per_kwh = order.price_per_kwh.to_f64().unwrap_or_default();
        res.filled_amount = order.filled_amount.to_f64().unwrap_or_default();
        res.side = order.side.to_string().to_lowercase();
        res.status = order.status.to_string().to_lowercase();
        res.created_at = order.created_at.map(|t| t.to_rfc3339()).unwrap_or_default();
        res.zone_id = order.zone_id;

        Ok((res, _ctx))
    }

    async fn cancel_order(
        &self,
        _ctx: Context,
        request: OwnedView<CancelOrderRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        let order_id = Uuid::parse_str(request.order_id).map_err(|e| {
            ConnectError::new(
                ErrorCode::InvalidArgument,
                format!("Invalid order_id: {}", e),
            )
        })?;
        let user_id = Uuid::parse_str(request.user_id).map_err(|e| {
            ConnectError::new(
                ErrorCode::InvalidArgument,
                format!("Invalid user_id: {}", e),
            )
        })?;

        let success = self
            .state
            .order_repo
            .cancel_order(order_id, user_id)
            .await
            .map_err(|_| ConnectError::new(ErrorCode::Internal, "Database error"))?;

        let mut res = TradingResponse::default();
        res.success = success;
        res.message = if success {
            "Order cancelled"
        } else {
            "Order not found or already closed"
        }
        .to_string();
        res.id = Some(order_id.to_string());

        Ok((res, _ctx))
    }

    async fn list_orders(
        &self,
        _ctx: Context,
        request: OwnedView<ListOrdersRequestView<'static>>,
    ) -> Result<(ListOrdersResponse, Context), ConnectError> {
        let user_id = Uuid::parse_str(request.user_id).map_err(|e| {
            ConnectError::new(
                ErrorCode::InvalidArgument,
                format!("Invalid user_id: {}", e),
            )
        })?;

        let orders = self
            .state
            .order_repo
            .get_orders_by_user(user_id, 50, 0)
            .await
            .map_err(|_| ConnectError::new(ErrorCode::Internal, "Database error"))?;

        let order_responses = orders
            .into_iter()
            .map(|o| {
                let mut or = OrderResponse::default();
                or.id = o.id.to_string();
                or.user_id = o.user_id.to_string();
                or.energy_amount = o.energy_amount.to_f64().unwrap_or_default();
                or.price_per_kwh = o.price_per_kwh.to_f64().unwrap_or_default();
                or.filled_amount = o.filled_amount.to_f64().unwrap_or_default();
                or.side = o.side.to_string().to_lowercase();
                or.status = o.status.to_string().to_lowercase();
                or.created_at = o.created_at.map(|t| t.to_rfc3339()).unwrap_or_default();
                or.zone_id = o.zone_id;
                or
            })
            .collect();

        let mut res = ListOrdersResponse::default();
        res.orders = order_responses;

        Ok((res, _ctx))
    }

    async fn update_order(
        &self,
        _ctx: Context,
        _req: OwnedView<UpdateOrderRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse::default(), _ctx))
    }
    async fn notify_order(
        &self,
        _ctx: Context,
        _req: OwnedView<NotifyOrderRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse::default(), _ctx))
    }
    async fn get_order_book(
        &self,
        _ctx: Context,
        _req: OwnedView<GetOrderBookRequestView<'static>>,
    ) -> Result<(ListOrdersResponse, Context), ConnectError> {
        Ok((ListOrdersResponse::default(), _ctx))
    }
    async fn list_trades(
        &self,
        _ctx: Context,
        _req: OwnedView<ListTradesRequestView<'static>>,
    ) -> Result<(ListTradesResponse, Context), ConnectError> {
        Ok((ListTradesResponse::default(), _ctx))
    }
    async fn execute_settlement(
        &self,
        _ctx: Context,
        _req: OwnedView<ExecuteSettlementRequestView<'static>>,
    ) -> Result<(SettlementResponse, Context), ConnectError> {
        Ok((SettlementResponse::default(), _ctx))
    }
    async fn batch_execute_settlements(
        &self,
        _ctx: Context,
        request: OwnedView<BatchExecuteSettlementsRequestView<'static>>,
    ) -> Result<(BatchSettlementResponse, Context), ConnectError> {
        info!("💠 BatchExecuteSettlements: count={}", request.settlements.len());

        let mut settlements = Vec::new();
        for req in request.settlements.iter() {
            let id = Uuid::parse_str(req.settlement_id).map_err(|e| {
                ConnectError::new(ErrorCode::InvalidArgument, format!("Invalid settlement_id: {}", e))
            })?;

            // Reconstruct a partial settlement for the service to process
            // The service will fetch the full record from DB anyway if needed, 
            // but we can pass the PDAs/wallets if we want to bypass DB.
            // For now, we'll let the service fetch the full records.
            if let Ok(Some(s)) = self.state.settlement_repo.get_settlement(id).await {
                settlements.push(s);
            }
        }

        if settlements.is_empty() {
            return Ok((BatchSettlementResponse::default(), _ctx));
        }

        // Execute via SettlementService
        let tx_results = self
            .state
            .settlement
            .execute_batched_settlements(settlements)
            .await
            .map_err(|e| {
                error!("Batch settlement failed: {}", e);
                ConnectError::new(ErrorCode::Internal, "Blockchain execution failed")
            })?;

        let first_sig = tx_results.first().map(|r| r.signature.clone()).unwrap_or_default();
        let ids = tx_results.into_iter().map(|r| r.settlement_id.to_string()).collect();

        let mut res = BatchSettlementResponse::default();
        res.success = true;
        res.signature = first_sig;
        res.settlement_ids = ids;
        res.message = "Batch processed successfully".to_string();

        Ok((res, _ctx))
    }
    async fn issue_erc(
        &self,
        _ctx: Context,
        _req: OwnedView<IssueERCRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse::default(), _ctx))
    }
    async fn transfer_erc(
        &self,
        _ctx: Context,
        _req: OwnedView<TransferERCRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse::default(), _ctx))
    }
    async fn retire_erc(
        &self,
        _ctx: Context,
        _req: OwnedView<RetireERCRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse::default(), _ctx))
    }
    async fn get_erc_balance(
        &self,
        _ctx: Context,
        _req: OwnedView<GetERCBalanceRequestView<'static>>,
    ) -> Result<(ERCBalanceResponse, Context), ConnectError> {
        Ok((ERCBalanceResponse::default(), _ctx))
    }
    async fn calculate_p2p_cost(
        &self,
        _ctx: Context,
        _req: OwnedView<CalculateP2PCostRequestView<'static>>,
    ) -> Result<(P2PTransactionCost, Context), ConnectError> {
        Ok((P2PTransactionCost::default(), _ctx))
    }
    async fn get_market_prices(
        &self,
        _ctx: Context,
        _req: OwnedView<trading_protocol::google::protobuf::EmptyView<'static>>,
    ) -> Result<(P2PMarketPrices, Context), ConnectError> {
        Ok((P2PMarketPrices::default(), _ctx))
    }
    async fn relay_order(
        &self,
        _ctx: Context,
        _req: OwnedView<RelayOrderRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse::default(), _ctx))
    }
    async fn get_blockchain_market_data(
        &self,
        _ctx: Context,
        _req: OwnedView<trading_protocol::google::protobuf::EmptyView<'static>>,
    ) -> Result<(BlockchainMarketDataResponse, Context), ConnectError> {
        Ok((BlockchainMarketDataResponse::default(), _ctx))
    }
    async fn get_market_stats(
        &self,
        _ctx: Context,
        _req: OwnedView<trading_protocol::google::protobuf::EmptyView<'static>>,
    ) -> Result<(MarketStatsResponse, Context), ConnectError> {
        Ok((MarketStatsResponse::default(), _ctx))
    }
    async fn get_matching_status(
        &self,
        _ctx: Context,
        _req: OwnedView<trading_protocol::google::protobuf::EmptyView<'static>>,
    ) -> Result<(MatchingStatusResponse, Context), ConnectError> {
        Ok((MatchingStatusResponse::default(), _ctx))
    }
    async fn get_settlement_stats(
        &self,
        _ctx: Context,
        _req: OwnedView<trading_protocol::google::protobuf::EmptyView<'static>>,
    ) -> Result<(SettlementStatsResponse, Context), ConnectError> {
        Ok((SettlementStatsResponse::default(), _ctx))
    }
    async fn get_token_balance(
        &self,
        _ctx: Context,
        _req: OwnedView<GetTokenBalanceRequestView<'static>>,
    ) -> Result<(TokenBalanceResponse, Context), ConnectError> {
        Ok((TokenBalanceResponse::default(), _ctx))
    }
    async fn settle_generation_mint(
        &self,
        _ctx: Context,
        request: OwnedView<SettleGenerationMintRequestView<'static>>,
    ) -> Result<(SettleGenerationMintResponse, Context), ConnectError> {
        info!(
            "💠 SettleGenerationMint: meter={}, user={}",
            request.meter_serial, request.user_id
        );

        // 1. Verify Oracle Signature
        // (Reusable logic across REST and gRPC)
        let is_verified = verify_oracle_signature(
            &request.meter_serial,
            &request.user_id,
            &request.start_time,
            &request.end_time,
            request.energy_generated_kwh,
            request.energy_consumed_kwh,
            &request.signature,
            &self.state.settlement.oracle_bridge_public_key,
        )
        .map_err(|e| ConnectError::new(ErrorCode::InvalidArgument, e))?;

        if !is_verified {
            return Err(ConnectError::new(
                ErrorCode::Unauthenticated,
                "Invalid Oracle signature",
            ));
        }

        let user_id = Uuid::parse_str(request.user_id).map_err(|e| {
            ConnectError::new(
                ErrorCode::InvalidArgument,
                format!("Invalid user_id: {}", e),
            )
        })?;

        let amount = Decimal::from_f64_retain(request.energy_generated_kwh).ok_or_else(|| {
            ConnectError::new(ErrorCode::InvalidArgument, "Invalid energy_amount")
        })?;

        // 2. Execute via SettlementService
        let tx_sig = self
            .state
            .settlement
            .execute_generation_mint(user_id, amount, chrono::Utc::now().timestamp())
            .await
            .map_err(|e| {
                error!("Settlement failed: {}", e);
                ConnectError::new(ErrorCode::Internal, "Blockchain execution failed")
            })?;

        let mut res = SettleGenerationMintResponse::default();
        res.tx_signature = tx_sig;
        res.status = "success".to_string();

        Ok((res, _ctx))
    }

    async fn batch_settle_generation_mint(
        &self,
        _ctx: Context,
        request: OwnedView<BatchSettleGenerationMintRequestView<'static>>,
    ) -> Result<(BatchSettleGenerationMintResponse, Context), ConnectError> {
        info!("💠 BatchSettleGenerationMint: count={}", request.requests.len());

        let mut settlements = Vec::new();
        let mut serials = Vec::new();

        for req in request.requests.iter() {
            // 1. Verify Oracle Signature
            let is_verified = verify_oracle_signature(
                &req.meter_serial,
                &req.user_id,
                &req.start_time,
                &req.end_time,
                req.energy_generated_kwh,
                req.energy_consumed_kwh,
                &req.signature,
                &self.state.settlement.oracle_bridge_public_key,
            )
            .map_err(|e| ConnectError::new(ErrorCode::InvalidArgument, e))?;

            if !is_verified {
                return Err(ConnectError::new(
                    ErrorCode::Unauthenticated,
                    format!("Invalid Oracle signature for meter {}", req.meter_serial),
                ));
            }

            let user_id = Uuid::parse_str(req.user_id).map_err(|e| {
                ConnectError::new(
                    ErrorCode::InvalidArgument,
                    format!("Invalid user_id: {}", e),
                )
            })?;

            let amount = Decimal::from_f64_retain(req.energy_generated_kwh).ok_or_else(|| {
                ConnectError::new(ErrorCode::InvalidArgument, "Invalid energy_amount")
            })?;

            // Create a virtual settlement object for batching
            let settlement = trading_core::models::Settlement {
                id: Uuid::new_v4(),
                trade_id: None,
                epoch_id: Uuid::nil(),
                buyer_id: Uuid::nil(),
                seller_id: user_id,
                buy_order_id: Uuid::nil(),
                sell_order_id: Uuid::nil(),
                energy_amount: amount,
                price: Decimal::ZERO,
                total_amount: Decimal::ZERO,
                fee_amount: Decimal::ZERO,
                net_amount: Decimal::ZERO,
                status: trading_core::models::SettlementStatus::Pending,
                blockchain_tx: None,
                created_at: chrono::Utc::now(),
                confirmed_at: None,
                wheeling_charge: None,
                loss_factor: None,
                loss_cost: None,
                effective_energy: None,
                buyer_zone_id: None,
                seller_zone_id: None,
                buyer_session_token: None,
                seller_session_token: None,
                erc_certificate_id: None,
                erc_transfer_tx: None,
                retry_count: 0,
                error_message: None,
            };

            settlements.push(settlement);
            serials.push(req.meter_serial.to_string());
        }

        if settlements.is_empty() {
             return Ok((BatchSettleGenerationMintResponse::default(), _ctx));
        }

        // 2. Execute via SettlementService (which now uses batched blockchain calls)
        let tx_results = self
            .state
            .settlement
            .execute_batched_settlements(settlements)
            .await
            .map_err(|e| {
                error!("Batch settlement failed: {}", e);
                ConnectError::new(ErrorCode::Internal, "Blockchain execution failed")
            })?;

        let tx_signature = tx_results.first().map(|r| r.signature.clone()).unwrap_or_default();

        let mut res = BatchSettleGenerationMintResponse::default();
        res.success = true;
        res.tx_signature = tx_signature;
        res.meter_serials = serials;
        res.message = "Batch processed".to_string();

        Ok((res, _ctx))
    }

    async fn create_conditional_order(
        &self,
        _ctx: Context,
        _req: OwnedView<CreateConditionalOrderRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse::default(), _ctx))
    }
    async fn list_conditional_orders(
        &self,
        _ctx: Context,
        _req: OwnedView<ListConditionalOrdersRequestView<'static>>,
    ) -> Result<(ListConditionalOrdersResponse, Context), ConnectError> {
        Ok((ListConditionalOrdersResponse::default(), _ctx))
    }
    async fn cancel_conditional_order(
        &self,
        _ctx: Context,
        _req: OwnedView<CancelConditionalOrderRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse::default(), _ctx))
    }
    async fn get_conditional_order(
        &self,
        _ctx: Context,
        _req: OwnedView<GetConditionalOrderRequestView<'static>>,
    ) -> Result<(ConditionalOrderData, Context), ConnectError> {
        Ok((ConditionalOrderData::default(), _ctx))
    }
    async fn create_recurring_order(
        &self,
        _ctx: Context,
        _req: OwnedView<CreateRecurringOrderRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse::default(), _ctx))
    }
    async fn list_recurring_orders(
        &self,
        _ctx: Context,
        _req: OwnedView<ListRecurringOrdersRequestView<'static>>,
    ) -> Result<(ListRecurringOrdersResponse, Context), ConnectError> {
        Ok((ListRecurringOrdersResponse::default(), _ctx))
    }
    async fn get_recurring_order(
        &self,
        _ctx: Context,
        _req: OwnedView<GetRecurringOrderRequestView<'static>>,
    ) -> Result<(RecurringOrderResponse, Context), ConnectError> {
        Ok((RecurringOrderResponse::default(), _ctx))
    }
    async fn cancel_recurring_order(
        &self,
        _ctx: Context,
        _req: OwnedView<CancelRecurringOrderRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse::default(), _ctx))
    }
    async fn pause_recurring_order(
        &self,
        _ctx: Context,
        _req: OwnedView<PauseRecurringOrderRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse::default(), _ctx))
    }
    async fn resume_recurring_order(
        &self,
        _ctx: Context,
        _req: OwnedView<ResumeRecurringOrderRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse::default(), _ctx))
    }
    async fn get_vpp_cluster(
        &self,
        _ctx: Context,
        _req: OwnedView<GetVppClusterRequestView<'static>>,
    ) -> Result<(VppClusterResponse, Context), ConnectError> {
        Ok((VppClusterResponse::default(), _ctx))
    }
    async fn list_vpp_clusters(
        &self,
        _ctx: Context,
        _req: OwnedView<ListVppClustersRequestView<'static>>,
    ) -> Result<(ListVppClustersResponse, Context), ConnectError> {
        Ok((ListVppClustersResponse::default(), _ctx))
    }
    async fn dispatch_vpp(
        &self,
        _ctx: Context,
        request: OwnedView<DispatchVppRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        info!("🚀 DispatchVpp: cluster={}, target={}kW", request.cluster_id, request.target_kw);

        let dispatches = self.state.vpp.optimize_dispatch(
            request.cluster_id,
            request.target_kw,
            None, // No nodal prices for now
            None, // No carbon intensity for now
        ).await
        .map_err(|e| {
            error!("VPP Dispatch optimization failed: {}", e);
            ConnectError::new(ErrorCode::Internal, "Optimization failed")
        })?;

        info!("✅ VPP Dispatch optimized: {} members to be commanded", dispatches.len());

        // In a real system, we would now push these commands to the Oracle Bridge 
        // via NATS or gRPC. For now, we return success.

        let mut res = TradingResponse::default();
        res.success = true;
        res.message = format!("Dispatched {} members", dispatches.len());

        Ok((res, _ctx))
    }
}

/// Helper to verify Ed25519 signature (Shared logic)
fn verify_oracle_signature(
    meter_serial: &str,
    user_id: &str,
    start_time: &str,
    end_time: &str,
    energy_generated: f64,
    energy_consumed: f64,
    signature_hex: &str,
    public_key_bs58: &str,
) -> Result<bool, String> {
    use ed25519_dalek::{Signature, Verifier, VerifyingKey};
    use std::convert::TryFrom;

    let message = format!(
        "{}:{}:{}:{}:{}:{}",
        meter_serial, user_id, start_time, end_time, energy_generated, energy_consumed
    );

    let pubkey_bytes = bs58::decode(public_key_bs58)
        .into_vec()
        .map_err(|e| format!("Invalid public key: {}", e))?;

    let verifying_key = VerifyingKey::try_from(pubkey_bytes.as_slice())
        .map_err(|e| format!("Invalid public key bytes: {}", e))?;

    let sig_bytes =
        hex::decode(signature_hex).map_err(|e| format!("Invalid signature hex: {}", e))?;

    let signature =
        Signature::from_slice(&sig_bytes).map_err(|e| format!("Invalid signature bytes: {}", e))?;

    Ok(verifying_key.verify(message.as_bytes(), &signature).is_ok())
}
