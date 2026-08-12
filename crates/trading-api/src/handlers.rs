// Response structs are buffa-generated without constructors; building them
// mutably from Default is the idiom their API affords.
#![allow(clippy::field_reassign_with_default)]
use buffa::view::OwnedView;
use connectrpc::{ConnectError, Context, ErrorCode};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use tracing::{error, info};
use uuid::Uuid;

use crate::state::AppState;
use trading_core::models::TradingOrder;
use trading_core::types::{OrderSide, OrderStatus, OrderType, TimeInForce};
use trading_protocol::trading_proto::{
    BatchExecuteSettlementsRequestView, BatchSettlementResponse, BlockchainMarketDataResponse,
    CalculateP2PCostRequestView, CancelConditionalOrderRequestView, CancelOrderRequestView,
    CancelRecurringOrderRequestView, ConditionalOrderData, CreateConditionalOrderRequestView,
    CreateRecurringOrderRequestView, DispatchVppRequestView, ERCBalanceResponse,
    ExecuteSettlementRequestView, GetConditionalOrderRequestView, GetERCBalanceRequestView,
    GetOrderBookRequestView, GetOrderRequestView, GetRecurringOrderRequestView,
    GetTokenBalanceRequestView, GetVppClusterRequestView, IssueERCRequestView,
    ListConditionalOrdersRequestView, ListConditionalOrdersResponse, ListOrdersRequestView,
    ListOrdersResponse, ListRecurringOrdersRequestView, ListRecurringOrdersResponse,
    ListTradesRequestView, ListTradesResponse, ListVppClustersRequestView, ListVppClustersResponse,
    MarketStatsResponse, MatchingStatusResponse, NotifyOrderRequestView, OrderResponse,
    P2PMarketPrices, P2PTransactionCost, PauseRecurringOrderRequestView, RecurringOrderResponse,
    RelayOrderRequestView, ResumeRecurringOrderRequestView, RetireERCRequestView,
    SettlementResponse, SettlementStatsResponse, SubmitOrderRequestView, TokenBalanceResponse,
    TradingResponse, TradingService, TransferERCRequestView, UpdateOrderRequestView,
    VppClusterResponse,
};

pub struct TradingGrpcService {
    state: AppState,
}

impl TradingGrpcService {
    #[must_use]
    pub fn new(state: AppState) -> Self {
        Self { state }
    }
}

impl TradingService for TradingGrpcService {
    // Mirrors REST submit_order: one gate-by-gate sequence, kept linear.
    #[allow(clippy::too_many_lines)]
    async fn submit_order(
        &self,
        ctx: Context,
        request: OwnedView<SubmitOrderRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        info!(
            "📝 SubmitOrder: userId={}, side={}",
            request.user_id, request.side
        );

        let user_id = Uuid::parse_str(request.user_id).map_err(|e| {
            ConnectError::new(ErrorCode::InvalidArgument, format!("Invalid user_id: {e}"))
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
            #[allow(clippy::match_same_arms)] // explicit tokens beat a merged arm
            "limit" => OrderType::Limit,
            "market" => OrderType::Market,
            _ => OrderType::Limit,
        };

        let time_in_force = match request.time_in_force.map(str::to_lowercase).as_deref() {
            // Market orders default to IOC (fill now, never rest); limit → GTC.
            None => match order_type {
                OrderType::Market => TimeInForce::Ioc,
                OrderType::Limit => TimeInForce::Gtc,
            },
            Some("gtc") => TimeInForce::Gtc,
            Some("ioc") => TimeInForce::Ioc,
            Some("fok") => TimeInForce::Fok,
            Some(_) => {
                return Err(ConnectError::new(
                    ErrorCode::InvalidArgument,
                    "Invalid time_in_force (expected gtc|ioc|fok)",
                ))
            }
        };

        let market_segment = match request.market_segment.map(str::to_lowercase).as_deref() {
            None | Some("realtime") => trading_core::types::MarketSegment::Realtime,
            Some("interval") => trading_core::types::MarketSegment::Interval,
            Some(_) => {
                return Err(ConnectError::new(
                    ErrorCode::InvalidArgument,
                    "Invalid market_segment (expected realtime|interval)",
                ))
            }
        };

        // Interval orders clear in a 15-min uniform-price batch; IOC/FOK only make
        // sense under continuous matching, and the CDA IOC sweep never sees interval
        // orders (the matcher filters to Realtime), so an interval IOC remainder
        // would never be cancelled. Reject the combination.
        if market_segment == trading_core::types::MarketSegment::Interval
            && time_in_force != TimeInForce::Gtc
        {
            return Err(ConnectError::new(
                ErrorCode::InvalidArgument,
                "interval orders must be gtc (ioc/fok require continuous matching)",
            ));
        }

        let amount = Decimal::from_f64_retain(request.energy_amount).ok_or_else(|| {
            ConnectError::new(ErrorCode::InvalidArgument, "Invalid energy_amount")
        })?;

        // Parse the optional price input (limit price OR market-buy slippage cap),
        // then apply the shared admission policy. The proto price_per_kwh is a
        // non-optional f64, so exactly 0.0 means "absent" (a market buy falls back
        // to the ceiling; a limit order is then rejected as missing). A negative
        // value stays present and is rejected by the policy. NOTE: REST can tell
        // "0" from an omitted field and so rejects an explicit 0 cap; gRPC cannot
        // (0.0 == unset here), so a literal 0.0 is treated as "no cap". This
        // divergence is inherent to the proto and only affects the exact-zero case.
        // See `trading_core::order_policy::resolve_order_price`.
        let price_input = if request.price_per_kwh == 0.0 {
            None
        } else {
            Some(
                Decimal::from_f64_retain(request.price_per_kwh).ok_or_else(|| {
                    ConnectError::new(ErrorCode::InvalidArgument, "Invalid price_per_kwh")
                })?,
            )
        };
        let price = trading_core::order_policy::resolve_order_price(
            order_type,
            side,
            time_in_force,
            price_input,
        )
        .map_err(|e| ConnectError::new(ErrorCode::InvalidArgument, e.message()))?;

        // ── Sell-side meter-verification gate ────────────────────────────────
        // Same rule, same shared policy, as the REST edge: a sell must be backed
        // by a meter the seller owns and metering has verified. Enforced before
        // the epoch lookup so a refused order costs no writes.
        let meter = match (!request.meter_id.is_empty())
            .then(|| Uuid::parse_str(request.meter_id))
            .transpose()
            .map_err(|e| {
                ConnectError::new(ErrorCode::InvalidArgument, format!("Invalid meter_id: {e}"))
            })? {
            Some(id) => Some(
                self.state
                    .meter_repo
                    .lookup_by_id(id)
                    .await
                    .map_err(|e| {
                        error!("meter lookup failed: {e}");
                        ConnectError::new(ErrorCode::Internal, "Failed to resolve meter")
                    })?
                    .ok_or_else(|| {
                        ConnectError::new(
                            ErrorCode::InvalidArgument,
                            format!("unknown meter_id: {id}"),
                        )
                    })?,
            ),
            None => None,
        };

        let has_verified_meter =
            if trading_core::order_policy::needs_any_verified_meter_lookup(side, meter.as_ref()) {
                self.state
                    .meter_repo
                    .has_verified_meter(user_id)
                    .await
                    .map_err(|e| {
                        error!("verified-meter lookup failed: {e}");
                        ConnectError::new(ErrorCode::Internal, "Failed to resolve meter")
                    })?
            } else {
                false
            };
        if let Err(e) = trading_core::order_policy::check_sell_eligibility(
            user_id,
            side,
            meter.as_ref(),
            has_verified_meter,
        ) {
            tracing::warn!("sell order from {user_id} refused: {e:?}");
            // PermissionDenied mirrors REST's 403: well-formed request, caller
            // simply not authorized to sell yet.
            return Err(ConnectError::new(ErrorCode::PermissionDenied, e.message()));
        }

        // ── Sell-side price floor ────────────────────────────────────────────
        // Same shared policy as the REST edge (400 there): an ask below the
        // settleable floor can only ever be rejected `ChargesExceedCap` on-chain,
        // so refuse it rather than match it and strand the settlement.
        if let Err(e) = trading_core::order_policy::check_sell_price_floor(
            side,
            price,
            self.state.config.market.min_price_per_kwh,
            trading_core::charges::min_settleable_price_per_kwh(self.state.charge_rates.as_ref()),
        ) {
            tracing::warn!("sell order from {user_id} refused on price: {e:?} (price={price})");
            return Err(ConnectError::new(ErrorCode::InvalidArgument, e.message()));
        }

        // ── Buy-side funding gate ────────────────────────────────────────────
        // Same shared policy as the REST edge (402 there): refuse a bid whose
        // maximum spend knowably exceeds the buyer's currency balance. Fails
        // OPEN when the balance cannot be read — a Chain Bridge blip must not
        // refuse every buy; settlement's atomic swap is the real enforcement.
        let funding = trading_core::order_policy::required_buy_funding(
            side,
            order_type,
            price_input,
            price,
            amount,
        );
        if funding != trading_core::order_policy::BuyFundingRequirement::None {
            match self.state.blockchain.get_user_wallet(user_id).await {
                Ok(None) => {
                    return Err(ConnectError::new(
                        ErrorCode::PermissionDenied,
                        trading_core::order_policy::BuyFundingError::NoWallet.message(),
                    ));
                }
                Ok(Some(wallet)) => {
                    match self.state.blockchain.get_currency_balance(&wallet).await {
                        Ok(available) => {
                            if let Err(e) =
                                trading_core::order_policy::check_buy_funding(funding, available)
                            {
                                tracing::warn!("buy order from {user_id} refused: {e:?}");
                                return Err(ConnectError::new(
                                    ErrorCode::FailedPrecondition,
                                    e.message(),
                                ));
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                "buy funding gate skipped for {user_id}: currency balance read failed: {e}"
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "buy funding gate skipped for {user_id}: wallet lookup failed: {e}"
                    );
                }
            }
        }

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

        // Order lifetime, resolved through the same policy as the REST edge so the
        // two transports cannot drift. `expires_at` arrives as an RFC 3339 string
        // (proto has no timestamp type here); a malformed one is InvalidArgument
        // rather than a silent fallback to the default, which would hand back a
        // lifetime the client did not ask for.
        let requested_expires_at = match request.expires_at {
            None => None,
            Some(raw) => Some(
                chrono::DateTime::parse_from_rfc3339(raw)
                    .map_err(|e| {
                        ConnectError::new(
                            ErrorCode::InvalidArgument,
                            format!("Invalid expires_at (expected RFC 3339): {e}"),
                        )
                    })?
                    .with_timezone(&chrono::Utc),
            ),
        };
        let expires_at = trading_core::order_policy::resolve_expires_at(
            gridtokenx_telemetry::time::now(),
            requested_expires_at,
            request.expires_in_secs,
            None,
            self.state.config.order_expiry.default_ttl_secs,
            self.state.config.order_expiry.max_ttl_secs,
        )
        .map_err(|e| ConnectError::new(ErrorCode::InvalidArgument, e.message()))?;

        let order = TradingOrder {
            id: Uuid::new_v4(),
            user_id,
            order_type,
            side,
            energy_amount: amount,
            price_per_kwh: price,
            filled_amount: Decimal::ZERO,
            status: OrderStatus::Pending,
            expires_at: Some(expires_at),
            created_at: Some(gridtokenx_telemetry::time::now()),
            filled_at: None,
            epoch_id: Some(epoch_id),
            zone_id: request.zone_id,
            // Already parsed and existence-checked by the gate above; reuse that
            // result rather than re-parsing (the old `.ok()` silently dropped a
            // malformed id, storing NULL for an order the client meant to bind).
            meter_id: meter.map(|m| m.meter_id),
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
            time_in_force,
            market_segment,
        };

        // Insert the order and its OrderCreated event in one transaction so the
        // event can never be lost relative to the state change (the outbox row
        // is written atomically with the order; OutboxWorker relays it later).
        let event =
            trading_core::events::Event::OrderCreated(trading_core::events::OrderCreatedPayload {
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

        self.state
            .order_repo
            .insert_order_with_event(&order, &event)
            .await
            .map_err(|e| {
                error!("Database error: {}", e);
                ConnectError::new(ErrorCode::Internal, "Failed to insert order")
            })?;

        // Custodial on-chain order placement (Option A). Best-effort and gated by the
        // settlement feature flag: a failure leaves order_pda NULL so the settlement
        // worker skips it (unchanged behaviour) — it never fails the API call. Only
        // when enabled + on-chain succeeds does the order become settle-eligible.
        if self.state.config.trade_settlement_enabled {
            let seed = u64::from_le_bytes(
                order.id.as_bytes()[0..8]
                    .try_into()
                    .expect("uuid has 16 bytes"),
            );
            let is_buy = matches!(order.side, trading_core::types::OrderSide::Buy);
            match self
                .state
                .blockchain
                .place_order_on_chain(
                    order.user_id,
                    is_buy,
                    order.energy_amount,
                    order.price_per_kwh,
                    order.zone_id.unwrap_or(0),
                    seed,
                    // Row expiry → Order PDA (0 = no expiry). Resolved by
                    // order_policy::resolve_expires_at above, not by the program.
                    order.expires_at.map_or(0, |t| t.timestamp()),
                )
                .await
            {
                Ok((sig, pda)) => {
                    if let Err(e) = self
                        .state
                        .order_repo
                        .update_order_pda(order.id, &pda, i64::try_from(seed).unwrap_or(i64::MAX))
                        .await
                    {
                        tracing::warn!("order {}: persist order_pda failed: {}", order.id, e);
                    } else {
                        info!(
                            "order {} placed on-chain: pda={} sig={}",
                            order.id, pda, sig
                        );
                    }
                }
                Err(e) => {
                    // Same rule as the REST path, different remedy: here the row is already
                    // durable, so a final program rejection is neutralised by cancelling the
                    // order rather than by refusing the request. Leaving it Active would let
                    // the CDA match an order that can never settle — the incident where 14
                    // settlements burned their retry budget after `Custom 3007` placements.
                    let msg = e.to_string();
                    if trading_core::error::is_deterministic_chain_rejection(&msg) {
                        tracing::warn!(
                            "order {}: the program refused on-chain placement; cancelling so it \
                             cannot be matched into an unsettleable trade: {}",
                            order.id,
                            msg
                        );
                        if let Err(e2) = self
                            .state
                            .order_repo
                            .update_order_status(
                                order.id,
                                trading_core::types::OrderStatus::Cancelled,
                            )
                            .await
                        {
                            tracing::error!(
                                "order {}: placement was refused on-chain but the order could not \
                                 be cancelled ({}); it remains matchable and its settlement cannot land",
                                order.id,
                                e2
                            );
                        }
                    } else {
                        // No verdict reached — keep best-effort, but say what that costs.
                        tracing::warn!(
                            "order {}: on-chain placement failed to reach a verdict; left with a NULL \
                             order_pda, which nothing retries, so its settlement cannot land: {}",
                            order.id,
                            msg
                        );
                    }
                }
            }
        }

        // Realtime matching: wake the matcher now the order is durable, mirroring
        // the REST submit path in rest.rs. Fire-and-forget (never awaits, never
        // fails), and only for Realtime-segment orders — Interval orders are
        // cleared by the uniform-price path, which the CDA matcher skips.
        if order.market_segment == trading_core::types::MarketSegment::Realtime {
            self.state.matcher.request_cycle();
        }

        let mut res = TradingResponse::default();
        res.success = true;
        res.message = "Order submitted successfully".to_string();
        res.id = Some(order.id.to_string());

        Ok((res, ctx))
    }

    async fn get_order(
        &self,
        ctx: Context,
        request: OwnedView<GetOrderRequestView<'static>>,
    ) -> Result<(OrderResponse, Context), ConnectError> {
        let order_id = Uuid::parse_str(request.order_id).map_err(|e| {
            ConnectError::new(ErrorCode::InvalidArgument, format!("Invalid order_id: {e}"))
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

        Ok((res, ctx))
    }

    async fn cancel_order(
        &self,
        ctx: Context,
        request: OwnedView<CancelOrderRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        let order_id = Uuid::parse_str(request.order_id).map_err(|e| {
            ConnectError::new(ErrorCode::InvalidArgument, format!("Invalid order_id: {e}"))
        })?;
        let user_id = Uuid::parse_str(request.user_id).map_err(|e| {
            ConnectError::new(ErrorCode::InvalidArgument, format!("Invalid user_id: {e}"))
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

        Ok((res, ctx))
    }

    async fn list_orders(
        &self,
        ctx: Context,
        request: OwnedView<ListOrdersRequestView<'static>>,
    ) -> Result<(ListOrdersResponse, Context), ConnectError> {
        let user_id = Uuid::parse_str(request.user_id).map_err(|e| {
            ConnectError::new(ErrorCode::InvalidArgument, format!("Invalid user_id: {e}"))
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

        Ok((res, ctx))
    }

    async fn update_order(
        &self,
        ctx: Context,
        _req: OwnedView<UpdateOrderRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse::default(), ctx))
    }
    async fn notify_order(
        &self,
        ctx: Context,
        _req: OwnedView<NotifyOrderRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse::default(), ctx))
    }
    async fn get_order_book(
        &self,
        ctx: Context,
        request: OwnedView<GetOrderBookRequestView<'static>>,
    ) -> Result<(ListOrdersResponse, Context), ConnectError> {
        // Zone-partitioned book: active orders for one grid zone, mirroring the
        // REST `/api/v1/zones/{zone_id}/book` surface (rest.rs get_order_book).
        let zone_id = request
            .zone_id
            .ok_or_else(|| ConnectError::new(ErrorCode::InvalidArgument, "zone_id is required"))?;

        // Optional side filter (buy|sell). Compared as lowercase strings to keep
        // this independent of OrderSide's trait impls.
        let side_filter: Option<String> = match request.side.map(str::to_lowercase) {
            None => None,
            Some(s) if s == "buy" || s == "sell" => Some(s),
            Some(_) => {
                return Err(ConnectError::new(
                    ErrorCode::InvalidArgument,
                    "Invalid side (expected buy|sell)",
                ))
            }
        };

        let entries = self
            .state
            .order_repo
            .get_active_orders_by_zone(zone_id)
            .await
            .map_err(|_| ConnectError::new(ErrorCode::Internal, "Database error"))?;

        let orders = entries
            .into_iter()
            .filter(|e| match &side_filter {
                Some(s) => e.side.to_string().to_lowercase() == *s,
                None => true,
            })
            .map(|e| {
                let mut or = OrderResponse::default();
                or.id = e.order_id.to_string();
                or.user_id = e.user_id.to_string();
                // energy_amount is the remaining (unfilled) quantity; original_amount
                // is the total the order was placed with.
                or.energy_amount = e.energy_amount.to_f64().unwrap_or_default();
                or.price_per_kwh = e.price_per_kwh.to_f64().unwrap_or_default();
                or.filled_amount = (e.original_amount - e.energy_amount)
                    .to_f64()
                    .unwrap_or_default();
                or.side = e.side.to_string().to_lowercase();
                or.status = "open".to_string();
                or.created_at = e.created_at.to_rfc3339();
                or.zone_id = e.zone_id;
                or
            })
            .collect();

        let mut res = ListOrdersResponse::default();
        res.orders = orders;

        Ok((res, ctx))
    }
    async fn list_trades(
        &self,
        ctx: Context,
        _req: OwnedView<ListTradesRequestView<'static>>,
    ) -> Result<(ListTradesResponse, Context), ConnectError> {
        Ok((ListTradesResponse::default(), ctx))
    }
    async fn execute_settlement(
        &self,
        ctx: Context,
        _req: OwnedView<ExecuteSettlementRequestView<'static>>,
    ) -> Result<(SettlementResponse, Context), ConnectError> {
        Ok((SettlementResponse::default(), ctx))
    }
    async fn batch_execute_settlements(
        &self,
        ctx: Context,
        request: OwnedView<BatchExecuteSettlementsRequestView<'static>>,
    ) -> Result<(BatchSettlementResponse, Context), ConnectError> {
        info!(
            "💠 BatchExecuteSettlements: count={}",
            request.settlements.len()
        );

        let mut settlements = Vec::new();
        for req in &request.settlements {
            let id = Uuid::parse_str(req.settlement_id).map_err(|e| {
                ConnectError::new(
                    ErrorCode::InvalidArgument,
                    format!("Invalid settlement_id: {e}"),
                )
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
            return Ok((BatchSettlementResponse::default(), ctx));
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

        let first_sig = tx_results
            .first()
            .map(|r| r.signature.clone())
            .unwrap_or_default();
        let ids = tx_results
            .into_iter()
            .map(|r| r.settlement_id.to_string())
            .collect();

        let mut res = BatchSettlementResponse::default();
        res.success = true;
        res.signature = first_sig;
        res.settlement_ids = ids;
        res.message = "Batch processed successfully".to_string();

        Ok((res, ctx))
    }
    async fn issue_erc(
        &self,
        ctx: Context,
        _req: OwnedView<IssueERCRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse::default(), ctx))
    }
    async fn transfer_erc(
        &self,
        ctx: Context,
        _req: OwnedView<TransferERCRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse::default(), ctx))
    }
    async fn retire_erc(
        &self,
        ctx: Context,
        _req: OwnedView<RetireERCRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse::default(), ctx))
    }
    async fn get_erc_balance(
        &self,
        ctx: Context,
        _req: OwnedView<GetERCBalanceRequestView<'static>>,
    ) -> Result<(ERCBalanceResponse, Context), ConnectError> {
        Ok((ERCBalanceResponse::default(), ctx))
    }
    async fn calculate_p2p_cost(
        &self,
        ctx: Context,
        _req: OwnedView<CalculateP2PCostRequestView<'static>>,
    ) -> Result<(P2PTransactionCost, Context), ConnectError> {
        Ok((P2PTransactionCost::default(), ctx))
    }
    async fn get_market_prices(
        &self,
        ctx: Context,
        _req: OwnedView<trading_protocol::google::protobuf::EmptyView<'static>>,
    ) -> Result<(P2PMarketPrices, Context), ConnectError> {
        Ok((P2PMarketPrices::default(), ctx))
    }
    async fn relay_order(
        &self,
        ctx: Context,
        _req: OwnedView<RelayOrderRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse::default(), ctx))
    }
    async fn get_blockchain_market_data(
        &self,
        ctx: Context,
        _req: OwnedView<trading_protocol::google::protobuf::EmptyView<'static>>,
    ) -> Result<(BlockchainMarketDataResponse, Context), ConnectError> {
        Ok((BlockchainMarketDataResponse::default(), ctx))
    }
    async fn get_market_stats(
        &self,
        ctx: Context,
        _req: OwnedView<trading_protocol::google::protobuf::EmptyView<'static>>,
    ) -> Result<(MarketStatsResponse, Context), ConnectError> {
        Ok((MarketStatsResponse::default(), ctx))
    }
    async fn get_matching_status(
        &self,
        ctx: Context,
        _req: OwnedView<trading_protocol::google::protobuf::EmptyView<'static>>,
    ) -> Result<(MatchingStatusResponse, Context), ConnectError> {
        Ok((MatchingStatusResponse::default(), ctx))
    }
    async fn get_settlement_stats(
        &self,
        ctx: Context,
        _req: OwnedView<trading_protocol::google::protobuf::EmptyView<'static>>,
    ) -> Result<(SettlementStatsResponse, Context), ConnectError> {
        Ok((SettlementStatsResponse::default(), ctx))
    }
    async fn get_token_balance(
        &self,
        ctx: Context,
        _req: OwnedView<GetTokenBalanceRequestView<'static>>,
    ) -> Result<(TokenBalanceResponse, Context), ConnectError> {
        Ok((TokenBalanceResponse::default(), ctx))
    }
    async fn create_conditional_order(
        &self,
        ctx: Context,
        _req: OwnedView<CreateConditionalOrderRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse::default(), ctx))
    }
    async fn list_conditional_orders(
        &self,
        ctx: Context,
        _req: OwnedView<ListConditionalOrdersRequestView<'static>>,
    ) -> Result<(ListConditionalOrdersResponse, Context), ConnectError> {
        Ok((ListConditionalOrdersResponse::default(), ctx))
    }
    async fn cancel_conditional_order(
        &self,
        ctx: Context,
        _req: OwnedView<CancelConditionalOrderRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse::default(), ctx))
    }
    async fn get_conditional_order(
        &self,
        ctx: Context,
        _req: OwnedView<GetConditionalOrderRequestView<'static>>,
    ) -> Result<(ConditionalOrderData, Context), ConnectError> {
        Ok((ConditionalOrderData::default(), ctx))
    }
    async fn create_recurring_order(
        &self,
        ctx: Context,
        _req: OwnedView<CreateRecurringOrderRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse::default(), ctx))
    }
    async fn list_recurring_orders(
        &self,
        ctx: Context,
        _req: OwnedView<ListRecurringOrdersRequestView<'static>>,
    ) -> Result<(ListRecurringOrdersResponse, Context), ConnectError> {
        Ok((ListRecurringOrdersResponse::default(), ctx))
    }
    async fn get_recurring_order(
        &self,
        ctx: Context,
        _req: OwnedView<GetRecurringOrderRequestView<'static>>,
    ) -> Result<(RecurringOrderResponse, Context), ConnectError> {
        Ok((RecurringOrderResponse::default(), ctx))
    }
    async fn cancel_recurring_order(
        &self,
        ctx: Context,
        _req: OwnedView<CancelRecurringOrderRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse::default(), ctx))
    }
    async fn pause_recurring_order(
        &self,
        ctx: Context,
        _req: OwnedView<PauseRecurringOrderRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse::default(), ctx))
    }
    async fn resume_recurring_order(
        &self,
        ctx: Context,
        _req: OwnedView<ResumeRecurringOrderRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        Ok((TradingResponse::default(), ctx))
    }
    async fn get_vpp_cluster(
        &self,
        ctx: Context,
        _req: OwnedView<GetVppClusterRequestView<'static>>,
    ) -> Result<(VppClusterResponse, Context), ConnectError> {
        Ok((VppClusterResponse::default(), ctx))
    }
    async fn list_vpp_clusters(
        &self,
        ctx: Context,
        _req: OwnedView<ListVppClustersRequestView<'static>>,
    ) -> Result<(ListVppClustersResponse, Context), ConnectError> {
        Ok((ListVppClustersResponse::default(), ctx))
    }
    async fn dispatch_vpp(
        &self,
        ctx: Context,
        request: OwnedView<DispatchVppRequestView<'static>>,
    ) -> Result<(TradingResponse, Context), ConnectError> {
        info!(
            "🚀 DispatchVpp: cluster={}, target={}kW",
            request.cluster_id, request.target_kw
        );

        let dispatches = self
            .state
            .vpp
            .optimize_dispatch(
                request.cluster_id,
                request.target_kw,
                None, // No nodal prices for now
                None, // No carbon intensity for now
            )
            .await
            .map_err(|e| {
                error!("VPP Dispatch optimization failed: {}", e);
                ConnectError::new(ErrorCode::Internal, "Optimization failed")
            })?;

        info!(
            "✅ VPP Dispatch optimized: {} members to be commanded",
            dispatches.len()
        );

        // In a real system, we would now push these commands to the Oracle Bridge
        // via NATS or gRPC. For now, we return success.

        let mut res = TradingResponse::default();
        res.success = true;
        res.message = format!("Dispatched {} members", dispatches.len());

        Ok((res, ctx))
    }
}
