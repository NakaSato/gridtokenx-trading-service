//! Order lifecycle — submit, book, list, cancel, quote.
//!
//! Split out of the former 3.3k-line `rest.rs` for readability. Pure code move:
//! handlers are re-exported from `rest/mod.rs`, so every `crate::rest::<name>`
//! path (router wiring, openapi.rs) resolves exactly as before.

use super::{
    dec_f64, info, ActiveOrderMeter, ActiveOrderMetersResponse, AppState, Decimal, FromStr,
    GridMetrics, HashMap, Json, ListOrdersParams, ListOrdersResponse, OrderBookResponse, OrderData,
    OrderSide, OrderStatus, OrderType, Pagination, Path, Query, QuoteBreakdown, QuoteRequest,
    QuoteResponse, Serialize, ServiceRole, State, SubmitOrderRequest, SubmitOrderResponse,
    TimeInForce, ToSchema, TradingOrder, UserContext, Uuid,
};

/// Submit a spot order (limit or market) into the CDA or interval market.
#[utoipa::path(
    post,
    path = "/api/v1/orders",
    tag = "orders",
    request_body = SubmitOrderRequest,
    responses(
        (status = 200, description = "Order accepted (status `open`)", body = SubmitOrderResponse),
        (status = 400, description = "Invalid side/type/amount/price/TIF/segment combination, or an unknown meter_serial/meter_id", body = String),
        (status = 401, description = "Missing or invalid user id header", body = String),
        (status = 402, description = "Buy order whose maximum spend exceeds the buyer's currency balance", body = String),
        (status = 403, description = "Caller role not allowed, a sell order with no verified meter behind it, or a buy order from a user with no on-chain wallet", body = String),
        (status = 500, description = "Database or epoch resolution error", body = String),
    ),
    security(("gateway_role" = [], "user_id" = [])),
)]
/// # Panics
/// Panics only on a poisoned internal lock — process-fatal by design.
// Gate-by-gate order intake; the sequence reads top-to-bottom.
#[allow(clippy::too_many_lines)]
pub async fn submit_order(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Json(req): Json<SubmitOrderRequest>,
) -> Result<Json<SubmitOrderResponse>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let submit_started = std::time::Instant::now();
    tracing::info!("Submit order request: {:?}", req);

    let amount = Decimal::from_str(&req.energy_amount_kwh).map_err(|e| {
        (
            axum::http::StatusCode::BAD_REQUEST,
            format!("Invalid energy_amount_kwh: {e}"),
        )
    })?;

    let side = match req.side.to_lowercase().as_str() {
        "buy" => OrderSide::Buy,
        "sell" => OrderSide::Sell,
        _ => {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                "Invalid side".to_string(),
            ))
        }
    };

    let order_type = match req.order_type.to_lowercase().as_str() {
        #[allow(clippy::match_same_arms)] // explicit tokens beat a merged arm
        "limit" => OrderType::Limit,
        "market" => OrderType::Market,
        _ => OrderType::Limit,
    };

    let time_in_force = match req
        .time_in_force
        .as_deref()
        .map(str::to_lowercase)
        .as_deref()
    {
        // A market order with no explicit TIF defaults to IOC — it should fill at
        // whatever's resting and never sit in the book as a price-less GTC order.
        // A limit order defaults to GTC.
        None => match order_type {
            OrderType::Market => TimeInForce::Ioc,
            OrderType::Limit => TimeInForce::Gtc,
        },
        Some("gtc") => TimeInForce::Gtc,
        Some("ioc") => TimeInForce::Ioc,
        Some("fok") => TimeInForce::Fok,
        Some(other) => {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                format!("Invalid time_in_force: {other} (expected gtc|ioc|fok)"),
            ))
        }
    };

    // Parse the optional price input (limit price OR market-buy slippage cap),
    // then apply the shared admission policy. See
    // `trading_core::order_policy::resolve_order_price` for the rules.
    // A present-but-non-positive value (e.g. "0") is kept present so the policy
    // REJECTS it — REST can distinguish "0" from an omitted field. (The gRPC edge
    // can't: its proto f64 default 0.0 is indistinguishable from unset, so there
    // a market buy's 0.0 is treated as "no cap". That literal-zero divergence is
    // inherent to the proto and is the safe direction — REST never silently drops
    // slippage protection.)
    let price_input = match req.price_per_kwh.as_deref() {
        Some(raw) => Some(Decimal::from_str(raw).map_err(|e| {
            (
                axum::http::StatusCode::BAD_REQUEST,
                format!("Invalid price_per_kwh: {e}"),
            )
        })?),
        None => None,
    };
    let price = trading_core::order_policy::resolve_order_price(
        order_type,
        side,
        time_in_force,
        price_input,
    )
    .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.message().to_string()))?;

    let market_segment = match req
        .market_segment
        .as_deref()
        .map(str::to_lowercase)
        .as_deref()
    {
        None | Some("realtime") => trading_core::types::MarketSegment::Realtime,
        Some("interval") => trading_core::types::MarketSegment::Interval,
        Some(other) => {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                format!("Invalid market_segment: {other} (expected realtime|interval)"),
            ))
        }
    };

    // Interval orders clear in a 15-min uniform-price batch, not continuously, so
    // the "immediate" time-in-force modes (IOC/FOK) have no meaning there — and the
    // CDA IOC sweep never sees interval orders (the matcher filters to Realtime), so
    // an interval IOC remainder would never be cancelled. Reject the combination.
    if market_segment == trading_core::types::MarketSegment::Interval
        && time_in_force != TimeInForce::Gtc
    {
        return Err((
            axum::http::StatusCode::BAD_REQUEST,
            "interval orders must be gtc (ioc/fok require continuous matching)".to_string(),
        ));
    }

    // `meter_serial` is the id space every user-facing surface holds (the grid
    // map's node ids). Resolve it to `meters.id` here — sending a serial through
    // as `meter_id` violates `trading_orders_meter_id_fkey`. The full identity
    // (owner + verification state) comes back in the same round-trip, because
    // the sell-side gate below needs it.
    let meter = match req.meter_serial.as_deref() {
        Some(serial) => {
            let resolved = state
                .meter_repo
                .lookup_by_serial(serial)
                .await
                .map_err(|e| {
                    (
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Database error: {e}"),
                    )
                })?;
            Some(resolved.ok_or_else(|| {
                (
                    axum::http::StatusCode::BAD_REQUEST,
                    format!("unknown meter_serial: {serial}"),
                )
            })?)
        }
        // A client that sent `meter_id` directly gets the same treatment: an id
        // naming no mirrored meter is refused rather than silently ungated.
        None => match req.meter_id {
            Some(id) => Some(
                state
                    .meter_repo
                    .lookup_by_id(id)
                    .await
                    .map_err(|e| {
                        (
                            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                            format!("Database error: {e}"),
                        )
                    })?
                    .ok_or_else(|| {
                        (
                            axum::http::StatusCode::BAD_REQUEST,
                            format!("unknown meter_id: {id}"),
                        )
                    })?,
            ),
            None => None,
        },
    };
    let meter_id = meter.map(|m| m.meter_id);

    // ── Sell-side meter-verification gate ────────────────────────────────────
    // Selling energy is a claim to have produced it; the meter is what
    // substantiates the claim, and registration alone does not — it records an
    // unproven assertion that a serial is yours. Refuse here, before the row is
    // inserted and before any on-chain placement, so an ungrounded sell never
    // reaches the book. Buys are untouched.
    let has_verified_meter =
        if trading_core::order_policy::needs_any_verified_meter_lookup(side, meter.as_ref()) {
            state
                .meter_repo
                .has_verified_meter(user.user_id)
                .await
                .map_err(|e| {
                    (
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Database error: {e}"),
                    )
                })?
        } else {
            false
        };
    if let Err(e) = trading_core::order_policy::check_sell_eligibility(
        user.user_id,
        side,
        meter.as_ref(),
        has_verified_meter,
    ) {
        tracing::warn!(
            "sell order from {} refused: {:?} (meter_serial={:?})",
            user.user_id,
            e,
            req.meter_serial
        );
        // 403, not 400: the request is well-formed and the caller is
        // authenticated — they are simply not authorized to sell yet.
        return Err((axum::http::StatusCode::FORBIDDEN, e.message().to_string()));
    }

    // ── Buy-side funding gate ────────────────────────────────────────────────
    // A bid is a promise to pay at settlement; refuse it up front when the
    // buyer's currency balance knowably cannot cover the order's maximum spend
    // (matching price × amount; an uncapped market buy only needs a non-zero
    // balance since its spend is unbounded by construction). Admission-time
    // only — settlement's atomic swap remains the real enforcement — so the
    // gate fails OPEN when the balance cannot be read: a Chain Bridge blip must
    // not refuse every buy (same stance as the settlement pre-flight).
    let funding = trading_core::order_policy::required_buy_funding(
        side,
        order_type,
        price_input,
        price,
        amount,
    );
    if funding != trading_core::order_policy::BuyFundingRequirement::None {
        match state.blockchain.get_user_wallet(user.user_id).await {
            Ok(None) => {
                return Err((
                    axum::http::StatusCode::FORBIDDEN,
                    trading_core::order_policy::BuyFundingError::NoWallet.message(),
                ));
            }
            Ok(Some(wallet)) => match state.blockchain.get_currency_balance(&wallet).await {
                Ok(available) => {
                    if let Err(e) =
                        trading_core::order_policy::check_buy_funding(funding, available)
                    {
                        tracing::warn!("buy order from {} refused: {:?}", user.user_id, e);
                        return Err((axum::http::StatusCode::PAYMENT_REQUIRED, e.message()));
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "buy funding gate skipped for {}: currency balance read failed: {e}",
                        user.user_id
                    );
                }
            },
            Err(e) => {
                tracing::warn!(
                    "buy funding gate skipped for {}: wallet lookup failed: {e}",
                    user.user_id
                );
            }
        }
    }

    // ── Per-user escrow settlement: verify the wallet signature ──────────────
    // With this flag on, settlement spends the parties' OWN escrow PDAs via
    // `settle_offchain_match`, which verifies an Ed25519 signature over the order
    // terms on-chain. Reject anything unsigned or mis-signed here, at placement,
    // rather than letting it rest in the book and fail at settlement.
    //
    // The message is rebuilt from the values this handler is about to persist —
    // never from client-supplied bytes — so a client cannot sign one price and
    // submit another. See `crate::order_signature` for the full rationale.
    let signed_order: Option<(Uuid, String, i64)> = if state.config.per_user_escrow_settlement {
        let order_id = req.order_id.ok_or((
            axum::http::StatusCode::BAD_REQUEST,
            "order_id is required when per-user escrow settlement is enabled".to_string(),
        ))?;
        let wallet_signature = req.wallet_signature.clone().ok_or((
            axum::http::StatusCode::BAD_REQUEST,
            "wallet_signature is required when per-user escrow settlement is enabled".to_string(),
        ))?;
        let signed_expires_at = req.signed_expires_at.ok_or((
            axum::http::StatusCode::BAD_REQUEST,
            "signed_expires_at is required when per-user escrow settlement is enabled".to_string(),
        ))?;

        let wallet = state
            .blockchain
            .get_user_wallet(user.user_id)
            .await
            .map_err(|e| {
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to resolve wallet: {e}"),
                )
            })?
            .ok_or((
                axum::http::StatusCode::BAD_REQUEST,
                "no on-chain wallet linked to this account".to_string(),
            ))?;

        let wallet_bytes: [u8; 32] = bs58::decode(&wallet)
            .into_vec()
            .ok()
            .and_then(|v| v.try_into().ok())
            .ok_or((
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "linked wallet is not a valid Ed25519 public key".to_string(),
            ))?;

        // Same truncating conversion the on-chain instruction and the browser use.
        let energy_base = trading_core::offchain_payload::energy_to_base_units(amount).ok_or((
            axum::http::StatusCode::BAD_REQUEST,
            "energy_amount_kwh is out of range".to_string(),
        ))?;
        let price_base = trading_core::offchain_payload::currency_to_base_units(price).ok_or((
            axum::http::StatusCode::BAD_REQUEST,
            "price_per_kwh is out of range".to_string(),
        ))?;

        let message = trading_core::offchain_payload::message_for(
            order_id.as_bytes(),
            &wallet_bytes,
            energy_base,
            price_base,
            match side {
                OrderSide::Buy => trading_core::offchain_payload::SIDE_BUY,
                OrderSide::Sell => trading_core::offchain_payload::SIDE_SELL,
            },
            u32::try_from(req.zone_id).unwrap_or(0), // non-negative by schema
            signed_expires_at,
        );

        crate::order_signature::verify_order_signature(&wallet, &wallet_signature, &message)
            .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

        Some((order_id, wallet_signature, signed_expires_at))
    } else {
        None
    };

    // Resolve the order's lifetime before building it: an inadmissible expiry is
    // a 400, not an order that rests unmatchable until the reaper collects it.
    let expires_at = trading_core::order_policy::resolve_expires_at(
        gridtokenx_telemetry::time::now(),
        req.expires_at,
        req.expires_in_secs,
        signed_order.as_ref().map(|(_, _, exp)| *exp),
        state.config.order_expiry.default_ttl_secs,
        state.config.order_expiry.max_ttl_secs,
    )
    .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.message().to_string()))?;

    let mut order = TradingOrder {
        id: signed_order
            .as_ref()
            .map_or_else(Uuid::new_v4, |(id, _, _)| *id),
        user_id: user.user_id,
        order_type,
        side,
        energy_amount: amount,
        price_per_kwh: price,
        filled_amount: Decimal::ZERO,
        status: OrderStatus::Pending,
        // Client expiry, the signed expiry, or the configured default — resolved
        // by `order_policy::resolve_expires_at` above so REST and gRPC cannot
        // drift. A signed expiry still wins: the settlement-time payload has to
        // match the bytes the user signed.
        expires_at: Some(expires_at),
        created_at: Some(gridtokenx_telemetry::time::now()),
        filled_at: None,
        epoch_id: None,
        zone_id: Some(req.zone_id),
        meter_id,
        refund_tx_signature: None,
        order_pda: None,
        order_index: None,
        session_token: None,
        blockchain_status: None,
        blockchain_tx_hash: None,
        blockchain_error: None,
        retry_count: 0,
        time_in_force,
        market_segment,
    };

    // ── Custodial On-Chain Placement (Option A) ───────
    // Record the order PDA + fund its escrow on the user's behalf (platform-signed,
    // no user signature). Fires when settlement is enabled or explicitly requested.
    // Best-effort: a failure leaves order_pda NULL so the settlement worker skips it
    // (unchanged behaviour) — it never fails the API.
    //
    // Skipped entirely under `per_user_escrow_settlement`: that is the whole point
    // of the flag. Platform funding is what makes a seller's own GRX never move —
    // the escrow is filled from the platform's ATA, so selling debits nobody and
    // the pool drains by the traded amount on every match. With the flag on, each
    // party funds their own `[b"escrow", user, mint]` PDA by wallet-signed
    // `deposit_escrow`, and `settle_offchain_match` spends those.
    if !state.config.per_user_escrow_settlement
        && (state.config.trade_settlement_enabled || req.custodial_sign.unwrap_or(false))
    {
        let seed = u64::from_le_bytes(
            order.id.as_bytes()[0..8]
                .try_into()
                .expect("uuid has 16 bytes"),
        );
        let is_buy = matches!(side, OrderSide::Buy);
        match state
            .blockchain
            // The same expiry stored on the row, so the Order PDA states this
            // order's real lifetime instead of the program's old 24h default.
            // `None` maps to the on-chain no-expiry sentinel (0).
            .place_order_on_chain(
                user.user_id,
                is_buy,
                amount,
                price,
                req.zone_id,
                seed,
                order.expires_at.map_or(0, |t| t.timestamp()),
            )
            .await
        {
            Ok((sig, pda)) => {
                info!("✅ On-chain order placed. Sig: {}, PDA: {}", sig, pda);
                order.order_pda = Some(pda);
                order.order_index = Some(i64::try_from(seed).unwrap_or(i64::MAX));
                order.blockchain_tx_hash = Some(sig);
                order.blockchain_status = Some("confirmed".to_string());
            }
            Err(e) => {
                // A program rejection is FINAL: the transaction executed and the program
                // refused it, so resubmitting the same order gets the same answer. Accepting
                // it anyway is what produced the observed incident — 18 orders in zones with
                // no `ZoneMarket` were rejected `Custom 3007`, kept with `order_pda = NULL`,
                // matched by the CDA, and their settlements then burned five retries each
                // before being parked `permanently_failed` blaming the wrong thing. Nothing
                // retries placement (no worker looks for a NULL PDA), so the old
                // "left for retry" was never true. Refuse the order instead — this runs
                // BEFORE the row is inserted, so nothing is persisted.
                let msg = e.to_string();
                if trading_core::error::is_deterministic_chain_rejection(&msg) {
                    tracing::warn!(
                        "order {} REJECTED: the program refused on-chain placement, so this order \
                         could never settle: {}",
                        order.id,
                        msg
                    );
                    return Err((
                        axum::http::StatusCode::UNPROCESSABLE_ENTITY,
                        format!(
                            "on-chain placement was rejected by the trading program, so this order \
                             cannot be accepted: {msg}. If this is a zone-market error (Custom 3007), \
                             zone {} has no initialized market — see scripts/init-zones.sh.",
                            req.zone_id
                        ),
                    ));
                }
                // Transport failure: no verdict was reached, so keep the existing
                // best-effort behaviour rather than turning a validator blip into a
                // rejected order. The order still rests with a NULL PDA and its settlement
                // will not land until placement is retried — which nothing currently does.
                tracing::warn!(
                    "order {}: on-chain placement failed to reach a verdict; accepted with NULL \
                     order_pda and NOT retried by anything, so its settlement cannot land: {}",
                    order.id,
                    msg
                );
            }
        }
    }

    // Stamp the order with the active market epoch so the matcher's settlement
    // and order_matches inserts satisfy their NOT NULL FK to market_epochs.
    order.epoch_id = Some(
        state
            .order_repo
            .get_or_create_active_epoch()
            .await
            .map_err(|e| {
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to resolve active epoch: {e}"),
                )
            })?,
    );

    // Insert the order and its OrderCreated event in one transaction so the
    // event can never be lost relative to the state change (the outbox row is
    // written atomically with the order; OutboxWorker relays it later). Mirrors
    // the ConnectRPC submit path in handlers.rs.
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

    let insert_res = state
        .order_repo
        .insert_order_with_event(&order, &event)
        .await;

    trading_infra::metrics::record_order_submission(
        &order.order_type.to_string(),
        &order.side.to_string(),
        insert_res.is_ok(),
        submit_started.elapsed().as_secs_f64() * 1000.0,
    );

    insert_res.map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Database error: {e}"),
        )
    })?;

    // Store the wallet signature the settlement builder will replay into the
    // Ed25519 verify instruction. Written after the insert (not as part of it)
    // because `TradingOrder` does not map the column — see `set_wallet_signature`.
    // Hard-fail on error: without the signature this order can never settle on the
    // per-user-escrow path, and silently resting an unsettleable order in the book
    // is exactly the failure mode that produced the endless re-match loop before.
    if let Some((_, signature, _)) = signed_order.as_ref() {
        state
            .order_repo
            .set_wallet_signature(order.id, signature)
            .await
            .map_err(|e| {
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to persist order signature: {e}"),
                )
            })?;
    }

    // Realtime matching: wake the matcher now that the order is fully durable —
    // deliberately AFTER `set_wallet_signature`, because a cycle that matched the
    // order before its signature landed would persist a settlement that can never
    // execute on the per-user-escrow path. Fire-and-forget: `request_cycle` neither
    // awaits nor fails, so submit latency is unchanged, and a wake-up arriving
    // mid-cycle is held as a permit and served immediately after. Interval-segment
    // orders are cleared by the uniform-price path, not the CDA matcher, so waking
    // it for them would only buy a wasted book scan.
    if order.market_segment == trading_core::types::MarketSegment::Realtime {
        state.matcher.request_cycle();
    }

    Ok(Json(SubmitOrderResponse {
        id: order.id,
        status: "open".to_string(),
        created_at: order
            .created_at
            .unwrap_or_else(gridtokenx_telemetry::time::now),
    }))
}

/// Zone order book: remaining energy aggregated by price level.
#[utoipa::path(
    get,
    path = "/api/v1/zones/{zone_id}/book",
    tag = "orders",
    params(("zone_id" = i32, Path, description = "Grid zone id")),
    responses(
        (status = 200, description = "Price-level book; asks ascend, bids descend; entries are [price, amount] decimal strings", body = OrderBookResponse),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [])),
)]
pub async fn get_order_book(
    role: ServiceRole,
    State(state): State<AppState>,
    Path(zone_id): Path<i32>,
) -> Result<Json<OrderBookResponse>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let entries = state
        .order_repo
        .get_active_orders_by_zone(zone_id)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {e}"),
            )
        })?;

    // Aggregate remaining (unfilled) energy by price level. BTreeMap keeps the
    // levels ordered by price; asks (sells) ascend from the best (lowest) ask,
    // bids (buys) descend from the best (highest) bid.
    let mut ask_levels = std::collections::BTreeMap::<Decimal, Decimal>::default();
    let mut bid_levels = std::collections::BTreeMap::<Decimal, Decimal>::default();
    for e in &entries {
        let book = match e.side {
            trading_core::types::OrderSide::Sell => &mut ask_levels,
            trading_core::types::OrderSide::Buy => &mut bid_levels,
        };
        *book.entry(e.price_per_kwh).or_insert(Decimal::ZERO) += e.energy_amount;
    }

    let asks: Vec<[String; 2]> = ask_levels
        .iter()
        .map(|(price, amount)| [price.to_string(), amount.to_string()])
        .collect();
    let bids: Vec<[String; 2]> = bid_levels
        .iter()
        .rev()
        .map(|(price, amount)| [price.to_string(), amount.to_string()])
        .collect();

    Ok(Json(OrderBookResponse {
        zone_id,
        // Was `entries.len()` — the resting-order count, a placeholder standing in
        // for the sequence source that did not exist yet. It does now: the WS
        // gateway stamps a per-zone sequence. See the field docs for why this is a
        // staleness hint rather than a resume point.
        last_update_id: state.ws_hub.current_seq(zone_id),
        asks,
        bids,
    }))
}

/// Meters that currently have resting orders, market-wide, grouped by meter.
///
/// The map uses this to show only meters that are actually trading, matching on
/// `meter_serial` (its node id space), not `meter_id`.
///
/// Orders carry a meter only when placed against a specific one (the map's node
/// form) — orders without one are simply absent here, so a meter silent on this
/// endpoint is not proof it has no orders at all.
#[utoipa::path(
    get,
    path = "/api/v1/markets/active-order-meters",
    tag = "markets",
    responses(
        (status = 200, description = "Meters with at least one resting (pending/active/partially-filled) order", body = ActiveOrderMetersResponse),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [])),
)]
pub async fn list_active_order_meters(
    role: ServiceRole,
    State(state): State<AppState>,
) -> Result<Json<ActiveOrderMetersResponse>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let data = fetch_active_order_meters(&state).await?;
    Ok(Json(ActiveOrderMetersResponse { data }))
}

/// Public, unauthenticated variant of [`list_active_order_meters`] for the grid
/// map. Returns only market-level order *presence* — which meters have a resting
/// buy/sell order — which is strictly less than the already-public order book
/// (`/api/v1/zones/{zone_id}/book`): no prices, amounts, or account identity. The
/// logged-out map needs it to hide non-trading meters without a JWT.
#[utoipa::path(
    get,
    path = "/api/v1/public/active-order-meters",
    tag = "markets",
    responses(
        (status = 200, description = "Meters with at least one resting (pending/active/partially-filled) order", body = ActiveOrderMetersResponse),
        (status = 500, description = "Database error", body = String),
    ),
)]
pub async fn list_public_active_order_meters(
    State(state): State<AppState>,
) -> Result<Json<ActiveOrderMetersResponse>, (axum::http::StatusCode, String)> {
    let data = fetch_active_order_meters(&state).await?;
    Ok(Json(ActiveOrderMetersResponse { data }))
}

/// Shared query behind both the authed and public active-order-meters endpoints:
/// every meter with at least one resting order, grouped, with `meter_id`
/// translated to the map's `meter_serial` id space.
pub(super) async fn fetch_active_order_meters(
    state: &AppState,
) -> Result<Vec<ActiveOrderMeter>, (axum::http::StatusCode, String)> {
    // `bootstrap_active_orders` is named for the matcher's warm-up path, but it is
    // exactly this query: every order in ('pending','active','partially_filled'),
    // as a full `TradingOrder` (so `meter_id` survives — `get_all_active_orders`
    // projects to `OrderBookEntry`, which drops it).
    let orders = state
        .order_repo
        .bootstrap_active_orders()
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {e}"),
            )
        })?;

    // Group by meter first, then translate ids in one round-trip rather than
    // per-order.
    let mut sides: HashMap<Uuid, (i32, bool, bool)> = HashMap::new();
    for o in &orders {
        let Some(meter_id) = o.meter_id else { continue };
        let entry = sides
            .entry(meter_id)
            .or_insert((o.zone_id.unwrap_or(0), false, false));
        match o.side {
            OrderSide::Buy => entry.1 = true,
            OrderSide::Sell => entry.2 = true,
        }
    }

    let ids: Vec<Uuid> = sides.keys().copied().collect();
    let serials = state
        .meter_repo
        .get_serials_for_ids(&ids)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {e}"),
            )
        })?;

    // A meter_id with no `meters` row can't be matched to a map node, so it is
    // dropped rather than emitted with a placeholder serial.
    let mut data: Vec<ActiveOrderMeter> = sides
        .into_iter()
        .filter_map(|(meter_id, (zone_id, has_open_buy, has_open_sell))| {
            Some(ActiveOrderMeter {
                meter_id,
                meter_serial: serials.get(&meter_id)?.clone(),
                zone_id,
                has_open_buy,
                has_open_sell,
            })
        })
        .collect();
    // Stable order so clients can diff responses without re-sorting.
    data.sort_by_key(|m| m.meter_id);

    Ok(data)
}

/// List the authenticated user's orders (optionally filtered by status).
#[utoipa::path(
    get,
    path = "/api/v1/orders",
    tag = "orders",
    params(ListOrdersParams),
    responses(
        (status = 200, description = "Page of the user's orders (status filter applies after pagination)", body = ListOrdersResponse),
        (status = 401, description = "Missing or invalid user id header", body = String),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [], "user_id" = [])),
)]
pub async fn list_orders(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Query(params): Query<ListOrdersParams>,
) -> Result<Json<ListOrdersResponse>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;
    let limit = params.limit.unwrap_or(20);
    let offset = params.offset.unwrap_or(0);

    let orders = state
        .order_repo
        .get_orders_by_user(
            user.user_id,
            i64::try_from(limit).unwrap_or(i64::MAX),
            i64::try_from(offset).unwrap_or(i64::MAX),
        )
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {e}"),
            )
        })?;

    // `params.status` was previously accepted but never read — every cancelled/filled
    // order kept showing in `?status=active` callers (e.g. the trading UI's "My
    // Orders" list) forever. Filtered post-fetch since get_orders_by_user has no
    // status-aware query variant; note this applies after the repo's limit/offset, so
    // a page can return fewer than `limit` matches once a user has enough non-matching
    // orders to span pages — acceptable for the common (small, mostly-active) case.
    let orders: Vec<_> = match params.status.as_deref() {
        Some(status) => orders
            .into_iter()
            .filter(|o| o.status.as_str() == status)
            .collect(),
        None => orders,
    };

    let data = orders.iter().map(OrderData::from_order).collect::<Vec<_>>();

    let total = data.len();
    Ok(Json(ListOrdersResponse {
        data,
        pagination: Pagination {
            total,
            limit,
            offset,
        },
    }))
}

/// Fetch one order. Non-admin callers only see their own orders (404 otherwise).
#[utoipa::path(
    get,
    path = "/api/v1/orders/{id}",
    tag = "orders",
    params(("id" = Uuid, Path, description = "Order id")),
    responses(
        (status = 200, description = "The order", body = OrderData),
        (status = 401, description = "Missing or invalid user id header", body = String),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 404, description = "Not found (or owned by another user)", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [], "user_id" = [])),
)]
pub async fn get_order_by_id(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<OrderData>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let order = state
        .order_repo
        .get_order(id)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {e}"),
            )
        })?
        .ok_or((
            axum::http::StatusCode::NOT_FOUND,
            "Order not found".to_string(),
        ))?;

    // Ownership scoping: a gateway-scoped caller may only read its own user's
    // order; admins may read any. 404 (not 403) so an id's existence isn't
    // leaked across users.
    if role != ServiceRole::Admin && order.user_id != user.user_id {
        return Err((
            axum::http::StatusCode::NOT_FOUND,
            "Order not found".to_string(),
        ));
    }

    Ok(Json(OrderData::from_order(&order)))
}

/// Cancel an order owned by the authenticated user.
#[utoipa::path(
    delete,
    path = "/api/v1/orders/{id}",
    tag = "orders",
    params(("id" = Uuid, Path, description = "Order id")),
    responses(
        (status = 200, description = "`{\"status\": \"cancelled\", \"order_id\": ...}`", body = serde_json::Value),
        (status = 401, description = "Missing or invalid user id header", body = String),
        (status = 403, description = "Caller role not allowed", body = String),
        (status = 500, description = "Database error", body = String),
    ),
    security(("gateway_role" = [], "user_id" = [])),
)]
pub async fn cancel_order(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    state
        .order_repo
        .cancel_order(id, user.user_id)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {e}"),
            )
        })?;

    Ok(Json(serde_json::json!({
        "status": "cancelled",
        "order_id": id,
    })))
}

/// Price quote with wheeling/loss breakdown, computed from the request and the
/// service market config (`state.config.market`) — the same schedule surfaced by
/// `/api/v1/markets/p2p/market-prices`.
///
/// Model (per kWh, THB):
/// - `energy_cost   = energy * price`  (price defaults to `base_price` when ≤ 0)
/// - `wheeling      = energy * wheeling_rate`  (intra- vs cross-zone)
/// - `loss_fraction = loss_factor - 1`  (config stores 1.01 / 1.03 multipliers)
/// - `loss_cost     = energy_cost * loss_fraction`
/// - `effective_kwh = energy * (1 - loss_fraction)`
/// - `total         = energy_cost + wheeling + loss_cost`
///
/// `zone_distance_km` is a 10 km-per-zone-hop heuristic (config carries no grid
/// topology). `is_grid_compliant` = price within `[min_price, max_price]`.
#[utoipa::path(
    post,
    path = "/api/v1/quotes",
    tag = "quotes",
    request_body = QuoteRequest,
    responses(
        (status = 200, description = "Computed quote for the requested trade", body = QuoteResponse),
        (status = 400, description = "Invalid energy amount", body = String),
        (status = 403, description = "Caller role not allowed", body = String),
    ),
    security(("gateway_role" = [])),
)]
pub async fn create_quote(
    role: ServiceRole,
    State(state): State<AppState>,
    Json(req): Json<QuoteRequest>,
) -> Result<Json<QuoteResponse>, (axum::http::StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (axum::http::StatusCode::FORBIDDEN, msg.to_string()))?;

    let m = &state.config.market;

    let energy = Decimal::from_str(req.energy_amount_kwh.trim()).map_err(|e| {
        (
            axum::http::StatusCode::BAD_REQUEST,
            format!("invalid energy_amount_kwh: {e}"),
        )
    })?;
    if energy <= Decimal::ZERO {
        return Err((
            axum::http::StatusCode::BAD_REQUEST,
            "energy_amount_kwh must be positive".to_string(),
        ));
    }

    // An absent/zero agreed price falls back to the real market price (24h VWAP,
    // widened to all-time), never a static config default. If the market has
    // never traded there is no price to quote — reject rather than invent one.
    let mut price = Decimal::from_str(req.agreed_price.trim()).unwrap_or(Decimal::ZERO);
    if price <= Decimal::ZERO {
        let quote_err = |e: String| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e);
        let mut mp = state
            .settlement_repo
            .get_market_price(24)
            .await
            .map_err(|e| quote_err(format!("Database error: {e}")))?;
        if mp.trade_count == 0 {
            mp = state
                .settlement_repo
                .get_market_price(0)
                .await
                .map_err(|e| quote_err(format!("Database error: {e}")))?;
        }
        if mp.trade_count == 0 {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                "no agreed_price supplied and no market price yet (no completed trades); supply agreed_price".to_string(),
            ));
        }
        price = mp.vwap;
    }

    let same_zone = req.buyer_zone_id == req.seller_zone_id;
    let wheeling_rate = if same_zone {
        m.intra_zone_wheeling_charge
    } else {
        m.cross_zone_wheeling_charge
    };
    let loss_mult = if same_zone {
        m.intra_zone_loss_factor
    } else {
        m.cross_zone_loss_factor
    };
    let loss_fraction = loss_mult - Decimal::ONE;

    let energy_cost = energy * price;
    let wheeling_charge = energy * wheeling_rate;
    let loss_cost = energy_cost * loss_fraction;
    let total_cost = energy_cost + wheeling_charge + loss_cost;
    let effective_energy = energy * (Decimal::ONE - loss_fraction);

    let zone_gap = (req.buyer_zone_id - req.seller_zone_id).abs();
    let zone_distance_km = Decimal::from(zone_gap) * Decimal::from(10);

    let is_grid_compliant = price >= m.min_price_per_kwh && price <= m.max_price_per_kwh;

    let qid = format!("q_{}", &Uuid::new_v4().to_string()[..8]);
    Ok(Json(QuoteResponse {
        quote_id: qid,
        expires_at: gridtokenx_telemetry::time::now() + chrono::Duration::minutes(5),
        breakdown: QuoteBreakdown {
            energy_cost: format!("{:.2}", dec_f64(energy_cost)),
            wheeling_charge: format!("{:.2}", dec_f64(wheeling_charge)),
            loss_cost: format!("{:.2}", dec_f64(loss_cost)),
            total_cost: format!("{:.2}", dec_f64(total_cost)),
        },
        grid_metrics: GridMetrics {
            effective_energy_kwh: format!("{:.4}", dec_f64(effective_energy)),
            loss_factor: format!("{:.4}", dec_f64(loss_fraction)),
            zone_distance_km: format!("{:.1}", dec_f64(zone_distance_km)),
            is_grid_compliant,
        },
    }))
}

#[derive(Debug, Serialize, ToSchema)]
pub struct MarketStatsResponse {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Total settled energy over the last 24h (kWh), decimal string.
    pub total_volume_24h_kwh: String,
    /// 24h VWAP price (THB/kWh), decimal string. "0" when no trades in 24h.
    pub avg_price_24h: String,
    /// Distinct users who traded (buyer or seller) in the last 24h.
    pub active_users: i64,
    /// Number of completed settlements in the last 24h.
    pub trade_count_24h: i64,
}
