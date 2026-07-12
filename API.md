# Trading Service — REST API Reference

> **Generated spec available:** the service serves a live OpenAPI document at
> `GET /api-docs/openapi.json` with Swagger UI at `/docs` (utoipa annotations in
> [`crates/trading-api/src/rest.rs`](crates/trading-api/src/rest.rs), doc assembled in
> [`crates/trading-api/src/openapi.rs`](crates/trading-api/src/openapi.rs)). Prefer the
> generated spec for exact schemas; this file adds narrative conventions and caveats.

HTTP router: [`crates/trading-api/src/startup.rs:79`](crates/trading-api/src/startup.rs) (`build_router`).
Handlers + request/response DTOs: [`crates/trading-api/src/rest.rs`](crates/trading-api/src/rest.rs).
Typed models: [`crates/trading-core/src/models.rs`](crates/trading-core/src/models.rs); enums [`crates/trading-core/src/types.rs`](crates/trading-core/src/types.rs).

Reached by the Trading UI (`gridtokenx-trading`) via the APISIX gateway. Also exposes a
gRPC (ConnectRPC) service alongside REST — this doc covers REST only.

## Conventions

- **Auth**: every `/api/v1/*` handler extracts `UserContext` (JWT-scoped) + `ServiceRole` (RBAC).
  Health/metrics are open.
- **Money/energy amounts are decimal *strings*** on the spot/trade/recurring DTOs and on all
  `trading_core` models (`rust_decimal::Decimal` serializes as a string). Parse as decimal, **not**
  a JSON number. Exceptions that use real JSON floats: `/markets/config`,
  `/markets/p2p/market-prices`, `UserAnalytics.reliability_score`, and the futures
  `CreateFuturesOrderRequest` (`quantity`/`price` are `f64` on input).
- **Timestamps**: ISO-8601 / RFC-3339 UTC (`chrono::DateTime<Utc>`).
- **`opaque JSON`** below = handler returns `serde_json::Value` (no compile-time contract; shape
  is runtime-defined — treat as unstable).
- **Stubbed**: `create_quote`, `create_futures_order`, `transfer_carbon_credits` accept the request
  body but bind it to `_req` (unused) — responses are currently mock. Verify before relying on them.

---

## Orders

### `POST /api/v1/orders` — submit order
Request `SubmitOrderRequest`:
```json
{
  "side": "buy | sell",
  "order_type": "limit | market",
  "energy_amount_kwh": "10.5",
  "price_per_kwh": "4.20",          // optional; required for limit, market-buy = slippage cap
  "zone_id": 3,
  "meter_id": "uuid",               // optional
  "custodial_sign": true,           // optional
  "time_in_force": "gtc | ioc | fok",       // optional, default gtc
  "market_segment": "realtime | interval"   // optional, default realtime (interval = 15-min clearing)
}
```
Response `SubmitOrderResponse`:
```json
{ "id": "uuid", "status": "…", "created_at": "ISO8601" }
```

### `GET /api/v1/orders?status&limit&offset` — list own orders
Response `ListOrdersResponse`:
```json
{
  "data": [
    {
      "id": "uuid", "zone_id": 3, "side": "buy", "order_type": "limit",
      "status": "open", "energy_amount_kwh": "10.5",
      "price_per_kwh": "4.2 | null",   // null for market orders (synthetic bid)
      "filled_amount_kwh": "2.0", "created_at": "ISO"
    }
  ],
  "pagination": { "total": 100, "limit": 20, "offset": 0 }
}
```

### `GET /api/v1/orders/{id}` — single order → `OrderData` (same shape as an element of `data` above)
### `DELETE /api/v1/orders/{id}` — cancel → opaque JSON

---

## Quotes

### `POST /api/v1/quotes` — price a P2P transfer *(stubbed)*
Request `QuoteRequest`:
```json
{ "buyer_zone_id": 1, "seller_zone_id": 2, "energy_amount_kwh": "10", "agreed_price": "4.2" }
```
Response `QuoteResponse`:
```json
{
  "quote_id": "…", "expires_at": "ISO",
  "breakdown": { "energy_cost": "42", "wheeling_charge": "1.5", "loss_cost": "0.8", "total_cost": "44.3" },
  "grid_metrics": { "effective_energy_kwh": "9.8", "loss_factor": "0.02", "zone_distance_km": "12", "is_grid_compliant": true }
}
```

---

## Order book & market stats

### `GET /api/v1/zones/{zone_id}/book` — `OrderBookResponse`
```json
{ "zone_id": 3, "last_update_id": 42, "asks": [["price", "qty"]], "bids": [["price", "qty"]] }
```

### `GET /api/v1/stats` — `MarketStatsResponse`
```json
{ "timestamp": "ISO", "total_volume_24h_kwh": "…", "avg_price_24h": "…",
  "active_users": 12, "grid_stability_index": "…", "renewable_ratio": "…" }
```

---

## Markets (read-only)

### `GET /api/v1/markets/config` — `MarketConfigResponse` *(floats)*
```json
{ "base_price_thb_kwh": 4.0, "grid_import_price_thb_kwh": 5.0, "grid_export_price_thb_kwh": 3.0,
  "transaction_fee_bps": 25, "min_price_per_kwh": 1.0, "max_price_per_kwh": 10.0 }
```

### `GET /api/v1/markets/p2p/market-prices` — `P2PMarketPricesResponse` *(floats)*
```json
{ "base_price_thb_kwh": 4.0, "grid_import_price_thb_kwh": 5.0, "grid_export_price_thb_kwh": 3.0,
  "loss_allocation_model": "…",
  "wheeling_charges": { "<zone>": 1.5 }, "loss_factors": { "<zone>": 0.02 } }
```

### `GET /api/v1/markets/matching-status` — `MatchingStatusResponse`
```json
{ "pending_buy_orders": 0, "pending_sell_orders": 0, "pending_matches": 0,
  "buy_price_range": { "min": 0.0, "max": 0.0 }, "sell_price_range": { "min": 0.0, "max": 0.0 },
  "can_match": false, "match_reason": "…" }
```

### `GET /api/v1/markets/settlement-stats` — `SettlementStatsResponse`
```json
{ "pending_count": 0, "processing_count": 0, "confirmed_count": 0, "failed_count": 0,
  "total_settled_value": 0.0 }
```

### `GET /api/v1/markets/orderbook` — `P2POrderBookResponse`
```json
{ "asks": [["price", "qty"]], "bids": [["price", "qty"]] }
```

### `GET /api/v1/markets/clearing-epochs?limit` — `[ClearingEpochResponse]`
`limit` default 20, clamped 1..=100.
```json
[
  {
    "epoch_id": "uuid", "epoch_number": 7, "start_time": "ISO", "end_time": "ISO", "status": "…",
    "clearing_price": "… | null", "total_volume": "… | null",
    "total_orders": "i64 | null", "matched_orders": "i64 | null"
  }
]
```

---

## Trades

### `GET /api/v1/trades?limit&offset` — `TradesListResponse`
```json
{ "trades": [ /* TradeRecordResponse */ ], "total_count": 0, "total": 0 }
```
`TradeRecordResponse` (all amounts are strings):
```json
{
  "id": "uuid", "buyer_id": "uuid", "seller_id": "uuid", "counterparty_id": "uuid",
  "role": "buyer | seller", "quantity": "…", "energy_amount": "…", "price": "…",
  "price_per_kwh": "…", "total_value": "…", "fee_amount": "…", "wheeling_charge": "…",
  "loss_cost": "…", "effective_energy": "…", "status": "…",
  "transaction_hash": "… | null", "buy_order_id": "uuid", "sell_order_id": "uuid",
  "buyer_zone_id": "i32 | null", "seller_zone_id": "i32 | null",
  "executed_at": "ISO", "created_at": "ISO"
}
```

### `GET /api/v1/trades/export?format=csv|json` — raw file
Returns a raw response body (CSV by default), **not** a JSON envelope.

---

## Price alerts

### `POST /api/v1/price-alerts` — `CreatePriceAlertRequest` → `PriceAlertResponse`
```json
// request
{ "symbol": "…"?, "target_price": "4.2", "condition": "above | below" }
// response
{ "id": "uuid", "user_id": "uuid", "symbol": "…", "target_price": "4.2",
  "condition": "above | below", "is_active": true, "created_at": "ISO" }
```
### `GET /api/v1/price-alerts` — `[PriceAlertResponse]`
### `DELETE /api/v1/price-alerts/{id}` — opaque JSON

---

## Recurring orders

### `POST /api/v1/orders/recurring` — `CreateRecurringRequest` → `RecurringOrderWire`
```json
// request
{
  "side": "buy | sell", "energy_amount": "5",
  "max_price_per_kwh": "5"?, "min_price_per_kwh": "3"?,
  "interval_type": "…", "interval_value": 1?, "max_executions": 10?,
  "name": "…"?, "description": "…"?, "session_token": "…"?
}
// response
{
  "id": "uuid", "user_id": "uuid", "side": "buy", "energy_amount": "5",
  "max_price_per_kwh": "… | null", "min_price_per_kwh": "… | null",
  "interval_type": "…", "interval_value": 1,
  "next_execution_at": "ISO", "last_executed_at": "ISO | null",
  "status": "…", "total_executions": 0, "max_executions": "i32 | null",
  "name": "… | null", "description": "… | null", "created_at": "ISO", "updated_at": "ISO"
}
```
### `GET /api/v1/orders/recurring` — `[RecurringOrderWire]`
### `GET /api/v1/orders/recurring/{id}` — `RecurringOrderWire`
### `DELETE /api/v1/orders/recurring/{id}` — opaque JSON
### `POST /api/v1/orders/recurring/{id}/pause` · `.../resume` — opaque JSON

---

## Futures (nested under `/api/v1/futures`)

### `GET /products` — `[FuturesProduct]`
```json
{ "id": "uuid", "symbol": "…", "base_asset": "…", "quote_asset": "…",
  "contract_size": "1.0", "expiration_date": "ISO", "current_price": "4.20", "is_active": true }
```
### `POST /orders` — `CreateFuturesOrderRequest` → opaque JSON *(stubbed)*
```json
{ "product_id": "…", "side": "…", "order_type": "…", "quantity": 1.0, "price": 4.2, "leverage": 5 }
```
### `GET /orders` — `[FuturesOrder]`
```json
{ "id": "uuid", "user_id": "uuid", "product_id": "uuid",
  "side": "long | short", "order_type": "market | limit",
  "quantity": "1.0", "price": "4.2", "leverage": 5,
  "status": "pending | open | filled | cancelled | liquidated",
  "filled_quantity": "0.0", "average_fill_price": "4.2 | null", "created_at": "ISO" }
```
### `GET /positions` — `[FuturesPosition]`
```json
{ "id": "uuid", "user_id": "uuid", "product_id": "uuid", "side": "long | short",
  "quantity": "1.0", "entry_price": "4.0", "current_price": "4.2", "leverage": 5,
  "margin_used": "0.8", "unrealized_pnl": "0.2", "liquidation_price": "3.5 | null" }
```
### `DELETE /positions/{id}` — opaque JSON
### `GET /candles` · `GET /book` — opaque JSON *(stubs)*

---

## User data & analytics

### `GET /api/v1/wallets/{address}/balance` — opaque JSON
### `GET /api/v1/analytics/stats` — `UserAnalytics`
```json
{ "total_traded_kwh": "120.5", "total_spent_grid": "500.0", "total_earned_grid": "480.0",
  "carbon_offset_tons": "1.2", "reliability_score": 0.95 }   // reliability_score is a real f64
```
### `GET /api/v1/analytics/history` — opaque JSON
### `GET /api/v1/transactions` — `[TransactionData]`
```json
{ "id": "uuid", "transaction_type": "…", "amount": "42.0", "asset": "GRID | THBG | …",
  "status": "…", "timestamp": "ISO", "reference_id": "uuid | null" }
```

---

## Carbon / ESG (nested under `/api/v1/carbon`)

### `GET /balance` — opaque JSON
### `GET /history` — `[CarbonCredit]`
```json
{ "id": "uuid", "user_id": "uuid", "amount": "1.5", "source": "…",
  "status": "active | retired | transferred", "created_at": "ISO" }
```
### `GET /transactions` — opaque JSON
### `POST /transfers` — opaque JSON in/out *(stubbed)*

---

## Ops

- `GET /health` · `GET /health/ready` · `GET /metrics` — no body (Prometheus text on `/metrics`).

---

## Enums (JSON serialization)

| Type | Values (wire) | Rename |
| ---- | ------------- | ------ |
| `FuturesOrderSide` | `long`, `short` | lowercase |
| `FuturesOrderType` | `market`, `limit` | lowercase |
| `FuturesOrderStatus` | `pending`, `open`, `filled`, `cancelled`, `liquidated` | snake_case |
| `CarbonStatus` | `active`, `retired`, `transferred` | lowercase |

> Generated from source; keep in sync with `rest.rs` / `models.rs` / `types.rs` when handlers change.
