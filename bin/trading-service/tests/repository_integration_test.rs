#![allow(clippy::unwrap_used)] // unwrap is idiomatic in integration tests

use chrono::Utc;
use rust_decimal_macros::dec;
use sqlx::PgPool;
use trading_core::models::{Settlement, SettlementStatus, TradingOrder};
use trading_core::traits::{MeterRepository, OrderRepository, SettlementRepository};
use trading_core::types::{OrderSide, OrderStatus, OrderType, TimeInForce};
use trading_persistence::repositories::{
    PostgresMeterRepository, PostgresOrderRepository, PostgresSettlementRepository,
};
use uuid::Uuid;

mod common;

#[tokio::test]
async fn test_postgres_order_repository_e2e() {
    // 1. Establish central database connection
    let db_url = common::test_db_url();

    let pool = PgPool::connect(&db_url)
        .await
        .expect("Failed to connect to postgres");

    // No `users` rows are seeded or cleaned up here: migration 20260728000000
    // (the DB-per-service split) dropped the cross-domain FK to IAM `users`, and
    // `gridtokenx_trading` has no such table at all — identities live in
    // `gridtokenx_iam`. Verified: the only FK on `trading_orders`/`settlements` is
    // to `market_epochs(id)`. The leftover INSERT/DELETE failed with
    // `relation "users" does not exist`.
    let user_id = Uuid::new_v4();

    // 3. Insert test epoch to satisfy epoch foreign key constraint
    let epoch_id = Uuid::new_v4();
    // Unique-id-derived, not wall-clock — avoids parallel-test collisions on
    // market_epochs_epoch_number_key.
    let epoch_number = (epoch_id.as_u128() as u64 >> 1) as i64;
    let start_time = Utc::now();
    let end_time = Utc::now() + chrono::Duration::minutes(15);

    sqlx::query(
        "INSERT INTO market_epochs (id, epoch_number, start_time, end_time, status) VALUES ($1, $2, $3, $4, 'pending')"
    )
    .bind(epoch_id)
    .bind(epoch_number)
    .bind(start_time)
    .bind(end_time)
    .execute(&pool)
    .await
    .expect("Failed to insert test epoch");

    let order_repo = PostgresOrderRepository::new(pool.clone());
    let settlement_repo = PostgresSettlementRepository::new(pool.clone());

    // 4. Prepare test entities with unique IDs
    let order_id = Uuid::new_v4();
    let order = TradingOrder {
        id: order_id,
        user_id,
        order_type: OrderType::Limit,
        side: OrderSide::Buy,
        energy_amount: dec!(15.5),
        price_per_kwh: dec!(4.25),
        filled_amount: dec!(0.0),
        status: OrderStatus::Active,
        expires_at: None,
        created_at: Some(Utc::now()),
        filled_at: None,
        epoch_id: Some(epoch_id),
        zone_id: Some(1),
        meter_id: None,
        refund_tx_signature: None,
        order_pda: Some("OrderPDA111111111111111111111111111111111".to_string()),
        order_index: Some(42),
        session_token: None,
        blockchain_status: None,
        blockchain_tx_hash: None,
        blockchain_error: None,
        retry_count: 0,
        time_in_force: TimeInForce::Gtc,
        market_segment: trading_core::types::MarketSegment::Realtime,
    };

    // 5. Test insert and fetch
    order_repo
        .insert_order(&order)
        .await
        .expect("Failed to insert order");

    let fetched = order_repo
        .get_order(order_id)
        .await
        .expect("Failed to get order")
        .expect("Order not found");
    assert_eq!(fetched.id, order_id);
    assert_eq!(fetched.user_id, user_id);
    assert_eq!(fetched.energy_amount, dec!(15.5));
    assert_eq!(fetched.price_per_kwh, dec!(4.25));
    assert_eq!(fetched.order_index, Some(42));

    // 6. Test status and fill updates
    order_repo
        .update_filled_amount(order_id, dec!(5.5), OrderStatus::PartiallyFilled)
        .await
        .expect("Failed to update filled amount");
    let fetched_after_update = order_repo.get_order(order_id).await.unwrap().unwrap();
    assert_eq!(fetched_after_update.filled_amount, dec!(5.5));
    assert_eq!(fetched_after_update.status, OrderStatus::PartiallyFilled);

    // 7. Test settlement operations
    let settlement_id = Uuid::new_v4();
    let buy_order_id = order_id;
    let sell_order_id = Uuid::new_v4();

    // To satisfy sell_order_id foreign key constraint, we must also insert a sell order!
    let sell_order = TradingOrder {
        id: sell_order_id,
        user_id,
        order_type: OrderType::Limit,
        side: OrderSide::Sell,
        energy_amount: dec!(15.5),
        price_per_kwh: dec!(4.25),
        filled_amount: dec!(0.0),
        status: OrderStatus::Active,
        expires_at: None,
        created_at: Some(Utc::now()),
        filled_at: None,
        epoch_id: Some(epoch_id),
        zone_id: Some(1),
        meter_id: None,
        refund_tx_signature: None,
        order_pda: Some("OrderPDA222222222222222222222222222222222".to_string()),
        order_index: Some(43),
        session_token: None,
        blockchain_status: None,
        blockchain_tx_hash: None,
        blockchain_error: None,
        retry_count: 0,
        time_in_force: TimeInForce::Gtc,
        market_segment: trading_core::types::MarketSegment::Realtime,
    };
    order_repo
        .insert_order(&sell_order)
        .await
        .expect("Failed to insert sell order");

    let settlement = Settlement {
        id: settlement_id,
        trade_id: None,
        epoch_id,
        buyer_id: user_id,
        seller_id: user_id, // buy and sell user can be same for this schema test
        buy_order_id,
        sell_order_id,
        energy_amount: dec!(5.5),
        price: dec!(4.25),
        total_amount: dec!(23.375),
        fee_amount: dec!(0.0),
        net_amount: dec!(23.375),
        status: SettlementStatus::Pending,
        blockchain_tx: None,
        created_at: Utc::now(),
        confirmed_at: None,
        wheeling_charge: Some(dec!(0.1)),
        loss_factor: Some(dec!(1.02)),
        loss_cost: Some(dec!(0.05)),
        effective_energy: Some(dec!(5.5)),
        buyer_zone_id: Some(1),
        seller_zone_id: Some(1),
        buyer_session_token: None,
        seller_session_token: None,
        erc_certificate_id: None,
        erc_transfer_tx: None,
        retry_count: 0,
        error_message: None,
    };

    settlement_repo
        .insert_settlement(&settlement)
        .await
        .expect("Failed to insert settlement");
    let fetched_settlement = settlement_repo
        .get_settlement(settlement_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetched_settlement.id, settlement_id);
    assert_eq!(fetched_settlement.energy_amount, dec!(5.5));

    // 8. Cleanup — delete the rows this test created, explicitly.
    // These used to disappear via `trading_orders_user_id_fkey ON DELETE CASCADE`,
    // but the DB-per-service split dropped every cross-domain FK to IAM `users`
    // (migration 20260728000000). Without an explicit delete the fixture orders
    // survive teardown, and `market_epochs` deletion only NULLs their epoch
    // (ON DELETE SET NULL) — leaving orphaned active orders in the shared dev
    // order book that the live matcher then tries, and fails, to settle.
    sqlx::query("DELETE FROM settlements WHERE id = $1")
        .bind(settlement_id)
        .execute(&pool)
        .await
        .ok();
    sqlx::query("DELETE FROM trading_orders WHERE user_id = $1")
        .bind(user_id)
        .execute(&pool)
        .await
        .ok();
    sqlx::query("DELETE FROM market_epochs WHERE id = $1")
        .bind(epoch_id)
        .execute(&pool)
        .await
        .ok();
}

/// db-split guard: meter identity lookups must read the Trading-owned
/// `meter_read_model`, NOT the metering `meters` table (which no longer lives in
/// the trading DB). A regression here 500s both active-order-meters endpoints —
/// `get_serials_for_ids` runs on every populated response to translate
/// `meter_id` into the map's `meter_serial` id space.
#[tokio::test]
async fn test_meter_repository_reads_read_model() {
    let db_url = common::test_db_url();
    let pool = PgPool::connect(&db_url).await.expect("connect");

    let meter_id = Uuid::new_v4();
    let user_id = Uuid::new_v4();
    let serial = format!("SN-REPO-{}", &meter_id.to_string()[..8]);

    // Read model is fed by meter NATS events; here we seed one row directly.
    // updated_at defaults to now(); user_id has no FK (read-model table).
    sqlx::query(
        "INSERT INTO meter_read_model (meter_id, serial_number, user_id, zone_id) VALUES ($1, $2, $3, $4)"
    )
    .bind(meter_id)
    .bind(&serial)
    .bind(user_id)
    .bind(7_i32)
    .execute(&pool)
    .await
    .expect("Failed to insert meter_read_model row");

    let repo = PostgresMeterRepository::new(pool.clone());

    // meter_id -> serial (the response-shaping path)
    let serials = repo
        .get_serials_for_ids(&[meter_id])
        .await
        .expect("get_serials_for_ids");
    assert_eq!(
        serials.get(&meter_id).map(String::as_str),
        Some(serial.as_str()),
        "meter_id must resolve to its serial via meter_read_model"
    );

    // serial -> meter_id (the order-submission path)
    let resolved = repo
        .resolve_id_by_serial(&serial)
        .await
        .expect("resolve_id_by_serial");
    assert_eq!(
        resolved,
        Some(meter_id),
        "serial must resolve back to the meter_id"
    );

    // Empty input short-circuits without touching the DB.
    let empty = repo.get_serials_for_ids(&[]).await.expect("empty ids");
    assert!(empty.is_empty());

    // Unknown serial yields None, not an error.
    let missing = repo
        .resolve_id_by_serial("SN-DOES-NOT-EXIST")
        .await
        .expect("missing serial");
    assert_eq!(missing, None);

    sqlx::query("DELETE FROM meter_read_model WHERE meter_id = $1")
        .bind(meter_id)
        .execute(&pool)
        .await
        .ok();
}

/// Phase 0: the market_segment column round-trips. An Interval order inserted
/// via the repository must read back as Interval — not silently default to
/// Realtime (which would route it to the CDA matcher instead of the clearing
/// worker).
#[tokio::test]
async fn test_market_segment_round_trips() {
    let db_url = common::test_db_url();
    let pool = PgPool::connect(&db_url).await.expect("connect");

    let user_id = Uuid::new_v4(); // no `users` seed needed — see the note above

    let epoch_id = Uuid::new_v4();
    // Derive epoch_number from the epoch UUID (epoch_number is UNIQUE) — a
    // timestamp-based number would collide with the sibling test running in
    // parallel at the same nanosecond.
    let epoch_num = (epoch_id.as_u128() & 0x7FFF_FFFF_FFFF_FFFF) as i64;
    sqlx::query("INSERT INTO market_epochs (id, epoch_number, start_time, end_time, status) VALUES ($1,$2,$3,$4,'active'::epoch_status)")
        .bind(epoch_id)
        .bind(epoch_num)
        .bind(Utc::now())
        .bind(Utc::now() + chrono::Duration::minutes(15))
        .execute(&pool).await.expect("insert epoch");

    let repo = PostgresOrderRepository::new(pool.clone());
    let order_id = Uuid::new_v4();
    let mut order = TradingOrder {
        id: order_id,
        user_id,
        order_type: OrderType::Limit,
        side: OrderSide::Buy,
        energy_amount: dec!(5.0),
        price_per_kwh: dec!(2.0),
        filled_amount: dec!(0.0),
        status: OrderStatus::Active,
        expires_at: None,
        created_at: Some(Utc::now()),
        filled_at: None,
        epoch_id: Some(epoch_id),
        zone_id: Some(1),
        meter_id: None,
        refund_tx_signature: None,
        order_pda: None,
        order_index: None,
        session_token: None,
        blockchain_status: None,
        blockchain_tx_hash: None,
        blockchain_error: None,
        retry_count: 0,
        time_in_force: TimeInForce::Gtc,
        market_segment: trading_core::types::MarketSegment::Interval,
    };
    repo.insert_order(&order)
        .await
        .expect("insert interval order");
    let fetched = repo.get_order(order_id).await.unwrap().unwrap();
    assert_eq!(
        fetched.market_segment,
        trading_core::types::MarketSegment::Interval,
        "interval segment must survive the DB round-trip"
    );

    // A defaulted (realtime) order also round-trips.
    order.id = Uuid::new_v4();
    order.market_segment = trading_core::types::MarketSegment::Realtime;
    repo.insert_order(&order)
        .await
        .expect("insert realtime order");
    let rt = repo.get_order(order.id).await.unwrap().unwrap();
    assert_eq!(
        rt.market_segment,
        trading_core::types::MarketSegment::Realtime
    );

    // Explicit order cleanup — no cross-domain FK cascade to rely on any more
    // (see the teardown note in the repository round-trip test above).
    sqlx::query("DELETE FROM trading_orders WHERE user_id = $1")
        .bind(user_id)
        .execute(&pool)
        .await
        .ok();
    sqlx::query("DELETE FROM market_epochs WHERE id = $1")
        .bind(epoch_id)
        .execute(&pool)
        .await
        .ok();
}

/// `expires_at` must survive the insert, and a lapsed order must not appear in the
/// live-book views.
///
/// Two regressions in one test. First, both insert paths omitted `expires_at` from
/// their column list, so it was silently dropped: every order landed with NULL and
/// the ReaperWorker (which matches `expires_at < now()`) had nothing to reap —
/// order expiry was inert for anything created through the API. Second, once
/// expiry actually persists, the live-book readers start seeing lapsed-but-not-yet-
/// reaped rows; `OrderBookEntry` carries no expiry, so a caller cannot filter them
/// and the book would advertise depth the matcher will never fill.
#[tokio::test]
async fn test_expires_at_persists_and_expired_orders_leave_the_live_book() {
    let db_url = common::test_db_url();
    let pool = PgPool::connect(&db_url).await.expect("connect");

    let user_id = Uuid::new_v4();
    let epoch_id = Uuid::new_v4();
    let epoch_num = (epoch_id.as_u128() & 0x7FFF_FFFF_FFFF_FFFF) as i64;
    sqlx::query("INSERT INTO market_epochs (id, epoch_number, start_time, end_time, status) VALUES ($1,$2,$3,$4,'active'::epoch_status)")
        .bind(epoch_id)
        .bind(epoch_num)
        .bind(Utc::now())
        .bind(Utc::now() + chrono::Duration::minutes(15))
        .execute(&pool).await.expect("insert epoch");

    // A zone of its own, so the assertions see only this test's orders even with
    // sibling tests writing to the same dev database.
    let zone = 900 + (epoch_num % 90) as i32;
    let repo = PostgresOrderRepository::new(pool.clone());

    let mut order = TradingOrder {
        id: Uuid::new_v4(),
        user_id,
        order_type: OrderType::Limit,
        side: OrderSide::Sell,
        energy_amount: dec!(5.0),
        price_per_kwh: dec!(3.0),
        filled_amount: dec!(0.0),
        status: OrderStatus::Active,
        expires_at: Some(Utc::now() + chrono::Duration::hours(1)),
        created_at: Some(Utc::now()),
        filled_at: None,
        epoch_id: Some(epoch_id),
        zone_id: Some(zone),
        meter_id: None,
        refund_tx_signature: None,
        order_pda: None,
        order_index: None,
        session_token: None,
        blockchain_status: None,
        blockchain_tx_hash: None,
        blockchain_error: None,
        retry_count: 0,
        time_in_force: TimeInForce::Gtc,
        market_segment: trading_core::types::MarketSegment::Realtime,
    };

    // 1. A future expiry round-trips instead of being dropped to NULL.
    let live_id = order.id;
    let live_expiry = order.expires_at;
    repo.insert_order(&order).await.expect("insert live order");
    let fetched = repo.get_order(live_id).await.unwrap().unwrap();
    assert_eq!(
        fetched.expires_at.map(|t| t.timestamp()),
        live_expiry.map(|t| t.timestamp()),
        "expires_at must survive the insert (it used to be dropped silently)"
    );

    // 2. The same via the transactional path the API actually uses.
    order.id = Uuid::new_v4();
    let event = trading_core::events::Event::OrderUpdate {
        id: order.id,
        user_id: Some(user_id),
        filled_amount: dec!(0),
        status: OrderStatus::Active.to_string(),
        zone_id: Some(zone),
    };
    repo.insert_order_with_event(&order, &event)
        .await
        .expect("insert with event");
    assert!(
        repo.get_order(order.id)
            .await
            .unwrap()
            .unwrap()
            .expires_at
            .is_some(),
        "insert_order_with_event must persist expires_at too"
    );

    // 3. An order already past its expiry but NOT yet reaped (status still Active)
    //    must be absent from both live-book views.
    let expired_id = Uuid::new_v4();
    order.id = expired_id;
    order.expires_at = Some(Utc::now() - chrono::Duration::minutes(5));
    repo.insert_order(&order)
        .await
        .expect("insert expired order");
    assert_eq!(
        repo.get_order(expired_id).await.unwrap().unwrap().status,
        OrderStatus::Active,
        "precondition: the reaper has not touched it, so only the expiry filter can hide it"
    );

    let zone_book = repo
        .get_active_orders_by_zone(zone)
        .await
        .expect("zone book");
    let ids: Vec<Uuid> = zone_book.iter().map(|e| e.order_id).collect();
    assert!(
        ids.contains(&live_id),
        "the unexpired order must still be quoted"
    );
    assert!(
        !ids.contains(&expired_id),
        "an expired order must not appear as depth in the zone book"
    );

    let all = repo.get_all_active_orders().await.expect("all active");
    assert!(
        !all.iter().any(|e| e.order_id == expired_id),
        "an expired order must not reach get_all_active_orders (it sets the \
         best bid/ask that price alerts fire on)"
    );

    sqlx::query("DELETE FROM trading_orders WHERE user_id = $1")
        .bind(user_id)
        .execute(&pool)
        .await
        .ok();
    sqlx::query("DELETE FROM market_epochs WHERE id = $1")
        .bind(epoch_id)
        .execute(&pool)
        .await
        .ok();
}

/// `settlements_with_lapsed_orders` against a real database.
///
/// This is the query behind the settlement expiry pre-flight, and it is written with the
/// runtime SQLx API — nothing checks its columns or joins at compile time, so a typo would
/// only surface as the pre-flight silently erroring and degrading to "attempt everything".
/// It is exercised here against the actual schema, with both legs and both boundaries.
#[tokio::test]
async fn settlements_with_lapsed_orders_matches_only_the_lapsed_legs() {
    let pool = PgPool::connect(&common::test_db_url())
        .await
        .expect("connect to the *_test database");
    let order_repo = PostgresOrderRepository::new(pool.clone());
    let settlement_repo = PostgresSettlementRepository::new(pool.clone());

    let user_id = Uuid::new_v4();
    let epoch_id = Uuid::new_v4();
    // Id-derived, not wall-clock: parallel tests would collide on epoch_number.
    let epoch_number = (epoch_id.as_u128() as u64 >> 1) as i64;
    sqlx::query(
        "INSERT INTO market_epochs (id, epoch_number, start_time, end_time, status) \
         VALUES ($1, $2, $3, $4, 'pending')",
    )
    .bind(epoch_id)
    .bind(epoch_number)
    .bind(Utc::now())
    .bind(Utc::now() + chrono::Duration::minutes(15))
    .execute(&pool)
    .await
    .expect("insert epoch");

    let past = Utc::now() - chrono::Duration::minutes(5);
    let future = Utc::now() + chrono::Duration::minutes(30);

    let mut idx = 0i64;
    // `Filled`, not `Active`, and deliberately so on two counts. It is what a settled
    // trade's orders actually look like, and — because the reaper only touches OPEN
    // statuses — it keeps these rows invisible to `expire_stale_orders`. These fixtures
    // carry PAST expiries, so if this test ever aborts before its cleanup runs (a failing
    // assertion, a panic), leftover Active rows would be reaped by
    // order_expiry_integration_test and fail ITS exact-set assertion. That is not
    // hypothetical: it happened while this test was being written.
    let make_order =
        |side: OrderSide, expires_at: Option<chrono::DateTime<Utc>>, i: i64| TradingOrder {
            id: Uuid::new_v4(),
            user_id,
            order_type: OrderType::Limit,
            side,
            energy_amount: dec!(1.0),
            price_per_kwh: dec!(2.0),
            filled_amount: dec!(1.0),
            status: OrderStatus::Filled,
            expires_at,
            created_at: Some(Utc::now()),
            filled_at: None,
            epoch_id: Some(epoch_id),
            zone_id: Some(1),
            meter_id: None,
            refund_tx_signature: None,
            order_pda: None,
            order_index: Some(i),
            session_token: None,
            blockchain_status: None,
            blockchain_tx_hash: None,
            blockchain_error: None,
            retry_count: 0,
            time_in_force: TimeInForce::Gtc,
            market_segment: trading_core::types::MarketSegment::Realtime,
        };

    // Each case is one settlement over a (buy, sell) pair with the given expiries.
    let cases: Vec<(
        &str,
        Option<chrono::DateTime<Utc>>,
        Option<chrono::DateTime<Utc>>,
        bool,
    )> = vec![
        ("both legs no expiry (the NULL sentinel)", None, None, false),
        ("both legs still live", Some(future), Some(future), false),
        ("buy leg lapsed", Some(past), Some(future), true),
        ("sell leg lapsed, buy has no expiry", None, Some(past), true),
        ("both legs lapsed", Some(past), Some(past), true),
    ];

    let mut expected_lapsed: Vec<Uuid> = Vec::new();
    let mut all_ids: Vec<Uuid> = Vec::new();
    let mut labels: Vec<(Uuid, &str)> = Vec::new();

    for (label, buy_exp, sell_exp, should_be_lapsed) in cases {
        idx += 1;
        let buy = make_order(OrderSide::Buy, buy_exp, idx);
        idx += 1;
        let sell = make_order(OrderSide::Sell, sell_exp, idx);
        order_repo
            .insert_order(&buy)
            .await
            .expect("insert buy order");
        order_repo
            .insert_order(&sell)
            .await
            .expect("insert sell order");

        let settlement = Settlement {
            id: Uuid::new_v4(),
            trade_id: None,
            epoch_id,
            buyer_id: user_id,
            seller_id: user_id,
            buy_order_id: buy.id,
            sell_order_id: sell.id,
            energy_amount: dec!(1.0),
            price: dec!(2.0),
            total_amount: dec!(2.0),
            fee_amount: dec!(0.0),
            net_amount: dec!(2.0),
            status: SettlementStatus::Pending,
            blockchain_tx: None,
            created_at: Utc::now(),
            confirmed_at: None,
            wheeling_charge: None,
            loss_factor: None,
            loss_cost: None,
            effective_energy: None,
            buyer_zone_id: Some(1),
            seller_zone_id: Some(1),
            buyer_session_token: None,
            seller_session_token: None,
            erc_certificate_id: None,
            erc_transfer_tx: None,
            retry_count: 0,
            error_message: None,
        };
        settlement_repo
            .insert_settlement(&settlement)
            .await
            .expect("insert settlement");

        all_ids.push(settlement.id);
        labels.push((settlement.id, label));
        if should_be_lapsed {
            expected_lapsed.push(settlement.id);
        }
    }

    let mut got = settlement_repo
        .settlements_with_lapsed_orders(&all_ids)
        .await
        .expect("the lapsed-order query must run against the real schema");
    got.sort();
    let mut want = expected_lapsed.clone();
    want.sort();

    for (id, label) in &labels {
        let flagged = got.contains(id);
        let should = want.contains(id);
        assert_eq!(
            flagged, should,
            "case '{label}' — flagged={flagged}, expected={should}"
        );
    }
    assert_eq!(got, want, "exactly the lapsed settlements, nothing else");

    // Only ids that were ASKED about come back: pass a single live settlement and a
    // single lapsed one, and the other lapsed rows must not leak into the result.
    let subset = vec![labels[1].0, expected_lapsed[0]];
    let scoped = settlement_repo
        .settlements_with_lapsed_orders(&subset)
        .await
        .expect("scoped query");
    assert_eq!(
        scoped,
        vec![expected_lapsed[0]],
        "the query must respect its id filter"
    );

    // Empty input must not become "every lapsed settlement in the table".
    assert!(settlement_repo
        .settlements_with_lapsed_orders(&[])
        .await
        .expect("empty query")
        .is_empty());

    sqlx::query("DELETE FROM settlements WHERE epoch_id = $1")
        .bind(epoch_id)
        .execute(&pool)
        .await
        .ok();
    sqlx::query("DELETE FROM trading_orders WHERE user_id = $1")
        .bind(user_id)
        .execute(&pool)
        .await
        .ok();
    sqlx::query("DELETE FROM market_epochs WHERE id = $1")
        .bind(epoch_id)
        .execute(&pool)
        .await
        .ok();
}
