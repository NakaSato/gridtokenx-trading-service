#![allow(clippy::unwrap_used)] // unwrap is idiomatic in integration tests

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use chrono::Utc;
use serde_json::json;
use sqlx::PgPool;
use tower::ServiceExt; // for oneshot
use uuid::Uuid;

use gridtokenx_blockchain_core::auth::{GATEWAY_SECRET_HEADER, INTERNAL_ROLE_HEADER};
use trading_api::auth::USER_ID_HEADER;
use trading_api::startup::build_router;
use trading_api::state::AppState;
use trading_service::builder::ServiceBuilder;

mod common;

#[tokio::test]
async fn test_api_routing_e2e() {
    // 1. Establish database connection
    let db_url = common::test_db_url();

    let pool = PgPool::connect(&db_url)
        .await
        .expect("Failed to connect to postgres");

    // 2. Setup environment variables for config
    std::env::set_var("TRADING_DATABASE_URL", &db_url);
    std::env::set_var("CHAIN_BRIDGE_INSECURE", "true");
    std::env::set_var(
        "ENERGY_TOKEN_MINT",
        "EneryTokenMint111111111111111111111111111",
    );
    std::env::set_var(
        "CURRENCY_TOKEN_MINT",
        "8BGFtQLRaY9Nh5BGUwjJvdeXEsscCgJAi5zTgALk1Vg5",
    );
    std::env::set_var(
        "FEE_COLLECTOR_WALLET",
        "EzudwoHvNPAc4dpPi5ndU8MEZVHVzq3Pj3Thm9ooKmiJ",
    );
    std::env::set_var(
        "WHEELING_COLLECTOR_WALLET",
        "EzudwoHvNPAc4dpPi5ndU8MEZVHVzq3Pj3Thm9ooKmiJ",
    );
    std::env::set_var(
        "LOSS_COLLECTOR_WALLET",
        "EzudwoHvNPAc4dpPi5ndU8MEZVHVzq3Pj3Thm9ooKmiJ",
    );
    std::env::set_var(
        "SOLANA_TRADING_PROGRAM_ID",
        "DA9TdkcToi5r7oS7X5CddoMBiGNF3sAGqwPQph1CfLwd",
    );
    std::env::set_var("REDIS_URL", "redis://localhost:7010");
    std::env::set_var("SOLANA_RPC_URL", "http://localhost:8899");
    std::env::set_var("SOLANA_WS_URL", "ws://localhost:8900");

    // Install the Prometheus recorder so /metrics renders emitted metrics
    // (mirrors main.rs; without it render() returns an empty string).
    trading_infra::metrics::install_recorder();

    let config = std::sync::Arc::new(
        trading_core::config::Config::from_env().expect("Failed to load config"),
    );

    // 3. Build system components (with real DB and Redis connection, no mock repositories)
    let (infra, services) = ServiceBuilder::build(pool.clone(), config.clone())
        .await
        .expect("Failed to build system");

    let state = AppState {
        config,
        order_repo: infra.order_repo,
        meter_repo: infra.meter_repo,
        settlement_repo: infra.settlement_repo,
        futures_repo: infra.futures_repo,
        carbon_repo: infra.carbon_repo,
        analytics_repo: infra.analytics_repo,
        price_alert_repo: infra.price_alert_repo,
        recurring_repo: infra.recurring_repo,
        events: infra.events,
        blockchain: infra.blockchain,
        identity: infra.identity,
        audit: infra.audit,
        matcher: services.matcher,
        settlement: services.settlement,
        vpp: services.vpp,
        ws_hub: std::sync::Arc::new(trading_api::websocket::ZoneHub::new()),
    };

    // 4. Initialize in-memory Axum Router
    let app = build_router(state);

    // 5. Test Case 1: GET /health (Public)
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body_json["status"], "ok");
    assert_eq!(body_json["service"], "trading");

    // 6. Test Case 2: GET /metrics (Public)
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    // The recorder only renders metrics after their first emission, so at this
    // point the body just has to be valid text; content is asserted at step 12b
    // once an order submission has emitted trading_orders_submitted_total.
    std::str::from_utf8(&body).unwrap();

    // 6b. Test Case 2b: GET /api-docs/openapi.json + /docs (Public, OpenAPI)
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api-docs/openapi.json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let spec: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(spec["paths"]["/api/v1/orders"].is_object());
    assert!(spec["paths"]["/health"].is_object());

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/docs")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // SwaggerUi serves the page at /docs (or redirects to /docs/).
    assert!(
        response.status() == StatusCode::OK || response.status().is_redirection(),
        "unexpected /docs status: {}",
        response.status()
    );

    // 7. Test Case 3: POST /api/v1/orders (Fails: Missing Auth/Role Headers)
    let payload = json!({
        "side": "buy",
        "order_type": "limit",
        "energy_amount_kwh": "12.5",
        "price_per_kwh": "4.25",
        "zone_id": 1
    });

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/orders")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&payload).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    // Custom extractor returns 401 Unauthorized when USER_ID_HEADER is missing
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    // 8. Test Case 4: POST /api/v1/orders (Fails: Missing Gateway Secret for api-gateway role)
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/orders")
                .header(INTERNAL_ROLE_HEADER, "api-gateway")
                .header(USER_ID_HEADER, Uuid::new_v4().to_string())
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&payload).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    // ServiceRole: ApiGateway requires GATEWAY_SECRET_HEADER check; mismatch defaults to
    // Unknown role, which require_any() denies as 403 Forbidden (blockchain-core auth.rs).
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    // 9. Database Setup for Successful Operations
    //
    // No `users` row is seeded: after the DB-per-service split this service owns
    // `gridtokenx_trading`, which has no `users` table at all (identities live in
    // `gridtokenx_iam`) and no FK from `trading_orders.user_id` to one — the only
    // FK on the table is `epoch_id -> market_epochs(id)`. The old INSERT here
    // targeted the pre-split shared `gridtokenx` DB and failed with
    // `relation "users" does not exist`, so any caller-supplied uuid works.
    let user_id = Uuid::new_v4();

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

    // 10. Test Case 5: POST /api/v1/orders (Success: Authenticated & Valid Request)
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/orders")
                .header(INTERNAL_ROLE_HEADER, "api-gateway")
                .header(GATEWAY_SECRET_HEADER, "gridtokenx-gateway-secret-2025")
                .header(USER_ID_HEADER, user_id.to_string())
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&payload).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let order_resp: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let order_id_str = order_resp["id"]
        .as_str()
        .expect("Order response should contain UUID id");
    let order_uuid = Uuid::parse_str(order_id_str).unwrap();

    // 11. Test Case 6: GET /api/v1/orders (Success: Retrieve User Orders)
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/orders")
                .header(INTERNAL_ROLE_HEADER, "api-gateway")
                .header(GATEWAY_SECRET_HEADER, "gridtokenx-gateway-secret-2025")
                .header(USER_ID_HEADER, user_id.to_string())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let list_resp: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let orders_list = list_resp["data"]
        .as_array()
        .expect("Response data should be an array");
    assert!(orders_list.iter().any(|o| o["id"] == order_id_str));

    // 12. Test Case 7: DELETE /api/v1/orders/{id} (Success: Cancel Order)
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(&format!("/api/v1/orders/{}", order_uuid))
                .header(INTERNAL_ROLE_HEADER, "api-gateway")
                .header(GATEWAY_SECRET_HEADER, "gridtokenx-gateway-secret-2025")
                .header(USER_ID_HEADER, user_id.to_string())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let cancel_resp: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(cancel_resp["status"], "cancelled");
    assert_eq!(cancel_resp["order_id"], order_id_str);

    // 12b. Test Case 7b: GET /metrics now carries the submission counter emitted
    // by the successful POST /api/v1/orders above.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let metrics_str = std::str::from_utf8(&body).unwrap();
    assert!(
        metrics_str.contains("trading_orders_submitted_total"),
        "submission counter missing from /metrics"
    );

    // 13. Teardown Database
    sqlx::query("DELETE FROM market_epochs WHERE id = $1")
        .bind(epoch_id)
        .execute(&pool)
        .await
        .ok();
}
