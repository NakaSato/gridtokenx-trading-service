//! Connection preflight: actively probe every internal dependency the trading
//! service talks to, print a pass/fail table, and exit non-zero on any failure.
//!
//! Real (auth-level) probes: Postgres (`SELECT 1`), Redis (`PING` via `CacheService`).
//! Transport-level probes (TCP reachability): Chain Bridge gRPC, NATS, IAM, Kafka.
//! Functional RPC probes (e.g. `GetSlot`, order submit) are out of scope here —
//! run those against the live service with grpcurl (see verification plan Phase 3).
//!
//! Usage: `cargo run -p trading-service --bin verify-connections`

use std::time::Duration;

use trading_core::config::Config;

const TCP_TIMEOUT: Duration = Duration::from_secs(5);

/// Outcome of a single dependency probe.
enum Probe {
    Ok(String),
    Fail(String),
    Skip(String),
}

#[tokio::main]
async fn main() {
    trading_infra::init_telemetry("verify-connections");

    let config = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ Failed to load config from env: {e}");
            std::process::exit(2);
        }
    };

    let mut results: Vec<(&str, Probe)> = Vec::new();

    // 1. Postgres — real query.
    results.push(("Postgres", probe_postgres(&config.database_url).await));

    // 2. Redis — real PING (covers cache + events EventBus, same REDIS_URL).
    results.push(("Redis", probe_redis(&config.redis_url).await));

    // 3. Chain Bridge gRPC — functional read (GetSlot) over the real tonic client.
    results.push(("Chain Bridge (gRPC)", probe_chain_bridge(&config).await));

    // 4. NATS — TCP reachability. NATS_URL is read straight from env by
    //    blockchain-core (not in Config); skip if unset.
    results.push((
        "NATS",
        match std::env::var("NATS_URL") {
            Ok(url) if !url.trim().is_empty() => probe_nats(&url).await,
            _ => Probe::Skip("NATS_URL unset — Chain Bridge writes fall back to gRPC".into()),
        },
    ));

    // 5. IAM Identity (ConnectRPC) — TCP reachability only, by design.
    //    Trading does NOT call IAM at runtime: request auth is header-based,
    //    injected by the APISIX gateway (see trading-api `auth.rs`,
    //    `x-gridtokenx-user-id`), and `IamIdentityGateway::sign_message` is
    //    inert (IAM's SignMessage RPC was removed; custodial signing moved to
    //    Chain Bridge). The IdentityServiceClient is constructed but never
    //    invoked. A functional RPC probe (e.g. VerifyApiKey) would exercise a
    //    path the service itself never uses, so we only confirm the listener is
    //    reachable and label it informational.
    results.push((
        "IAM Identity",
        annotate(
            probe_tcp(&config.iam_service_url, 8081).await,
            "transport only — IAM is gateway-mediated, no runtime RPC from trading",
        ),
    ));

    // 6. Kafka — TCP reachability to first bootstrap server, only if enabled.
    results.push((
        "Kafka",
        if config.kafka_enabled {
            probe_tcp(&config.kafka_bootstrap_servers, 9092).await
        } else {
            Probe::Skip("KAFKA_EVENTS_ENABLED=false".into())
        },
    ));

    // Report.
    println!("\nConnection verification — trading-service dependencies\n");
    let mut failed = false;
    for (name, probe) in &results {
        let (icon, detail) = match probe {
            Probe::Ok(d) => ("✅", d.as_str()),
            Probe::Skip(d) => ("⏭️ ", d.as_str()),
            Probe::Fail(d) => {
                failed = true;
                ("❌", d.as_str())
            }
        };
        println!("  {icon} {name:<22} {detail}");
    }
    println!();

    if failed {
        eprintln!("One or more connections FAILED.");
        std::process::exit(1);
    }
    println!("All probed connections OK.");
}

async fn probe_postgres(url: &str) -> Probe {
    use sqlx::postgres::PgPoolOptions;
    let pool = match PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(TCP_TIMEOUT)
        .connect(url)
        .await
    {
        Ok(p) => p,
        Err(e) => return Probe::Fail(format!("connect failed: {e}")),
    };
    match sqlx::query("SELECT 1").execute(&pool).await {
        Ok(_) => Probe::Ok(format!("SELECT 1 ok ({})", redact(url))),
        Err(e) => Probe::Fail(format!("SELECT 1 failed: {e}")),
    }
}

async fn probe_redis(url: &str) -> Probe {
    // CacheService::new performs a PING during construction, but its
    // ConnectionManager retries internally and will hang on an unreachable
    // host — bound it with our own timeout.
    match tokio::time::timeout(TCP_TIMEOUT, trading_infra::cache::CacheService::new(url)).await {
        Ok(Ok(_)) => Probe::Ok(format!("PING ok ({})", redact(url))),
        Ok(Err(e)) => Probe::Fail(format!("PING failed: {e}")),
        Err(_) => Probe::Fail(format!(
            "PING timed out after {TCP_TIMEOUT:?} ({})",
            redact(url)
        )),
    }
}

/// Functional Chain Bridge probe: build the real gRPC client and issue a cheap
/// `GetSlot` read. This forces the tonic channel to actually connect (insecure
/// mode uses `connect_lazy`, so a TCP-only check would pass even when the bridge
/// is down — `get_slot` round-trips end to end).
async fn probe_chain_bridge(config: &Config) -> Probe {
    use trading_infra::blockchain::BlockchainService;

    let build = BlockchainService::new(
        config.chain_bridge_url.clone(),
        config.solana_cluster.clone(),
        config.solana_programs.clone(),
        None,
        None,
    );
    let svc = match tokio::time::timeout(TCP_TIMEOUT, build).await {
        Ok(Ok(svc)) => svc,
        Ok(Err(e)) => return Probe::Fail(format!("client init failed: {e}")),
        Err(_) => return Probe::Fail(format!("client init timed out after {TCP_TIMEOUT:?}")),
    };

    match tokio::time::timeout(TCP_TIMEOUT, svc.get_slot()).await {
        Ok(Ok(slot)) => Probe::Ok(format!(
            "GetSlot ok (slot={slot}, {})",
            config.chain_bridge_url
        )),
        Ok(Err(e)) => Probe::Fail(format!("GetSlot failed: {e}")),
        Err(_) => Probe::Fail(format!("GetSlot timed out after {TCP_TIMEOUT:?}")),
    }
}

/// NATS protocol probe: a NATS server pushes an `INFO {...}\r\n` line the
/// instant a client connects. Read it and assert the banner — proves a real
/// NATS server is listening, not just any open TCP port. Avoids pulling the
/// heavyweight `async-nats` client into this workspace for a preflight check.
async fn probe_nats(raw: &str) -> Probe {
    use tokio::io::AsyncReadExt;

    let Some((host, port)) = host_port(raw, 4222) else {
        return Probe::Fail(format!("could not parse host:port from '{raw}'"));
    };
    let addr = format!("{host}:{port}");

    let work = async {
        let mut stream = tokio::net::TcpStream::connect(&addr).await?;
        let mut buf = [0u8; 64];
        let n = stream.read(&mut buf).await?;
        Ok::<_, std::io::Error>(buf[..n].to_vec())
    };

    match tokio::time::timeout(TCP_TIMEOUT, work).await {
        Ok(Ok(bytes)) if bytes.starts_with(b"INFO ") => {
            Probe::Ok(format!("INFO banner ok ({addr})"))
        }
        Ok(Ok(bytes)) => Probe::Fail(format!(
            "connected {addr} but no NATS INFO banner (got {} bytes)",
            bytes.len()
        )),
        Ok(Err(e)) => Probe::Fail(format!("connect {addr} failed: {e}")),
        Err(_) => Probe::Fail(format!("timed out after {TCP_TIMEOUT:?} ({addr})")),
    }
}

/// TCP connect to host:port parsed from `raw` (scheme/creds/path/list-tail stripped).
async fn probe_tcp(raw: &str, default_port: u16) -> Probe {
    let Some((host, port)) = host_port(raw, default_port) else {
        return Probe::Fail(format!("could not parse host:port from '{raw}'"));
    };
    let addr = format!("{host}:{port}");
    match tokio::time::timeout(TCP_TIMEOUT, tokio::net::TcpStream::connect(&addr)).await {
        Ok(Ok(_)) => Probe::Ok(format!("TCP reachable ({addr})")),
        Ok(Err(e)) => Probe::Fail(format!("TCP connect {addr} failed: {e}")),
        Err(_) => Probe::Fail(format!(
            "TCP connect {addr} timed out after {TCP_TIMEOUT:?}"
        )),
    }
}

/// Extract (host, port) from a URL-ish string. Handles `scheme://`, `user@`,
/// trailing `/path`, comma-separated broker lists (takes the first), and a
/// bare `host` (falls back to `default_port`).
fn host_port(raw: &str, default_port: u16) -> Option<(String, u16)> {
    let s = raw.trim();
    if s.is_empty() {
        return None;
    }
    let s = s.rsplit("://").next().unwrap_or(s); // strip scheme
    let s = s.split(',').next().unwrap_or(s); // first of broker list
    let s = s.rsplit('@').next().unwrap_or(s); // strip credentials
    let s = s.split('/').next().unwrap_or(s); // strip /path
    if s.is_empty() {
        return None;
    }
    // IPv6 literal like [::1]:5040 — bail to host-only on the simple cases.
    if let Some(rest) = s.strip_prefix('[') {
        let (host, after) = rest.split_once(']')?;
        let port = after
            .strip_prefix(':')
            .and_then(|p| p.parse().ok())
            .unwrap_or(default_port);
        return Some((host.to_string(), port));
    }
    match s.rsplit_once(':') {
        Some((host, port)) if !host.is_empty() => {
            Some((host.to_string(), port.parse().unwrap_or(default_port)))
        }
        _ => Some((s.to_string(), default_port)),
    }
}

/// Append a clarifying note to a probe's detail line (any variant).
fn annotate(probe: Probe, note: &str) -> Probe {
    match probe {
        Probe::Ok(d) => Probe::Ok(format!("{d} — {note}")),
        Probe::Fail(d) => Probe::Fail(format!("{d} — {note}")),
        Probe::Skip(d) => Probe::Skip(format!("{d} — {note}")),
    }
}

/// Hide credentials in a connection URL before logging it.
fn redact(url: &str) -> String {
    match (url.find("://"), url.find('@')) {
        (Some(scheme_end), Some(at)) if at > scheme_end + 3 => {
            format!("{}://***@{}", &url[..scheme_end], &url[at + 1..])
        }
        _ => url.to_string(),
    }
}
