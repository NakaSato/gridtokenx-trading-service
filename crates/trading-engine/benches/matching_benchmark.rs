// Bench indices are small loop counters; wrap/truncation on `as` casts is
// impossible here and would only add noise to the benchmark code.
#![allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use trading_core::fast_price::FastPrice;
use trading_core::types::TimeInForce;
use trading_engine::engine::{MatchingEngine, TopologySnapshot};
use trading_engine::types::{FastOrder, OrderMetadata};
use uuid::Uuid;

struct BenchmarkTopology;
impl TopologySnapshot for BenchmarkTopology {
    fn can_accommodate_flow(&self, _f: Option<i32>, _t: Option<i32>, _a: Decimal) -> bool {
        true
    }
    fn calculate_wheeling_charge(&self, _f: Option<i32>, _t: Option<i32>) -> FastPrice {
        FastPrice::from(dec!(0.01))
    }
    fn calculate_loss_factor(&self, _f: Option<i32>, _t: Option<i32>) -> FastPrice {
        FastPrice::from(dec!(1.02))
    }
}

/// Build one order-book side of `n` orders. `price_fn`/`zone_fn` are indexed by
/// position so a scenario can shape price depth and zone spread independently.
fn make_side(
    n: usize,
    price_fn: impl Fn(usize) -> Decimal,
    zone_fn: impl Fn(usize) -> Option<i32>,
) -> (Vec<FastOrder>, Vec<OrderMetadata>) {
    let mut orders = Vec::with_capacity(n);
    let mut meta = Vec::with_capacity(n);
    for i in 0..n {
        orders.push(FastOrder {
            id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            price: FastPrice::from(price_fn(i)),
            energy_amount: dec!(100.0),
            filled_amount: dec!(0.0),
            zone_id: zone_fn(i),
            created_at_ns: i as i64,
            expires_at_ns: None,
            time_in_force: TimeInForce::Gtc,
            metadata_index: i,
        });
        meta.push(OrderMetadata {
            epoch_id: None,
            order_pda: None,
            session_token: None,
        });
    }
    (orders, meta)
}

/// Time one `match_cycle` over a pre-built book. Orders are cloned each
/// iteration because the engine mutates fill state; only the match is timed.
fn bench_scenario(
    c: &mut Criterion,
    name: &str,
    buys: &[FastOrder],
    sells: &[FastOrder],
    buy_meta: &[OrderMetadata],
    sell_meta: &[OrderMetadata],
) {
    let topo = BenchmarkTopology;
    let multiplier = FastPrice::from(dec!(1.0));
    let max_matches = (buys.len().max(sells.len()) * 2) as i64;
    c.bench_function(name, |b| {
        b.iter(|| {
            let mut buys_clone = buys.to_vec();
            let mut sells_clone = sells.to_vec();
            MatchingEngine::match_cycle(
                black_box(&mut buys_clone),
                black_box(&mut sells_clone),
                black_box(buy_meta),
                black_box(sell_meta),
                black_box(&topo),
                black_box(multiplier),
                black_box(max_matches),
            )
        });
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    let one_zone = |_: usize| Some(1);

    // Full-cross at several sizes: every buy (1.00) crosses every sell (0.90),
    // so the matcher does maximal fill work. Measures scaling of the hot path.
    for n in [100usize, 1000, 5000] {
        let (buys, buy_meta) = make_side(n, |_| dec!(1.0), one_zone);
        let (sells, sell_meta) = make_side(n, |_| dec!(0.9), one_zone);
        bench_scenario(
            c,
            &format!("matching cycle {n}x{n}"),
            &buys,
            &sells,
            &buy_meta,
            &sell_meta,
        );
    }

    // No-cross: all buys (0.90) sit below all sells (1.10) → zero matches.
    // Isolates order-book construction + early no-cross exit from fill work.
    {
        let (buys, buy_meta) = make_side(1000, |_| dec!(0.9), one_zone);
        let (sells, sell_meta) = make_side(1000, |_| dec!(1.1), one_zone);
        bench_scenario(
            c,
            "matching cycle 1000x1000 no-cross",
            &buys,
            &sells,
            &buy_meta,
            &sell_meta,
        );
    }

    // Price-diverse depth: buys span 1.00..=1.49, sells span 0.51..=1.00.
    // Partial overlap exercises price-time priority walking through book depth
    // rather than a single flat price level.
    {
        let (buys, buy_meta) = make_side(1000, |i| Decimal::new(100 + (i % 50) as i64, 2), one_zone);
        let (sells, sell_meta) =
            make_side(1000, |i| Decimal::new(51 + (i % 50) as i64, 2), one_zone);
        bench_scenario(
            c,
            "matching cycle 1000x1000 price-diverse",
            &buys,
            &sells,
            &buy_meta,
            &sell_meta,
        );
    }

    // Multi-zone: full-cross prices but orders spread across 8 zones, stressing
    // the zone-segmented order-book map + per-zone wheeling/loss application.
    {
        let zone8 = |i: usize| Some((i % 8) as i32);
        let (buys, buy_meta) = make_side(1000, |_| dec!(1.0), zone8);
        let (sells, sell_meta) = make_side(1000, |_| dec!(0.9), zone8);
        bench_scenario(
            c,
            "matching cycle 1000x1000 multi-zone(8)",
            &buys,
            &sells,
            &buy_meta,
            &sell_meta,
        );
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
