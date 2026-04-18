use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rust_decimal_macros::dec;
use rust_decimal::Decimal;
use uuid::Uuid;
use trading_core::fast_price::FastPrice;
use trading_core::types::TimeInForce;
use trading_engine::engine::{MatchingEngine, TopologySnapshot};
use trading_engine::types::{FastOrder, OrderMetadata};

struct BenchmarkTopology;
impl TopologySnapshot for BenchmarkTopology {
    fn can_accommodate_flow(&self, _f: Option<i32>, _t: Option<i32>, _a: Decimal) -> bool { true }
    fn calculate_wheeling_charge(&self, _f: Option<i32>, _t: Option<i32>) -> FastPrice { FastPrice::from(dec!(0.01)) }
    fn calculate_loss_factor(&self, _f: Option<i32>, _t: Option<i32>) -> FastPrice { FastPrice::from(dec!(1.02)) }
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut buys = Vec::new();
    let mut buy_meta = Vec::new();
    for i in 0..1000 {
        buys.push(FastOrder {
            id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            price: FastPrice::from(dec!(1.0)),
            energy_amount: dec!(100.0),
            filled_amount: dec!(0.0),
            zone_id: Some(1),
            created_at_ns: i as i64,
            expires_at_ns: None,
            time_in_force: TimeInForce::Gtc,
            metadata_index: i,
        });
        buy_meta.push(OrderMetadata { epoch_id: None, order_pda: None, session_token: None });
    }

    let mut sells = Vec::new();
    let mut sell_meta = Vec::new();
    for i in 0..1000 {
        sells.push(FastOrder {
            id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            price: FastPrice::from(dec!(0.9)),
            energy_amount: dec!(100.0),
            filled_amount: dec!(0.0),
            zone_id: Some(1),
            created_at_ns: i as i64,
            expires_at_ns: None,
            time_in_force: TimeInForce::Gtc,
            metadata_index: i,
        });
        sell_meta.push(OrderMetadata { epoch_id: None, order_pda: None, session_token: None });
    }

    let topo = BenchmarkTopology;
    let multiplier = FastPrice::from(dec!(1.0));

    c.bench_function("matching cycle 1000x1000", |b| {
        b.iter(|| {
            let mut buys_clone = buys.clone();
            let mut sells_clone = sells.clone();
            MatchingEngine::match_cycle(
                black_box(&mut buys_clone),
                black_box(&mut sells_clone),
                black_box(&buy_meta),
                black_box(&sell_meta),
                black_box(&topo),
                black_box(multiplier),
                black_box(2000),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
