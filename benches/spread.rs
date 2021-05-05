use std::num::NonZeroUsize;

use criterion::{criterion_group, criterion_main, Criterion};

use libjerasure::ErasureCoder;

const N: u32 = 100;
const F: u32 = N / 4;

fn spread_benchmark(c: &mut Criterion) {
    let encoder = ErasureCoder::new(
        NonZeroUsize::new((N - 2 * F) as usize).unwrap(),
        NonZeroUsize::new((2 * F) as usize).unwrap(),
    )
    .unwrap();
}

criterion_group!(benches, spread_benchmark);
criterion_main!(benches);
