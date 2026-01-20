use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rand::{Rng, SeedableRng, rngs::StdRng};
use sketchlib_rust::{Count, FastPath, FixedMatrix, SketchInput, Vector2D};

const SAMPLE_COUNT: usize = 1_000_000;
const RNG_SEED: u64 = 0x5eed_c0de_1234_5678;
const ROWS: usize = 5;
const COLS: usize = 2048;

fn build_keys() -> Vec<i64> {
    let mut rng = StdRng::seed_from_u64(RNG_SEED);
    (0..SAMPLE_COUNT).map(|_| rng.random::<i64>()).collect()
}

fn bench_count(c: &mut Criterion) {
    let keys = build_keys();
    let mut group = c.benchmark_group("count_storage_compare");

    group.bench_function("insert_1m_i64_fixedmatrix", |b| {
        b.iter_with_setup(Count::<FixedMatrix, FastPath>::default, |mut sketch| {
            for &key in &keys {
                let input = SketchInput::I64(key);
                sketch.insert(&input);
            }
            black_box(sketch);
        });
    });

    group.bench_function("insert_1m_i64_vector2d", |b| {
        b.iter_with_setup(
            || Count::<Vector2D<i32>, FastPath>::with_dimensions(ROWS, COLS),
            |mut sketch| {
                for &key in &keys {
                    let input = SketchInput::I64(key);
                    sketch.insert(&input);
                }
                black_box(sketch);
            },
        );
    });

    group.finish();
}

criterion_group!(count_benches, bench_count);
criterion_main!(count_benches);
