use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rand::{Rng, SeedableRng, rngs::StdRng};
use sketchlib_rust::{CountMin, NitroBatch, RegularPath, SketchInput, Vector2D};

fn build_vals() -> Vec<i64> {
    let mut rng = StdRng::seed_from_u64(0x5eed_c0de_1234_5678);
    (0..1_000_000).map(|_| rng.random::<i64>()).collect()
}

fn bench_nitro_batch(c: &mut Criterion) {
    // let (keys_u64, keys_i64) = build_data();
    let keys_i64 = build_vals();

    {
        let mut group = c.benchmark_group("nitro_batch");

        group.bench_function("nitro_batch_rate_100", |b| {
            b.iter_with_setup(
                || {
                    let mut sk = Vector2D::init(5, 2048);
                    sk.fill(0_u32);
                    NitroBatch::with_target(1.0, sk)
                },
                |mut nitro| {
                    nitro.insert(&keys_i64);
                    black_box(nitro);
                },
            );
        });

        group.bench_function("nitro_batch_rate_1", |b| {
            b.iter_with_setup(
                || {
                    let mut sk = Vector2D::init(5, 2048);
                    sk.fill(0_u32);
                    NitroBatch::with_target(0.01, sk)
                },
                |mut nitro| {
                    nitro.insert(&keys_i64);
                    black_box(nitro);
                },
            );
        });

        group.bench_function("nitro_batch_cached_sampling", |b| {
            b.iter_with_setup(
                || {
                    let mut sk = Vector2D::init(5, 2048);
                    sk.fill(0_u32);
                    NitroBatch::with_target(0.01, sk)
                },
                |mut nitro| {
                    nitro.insert_cached_step(&keys_i64);
                    black_box(nitro);
                },
            );
        });

        group.bench_function("countmin_insert", |b| {
            b.iter_with_setup(
                || CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(5, 2048),
                |mut sketch| {
                    for key in &keys_i64 {
                        let input = SketchInput::I64(*key);
                        sketch.insert(&input);
                    }
                    black_box(sketch);
                },
            );
        });

        group.finish();
    }

    {
        let mut group_overhead = c.benchmark_group("nitro_batch_ctor_overhead");

        group_overhead.bench_function("init_nitro_only", |b| {
            b.iter(|| {
                let nitro = NitroBatch::init_nitro(1.0);
                black_box(nitro);
            });
        });

        group_overhead.bench_function("with_target_only", |b| {
            b.iter(|| {
                let mut sk = Vector2D::init(5, 2048);
                sk.fill(0_u32);
                let nitro = NitroBatch::with_target(1.0, sk);
                black_box(nitro);
            });
        });

        group_overhead.finish();
    }
}

criterion_group!(nitro_benches, bench_nitro_batch);
criterion_main!(nitro_benches);
