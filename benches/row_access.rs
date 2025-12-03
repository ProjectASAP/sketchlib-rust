use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rand::{Rng, SeedableRng, rngs::StdRng};
use sketchlib_rust::Nitro;

fn build_vals() -> Vec<u64> {
    let mut rng = StdRng::seed_from_u64(0x5eed_c0de_1234_5678);
    (0..1_000_000).map(|_| rng.random::<u64>()).collect()
}

fn init_data() -> Vec<u64> {
    vec![0; 5 * 2048]
}

fn bench_draw_geometric(c: &mut Criterion) {
    let mut nitro = Nitro::init_nitro(0.1);
    let mut group = c.benchmark_group("draw_geometric");
    group.bench_function("draw geometric", |b| {
        b.iter_with_setup(
            || {},
            |data| {
                for _ in 0..1_000_000 {
                    nitro.draw_geometric();
                }
                black_box(data);
            },
        )
    });
}

fn bench_row_update(c: &mut Criterion) {
    let value_to_insert = build_vals();
    let mut nitro = Nitro::init_nitro(0.1);
    let mut group = c.benchmark_group("row_update_style");
    let bit_mask = (1_usize << 11) - 1;
    let to_shift = 11_usize;

    // 1m * 5 insertions
    group.bench_function("skip nothing", |b| {
        b.iter_with_setup(init_data, |mut data| {
            for val in &value_to_insert {
                for r in 0..5 {
                    let idx = (*val >> (to_shift * r)) as usize & bit_mask;
                    data[2048 * r + idx] += 1;
                }
            }
            black_box(data);
        });
    });

    // 0.1 * 1m * 5 insertion + 0.1 * 1m random
    group.bench_function("skip packet", |b| {
        b.iter_with_setup(init_data, |mut data| {
            nitro.draw_geometric();
            for val in &value_to_insert {
                if nitro.to_skip > 0 {
                    nitro.reduce_to_skip();
                } else {
                    for r in 0..5 {
                        let idx = (*val >> (to_shift * r)) as usize & bit_mask;
                        data[2048 * r + idx] += 1;
                    }
                    nitro.draw_geometric();
                }
            }
            black_box(data);
        });
    });

    // 0.1 * 1m * 5 insertion + 0.1 * 5m random
    group.bench_function("skip rows", |b| {
        b.iter_with_setup(init_data, |mut data| {
            nitro.draw_geometric();
            for val in &value_to_insert {
                if nitro.to_skip >= 5 {
                    nitro.to_skip -= 5;
                    continue;
                } else {
                    loop {
                        let r = nitro.to_skip;
                        let idx = (*val >> (to_shift * r)) as usize & bit_mask;
                        data[2048 * r + idx] += 1;
                        nitro.draw_geometric();
                        if nitro.to_skip >= 5 {
                            break;
                        }
                    }
                }
            }
            black_box(data);
        });
    });
}

criterion_group!(row_update_bench, bench_row_update, bench_draw_geometric);
criterion_main!(row_update_bench);
