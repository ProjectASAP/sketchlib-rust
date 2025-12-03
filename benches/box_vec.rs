use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rand::{Rng, SeedableRng, rngs::StdRng};

const ROWS: usize = 100_000;
const COLS: usize = 16;
const ITERATIONS: usize = 10;

fn build_vals() -> Vec<u64> {
    let mut rng = StdRng::seed_from_u64(0x5eed_c0de_1234_5678);
    (0..1_000_000).map(|_| rng.random::<u64>()).collect()
}

fn bench_box_vec(c: &mut Criterion) {
    let value_to_insert = build_vals();
    let mut group = c.benchmark_group("one box insertion");

    group.bench_function("one box insertion", |b| {
        b.iter_with_setup(
            || vec![0; 16384].into_boxed_slice(),
            |mut data| {
                for (idx, val) in value_to_insert.iter().enumerate() {
                    data[idx % 16384] += val;
                }
                black_box(data);
            },
        );
    });

    group.bench_function("one vec insertion", |b| {
        b.iter_with_setup(
            || vec![0; 16384],
            |mut data| {
                for (idx, val) in value_to_insert.iter().enumerate() {
                    data[idx % 16384] += val;
                }
                black_box(data);
            },
        );
    });
}

fn bench_memory_layouts(c: &mut Criterion) {
    let mut group = c.benchmark_group("matrix_operations");

    group.bench_function("1. Vec<Vec>", |b| {
        b.iter_with_setup(
            || {
                (0..ROWS)
                    .map(|i| vec![i as u32; COLS])
                    .collect::<Vec<Vec<u32>>>()
            },
            |mut matrix| {
                for _ in 0..ITERATIONS {
                    for row in matrix.iter_mut() {
                        for cell in row.iter_mut() {
                            *cell = cell.wrapping_add(1);
                        }
                    }
                }
                black_box(matrix);
            },
        );
    });

    group.bench_function("2. Vec<Box<[T]>>", |b| {
        b.iter_with_setup(
            || {
                (0..ROWS)
                    .map(|i| vec![i as u32; COLS].into_boxed_slice())
                    .collect::<Vec<Box<[u32]>>>()
            },
            |mut matrix| {
                for _ in 0..ITERATIONS {
                    for row in matrix.iter_mut() {
                        for cell in row.iter_mut() {
                            *cell = cell.wrapping_add(1);
                        }
                    }
                }
                black_box(matrix);
            },
        );
    });

    group.bench_function("3. Flattened Vec", |b| {
        b.iter_with_setup(
            || vec![0u32; ROWS * COLS],
            |mut matrix| {
                for _ in 0..ITERATIONS {
                    for cell in matrix.iter_mut() {
                        *cell = cell.wrapping_add(1);
                    }
                }
                black_box(matrix);
            },
        );
    });
}

criterion_group!(box_vec_bench, bench_box_vec, bench_memory_layouts);
criterion_main!(box_vec_bench);
