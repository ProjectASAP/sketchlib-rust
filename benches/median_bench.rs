use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rand::{Rng, SeedableRng, rngs::StdRng};

fn build_three() -> Vec<[f64; 3]> {
    let mut rng = StdRng::seed_from_u64(0x5eed_c0de_1234_5678);
    (0..1_000)
        .map(|_| {
            [
                rng.random::<f64>(),
                rng.random::<f64>(),
                rng.random::<f64>(),
            ]
        })
        .collect()
}

fn build_four() -> Vec<[f64; 4]> {
    let mut rng = StdRng::seed_from_u64(0x5eed_c0de_1234_5678);
    (0..1_000)
        .map(|_| {
            [
                rng.random::<f64>(),
                rng.random::<f64>(),
                rng.random::<f64>(),
                rng.random::<f64>(),
            ]
        })
        .collect()
}

fn build_five() -> Vec<[f64; 5]> {
    let mut rng = StdRng::seed_from_u64(0x5eed_c0de_1234_5678);
    (0..1_000)
        .map(|_| {
            [
                rng.random::<f64>(),
                rng.random::<f64>(),
                rng.random::<f64>(),
                rng.random::<f64>(),
                rng.random::<f64>(),
            ]
        })
        .collect()
}

fn median_of_three(values: &[f64; 3]) -> f64 {
    let (mut v0, mut v1, v2) = (values[0], values[1], values[2]);
    if v0 > v1 {
        let t = v0;
        v0 = v1;
        v1 = t;
    }
    if v1 > v2 {
        v1 = v2;
    }
    if v0 > v1 {
        v1 = v0;
    }
    v1
}

fn median_of_four(values: &[f64; 4]) -> f64 {
    let (mut v0, mut v1, mut v2, mut v3) = (values[0], values[1], values[2], values[3]);
    if v0 > v1 {
        let t = v0;
        v0 = v1;
        v1 = t;
    }
    if v2 > v3 {
        let t = v2;
        v2 = v3;
        v3 = t;
    }
    if v0 > v2 {
        v2 = v0;
    }
    if v1 > v3 {
        v1 = v3;
    }
    (v1 + v2) / 2.0
}

fn median_of_five(values: &[f64; 5]) -> f64 {
    let (mut v0, mut v1, mut v2, mut v3, mut v4) =
        (values[0], values[1], values[2], values[3], values[4]);
    if v0 > v1 {
        let t = v0;
        v0 = v1;
        v1 = t;
    }
    if v3 > v4 {
        let t = v3;
        v3 = v4;
        v4 = t;
    }
    if v0 > v3 {
        v3 = v0;
    }
    if v1 > v4 {
        v1 = v4;
    }
    if v1 > v2 {
        let t = v1;
        v1 = v2;
        v2 = t;
    }
    if v2 > v3 {
        v2 = v3;
    }
    if v1 > v2 {
        v2 = v1;
    }

    v2
}

fn median_three_sort(values: &mut [f64; 3]) -> f64 {
    values.sort_unstable_by(f64::total_cmp);
    let mid = values.len() / 2;
    if values.len() % 2 == 1 {
        values[mid]
    } else {
        (values[mid - 1] + values[mid]) / 2.0
    }
}

fn median_four_sort(values: &mut [f64; 4]) -> f64 {
    values.sort_unstable_by(f64::total_cmp);
    let mid = values.len() / 2;
    if values.len() % 2 == 1 {
        values[mid]
    } else {
        (values[mid - 1] + values[mid]) / 2.0
    }
}

fn median_five_sort(values: &mut [f64; 5]) -> f64 {
    values.sort_unstable_by(f64::total_cmp);
    let mid = values.len() / 2;
    if values.len() % 2 == 1 {
        values[mid]
    } else {
        (values[mid - 1] + values[mid]) / 2.0
    }
}

fn median_three(c: &mut Criterion) {
    let mut group = c.benchmark_group("median three");

    group.bench_function("median of 3 with if-else", |b| {
        b.iter_with_setup(
            || build_three(),
            |data| {
                for v in &data {
                    _ = median_of_three(v);
                }
                black_box(data);
            },
        );
    });

    group.bench_function("median of 3 with sort", |b| {
        b.iter_with_setup(
            || build_three(),
            |mut data| {
                for v in &mut data {
                    _ = median_three_sort(v);
                }
                black_box(data);
            },
        );
    });
}

fn median_four(c: &mut Criterion) {
    let mut group = c.benchmark_group("median four");

    group.bench_function("median of 4 with if-else", |b| {
        b.iter_with_setup(
            || build_four(),
            |data| {
                for v in &data {
                    _ = median_of_four(v);
                }
                black_box(data);
            },
        );
    });

    group.bench_function("median of 4 with sort", |b| {
        b.iter_with_setup(
            || build_four(),
            |mut data| {
                for v in &mut data {
                    _ = median_four_sort(v);
                }
                black_box(data);
            },
        );
    });
}

fn median_five(c: &mut Criterion) {
    let mut group = c.benchmark_group("median five");

    group.bench_function("median of 5 with if-else", |b| {
        b.iter_with_setup(
            || build_five(),
            |data| {
                for v in &data {
                    _ = median_of_five(v);
                }
                black_box(data);
            },
        );
    });

    group.bench_function("median of 5 with sort", |b| {
        b.iter_with_setup(
            || build_five(),
            |mut data| {
                for v in &mut data {
                    _ = median_five_sort(v);
                }
                black_box(data);
            },
        );
    });
}

criterion_group!(median_bench, median_three, median_four, median_five);
criterion_main!(median_bench);
