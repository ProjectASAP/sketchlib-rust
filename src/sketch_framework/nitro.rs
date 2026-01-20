//! Nitro Sketch, with batch processing
//! Assume the Nitro Sketch can get a batch of input
//! For streaming Nitro, please refers to Nitro struct in structure_utils.rs

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng, rng};
use serde::{Deserialize, Serialize};

use crate::{PRECOMPUTED_SAMPLE_RATE_1PERCENT, SketchInput, Vector2D, hash_it_to_128};

pub trait NitroTarget {
    fn rows(&self) -> usize;
    fn update_row(&mut self, row: usize, hashed: u128, delta: u64);
}

impl NitroTarget for Vector2D<u32> {
    #[inline(always)]
    fn rows(&self) -> usize {
        self.rows()
    }

    #[inline(always)]
    fn update_row(&mut self, row: usize, hashed: u128, delta: u64) {
        self.update_by_row(row, hashed, |a, b| *a += b as u32, delta);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NitroBatch<S: NitroTarget> {
    sampling_rate: f64,
    pub to_skip: usize,
    inv_ln_one_minus_p: f64,
    pub delta: u64,
    #[serde(skip)]
    #[serde(default = "new_small_rng")]
    generator: SmallRng,
    idx: usize,
    mask: usize,
    sk: S,
}

fn new_small_rng() -> SmallRng {
    let mut seed_rng = rng();
    SmallRng::from_rng(&mut seed_rng)
}

impl Default for NitroBatch<Vector2D<u32>> {
    fn default() -> Self {
        let mut n = NitroBatch {
            sampling_rate: 0.0,
            to_skip: 0,
            inv_ln_one_minus_p: 0.0,
            delta: 0,
            generator: new_small_rng(),
            idx: 0,
            mask: 0x10000,
            sk: Vector2D::init(5, 2048),
        };
        n.sk.fill(0);
        n
    }
}

impl NitroBatch<Vector2D<u32>> {
    pub fn init_nitro(rate: f64) -> Self {
        assert!(
            !rate.is_nan() && rate > 0.0 && rate <= 1.0,
            "sample_rate must be within (0.0, 1.0]"
        );
        let inv_ln = if (rate - 1.0).abs() <= f64::EPSILON {
            0.0 // Not used for full sampling
        } else {
            1.0 / (1.0 - rate).ln()
        };
        let mut nitro = Self {
            sampling_rate: rate,
            to_skip: 0,
            inv_ln_one_minus_p: inv_ln,
            generator: new_small_rng(),
            delta: 0,
            idx: 0,
            mask: 0x10000,
            sk: Vector2D::init(5, 2048),
        };
        nitro.sk.fill(0);
        nitro.delta = nitro.scaled_increment(1);
        nitro
    }
}

impl<S: NitroTarget> NitroBatch<S> {
    pub fn target(&self) -> &S {
        &self.sk
    }

    pub fn target_mut(&mut self) -> &mut S {
        &mut self.sk
    }

    pub fn into_target(self) -> S {
        self.sk
    }

    pub fn with_target(rate: f64, sk: S) -> Self {
        assert!(
            !rate.is_nan() && rate > 0.0 && rate <= 1.0,
            "sample_rate must be within (0.0, 1.0]"
        );
        let inv_ln = if (rate - 1.0).abs() <= f64::EPSILON {
            0.0 // Not used for full sampling
        } else {
            1.0 / (1.0 - rate).ln()
        };
        let mut nitro = Self {
            sampling_rate: rate,
            to_skip: 0,
            inv_ln_one_minus_p: inv_ln,
            generator: new_small_rng(),
            delta: 0,
            idx: 0,
            mask: 0x10000,
            sk,
        };
        nitro.delta = nitro.scaled_increment(1);
        nitro
    }

    // for profiling
    #[inline(always)]
    pub fn draw_geometric(&mut self) {
        if self.is_full_sampling() {
            self.to_skip = 0;
            return;
        }
        let k = loop {
            let r = self.generator.random::<f64>();
            if r != 0.0_f64 && r != 1.0_f64 {
                break r;
            }
        };
        self.to_skip = ((1.0 - k).ln() * self.inv_ln_one_minus_p).ceil() as usize;
        self.idx = (self.idx + 1) & self.mask;
    }

    #[inline(always)]
    pub fn reduce_to_skip(&mut self) {
        self.to_skip -= 1;
    }

    #[inline(always)]
    pub fn reduce_to_skip_by_count(&mut self, c: usize) {
        self.to_skip -= c;
    }

    #[inline(always)]
    pub fn get_sampling_rate(&self) -> f64 {
        self.sampling_rate
    }

    // #[inline]
    #[inline(always)]
    pub fn scaled_increment(&self, weight: u64) -> u64 {
        if self.is_full_sampling() {
            weight
        } else {
            ((weight as f64) / self.sampling_rate).ceil() as u64
        }
    }

    // #[inline]
    #[inline(always)]
    fn is_full_sampling(&self) -> bool {
        (self.sampling_rate - 1.0).abs() <= f64::EPSILON
    }

    #[inline(always)]
    pub fn get_ctx(&self) -> (usize, f64, usize, usize) {
        (self.idx, self.inv_ln_one_minus_p, self.to_skip, self.mask)
    }

    #[inline(always)]
    pub fn commit_ctx(&mut self, idx: usize, to_skip: usize) {
        self.idx = idx;
        self.to_skip = to_skip;
    }

    pub fn insert(&mut self, data: &[i64]) {
        let rows = self.sk.rows();
        self.draw_geometric();
        let mut position = self.to_skip;
        while position < data.len() {
            let row_to_update = position % rows;
            let hashed = hash_it_to_128(0, &SketchInput::I64(data[position]));
            self.sk.update_row(row_to_update, hashed, self.delta);
            self.draw_geometric();
            position += self.to_skip + 1;
        }
    }

    pub fn insert_cached_step(&mut self, data: &[i64]) {
        let rows = self.sk.rows();
        self.to_skip = PRECOMPUTED_SAMPLE_RATE_1PERCENT[self.idx].ceil() as usize;
        self.idx = (self.idx + 1) & self.mask;
        let mut position = self.to_skip;
        while position < data.len() {
            let row_to_update = position % rows;
            let hashed = hash_it_to_128(0, &SketchInput::I64(data[position]));
            self.sk.update_row(row_to_update, hashed, self.delta);
            self.to_skip = PRECOMPUTED_SAMPLE_RATE_1PERCENT[self.idx].ceil() as usize;
            self.idx = (self.idx + 1) & self.mask;
            position += self.to_skip + 1;
        }
    }
}
