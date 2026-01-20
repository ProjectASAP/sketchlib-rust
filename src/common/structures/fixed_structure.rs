//! A fixed integer matrix.
//! Size fixed at compile time and heap-backed via Box.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::ops::{Index, IndexMut, Range};

use crate::{MatrixStorage, compute_median_inline_f64};

pub const QUICKSTART_ROW_NUM: usize = 5;
pub const QUICKSTART_COL_NUM: usize = 2048;
pub const QUICKSTART_SIZE: usize = QUICKSTART_ROW_NUM * QUICKSTART_COL_NUM;
const QUICKSTART_MASK_BITS: usize = mask_bits(QUICKSTART_COL_NUM);
const QUICKSTART_MASK: u64 = (1u64 << QUICKSTART_MASK_BITS) - 1;

/// The greater is P, the smaller the error.
const HLL_P: usize = 14_usize;
const NUM_REGISTERS: usize = 1_usize << HLL_P;

const fn mask_bits(cols: usize) -> usize {
    let mut bits = 0;
    let mut value = 1;
    while value < cols {
        value <<= 1;
        bits += 1;
    }
    bits
}

#[derive(Clone, Debug)]
pub struct HllBucketList {
    pub registers: Box<[u8; NUM_REGISTERS]>,
}

impl Default for HllBucketList {
    fn default() -> Self {
        Self {
            registers: Box::new([0_u8; NUM_REGISTERS]),
        }
    }
}

impl Serialize for HllBucketList {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serde_big_array::BigArray::serialize(&*self.registers, serializer)
    }
}

impl<'de> Deserialize<'de> for HllBucketList {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data: [u8; NUM_REGISTERS] = serde_big_array::BigArray::deserialize(deserializer)?;
        Ok(Self {
            registers: Box::new(data),
        })
    }
}

impl Index<usize> for HllBucketList {
    type Output = u8;

    fn index(&self, index: usize) -> &Self::Output {
        debug_assert!(index < NUM_REGISTERS, "index out of bounds");
        &self.registers[index]
    }
}

impl IndexMut<usize> for HllBucketList {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        debug_assert!(index < NUM_REGISTERS, "index out of bounds");
        &mut self.registers[index]
    }
}

impl Index<Range<usize>> for HllBucketList {
    type Output = [u8];

    fn index(&self, range: Range<usize>) -> &Self::Output {
        debug_assert!(range.end <= NUM_REGISTERS, "range end out of bounds");
        &self.registers[range]
    }
}

impl IndexMut<Range<usize>> for HllBucketList {
    fn index_mut(&mut self, range: Range<usize>) -> &mut Self::Output {
        debug_assert!(range.end <= NUM_REGISTERS, "range end out of bounds");
        &mut self.registers[range]
    }
}

impl<'a> IntoIterator for &'a HllBucketList {
    type Item = &'a u8;
    type IntoIter = std::slice::Iter<'a, u8>;

    fn into_iter(self) -> Self::IntoIter {
        self.registers.iter()
    }
}

impl HllBucketList {
    pub fn len(&self) -> usize {
        NUM_REGISTERS as usize
    }
}

#[derive(Clone, Debug)]
pub struct FixedMatrix {
    pub data: Box<[i32; QUICKSTART_SIZE]>,
}

impl Default for FixedMatrix {
    fn default() -> Self {
        Self {
            data: Box::new([0_i32; QUICKSTART_SIZE]),
        }
    }
}

impl Serialize for FixedMatrix {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serde_big_array::BigArray::serialize(&*self.data, serializer)
    }
}

impl<'de> Deserialize<'de> for FixedMatrix {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data: [i32; QUICKSTART_SIZE] = serde_big_array::BigArray::deserialize(deserializer)?;
        Ok(Self {
            data: Box::new(data),
        })
    }
}

impl MatrixStorage<i32> for FixedMatrix {
    type HashValue = u64;
    #[inline(always)]
    fn rows(&self) -> usize {
        QUICKSTART_ROW_NUM
    }

    #[inline(always)]
    fn cols(&self) -> usize {
        QUICKSTART_COL_NUM
    }

    #[inline(always)]
    fn update_one_counter<F, V>(&mut self, row: usize, col: usize, op: F, value: V)
    where
        F: Fn(&mut i32, V),
    {
        let idx = row * QUICKSTART_COL_NUM + col;
        op(&mut self.data[idx], value);
    }

    #[inline(always)]
    fn increment_by_row(&mut self, row: usize, col: usize, value: i32) {
        let idx = row * QUICKSTART_COL_NUM + col;
        self.data[idx] += value;
    }

    #[inline(always)]
    fn fast_insert<F, V>(&mut self, op: F, value: V, hashed_val: u64)
    where
        F: Fn(&mut i32, &V, usize),
        V: Clone,
    {
        for row in 0..QUICKSTART_ROW_NUM {
            let hashed = (hashed_val >> (QUICKSTART_MASK_BITS * row)) & QUICKSTART_MASK;
            let col = (hashed as usize) % QUICKSTART_COL_NUM;
            let idx = row * QUICKSTART_COL_NUM + col;
            op(&mut self.data[idx], &value, row);
        }
    }

    #[inline(always)]
    fn fast_query_min<F, R>(&self, hashed_val: u64, op: F) -> R
    where
        F: Fn(&i32, usize, u64) -> R,
        R: Ord,
    {
        let hashed = hashed_val & QUICKSTART_MASK;
        let col = (hashed as usize) % QUICKSTART_COL_NUM;
        let mut min = op(&self.data[col], 0, hashed_val);
        for row in 1..QUICKSTART_ROW_NUM {
            let hashed = (hashed_val >> (QUICKSTART_MASK_BITS * row)) & QUICKSTART_MASK;
            let col = (hashed as usize) % QUICKSTART_COL_NUM;
            let idx = row * QUICKSTART_COL_NUM + col;
            let candidate = op(&self.data[idx], row, hashed_val);
            if candidate < min {
                min = candidate;
            }
        }
        min
    }

    #[inline(always)]
    fn fast_query_median<F>(&self, hashed_val: u64, op: F) -> f64
    where
        F: Fn(&i32, usize, u64) -> f64,
    {
        let mut estimates = Vec::with_capacity(QUICKSTART_ROW_NUM);
        for row in 0..QUICKSTART_ROW_NUM {
            let hashed = (hashed_val >> (QUICKSTART_MASK_BITS * row)) & QUICKSTART_MASK;
            let col = (hashed as usize) % QUICKSTART_COL_NUM;
            let idx = row * QUICKSTART_COL_NUM + col;
            estimates.push(op(&self.data[idx], row, hashed_val));
        }
        compute_median_inline_f64(&mut estimates)
    }

    #[inline(always)]
    fn query_one_counter(&self, row: usize, col: usize) -> i32 {
        self.data[row * QUICKSTART_COL_NUM + col]
    }
}
