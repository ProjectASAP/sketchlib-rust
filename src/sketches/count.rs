use crate::{LASTSTATE, SketchInput, Vector2D, hash_it, hash_it_to_128};
use rmp_serde::{
    decode::Error as RmpDecodeError, encode::Error as RmpEncodeError, from_slice, to_vec_named,
};
use serde::{Deserialize, Serialize};

const DEFAULT_ROW_NUM: usize = 3;
const DEFAULT_COL_NUM: usize = 4096;
const LOWER_32_MASK: u64 = (1u64 << 32) - 1;

/// Count Sketch based on Common structure
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Count {
    counts: Vector2D<i64>,
    row: usize,
    col: usize,
}

impl Default for Count {
    fn default() -> Self {
        Self::with_dimensions(DEFAULT_ROW_NUM, DEFAULT_COL_NUM)
    }
}


impl Count {
    /// Creates a sketch with the requested number of rows and columns.
    pub fn with_dimensions(rows: usize, cols: usize) -> Self {
        let mut sk = Count {
            counts: Vector2D::init(rows, cols),
            row: rows,
            col: cols,
        };
        sk.counts.fill(0);
        sk
    }

    /// Number of rows in the sketch.
    pub fn rows(&self) -> usize {
        self.row
    }

    /// Number of columns in the sketch.
    pub fn cols(&self) -> usize {
        self.col
    }

    /// Inserts an observation while using the standard Count-Min minimum row update rule.
    pub fn insert(&mut self, value: &SketchInput) {
        for r in 0..self.row {
            // let hashed = hash_it(r, value);
            // let hashed = hash_for_enough_bits(r, value, 64) as u64;
            let hashed = hash_it_to_128(r, value);
            let col = ((hashed as u64 & LOWER_32_MASK) as usize) % self.col;
            let sign_bit = (hashed >> 127) & 1;
            if sign_bit > 0 {
                self.counts
                .update_one_counter(r, col, std::ops::Add::add, 1_i64);
            } else {
                self.counts.update_one_counter(r, col, std::ops::Sub::sub, 1_i64);
            }
        }
    }

    /// Inserts an observation using the combined hash optimization.
    pub fn fast_insert(&mut self, value: &SketchInput) {
        // let bits_required = self.counts.get_required_bits();
        // let hashed_val = hash_for_enough_bits(0, value, bits_required);
        let hashed_val = hash_it_to_128(0, value);
        // let hashed_val = hash_for_enough_bits(0, value, 128);
        self.counts
            .fast_insert(std::ops::Add::add, 1_i64, hashed_val);
    }

    /// Returns the frequency estimate for the provided value.
    pub fn estimate(&self, value: &SketchInput) -> f64 {
        let mut estimates = Vec::with_capacity(self.row);
        for r in 0..self.row {
            // let hashed = hash_it(r, value);
            // let hashed = hash_for_enough_bits(r, value, 64) as u64;
            let hashed = hash_it_to_128(r, value);
            let col = ((hashed as u64 & LOWER_32_MASK) as usize) % self.col;
            // let idx = row * cols + col;
            estimates.push(self.counts.query_one_counter(r, col));
        }
        if estimates.is_empty() {
            return 0.0;
        }
        estimates.sort_unstable();
        let mid = estimates.len() / 2;
        if estimates.len() % 2 == 1 {
            estimates[mid] as f64
        } else {
            (estimates[mid - 1] as f64 + estimates[mid] as f64) / 2.0
        }
    }

    /// Returns the frequency estimate for the provided value, with hash optimization.
    pub fn fast_estimate(&self, value: &SketchInput) -> i64 {
        // self.counts.fast_query(hash_it(0, value))
        // let bits_required = self.counts.get_required_bits();
        // let hashed_val = hash_for_enough_bits(0, value, bits_required);
        let hashed_val = hash_it_to_128(0, value);
        self.counts.fast_query(hashed_val)
    }

    /// Merges another sketch while asserting compatible dimensions.
    pub fn merge(&mut self, other: &Self) {
        assert_eq!(
            (self.row, self.col),
            (other.row, other.col),
            "dimension mismatch while merging CountMin sketches"
        );

        for i in 0..self.row {
            for j in 0..self.col {
                self.counts.update_one_counter(
                    i,
                    j,
                    std::ops::Add::add,
                    other.counts.query_one_counter(i, j),
                );
            }
        }
    }

    /// Exposes the backing matrix for inspection/testing.
    pub fn as_storage(&self) -> &Vector2D<i64> {
        &self.counts
    }

    /// Mutable access used internally for testing scenarios.
    pub fn as_storage_mut(&mut self) -> &mut Vector2D<i64> {
        &mut self.counts
    }

    /// Human-friendly helper used by the serializer demo binaries.
    pub fn debug(&self) {
        for row in 0..self.row {
            println!("row {}: {:?}", row, &self.counts.row_slice(row));
        }
    }

    /// Serializes the sketch into MessagePack bytes.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        to_vec_named(self)
    }

    /// Convenience alias matching the previous API.
    pub fn serialize(&self) -> Result<Vec<u8>, RmpEncodeError> {
        self.serialize_to_bytes()
    }

    /// Deserializes a sketch from MessagePack bytes.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        from_slice(bytes)
    }

    /// Convenience alias matching the previous API.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        Self::deserialize_from_bytes(bytes)
    }

}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CountUniv {
    pub row: usize,
    pub col: usize,
    pub matrix: Vec<Vec<i64>>,
    pub l2: Vec<i64>,
}

// impl Count {
//     pub fn debug(&self) -> () {
//         println!("Counters: ");
//         for i in 0..self.row {
//             println!("row {}: {:?}", i, self.matrix[i]);
//         }
//     }

//     pub fn init_count() -> Self {
//         Count::init_count_with_rc(4, 32)
//     }

//     pub fn init_count_with_rc(r: usize, c: usize) -> Self {
//         assert!(r <= 5, "Too many rows, not supported now");
//         let mat = vec![vec![0; c]; r];
//         Count {
//             row: r,
//             col: c,
//             matrix: mat,
//         }
//     }

//     pub fn merge(&mut self, other: &Count) {
//         assert!(self.row == other.row, "Row number different, cannot merge");
//         assert!(self.col == other.col, "Col number different, cannot merge");
//         for i in 0..self.row {
//             for j in 0..self.col {
//                 self.matrix[i][j] += other.matrix[i][j];
//             }
//         }
//     }

//     // pub fn insert_count<T: Hash>(&mut self, val: &T) {
//     //     for i in 0..self.row {
//     //         let h = hash_it(i, &val);
//     //         let s = hash_it(LASTSTATE, &val);
//     //         // just use lower 32 bit, whatever
//     //         let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
//     //         let sign = s % 2;
//     //         if sign == 1 { self.matrix[i][idx] += 1; } else { self.matrix[i][idx] -= 1; }
//     //     }
//     // }
//     pub fn insert_count(&mut self, val: &SketchInput) {
//         for i in 0..self.row {
//             let h = hash_it(i, &val);
//             let s = hash_it(LASTSTATE, &val);
//             // just use lower 32 bit, whatever
//             let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
//             let sign = s % 2;
//             if sign == 1 {
//                 self.matrix[i][idx] += 1;
//             } else {
//                 self.matrix[i][idx] -= 1;
//             }
//         }
//     }

//     // pub fn get_est<T: Hash>(&self, val: &T) -> f64 {
//     //     let mut lst = Vec::new();
//     //     for i in 0..self.row {
//     //         let h = hash_it(i, &val);
//     //         let s = hash_it(LASTSTATE, &val);
//     //         // just use lower 32 bit, whatever
//     //         let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
//     //         let sign = s % 2;
//     //         if sign == 1 { lst.push(self.matrix[i][idx]); } else { lst.push(self.matrix[i][idx] * (-1)); }
//     //     }
//     //     lst.sort();
//     //     // get median
//     //     if self.row == 1 {
//     //         return lst[0] as f64;
//     //     } else if self.row == 2 {
//     //         return (lst[0] + lst[1]) as f64 / 2.0;
//     //     } else if self.row == 3 {
//     //         return lst[1] as f64;
//     //     } else if self.row % 2 == 0 {
//     //         return (lst[self.row/2] + lst[(self.row/2) - 1]) as f64 / 2.0;
//     //     } else {
//     //         return lst[self.row / 2] as f64;
//     //     }
//     // }
//     pub fn get_est(&self, val: &SketchInput) -> f64 {
//         let mut lst = Vec::new();
//         for i in 0..self.row {
//             let h = hash_it(i, &val);
//             let s = hash_it(LASTSTATE, &val);
//             // just use lower 32 bit, whatever
//             let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
//             let sign = s % 2;
//             if sign == 1 {
//                 lst.push(self.matrix[i][idx]);
//             } else {
//                 lst.push(self.matrix[i][idx] * (-1));
//             }
//         }
//         lst.sort();
//         // get median
//         if self.row == 1 {
//             return lst[0] as f64;
//         } else if self.row == 2 {
//             return (lst[0] + lst[1]) as f64 / 2.0;
//         } else if self.row == 3 {
//             return lst[1] as f64;
//         } else if self.row % 2 == 0 {
//             return (lst[self.row / 2] + lst[(self.row / 2) - 1]) as f64 / 2.0;
//         } else {
//             return lst[self.row / 2] as f64;
//         }
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SketchInput;

    #[test]
    fn count_tracks_exact_frequency_for_single_key() {
        // verify median-of-sign strategy returns the true count for repeated updates
        let mut sketch = Count::with_dimensions(3, 64);
        let key = SketchInput::Str("alpha");

        for _ in 0..25 {
            sketch.insert(&key);
        }

        assert_eq!(sketch.estimate(&key), 25.0);
    }

    #[test]
    fn count_merge_accumulates_rows_elementwise() {
        // ensure merging combines counters so estimates add up
        let mut left = Count::with_dimensions(3, 32);
        let mut right = Count::with_dimensions(3, 32);
        let key = SketchInput::String("beta".to_string());

        for _ in 0..10 {
            left.insert(&key);
        }
        for _ in 0..7 {
            right.insert(&key);
        }

        left.merge(&right);
        assert_eq!(left.estimate(&key), 17.0);
    }

    #[test]
    fn countuniv_estimates_and_l2_are_consistent() {
        // confirm CountUniv updates produce expected estimate and l2 tracking
        let mut sketch = CountUniv::init_countuniv_with_rc(3, 32);
        let key = SketchInput::Str("gamma");

        let est_after_first = sketch.update_and_est(&key, 5);
        assert_eq!(est_after_first, 5.0);

        let est_after_second = sketch.update_and_est(&key, -2);
        assert_eq!(est_after_second, 3.0);

        let l2 = sketch.get_l2();
        assert!(l2 >= 3.0, "expected non-trivial l2, got {}", l2);
    }

    #[test]
    fn countuniv_merge_combines_frequency_vectors() {
        // validate merging two sketches keeps per-row counters additive
        let mut left = CountUniv::init_countuniv_with_rc(3, 32);
        let mut right = CountUniv::init_countuniv_with_rc(3, 32);
        let key = SketchInput::U32(42);

        left.insert_with_count(&key, 4);
        right.insert_with_count(&key, 9);

        left.merge(&right);
        assert_eq!(left.get_est(&key), 13.0);
    }
}

impl Default for CountUniv {
    fn default() -> Self {
        Self::init_count()
    }
}

impl CountUniv {
    pub fn debug(&self) -> () {
        println!("Counters: ");
        for i in 0..self.row {
            println!("row {}: {:?}", i, self.matrix[i]);
        }
        println!("L2: {:?}", self.l2);
    }

    pub fn init_count() -> Self {
        CountUniv::init_countuniv_with_rc(4, 32)
    }

    pub fn init_countuniv_with_rc(r: usize, c: usize) -> Self {
        assert!(r <= 5, "Too many rows, not supported now");
        let mat = vec![vec![0; c]; r];
        CountUniv {
            row: r,
            col: c,
            matrix: mat,
            l2: vec![0; r],
        }
    }

    pub fn merge(&mut self, other: &CountUniv) {
        assert!(self.row == other.row, "Row number different, cannot merge");
        assert!(self.col == other.col, "Col number different, cannot merge");
        for i in 0..self.row {
            for j in 0..self.col {
                self.matrix[i][j] += other.matrix[i][j];
            }
        }
    }

    // pub fn insert_once<T: Hash+?Sized>(&mut self, val: &T) {
    //     self.insert_with_count(val, 1);
    // }
    pub fn insert_once(&mut self, val: &SketchInput) {
        self.insert_with_count(val, 1);
    }

    // pub fn insert_with_count<T: Hash+?Sized>(&mut self, val: &T, c: i64) {
    //     for i in 0..self.row {
    //         let h = hash_it(i, &val);
    //         let s = hash_it(LASTSTATE, &val);
    //         // just use lower 32 bit, whatever
    //         let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
    //         let sign = s % 2;
    //         let old_value = self.matrix[i][idx];
    //         if sign == 1 {
    //             self.matrix[i][idx] += c;
    //         } else {
    //             self.matrix[i][idx] -= c;
    //         }
    //         self.l2[i] = self.l2[i] + self.matrix[i][idx]*self.matrix[i][idx] - old_value*old_value;
    //     }
    // }
    pub fn insert_with_count(&mut self, val: &SketchInput, c: i64) {
        for i in 0..self.row {
            let h = hash_it(i, &val);
            let s = hash_it(LASTSTATE, &val);
            // just use lower 32 bit, whatever
            let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
            let sign = s % 2;
            let old_value = self.matrix[i][idx];
            if sign == 1 {
                self.matrix[i][idx] += c;
            } else {
                self.matrix[i][idx] -= c;
            }
            self.l2[i] =
                self.l2[i] + self.matrix[i][idx] * self.matrix[i][idx] - old_value * old_value;
        }
    }

    // pub fn insert_with_count_without_l2<T: Hash+?Sized>(&mut self, val: &T, c: i64) {
    //     for i in 0..self.row {
    //         let h = hash_it(i, &val);
    //         let s = hash_it(LASTSTATE, &val);
    //         // just use lower 32 bit, whatever
    //         let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
    //         let sign = s % 2;
    //         if sign == 1 {
    //             self.matrix[i][idx] += c;
    //         } else {
    //             self.matrix[i][idx] -= c;
    //         }
    //     }
    // }
    pub fn insert_with_count_without_l2(&mut self, val: &SketchInput, c: i64) {
        for i in 0..self.row {
            let h = hash_it(i, &val);
            let s = hash_it(LASTSTATE, &val);
            // just use lower 32 bit, whatever
            let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
            let sign = s % 2;
            if sign == 1 {
                self.matrix[i][idx] += c;
            } else {
                self.matrix[i][idx] -= c;
            }
        }
    }

    // pub fn update_and_est<T: Hash+?Sized>(&mut self, val: &T, c: i64) -> f64 {
    //     self.insert_with_count(val, c);
    //     self.get_est(val)
    // }
    pub fn update_and_est(&mut self, val: &SketchInput, c: i64) -> f64 {
        self.insert_with_count(val, c);
        self.get_est(val)
    }

    // pub fn update_and_est_without_l2<T: Hash+?Sized>(&mut self, val: &T, c: i64) -> f64 {
    //     self.insert_with_count_without_l2(val, c);
    //     self.get_est(val)
    // }
    pub fn update_and_est_without_l2(&mut self, val: &SketchInput, c: i64) -> f64 {
        self.insert_with_count_without_l2(val, c);
        self.get_est(val)
    }

    pub fn get_l2_sqr(&self) -> f64 {
        let mut lst = Vec::new();
        for i in 0..self.row {
            lst.push(self.l2[i]);
        }
        lst.sort();
        // get median
        if self.row == 1 {
            return lst[0] as f64;
        } else if self.row == 2 {
            return (lst[0] + lst[1]) as f64 / 2.0;
        } else if self.row == 3 {
            return lst[1] as f64;
        } else if self.row % 2 == 0 {
            return (lst[self.row / 2] + lst[(self.row / 2) - 1]) as f64 / 2.0;
        } else {
            return lst[self.row / 2] as f64;
        }
    }

    pub fn get_l2(&self) -> f64 {
        // let mut lst = Vec::new();
        // for i in 0..self.row {
        //     lst.push(self.l2[i]);
        // }
        // lst.sort();
        // // get median
        // let l2;
        // if self.row == 1 {
        //     l2 = lst[0] as f64;
        // } else if self.row == 2 {
        //     l2 = (lst[0] + lst[1]) as f64 / 2.0;
        // } else if self.row == 3 {
        //     l2 = lst[1] as f64;
        // } else if self.row % 2 == 0 {
        //     l2 =  (lst[self.row/2] + lst[(self.row/2) - 1]) as f64 / 2.0;
        // } else {
        //     l2 = lst[self.row / 2] as f64;
        // }
        let l2 = self.get_l2_sqr();
        return l2.sqrt();
    }

    // pub fn get_est<T: Hash+?Sized>(&self, val: &T) -> f64 {
    //     let mut lst = Vec::new();
    //     for i in 0..self.row {
    //         let h = hash_it(i, &val);
    //         let s = hash_it(LASTSTATE, &val);
    //         // just use lower 32 bit, whatever
    //         let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
    //         let sign = s % 2;
    //         if sign == 1 { lst.push(self.matrix[i][idx]); } else { lst.push(self.matrix[i][idx] * (-1)); }
    //     }
    //     lst.sort();
    //     // get median
    //     if self.row == 1 {
    //         return lst[0] as f64;
    //     } else if self.row == 2 {
    //         return (lst[0] + lst[1]) as f64 / 2.0;
    //     } else if self.row == 3 {
    //         return lst[1] as f64;
    //     } else if self.row % 2 == 0 {
    //         return (lst[self.row/2] + lst[(self.row/2) - 1]) as f64 / 2.0;
    //     } else {
    //         return lst[self.row / 2] as f64;
    //     }
    // }
    pub fn get_est(&self, val: &SketchInput) -> f64 {
        let mut lst = Vec::new();
        for i in 0..self.row {
            let h = hash_it(i, &val);
            let s = hash_it(LASTSTATE, &val);
            // just use lower 32 bit, whatever
            let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
            let sign = s % 2;
            if sign == 1 {
                lst.push(self.matrix[i][idx]);
            } else {
                lst.push(self.matrix[i][idx] * (-1));
            }
        }
        lst.sort();
        // get median
        if self.row == 1 {
            return lst[0] as f64;
        } else if self.row == 2 {
            return (lst[0] + lst[1]) as f64 / 2.0;
        } else if self.row == 3 {
            return lst[1] as f64;
        } else if self.row % 2 == 0 {
            return (lst[self.row / 2] + lst[(self.row / 2) - 1]) as f64 / 2.0;
        } else {
            return lst[self.row / 2] as f64;
        }
    }
}
