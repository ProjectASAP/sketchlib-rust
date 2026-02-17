use super::Chapter;
use crate::{KLL, SketchInput};

const SIZE_EPSILON: f64 = 1e-9;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EHNorm {
    L1,
    L2,
}

#[derive(Clone, Debug)]
pub struct EHIntervalResult<T> {
    pub merged: T,
    pub oldest_bucket_fraction: f64,
    pub oldest_bucket_index: usize,
}

pub trait EhSketch: Clone {
    fn insert(&mut self, val: &SketchInput);
    fn insert_weighted(
        &mut self,
        val: &SketchInput,
        delta: i64,
        norm: EHNorm,
    ) -> Result<(), String>;
    fn merge(&mut self, other: &Self) -> Result<(), String>;
    fn weight_for_norm(&self, norm: EHNorm, input: &SketchInput) -> Result<f64, String>;
    fn supports_norm(&self, norm: EHNorm) -> bool;
}

impl EhSketch for Chapter {
    fn insert(&mut self, val: &SketchInput) {
        self.insert(val);
    }

    fn insert_weighted(
        &mut self,
        val: &SketchInput,
        delta: i64,
        norm: EHNorm,
    ) -> Result<(), String> {
        self.insert_weighted(val, delta, norm)
    }

    fn merge(&mut self, other: &Self) -> Result<(), String> {
        Chapter::merge(self, other).map_err(|err| err.to_string())
    }

    fn weight_for_norm(&self, norm: EHNorm, input: &SketchInput) -> Result<f64, String> {
        self.eh_weight_for_norm(norm, input)
    }

    fn supports_norm(&self, norm: EHNorm) -> bool {
        self.supports_norm(norm)
    }
}

impl EhSketch for KLL {
    fn insert(&mut self, val: &SketchInput) {
        let _ = self.update(val);
    }

    fn insert_weighted(
        &mut self,
        val: &SketchInput,
        delta: i64,
        norm: EHNorm,
    ) -> Result<(), String> {
        if delta <= 0 {
            return Err(format!("weighted insert requires delta > 0, got {delta}"));
        }
        if norm == EHNorm::L2 {
            return Err("KLL only supports EHNorm::L1 in EH mode".to_string());
        }
        for _ in 0..delta {
            self.update(val)
                .map_err(|e| format!("KLL weighted insert failed: {e}"))?;
        }
        Ok(())
    }

    fn merge(&mut self, other: &Self) -> Result<(), String> {
        KLL::merge(self, other);
        Ok(())
    }

    fn weight_for_norm(&self, norm: EHNorm, input: &SketchInput) -> Result<f64, String> {
        if norm == EHNorm::L2 {
            return Err("KLL only supports EHNorm::L1 in EH mode".to_string());
        }

        fn numeric_input_to_f64(input: &SketchInput) -> Option<f64> {
            match input {
                SketchInput::I8(v) => Some(*v as f64),
                SketchInput::I16(v) => Some(*v as f64),
                SketchInput::I32(v) => Some(*v as f64),
                SketchInput::I64(v) => Some(*v as f64),
                SketchInput::I128(v) => Some(*v as f64),
                SketchInput::ISIZE(v) => Some(*v as f64),
                SketchInput::U8(v) => Some(*v as f64),
                SketchInput::U16(v) => Some(*v as f64),
                SketchInput::U32(v) => Some(*v as f64),
                SketchInput::U64(v) => Some(*v as f64),
                SketchInput::U128(v) => Some(*v as f64),
                SketchInput::USIZE(v) => Some(*v as f64),
                SketchInput::F32(v) => Some(*v as f64),
                SketchInput::F64(v) => Some(*v),
                SketchInput::Str(_) | SketchInput::String(_) | SketchInput::Bytes(_) => None,
            }
        }

        let numeric = numeric_input_to_f64(input);
        Ok(numeric.map(|v| v.abs()).unwrap_or(1.0))
    }

    fn supports_norm(&self, norm: EHNorm) -> bool {
        norm == EHNorm::L1
    }
}

#[derive(Clone, Debug)]
pub struct EHVolume<T: EhSketch> {
    pub volume: T,
    pub size: f64,
    pub min_time: u64,
    pub max_time: u64,
}

#[derive(Clone, Debug)]
pub struct ExponentialHistogram<T: EhSketch> {
    pub payload: Vec<EHVolume<T>>,
    pub window: u64,
    pub k: usize,
    pub epsilon: f64,
    pub norm: EHNorm,
    pub max_buckets_per_size: usize,
    pub type_to_clone: T,
}

fn normalize_weight(raw: f64) -> Result<f64, String> {
    if !raw.is_finite() {
        return Err("EH weight must be finite".to_string());
    }
    if raw <= 0.0 {
        return Err(format!("EH weight must be positive, got {raw}"));
    }
    Ok(raw)
}

fn size_eq(lhs: f64, rhs: f64) -> bool {
    (lhs - rhs).abs() <= SIZE_EPSILON
}

fn effective_k_for(norm: EHNorm, epsilon: f64) -> Result<usize, String> {
    if !epsilon.is_finite() || epsilon <= 0.0 {
        return Err(format!("epsilon must be finite and > 0, got {epsilon}"));
    }

    let k_float = match norm {
        EHNorm::L1 => (2.0 / epsilon).ceil(),
        EHNorm::L2 => (2.0 / (epsilon * epsilon)).ceil(),
    };

    Ok((k_float as usize).max(1))
}

fn max_buckets_for_k(k: usize) -> usize {
    (k / 2) + 1
}

fn weight_to_delta(weight: f64) -> i64 {
    let rounded = weight.round();
    if rounded < 1.0 {
        1
    } else if rounded > i64::MAX as f64 {
        i64::MAX
    } else {
        rounded as i64
    }
}

impl<T: EhSketch> EHVolume<T> {
    pub fn to_merge(&mut self, other: EHVolume<T>) -> Result<(), String> {
        self.volume.merge(&other.volume)?;
        self.size += other.size;
        self.max_time = self.max_time.max(other.max_time);
        self.min_time = self.min_time.min(other.min_time);
        Ok(())
    }
}

impl<T: EhSketch> ExponentialHistogram<T> {
    pub fn new(k: usize, window: u64, eh_type: T) -> Self {
        let k_eff = k.max(1);
        let epsilon = 2.0 / (k_eff as f64);
        let max_buckets_per_size = max_buckets_for_k(k_eff);
        ExponentialHistogram {
            payload: Vec::new(),
            window,
            k: k_eff,
            epsilon,
            norm: EHNorm::L1,
            max_buckets_per_size,
            type_to_clone: eh_type,
        }
    }

    pub fn new_with_epsilon(
        window: u64,
        epsilon: f64,
        norm: EHNorm,
        eh_type: T,
    ) -> Result<Self, String> {
        if !eh_type.supports_norm(norm) {
            return Err(format!("sketch does not support {:?} mode", norm));
        }

        let k_eff = effective_k_for(norm, epsilon)?;
        Ok(ExponentialHistogram {
            payload: Vec::new(),
            window,
            k: k_eff,
            epsilon,
            norm,
            max_buckets_per_size: max_buckets_for_k(k_eff),
            type_to_clone: eh_type,
        })
    }

    pub fn update_window(&mut self, window: u64) {
        self.window = window;
    }

    pub fn update(&mut self, time: u64, val: &SketchInput) -> Result<(), String> {
        self.update_with_weighted(time, val, |sketch, delta, norm| {
            sketch.insert_weighted(val, delta, norm)
        })
    }

    pub fn update_with_weighted<F>(
        &mut self,
        time: u64,
        val: &SketchInput,
        update_fn: F,
    ) -> Result<(), String>
    where
        F: FnOnce(&mut T, i64, EHNorm) -> Result<(), String>,
    {
        let expired_count = self
            .payload
            .iter()
            .take_while(|b| b.max_time < time.saturating_sub(self.window))
            .count();

        if expired_count > 0 {
            self.payload.drain(0..expired_count);
        }

        let mut sketch = self.type_to_clone.clone();
        let raw_weight = normalize_weight(sketch.weight_for_norm(self.norm, val)?)?;
        let delta = weight_to_delta(raw_weight);

        update_fn(&mut sketch, delta, self.norm)?;

        let new_eh_vol = EHVolume {
            volume: sketch,
            size: raw_weight,
            min_time: time,
            max_time: time,
        };
        self.payload.push(new_eh_vol);

        self.merge_volumes()
    }

    pub fn update_with<F>(
        &mut self,
        time: u64,
        val: &SketchInput,
        update_fn: F,
    ) -> Result<(), String>
    where
        F: FnOnce(&mut T),
    {
        self.update_with_weighted(time, val, |sketch, _delta, _norm| {
            update_fn(sketch);
            Ok(())
        })
    }

    fn merge_volumes(&mut self) -> Result<(), String> {
        while let Some(index) = self.find_oldest_merge_candidate() {
            self.merge_at_index(index)?;
        }
        Ok(())
    }

    fn find_oldest_merge_candidate(&self) -> Option<usize> {
        if self.payload.len() < 2 {
            return None;
        }

        let mut start = 0;
        while start < self.payload.len() {
            let size = self.payload[start].size;
            let mut end = start + 1;
            while end < self.payload.len() && size_eq(self.payload[end].size, size) {
                end += 1;
            }

            let run_len = end - start;
            if run_len > self.max_buckets_per_size {
                return Some(start);
            }

            start = end;
        }

        None
    }

    fn merge_at_index(&mut self, index: usize) -> Result<(), String> {
        if index + 1 >= self.payload.len() {
            return Ok(());
        }
        let vol_to_merge = self.payload[index + 1].clone();
        self.payload[index].to_merge(vol_to_merge)?;
        self.payload.remove(index + 1);
        Ok(())
    }

    pub fn cover(&self, mint: u64, maxt: u64) -> bool {
        if self.payload.is_empty() {
            return false;
        }

        let first = &self.payload[0];
        let last = &self.payload[self.payload.len() - 1];

        last.max_time >= maxt && first.min_time <= mint
    }

    pub fn get_max_time(&self) -> Option<u64> {
        self.payload.last().map(|b| b.max_time)
    }

    pub fn get_min_time(&self) -> Option<u64> {
        self.payload.first().map(|b| b.min_time)
    }

    pub fn volume_count(&self) -> usize {
        self.payload.len()
    }

    fn locate_interval_bounds(&self, t1: u64, t2: u64) -> Option<(usize, usize)> {
        if self.payload.is_empty() {
            return None;
        }

        let mut from_volume = 0;
        let mut to_volume = 0;

        for (i, vol) in self.payload.iter().enumerate() {
            if t1 >= vol.min_time && t1 <= vol.max_time {
                from_volume = i;
            }
            if t2 >= vol.min_time && t2 <= vol.max_time {
                to_volume = i;
            }
        }

        if t2 > self.payload[self.payload.len() - 1].max_time {
            to_volume = self.payload.len() - 1;
        }
        if t1 < self.payload[0].min_time {
            from_volume = 0;
        }
        if to_volume >= self.payload.len() {
            to_volume = self.payload.len() - 1;
        }

        Some((from_volume, to_volume))
    }

    pub fn query_interval_merge_with_boundary(
        &self,
        t1: u64,
        t2: u64,
    ) -> Option<EHIntervalResult<T>> {
        let (from_volume, to_volume) = self.locate_interval_bounds(t1, t2)?;

        let mut oldest_bucket_fraction = 1.0;
        let boundary_bucket = &self.payload[from_volume];
        if t1 > boundary_bucket.min_time && t1 <= boundary_bucket.max_time {
            oldest_bucket_fraction = 0.5;
        }

        let merged = if from_volume < to_volume {
            let mut merged = self.payload[from_volume].volume.clone();
            for i in (from_volume + 1)..=to_volume {
                if merged.merge(&self.payload[i].volume).is_err() {
                    return None;
                }
            }
            merged
        } else {
            self.payload[from_volume].volume.clone()
        };

        Some(EHIntervalResult {
            merged,
            oldest_bucket_fraction,
            oldest_bucket_index: from_volume,
        })
    }

    pub fn query_interval_merge(&self, t1: u64, t2: u64) -> Option<T> {
        self.query_interval_merge_with_boundary(t1, t2)
            .map(|result| result.merged)
    }

    /// Debug print bucket information
    pub fn print_buckets(&self) {
        println!("Bucket count: {}", self.payload.len());
        println!("k: {}", self.k);
        println!("epsilon: {}", self.epsilon);
        println!("norm: {:?}", self.norm);
        for (i, bucket) in self.payload.iter().enumerate() {
            println!(
                "{}: min_time={}, max_time={}, size={}",
                i, bucket.min_time, bucket.max_time, bucket.size
            );
        }
    }

    /// Get memory statistics
    pub fn get_memory_info(&self) -> (usize, Vec<f64>) {
        let count = self.payload.len();
        let sizes = self.payload.iter().map(|b| b.size).collect();
        (count, sizes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_exponential_histogram() {
        let eh = ExponentialHistogram::new(
            2,
            1000,
            Chapter::HLL(crate::HyperLogLog::<crate::DataFusion>::default()),
        );
        assert_eq!(eh.k, 2);
        assert_eq!(eh.window, 1000);
        assert_eq!(eh.volume_count(), 0);
        assert_eq!(eh.norm, EHNorm::L1);
    }

    #[test]
    fn test_basic_insertion() {
        let mut eh = ExponentialHistogram::new(
            2,
            1000,
            Chapter::CM(crate::CountMin::<crate::Vector2D<i32>, crate::FastPath>::default()),
        );

        assert!(eh.update(100, &SketchInput::I64(1)).is_ok());
        assert_eq!(eh.volume_count(), 1);
        assert_eq!(eh.get_min_time(), Some(100));
        assert_eq!(eh.get_max_time(), Some(100));
    }

    #[test]
    fn test_window_expiration() {
        let mut eh = ExponentialHistogram::new(
            2,
            100,
            Chapter::CM(crate::CountMin::<crate::Vector2D<i32>, crate::FastPath>::default()),
        );

        for i in 0..5 {
            assert!(eh.update(i * 30, &SketchInput::U64(i + 1)).is_ok());
        }

        assert!(eh.update(200, &SketchInput::U64(200)).is_ok());
        assert!(eh.volume_count() < 6);
        assert!(eh.get_min_time().unwrap() >= 90);
    }

    #[test]
    fn test_cover_functionality() {
        let mut eh = ExponentialHistogram::new(
            2,
            1000,
            Chapter::CM(crate::CountMin::<crate::Vector2D<i32>, crate::FastPath>::default()),
        );

        for i in 1..=3 {
            assert!(eh.update(i * 100, &SketchInput::U64(i)).is_ok());
        }

        assert!(eh.cover(100, 300));
        assert!(eh.cover(150, 250));
        assert!(!eh.cover(50, 250));
        assert!(!eh.cover(150, 400));
    }

    #[test]
    fn test_update_window() {
        let mut eh = ExponentialHistogram::new(
            2,
            1000,
            Chapter::CM(crate::CountMin::<crate::Vector2D<i32>, crate::FastPath>::default()),
        );
        assert_eq!(eh.window, 1000);

        eh.update_window(2000);
        assert_eq!(eh.window, 2000);
    }

    #[test]
    fn test_query_interval_merge() {
        let mut eh = ExponentialHistogram::new(
            2,
            10000,
            Chapter::CM(crate::CountMin::<crate::Vector2D<i32>, crate::FastPath>::default()),
        );

        for i in 0..5 {
            assert!(eh.update(i * 100, &SketchInput::U64(i + 1)).is_ok());
        }

        assert!(eh.query_interval_merge(100, 300).is_some());
        assert!(eh.query_interval_merge(0, 50).is_some());
    }

    #[test]
    fn test_empty_histogram() {
        let eh = ExponentialHistogram::new(
            2,
            1000,
            Chapter::CM(crate::CountMin::<crate::Vector2D<i32>, crate::FastPath>::default()),
        );

        assert_eq!(eh.volume_count(), 0);
        assert_eq!(eh.get_min_time(), None);
        assert_eq!(eh.get_max_time(), None);
        assert!(!eh.cover(0, 100));
        assert!(eh.query_interval_merge(0, 100).is_none());
    }

    #[test]
    fn test_volume_merging() {
        let mut eh = ExponentialHistogram::new(
            2,
            100,
            Chapter::CM(crate::CountMin::<crate::Vector2D<i32>, crate::FastPath>::default()),
        );

        for i in 0..10 {
            assert!(eh.update(i * 10, &SketchInput::I64(1)).is_ok());
        }

        assert!(eh.volume_count() < 10);
    }

    #[test]
    fn raw_weight_no_power_of_two_rounding() {
        let mut eh = ExponentialHistogram::new_with_epsilon(
            1000,
            0.5,
            EHNorm::L1,
            Chapter::CM(crate::CountMin::<crate::Vector2D<i32>, crate::FastPath>::default()),
        )
        .unwrap();

        assert!(eh.update(1, &SketchInput::F64(1.3)).is_ok());
        assert!((eh.payload[0].size - 1.3).abs() < 1e-9);
    }

    #[test]
    fn epsilon_drives_threshold_l1_vs_l2() {
        let l1 = ExponentialHistogram::new_with_epsilon(
            1000,
            0.5,
            EHNorm::L1,
            Chapter::CM(crate::CountMin::<crate::Vector2D<i32>, crate::FastPath>::default()),
        )
        .unwrap();
        let l2 = ExponentialHistogram::new_with_epsilon(
            1000,
            0.5,
            EHNorm::L2,
            Chapter::CU(crate::CountL2HH::with_dimensions(5, 2048)),
        )
        .unwrap();

        assert!(l2.k > l1.k);
        assert!(l2.max_buckets_per_size > l1.max_buckets_per_size);
    }

    #[test]
    fn kll_rejects_l2_mode() {
        let err = ExponentialHistogram::new_with_epsilon(1000, 0.5, EHNorm::L2, KLL::default());
        assert!(err.is_err(), "KLL should reject L2 EH mode");
    }

    #[test]
    fn query_interval_merge_backcompat_still_works() {
        let mut eh = ExponentialHistogram::new(
            2,
            1000,
            Chapter::CM(crate::CountMin::<crate::Vector2D<i32>, crate::FastPath>::default()),
        );

        for i in 0..3 {
            eh.update(i * 10, &SketchInput::I64(1)).unwrap();
        }

        let legacy = eh.query_interval_merge(0, 20);
        assert!(legacy.is_some());
    }

    #[test]
    fn interval_query_returns_boundary_fraction_half_when_partial_overlap() {
        let mut eh = ExponentialHistogram::new(
            2,
            1000,
            Chapter::CM(crate::CountMin::<crate::Vector2D<i32>, crate::FastPath>::default()),
        );

        eh.update(100, &SketchInput::I64(1)).unwrap();
        eh.update(200, &SketchInput::I64(1)).unwrap();
        eh.update(300, &SketchInput::I64(1)).unwrap();

        let result = eh
            .query_interval_merge_with_boundary(150, 300)
            .expect("boundary merge");
        assert_eq!(result.oldest_bucket_fraction, 0.5);
    }

    #[test]
    fn float_size_class_comparison_uses_epsilon() {
        let mut eh = ExponentialHistogram::new(
            2,
            1000,
            Chapter::CM(crate::CountMin::<crate::Vector2D<i32>, crate::FastPath>::default()),
        );

        eh.payload.push(EHVolume {
            volume: Chapter::CM(
                crate::CountMin::<crate::Vector2D<i32>, crate::FastPath>::default(),
            ),
            size: 1.0,
            min_time: 1,
            max_time: 1,
        });
        eh.payload.push(EHVolume {
            volume: Chapter::CM(
                crate::CountMin::<crate::Vector2D<i32>, crate::FastPath>::default(),
            ),
            size: 1.0 + 1e-10,
            min_time: 2,
            max_time: 2,
        });
        eh.payload.push(EHVolume {
            volume: Chapter::CM(
                crate::CountMin::<crate::Vector2D<i32>, crate::FastPath>::default(),
            ),
            size: 1.0 + 2e-10,
            min_time: 3,
            max_time: 3,
        });

        eh.merge_volumes().unwrap();
        assert!(eh.volume_count() <= 2);
    }

    #[test]
    fn oldest_first_merge_preserves_newer_resolution() {
        let mut eh = ExponentialHistogram::new(
            2,
            1000,
            Chapter::CM(crate::CountMin::<crate::Vector2D<i32>, crate::FastPath>::default()),
        );

        eh.update(10, &SketchInput::I64(1)).unwrap();
        eh.update(20, &SketchInput::I64(1)).unwrap();
        eh.update(30, &SketchInput::I64(1)).unwrap();

        // with k=2 -> max buckets per size=2, oldest two size-1 buckets should merge first
        assert_eq!(eh.payload[0].min_time, 10);
        assert_eq!(eh.payload[0].max_time, 20);
        assert_eq!(eh.payload[1].min_time, 30);
        assert_eq!(eh.payload[1].max_time, 30);
    }

    #[test]
    fn inner_outer_consistency_univmon_weighted_insert() {
        let mut eh = ExponentialHistogram::new_with_epsilon(
            1000,
            0.5,
            EHNorm::L2,
            Chapter::UNIVMON(crate::UnivMon::default()),
        )
        .unwrap();

        eh.update(1, &SketchInput::I64(3)).unwrap(); // weight 9 -> delta 9

        let merged = eh.query_interval_merge(1, 1).expect("merged sketch");
        match merged {
            Chapter::UNIVMON(um) => assert_eq!(um.bucket_size, 9),
            _ => panic!("expected UnivMon variant"),
        }
    }

    #[test]
    fn inner_outer_consistency_countl2hh_weighted_insert() {
        let mut eh = ExponentialHistogram::new_with_epsilon(
            1000,
            0.5,
            EHNorm::L2,
            Chapter::CU(crate::CountL2HH::with_dimensions(5, 2048)),
        )
        .unwrap();

        eh.update(1, &SketchInput::I64(3)).unwrap(); // weight 9 -> delta 9

        let merged = eh.query_interval_merge(1, 1).expect("merged sketch");
        let estimate = merged.query(&SketchInput::I64(3)).expect("count query");
        assert!(
            estimate >= 9.0,
            "expected weighted estimate >= 9, got {estimate}"
        );
    }

    #[derive(Clone, Debug)]
    struct FailingSketch;

    impl EhSketch for FailingSketch {
        fn insert(&mut self, _val: &SketchInput) {}

        fn insert_weighted(
            &mut self,
            _val: &SketchInput,
            _delta: i64,
            _norm: EHNorm,
        ) -> Result<(), String> {
            Ok(())
        }

        fn merge(&mut self, _other: &Self) -> Result<(), String> {
            Err("forced merge failure".to_string())
        }

        fn weight_for_norm(&self, _norm: EHNorm, _input: &SketchInput) -> Result<f64, String> {
            Ok(1.0)
        }

        fn supports_norm(&self, _norm: EHNorm) -> bool {
            true
        }
    }

    #[test]
    fn merge_error_not_silenced() {
        let mut eh = ExponentialHistogram::new(1, 100, FailingSketch);
        assert!(eh.update(1, &SketchInput::I64(1)).is_ok());
        let second = eh.update(2, &SketchInput::I64(1));
        assert!(second.is_err());
    }
}
