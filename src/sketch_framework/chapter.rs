use crate::{SketchInput, Vector2D};
use serde::{Deserialize, Serialize};

use super::super::sketches::*;
use super::UnivMon;
use super::eh::EHNorm;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Chapter {
    CM(CountMin<Vector2D<i32>, FastPath>),
    COCO(Coco),
    CU(CountL2HH),
    ELASTIC(Elastic),
    HLL(HyperLogLog<DataFusion>),
    KLL(KLL),
    UNIFORM(UniformSampling),
    // LOCHER(LocherSketch),
    UNIVMON(UnivMon),
}

/// this should be a temporary function
/// modify KLL to remove this function
pub fn iv_to_f64(i: &SketchInput) -> f64 {
    match i {
        SketchInput::I32(x) => *x as f64,
        SketchInput::I64(x) => *x as f64,
        SketchInput::U32(x) => *x as f64,
        SketchInput::U64(x) => *x as f64,
        SketchInput::F32(x) => *x as f64,
        SketchInput::F64(f) => *f,
        SketchInput::Str(_) => todo!(),
        SketchInput::String(_) => todo!(),
        SketchInput::Bytes(_) => todo!(),
        SketchInput::I8(_) => todo!(),
        SketchInput::I16(_) => todo!(),
        SketchInput::I128(_) => todo!(),
        SketchInput::ISIZE(_) => todo!(),
        SketchInput::U8(_) => todo!(),
        SketchInput::U16(_) => todo!(),
        SketchInput::U128(_) => todo!(),
        SketchInput::USIZE(_) => todo!(),
    }
}

impl Chapter {
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

    pub fn eh_weight_for_norm(&self, norm: EHNorm, input: &SketchInput) -> Result<f64, String> {
        if !self.supports_norm(norm) {
            return Err(format!(
                "{} does not support {:?} mode",
                self.sketch_type(),
                norm
            ));
        }
        let numeric = Self::numeric_input_to_f64(input);
        match norm {
            EHNorm::L1 => Ok(numeric.map(|v| v.abs()).unwrap_or(1.0)),
            EHNorm::L2 => {
                let value = numeric.ok_or_else(|| {
                    format!(
                        "L2 EH mode requires numeric input, got {:?} for {}",
                        input,
                        self.sketch_type()
                    )
                })?;
                Ok(value * value)
            }
        }
    }

    pub fn supports_norm(&self, norm: EHNorm) -> bool {
        match self {
            Chapter::CU(_) | Chapter::UNIVMON(_) => norm == EHNorm::L2,
            Chapter::CM(_)
            | Chapter::COCO(_)
            | Chapter::ELASTIC(_)
            | Chapter::HLL(_)
            | Chapter::KLL(_)
            | Chapter::UNIFORM(_) => norm == EHNorm::L1,
        }
    }

    pub fn insert_weighted(
        &mut self,
        val: &SketchInput,
        delta: i64,
        norm: EHNorm,
    ) -> Result<(), String> {
        if delta <= 0 {
            return Err(format!("weighted insert requires delta > 0, got {delta}"));
        }
        if !self.supports_norm(norm) {
            return Err(format!(
                "{} does not support {:?} mode",
                self.sketch_type(),
                norm
            ));
        }

        match self {
            Chapter::CM(sketch) => {
                for _ in 0..delta {
                    sketch.insert(val);
                }
                Ok(())
            }
            Chapter::COCO(sketch) => match val {
                SketchInput::Str(s) => {
                    sketch.insert(s, delta as u64);
                    Ok(())
                }
                SketchInput::String(s) => {
                    sketch.insert(s.as_str(), delta as u64);
                    Ok(())
                }
                _ => Err("Coco weighted insert requires string input".to_string()),
            },
            Chapter::CU(sketch) => {
                sketch.fast_insert_with_count(val, delta);
                Ok(())
            }
            Chapter::ELASTIC(sketch) => {
                let id = match val {
                    SketchInput::String(s) => s.clone(),
                    SketchInput::I32(i) => i.to_string(),
                    SketchInput::I64(i) => i.to_string(),
                    SketchInput::U32(u) => u.to_string(),
                    SketchInput::U64(u) => u.to_string(),
                    SketchInput::F32(f) => f.to_string(),
                    SketchInput::F64(f) => f.to_string(),
                    SketchInput::Str(s) => s.to_string(),
                    SketchInput::Bytes(items) => String::from_utf8_lossy(items).to_string(),
                    _ => {
                        return Err(
                            "Elastic weighted insert does not support this input".to_string()
                        );
                    }
                };
                for _ in 0..delta {
                    sketch.insert(id.clone());
                }
                Ok(())
            }
            Chapter::HLL(_) => Err("HLL does not support weighted inserts in EH mode".to_string()),
            Chapter::KLL(sketch) => {
                if norm == EHNorm::L2 {
                    return Err("KLL only supports EHNorm::L1 in EH mode".to_string());
                }
                for _ in 0..delta {
                    sketch
                        .update(val)
                        .map_err(|e| format!("KLL weighted insert failed: {e}"))?;
                }
                Ok(())
            }
            Chapter::UNIFORM(_) => {
                Err("UniformSampling does not support weighted inserts in EH mode".to_string())
            }
            Chapter::UNIVMON(sketch) => {
                sketch.insert(val, delta);
                Ok(())
            }
        }
    }

    /// Insert a value into the sketch
    pub fn insert(&mut self, val: &SketchInput) {
        match self {
            Chapter::CM(sketch) => sketch.insert(val),
            Chapter::COCO(sketch) => match val {
                SketchInput::Str(s) => sketch.insert(s, 1),
                SketchInput::String(s) => sketch.insert(s.as_str(), 1),
                _ => {}
            },
            Chapter::CU(sketch) => sketch.fast_insert_with_count(val, 1),
            Chapter::ELASTIC(sketch) => match val {
                SketchInput::String(s) => sketch.insert(s.to_string()),
                SketchInput::I32(i) => sketch.insert(i.to_string()),
                SketchInput::I64(i) => sketch.insert(i.to_string()),
                SketchInput::U32(u) => sketch.insert(u.to_string()),
                SketchInput::U64(u) => sketch.insert(u.to_string()),
                SketchInput::F32(f) => sketch.insert(f.to_string()),
                SketchInput::F64(f) => sketch.insert(f.to_string()),
                SketchInput::Str(s) => sketch.insert(s.to_string()),
                SketchInput::Bytes(items) => {
                    let s = String::from_utf8_lossy(items).to_string();
                    sketch.insert(s)
                }
                SketchInput::I8(_) => todo!(),
                SketchInput::I16(_) => todo!(),
                SketchInput::I128(_) => todo!(),
                SketchInput::ISIZE(_) => todo!(),
                SketchInput::U8(_) => todo!(),
                SketchInput::U16(_) => todo!(),
                SketchInput::U128(_) => todo!(),
                SketchInput::USIZE(_) => todo!(),
            },
            Chapter::HLL(sketch) => sketch.insert(val),
            Chapter::KLL(sketch) => {
                let _ = sketch.update(val);
            }
            Chapter::UNIFORM(sketch) => {
                let _ = sketch.update_input(val);
            }
            Chapter::UNIVMON(sketch) => sketch.insert(val, 1),
            // Chapter::LOCHER(sketch) => {
            //     // Locher requires a String
            //     if let SketchInput::String(s) = val {
            //         sketch.insert(s, 1);
            //     }
            // }
        }
    }

    /// Merge another sketch of the same type into this one
    pub fn merge(&mut self, other: &Chapter) -> Result<(), &'static str> {
        match (self, other) {
            (Chapter::CM(s), Chapter::CM(o)) => {
                s.merge(o);
                Ok(())
            }
            (Chapter::COCO(s), Chapter::COCO(o)) => {
                s.merge(o);
                Ok(())
            }
            (Chapter::CU(s), Chapter::CU(o)) => {
                s.merge(o);
                Ok(())
            }
            // (Bucket::ELASTIC(s), Bucket::ELASTIC(o)) => {
            //     s.merge(o);
            //     Ok(())
            // }, // not yet
            (Chapter::HLL(s), Chapter::HLL(o)) => {
                s.merge(o);
                Ok(())
            }
            (Chapter::KLL(s), Chapter::KLL(o)) => {
                s.merge(o);
                Ok(())
            }
            (Chapter::UNIFORM(s), Chapter::UNIFORM(o)) => s.merge(o),
            (Chapter::UNIVMON(s), Chapter::UNIVMON(o)) => {
                s.merge(o);
                Ok(())
            }
            // (Bucket::LOCHER(s), Bucket::LOCHER(o)) => {
            //     s.merge(o);
            //     Ok(())
            // }, // not yet
            _ => Err("Cannot merge sketches of different types"),
        }
    }

    pub fn query(&self, key: &SketchInput) -> Result<f64, &'static str> {
        match (self, key) {
            (Chapter::CM(count_min), _) => Ok(count_min.estimate(key) as f64),
            (Chapter::COCO(coco), SketchInput::Str(s)) => Ok(coco.clone().estimate(s) as f64),
            (Chapter::COCO(coco), SketchInput::String(s)) => {
                Ok(coco.clone().estimate(s.as_str()) as f64)
            }
            (Chapter::CU(count_univ), _) => Ok(count_univ.fast_get_est(key)),
            (Chapter::ELASTIC(elastic), SketchInput::String(s)) => {
                Ok(elastic.clone().query(s.clone()) as f64)
            }
            (Chapter::HLL(hll_df_modified), _) => Ok(hll_df_modified.estimate() as f64),
            (Chapter::KLL(kll), SketchInput::I32(i)) => Ok(kll.quantile(*i as f64)),
            (Chapter::KLL(kll), SketchInput::I64(i)) => Ok(kll.quantile(*i as f64)),
            (Chapter::KLL(kll), SketchInput::U32(u)) => Ok(kll.quantile(*u as f64)),
            (Chapter::KLL(kll), SketchInput::U64(u)) => Ok(kll.quantile(*u as f64)),
            (Chapter::KLL(kll), SketchInput::F32(f)) => Ok(kll.quantile(*f as f64)),
            (Chapter::KLL(kll), SketchInput::F64(f)) => Ok(kll.quantile(*f)),
            (Chapter::UNIFORM(sampler), SketchInput::U64(idx)) => sampler
                .sample_at(*idx as usize)
                .ok_or("Sample index out of bounds"),
            (Chapter::UNIFORM(sampler), SketchInput::U32(idx)) => sampler
                .sample_at(*idx as usize)
                .ok_or("Sample index out of bounds"),
            (Chapter::UNIFORM(sampler), SketchInput::I64(idx)) if *idx >= 0 => sampler
                .sample_at(*idx as usize)
                .ok_or("Sample index out of bounds"),
            (Chapter::UNIFORM(sampler), SketchInput::I32(idx)) if *idx >= 0 => sampler
                .sample_at(*idx as usize)
                .ok_or("Sample index out of bounds"),
            (Chapter::UNIFORM(sampler), SketchInput::Str(cmd)) => match *cmd {
                "len" => Ok(sampler.len() as f64),
                "total_seen" => Ok(sampler.total_seen() as f64),
                _ => Err("Unsupported command for UniformSampling"),
            },
            (Chapter::UNIFORM(sampler), SketchInput::String(cmd)) => match cmd.as_str() {
                "len" => Ok(sampler.len() as f64),
                "total_seen" => Ok(sampler.total_seen() as f64),
                _ => Err("Unsupported command for UniformSampling"),
            },
            (Chapter::UNIVMON(um), SketchInput::Str(cmd)) => match *cmd {
                "cardinality" | "card" => Ok(um.calc_card()),
                "l1" => Ok(um.calc_l1()),
                "l2" => Ok(um.calc_l2()),
                "entropy" => Ok(um.calc_entropy()),
                _ => Err("Unsupported command for UnivMon"),
            },
            (Chapter::UNIVMON(um), SketchInput::String(cmd)) => match cmd.as_str() {
                "cardinality" | "card" => Ok(um.calc_card()),
                "l1" => Ok(um.calc_l1()),
                "l2" => Ok(um.calc_l2()),
                "entropy" => Ok(um.calc_entropy()),
                _ => Err("Unsupported command for UnivMon"),
            },
            // (Chapter::LOCHER(locher_sketch), SketchInput::Str(s)) => Ok(locher_sketch.estimate(*s)),
            _ => Err("Parameter type and Sketch Type Mismatched"),
        }
    }

    /// Get the type of sketch as a string
    pub fn sketch_type(&self) -> &'static str {
        match self {
            Chapter::CM(_) => "CountMin",
            Chapter::COCO(_) => "Coco",
            Chapter::CU(_) => "CountUniv",
            Chapter::ELASTIC(_) => "Elastic",
            Chapter::HLL(_) => "HLL",
            Chapter::KLL(_) => "KLL",
            Chapter::UNIFORM(_) => "UniformSampling",
            Chapter::UNIVMON(_) => "UnivMon",
            // Chapter::LOCHER(_) => "Locher",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_weighted_routes_delta_to_univmon_and_cu() {
        let key = SketchInput::I64(7);

        let mut cu = Chapter::CU(CountL2HH::with_dimensions(5, 1024));
        cu.insert_weighted(&key, 9, EHNorm::L2)
            .expect("weighted CU insert");
        let cu_est = cu.query(&key).expect("query CU");
        assert!(cu_est >= 9.0, "expected CU estimate >= 9, got {cu_est}");

        let mut um = Chapter::UNIVMON(UnivMon::default());
        um.insert_weighted(&key, 6, EHNorm::L2)
            .expect("weighted UnivMon insert");
        match um {
            Chapter::UNIVMON(ref u) => assert_eq!(u.bucket_size, 6),
            _ => panic!("expected UnivMon chapter variant"),
        }
    }

    #[test]
    fn insert_weighted_rejects_unsupported_variants() {
        let mut hll = Chapter::HLL(crate::HyperLogLog::<crate::DataFusion>::default());
        let err = hll.insert_weighted(&SketchInput::I64(1), 3, EHNorm::L1);
        assert!(err.is_err(), "expected weighted HLL insert to fail");
    }

    #[test]
    fn eh_weight_for_norm_kll_l2_returns_err() {
        let kll = Chapter::KLL(KLL::default());
        let err = kll.eh_weight_for_norm(EHNorm::L2, &SketchInput::F64(1.5));
        assert!(err.is_err(), "expected KLL L2 weight to fail");
    }

    #[test]
    fn supports_norm_whitelist_is_enforced() {
        let cm = Chapter::CM(CountMin::<Vector2D<i32>, FastPath>::default());
        assert!(cm.supports_norm(EHNorm::L1));
        assert!(!cm.supports_norm(EHNorm::L2));

        let cu = Chapter::CU(CountL2HH::with_dimensions(5, 1024));
        assert!(cu.supports_norm(EHNorm::L2));
        assert!(!cu.supports_norm(EHNorm::L1));

        let um = Chapter::UNIVMON(UnivMon::default());
        assert!(um.supports_norm(EHNorm::L2));
        assert!(!um.supports_norm(EHNorm::L1));
    }
}
