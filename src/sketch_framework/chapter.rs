use crate::{SketchInput, Vector2D};
use serde::{Deserialize, Serialize};

use super::super::sketches::*;
use super::UnivMon;

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
