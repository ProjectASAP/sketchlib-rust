use crate::{Count, CountMin, HllDf, LASTSTATE, SketchInput, Vector1D, hash_it_to_128, input::AnySketch};

pub struct HashLayer {
    sketches: Vector1D<AnySketch>,
}

impl Default for HashLayer {
    fn default() -> Self {
        Self::new(vec![
            AnySketch::CountMin(CountMin::default()), 
            AnySketch::Count(Count::default()), 
            AnySketch::HllDf(HllDf::default())])
    }
}

impl HashLayer {
    pub fn new(lst: Vec<AnySketch>) -> Self {
        HashLayer { sketches: Vector1D::from_vec(lst),
         }
    }

    pub fn insert(&mut self, val: &SketchInput) {
        let hashed_val = hash_it_to_128(LASTSTATE, val);
        for i in 0..self.sketches.len() {
            self.sketches[i].insert_with_hash(hashed_val);
        }
    }
}