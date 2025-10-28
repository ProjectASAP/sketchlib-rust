use std::collections::BTreeMap;

//setting up a sketch structure
#[derive(Debug)]
struct DDSketch {
    alpha: f64,              
    gamma: f64,              
    log_gamma: f64,          
    store: BTreeMap<i32, u64>, 
    count: u64,              
    sum: f64,               
    min: f64,                
    max: f64,                
}

impl DDSketch {
    /// precomputing gamma and log_gamma.
    fn new(alpha: f64) -> Self {
        assert!((0.0..1.0).contains(&alpha), "alpha range should be (0,1)");
        let gamma = (1.0 + alpha) / (1.0 - alpha);
        let log_gamma = gamma.ln();
        Self {
            alpha,
            gamma,
            log_gamma,
            store: BTreeMap::new(),
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }

    /// Add a value to the sketch.
    fn add(&mut self, v: f64) {
        if !v.is_finite() { 
            return; 
        }
        self.count += 1;
        self.sum += v;
        if v < self.min { 
            self.min = v; 
        }
        if v > self.max {
             self.max = v; 
        }

        // compute which bucket this value belongs to
        let idx = self.key_for(v);
        *self.store.entry(idx).or_insert(0) += 1;
    }

    /// Estimate value at quantile (0.5 = p50, 0.9 = p90 and 0.99 = p99).
    fn get_value_at_quantile(&self, q: f64) -> Option<f64> {
        if self.count == 0 {
             return None;
             }
        if q <= 0.0 { 
            return Some(self.min);
         }
        if q >= 1.0 {
             return Some(self.max);
             }

        let rank = (q * self.count as f64).ceil() as u64;
        let mut seen = 0u64;

        // traverse bins in order and stop when cumulative >= rank
        for (bin, c) in self.store.iter() {
            seen += *c;
            if seen >= rank {
                return Some(self.value_for(*bin));
            }
        }
        Some(self.max)
    }

    /// Number of samples recorded.
    fn get_count(&self) -> u64 { self.count }

    //function to compute the bin index for a given value
    fn key_for(&self, v: f64) -> i32 {
        if v == 0.0 {
             return 0;
            }
        let sign = if v > 0.0 { 1 } else { -1 };
        let idx = (v.abs().ln() / self.log_gamma).floor() as i32;
        sign * idx.max(0)
    }
    //function to compute the value for a given bin index
    fn value_for(&self, k: i32) -> f64 {
        if k == 0 { return 0.0; }
        let sign = if k > 0 { 1.0 } else { -1.0 };
        let mag = k.abs() as f64;
        let upper = self.gamma.powf(mag + 1.0);
        sign * (upper / (1.0 + self.alpha))
    }
}

fn main() {
    // inputs for testing
    let data = vec![0.1, 0.2, 0.3, 0.9, 1.0, 1.1, 2.0, 2.1, 3.3, 5.0, 8.0, 13.0, 21.0, 34.0];

    let mut sk = DDSketch::new(0.01);  // 1% relative error entered
    for v in data {
        if v > 0.0 { 
            sk.add(v);
        }      
    }

    let p50 = sk.get_value_at_quantile(0.50).unwrap_or(f64::NAN);
    let p90 = sk.get_value_at_quantile(0.90).unwrap_or(f64::NAN);
    let p99 = sk.get_value_at_quantile(0.99).unwrap_or(f64::NAN);

    println!( "The values are: p50={:.6} p90={:.6} p99={:.6}",p50, p90, p99);
}
