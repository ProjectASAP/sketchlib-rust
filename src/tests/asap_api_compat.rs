//! Tests for asap-internal API compatibility methods
//! 
//! These tests verify that the convenience methods added for asap-internal
//! integration work correctly.

use crate::{CountMin, Vector2D, KeyByLabelValues, KLL, Hydra};
use crate::input::HydraCounter;
use crate::{HyperLogLog, DataFusion, SketchInput};

#[test]
fn test_countmin_key_by_label_values() {
    use crate::RegularPath;
    let mut cms = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(3, 4096);
    
    // Create a key
    let key = KeyByLabelValues::new_with_labels(vec![
        "service=api".to_string(),
        "endpoint=/users".to_string(),
    ]);
    
    // Update with key (should insert the key string)
    cms.update_key(&key, 1.0);
    cms.update_key(&key, 1.0);
    cms.update_key(&key, 1.0);
    
    // Query with key
    let freq = cms.query_key(&key);
    
    // Should have frequency of 3
    assert!(freq >= 3.0, "Expected frequency >= 3.0, got {}", freq);
}

#[test]
fn test_countmin_insert_value() {
    use crate::RegularPath;
    let mut cms = CountMin::<Vector2D<i32>, RegularPath>::default();
    
    // Insert f64 values directly
    cms.insert_value(42.5);
    cms.insert_value(42.5);
    
    // Query with SketchInput
    let freq = cms.estimate(&SketchInput::F64(42.5));
    assert_eq!(freq, 2);
}

#[test]
fn test_countmin_json_serialization() {
    use crate::RegularPath;
    let mut cms = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(3, 128);
    
    cms.insert(&SketchInput::U64(1));
    cms.insert(&SketchInput::U64(2));
    cms.insert(&SketchInput::U64(1));
    
    // Serialize to JSON
    let json = cms.serialize_to_json().unwrap();
    
    // Deserialize from JSON
    let restored = CountMin::<Vector2D<i32>, RegularPath>::deserialize_from_json(&json).unwrap();
    
    // Verify estimates match
    assert_eq!(cms.estimate(&SketchInput::U64(1)), restored.estimate(&SketchInput::U64(1)));
    assert_eq!(cms.estimate(&SketchInput::U64(2)), restored.estimate(&SketchInput::U64(2)));
}

#[test]
fn test_kll_update_value() {
    let mut kll = KLL::init_kll(200);
    
    // Insert values directly as f64
    for i in 0..100 {
        kll.update_value(i as f64).unwrap();
    }
    
    // Query quantile
    let median = kll.get_quantile(0.5);
    
    // Median should be around 49-50
    assert!(median >= 45.0 && median <= 55.0, "Median {} out of expected range", median);
}

#[test]
fn test_kll_merge_multiple() {
    let mut kll1 = KLL::init_kll(200);
    let mut kll2 = KLL::init_kll(200);
    let mut kll3 = KLL::init_kll(200);
    
    // Insert different ranges into each
    for i in 0..100 {
        kll1.update(&SketchInput::F64(i as f64)).unwrap();
        kll2.update(&SketchInput::F64((i + 100) as f64)).unwrap();
        kll3.update(&SketchInput::F64((i + 200) as f64)).unwrap();
    }
    
    // Merge all three
    let merged = KLL::merge_multiple(&[&kll1, &kll2, &kll3]).unwrap();
    
    // Median of 0-299 should be around 149-150
    let median = merged.quantile(0.5);
    assert!(median >= 140.0 && median <= 160.0, "Merged median {} out of range", median);
}

#[test]
fn test_kll_json_serialization() {
    let mut kll = KLL::init_kll(200);
    
    for i in 0..1000 {
        kll.update(&SketchInput::F64(i as f64)).unwrap();
    }
    
    // Serialize to JSON
    let json = kll.serialize_to_json().unwrap();
    
    // Deserialize from JSON
    let restored = KLL::deserialize_from_json(&json).unwrap();
    
    // Verify quantiles match
    for &q in &[0.1, 0.5, 0.9] {
        let original_q = kll.quantile(q);
        let restored_q = restored.quantile(q);
        let diff = (original_q - restored_q).abs();
        assert!(diff < 10.0, "Quantile {} mismatch: original={}, restored={}", q, original_q, restored_q);
    }
}

#[test]
fn test_hydra_with_key_by_label_values() {
    let mut hydra = Hydra::new_with_kll(3, 32, 200);
    
    let key1 = KeyByLabelValues::new_with_labels(vec!["region=us-west".to_string()]);
    let key2 = KeyByLabelValues::new_with_labels(vec!["region=us-east".to_string()]);
    
    // Insert values for different keys
    for i in 0..100 {
        hydra.update_with_key(&key1, i as f64, None);
    }
    
    for i in 0..50 {
        hydra.update_with_key(&key2, (i + 200) as f64, None);
    }
    
    // Query quantiles
    let median1 = hydra.query_key_quantile(&key1, 0.5);
    let median2 = hydra.query_key_quantile(&key2, 0.5);
    
    // Check that they're in reasonable ranges
    assert!(median1 >= 40.0 && median1 <= 60.0, "Key1 median {} out of range", median1);
    assert!(median2 >= 220.0 && median2 <= 240.0, "Key2 median {} out of range", median2);
}

#[test]
fn test_hydra_json_serialization() {
    let mut hydra = Hydra::new_with_kll(3, 16, 200);
    
    let key = KeyByLabelValues::new_with_labels(vec!["test=value".to_string()]);
    for i in 0..100 {
        hydra.update_with_key(&key, i as f64, None);
    }
    
    // Serialize to JSON
    let json = hydra.serialize_to_json().unwrap();
    
    // Deserialize from JSON
    let restored = Hydra::deserialize_from_json(&json).unwrap();
    
    // Verify quantile matches
    let original_q = hydra.query_key_quantile(&key, 0.5);
    let restored_q = restored.query_key_quantile(&key, 0.5);
    
    let diff = (original_q - restored_q).abs();
    assert!(diff < 10.0, "Quantile mismatch: original={}, restored={}", original_q, restored_q);
}

#[test]
fn test_hll_count_alias() {
    let mut hll = HyperLogLog::<DataFusion>::new();
    
    // Insert values using normal insert method
    for i in 0..1000 {
        hll.insert(&SketchInput::U64(i));
    }
    
    let card = hll.count();
    
    // Should be close to 1000 (within HLL error bounds)
    assert!(card >= 900 && card <= 1100, "Cardinality {} out of expected range", card);
}

#[test]
fn test_hll_json_serialization() {
    let mut hll = HyperLogLog::<DataFusion>::new();
    
    for i in 0..5000 {
        hll.insert(&SketchInput::U64(i));
    }
    
    // Serialize to JSON
    let json = hll.serialize_to_json().unwrap();
    
    // Deserialize from JSON
    let restored = HyperLogLog::<DataFusion>::deserialize_from_json(&json).unwrap();
    
    // Verify cardinality matches
    let original_card = hll.estimate();
    let restored_card = restored.estimate();
    
    let diff = (original_card as i64 - restored_card as i64).abs();
    assert!(diff < 200, "Cardinality mismatch: original={}, restored={}", original_card, restored_card);
}
