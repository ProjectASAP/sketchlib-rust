//! KeyByLabelValues type for multi-dimensional sketch queries
//! This module provides the `KeyByLabelValues` struct which is used to represent hierarchical keys in multi-population sketches. It's compatible with asap-internal's API.

use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyByLabelValues {
    pub labels: Vec<String>,
}

impl KeyByLabelValues {
    pub fn new() -> Self {
        Self { labels: Vec::new() }
    }

    pub fn new_with_labels(labels: Vec<String>) -> Self {
        Self { labels }
    }

    pub fn insert(&mut self, value: String) {
        self.labels.push(value);
    }

    pub fn get(&self, index: usize) -> Option<&String> {
        self.labels.get(index)
    }

    pub fn serialize_to_json(&self) -> serde_json::Value {
        serde_json::to_value(&self.labels).unwrap_or(serde_json::Value::Null)
    }

    pub fn deserialize_from_json(data: &serde_json::Value) -> Result<Self, serde_json::Error> {
        let labels: Vec<String> = serde_json::from_value(data.clone())?;
        Ok(Self { labels })
    }

    pub fn serialize_to_bytes(&self) -> Vec<u8> {
        bincode::serialize(&self.labels).unwrap_or_default()
    }

    pub fn deserialize_from_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        let labels: Vec<String> = bincode::deserialize(buffer)?;
        Ok(Self { labels })
    }

    pub fn is_empty(&self) -> bool {
        self.labels.is_empty()
    }

    pub fn len(&self) -> usize {
        self.labels.len()
    }

    /// Converts the key to a semicolon-separated string.
    /// Example: `["region=us-west", "service=api"]` → `"region=us-west;service=api"`
    pub fn to_string_key(&self) -> String {
        self.labels.join(";")
    }

    /// Creates a key from a semicolon-separated string.
    /// Example: `"region=us-west;service=api"` → `["region=us-west", "service=api"]`
    pub fn from_string_key(s: &str) -> Self {
        let labels: Vec<String> = s
            .split(';')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        Self { labels }
    }
}

impl Hash for KeyByLabelValues {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Create a sorted vector of labels for consistent hashing
        let mut sorted_labels: Vec<_> = self.labels.iter().collect();
        sorted_labels.sort();

        for label in sorted_labels {
            label.hash(state);
        }
    }
}

impl Default for KeyByLabelValues {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for KeyByLabelValues {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{")?;
        let mut first = true;
        for value in &self.labels {
            if !first {
                write!(f, ", ")?;
            }
            write!(f, "{}", value)?;
            first = false;
        }
        write!(f, "}}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_creation_and_access() {
        let mut key = KeyByLabelValues::new();
        key.insert("localhost:8080".to_string());
        key.insert("prometheus".to_string());

        assert_eq!(key.len(), 2);
        assert_eq!(key.get(0), Some(&"localhost:8080".to_string()));
        assert_eq!(key.get(1), Some(&"prometheus".to_string()));
    }

    #[test]
    fn test_json_serialization() {
        let mut key = KeyByLabelValues::new();
        key.insert("test".to_string());

        let json = key.serialize_to_json();
        let deserialized = KeyByLabelValues::deserialize_from_json(&json).unwrap();
        assert_eq!(key, deserialized);
    }

    #[test]
    fn test_byte_serialization() {
        let mut key = KeyByLabelValues::new();
        key.insert("test".to_string());

        let bytes = key.serialize_to_bytes();
        let deserialized = KeyByLabelValues::deserialize_from_bytes(&bytes).unwrap();
        assert_eq!(key, deserialized);
    }

    #[test]
    fn test_hash_consistency() {
        use std::collections::hash_map::DefaultHasher;

        let mut key1 = KeyByLabelValues::new();
        key1.insert("a".to_string());
        key1.insert("b".to_string());

        let mut key2 = KeyByLabelValues::new();
        key2.insert("b".to_string());
        key2.insert("a".to_string());

        // Should hash to the same value regardless of insertion order
        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();

        key1.hash(&mut hasher1);
        key2.hash(&mut hasher2);

        assert_eq!(hasher1.finish(), hasher2.finish());
    }

    #[test]
    fn test_string_conversion() {
        let key = KeyByLabelValues::new_with_labels(vec![
            "region=us-west".to_string(),
            "service=api".to_string(),
        ]);

        let string_key = key.to_string_key();
        assert_eq!(string_key, "region=us-west;service=api");

        let parsed_key = KeyByLabelValues::from_string_key(&string_key);
        assert_eq!(key, parsed_key);
    }

    #[test]
    fn test_display() {
        let key = KeyByLabelValues::new_with_labels(vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
        ]);

        let display_str = format!("{}", key);
        assert_eq!(display_str, "{a, b, c}");
    }

    #[test]
    fn test_empty_key() {
        let key = KeyByLabelValues::new();
        assert!(key.is_empty());
        assert_eq!(key.len(), 0);
        assert_eq!(key.to_string_key(), "");
    }

    #[test]
    fn test_from_string_key_with_empty() {
        let key = KeyByLabelValues::from_string_key("");
        assert!(key.is_empty());

        let key2 = KeyByLabelValues::from_string_key(";;;");
        assert!(key2.is_empty());
    }
}
