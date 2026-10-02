// Marabunta - Licensed under the MIT License.
//! Metric labels/tags for dimensional metrics
//!
//! Provides a flexible and efficient label system for dimensional metrics
//! with Prometheus compatibility and cardinality tracking.

use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// A set of labels (key-value pairs) for dimensional metrics
#[derive(Debug, Clone)]
pub struct MetricLabels {
    /// Labels stored as sorted key-value pairs for consistent hashing
    pairs: Vec<(String, String)>,
    /// Pre-computed hash for fast lookups
    hash: u64,
}

impl MetricLabels {
    /// Create new labels from key-value pairs
    pub fn new(pairs: &[(&str, &str)]) -> Self {
        let mut sorted: Vec<_> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        sorted.sort_by(|a, b| a.0.cmp(&b.0));

        let hash = Self::compute_hash(&sorted);
        Self { pairs: sorted, hash }
    }

    /// Create empty labels
    pub fn empty() -> Self {
        Self {
            pairs: Vec::new(),
            hash: 0,
        }
    }

    /// Create from a builder
    pub fn builder() -> LabelBuilder {
        LabelBuilder::new()
    }

    /// Get a label value by key
    pub fn get(&self, key: &str) -> Option<&str> {
        self.pairs
            .binary_search_by(|(k, _)| k.as_str().cmp(key))
            .ok()
            .map(|idx| self.pairs[idx].1.as_str())
    }

    /// Check if a key exists
    pub fn contains(&self, key: &str) -> bool {
        self.pairs
            .binary_search_by(|(k, _)| k.as_str().cmp(key))
            .is_ok()
    }

    /// Get the number of labels
    pub fn len(&self) -> usize {
        self.pairs.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.pairs.is_empty()
    }

    /// Iterate over labels
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.pairs.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// Format labels for Prometheus output
    pub fn prometheus_format(&self) -> String {
        if self.pairs.is_empty() {
            return String::new();
        }

        let pairs: Vec<String> = self
            .pairs
            .iter()
            .map(|(k, v)| format!("{}=\"{}\"", k, escape_prometheus_value(v)))
            .collect();

        format!("{{{}}}", pairs.join(","))
    }

    /// Format labels for Prometheus output, merging with additional labels
    pub fn prometheus_format_with(&self, additional: &[(&str, &str)]) -> String {
        let mut all_pairs: Vec<(&str, &str)> = self.iter().collect();
        all_pairs.extend(additional.iter().copied());
        all_pairs.sort_by(|a, b| a.0.cmp(b.0));

        if all_pairs.is_empty() {
            return String::new();
        }

        let pairs: Vec<String> = all_pairs
            .iter()
            .map(|(k, v)| format!("{}=\"{}\"", k, escape_prometheus_value(v)))
            .collect();

        format!("{{{}}}", pairs.join(","))
    }

    /// Create a new MetricLabels with an additional label
    pub fn with(&self, key: &str, value: &str) -> Self {
        let mut pairs = self.pairs.clone();
        let new_pair = (key.to_string(), value.to_string());

        // Find insertion point or update existing
        match pairs.binary_search_by(|(k, _)| k.as_str().cmp(key)) {
            Ok(idx) => pairs[idx] = new_pair,
            Err(idx) => pairs.insert(idx, new_pair),
        }

        let hash = Self::compute_hash(&pairs);
        Self { pairs, hash }
    }

    /// Create a new MetricLabels with a label removed
    pub fn without(&self, key: &str) -> Self {
        let pairs: Vec<_> = self
            .pairs
            .iter()
            .filter(|(k, _)| k != key)
            .cloned()
            .collect();

        let hash = Self::compute_hash(&pairs);
        Self { pairs, hash }
    }

    fn compute_hash(pairs: &[(String, String)]) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        for (k, v) in pairs {
            k.hash(&mut hasher);
            v.hash(&mut hasher);
        }
        hasher.finish()
    }
}

impl PartialEq for MetricLabels {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash && self.pairs == other.pairs
    }
}

impl Eq for MetricLabels {}

impl Hash for MetricLabels {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}

impl Default for MetricLabels {
    fn default() -> Self {
        Self::empty()
    }
}

/// Escape special characters in Prometheus label values
fn escape_prometheus_value(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// Builder for constructing labels
#[derive(Debug, Default)]
pub struct LabelBuilder {
    pairs: Vec<(String, String)>,
}

impl LabelBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self { pairs: Vec::new() }
    }

    /// Add a label
    pub fn label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.pairs.push((key.into(), value.into()));
        self
    }

    /// Add a label if the value is Some
    pub fn label_opt(self, key: impl Into<String>, value: Option<impl Into<String>>) -> Self {
        match value {
            Some(v) => self.label(key, v),
            None => self,
        }
    }

    /// Build the labels
    pub fn build(self) -> MetricLabels {
        let pairs_ref: Vec<(&str, &str)> = self.pairs.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        MetricLabels::new(&pairs_ref)
    }
}

/// Specification for allowed label names and values
#[derive(Debug, Clone)]
pub struct LabelSpec {
    /// Label key name
    pub name: String,
    /// Optional set of allowed values (None = any value allowed)
    pub allowed_values: Option<HashSet<String>>,
    /// Whether this label is required
    pub required: bool,
    /// Maximum length for the value
    pub max_value_length: usize,
}

impl LabelSpec {
    /// Create a new label specification
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            allowed_values: None,
            required: false,
            max_value_length: 256,
        }
    }

    /// Make this label required
    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    /// Set allowed values
    pub fn allowed_values(mut self, values: &[&str]) -> Self {
        self.allowed_values = Some(values.iter().map(|s| s.to_string()).collect());
        self
    }

    /// Set maximum value length
    pub fn max_length(mut self, len: usize) -> Self {
        self.max_value_length = len;
        self
    }

    /// Validate a label value
    pub fn validate(&self, value: &str) -> Result<(), LabelValidationError> {
        if value.len() > self.max_value_length {
            return Err(LabelValidationError::ValueTooLong {
                label: self.name.clone(),
                length: value.len(),
                max: self.max_value_length,
            });
        }

        if let Some(ref allowed) = self.allowed_values {
            if !allowed.contains(value) {
                return Err(LabelValidationError::InvalidValue {
                    label: self.name.clone(),
                    value: value.to_string(),
                    allowed: allowed.iter().cloned().collect(),
                });
            }
        }

        Ok(())
    }
}

/// Label validation error
#[derive(Debug, Clone)]
pub enum LabelValidationError {
    /// A required label is missing
    MissingRequired { label: String },
    /// Label value is too long
    ValueTooLong {
        label: String,
        length: usize,
        max: usize,
    },
    /// Label value is not in allowed set
    InvalidValue {
        label: String,
        value: String,
        allowed: Vec<String>,
    },
    /// Unknown label
    UnknownLabel { label: String },
}

impl std::fmt::Display for LabelValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingRequired { label } => {
                write!(f, "Required label '{}' is missing", label)
            }
            Self::ValueTooLong { label, length, max } => {
                write!(
                    f,
                    "Label '{}' value length {} exceeds maximum {}",
                    label, length, max
                )
            }
            Self::InvalidValue {
                label,
                value,
                allowed,
            } => {
                write!(
                    f,
                    "Label '{}' value '{}' is not in allowed values: {:?}",
                    label, value, allowed
                )
            }
            Self::UnknownLabel { label } => {
                write!(f, "Unknown label '{}'", label)
            }
        }
    }
}

impl std::error::Error for LabelValidationError {}

/// Schema for validating labels on a metric
#[derive(Debug, Clone)]
pub struct LabelSchema {
    /// Name of the metric this schema applies to
    pub metric_name: String,
    /// Label specifications
    pub specs: Vec<LabelSpec>,
    /// Whether to reject unknown labels
    pub strict: bool,
}

impl LabelSchema {
    /// Create a new schema
    pub fn new(metric_name: &str) -> Self {
        Self {
            metric_name: metric_name.to_string(),
            specs: Vec::new(),
            strict: false,
        }
    }

    /// Add a label specification
    pub fn label(mut self, spec: LabelSpec) -> Self {
        self.specs.push(spec);
        self
    }

    /// Enable strict mode (reject unknown labels)
    pub fn strict(mut self) -> Self {
        self.strict = true;
        self
    }

    /// Validate labels against this schema
    pub fn validate(&self, labels: &MetricLabels) -> Result<(), Vec<LabelValidationError>> {
        let mut errors = Vec::new();

        // Check required labels
        for spec in &self.specs {
            if spec.required && !labels.contains(&spec.name) {
                errors.push(LabelValidationError::MissingRequired {
                    label: spec.name.clone(),
                });
            }
        }

        // Validate each label
        for (key, value) in labels.iter() {
            if let Some(spec) = self.specs.iter().find(|s| s.name == key) {
                if let Err(e) = spec.validate(value) {
                    errors.push(e);
                }
            } else if self.strict {
                errors.push(LabelValidationError::UnknownLabel {
                    label: key.to_string(),
                });
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

/// Common label names used in Marabunta metrics
pub mod common_labels {
    /// Job-related labels
    pub const JOB_ID: &str = "job_id";
    pub const JOB_TYPE: &str = "job_type";
    pub const JOB_STATUS: &str = "job_status";

    /// Task-related labels
    pub const TASK_ID: &str = "task_id";
    pub const TASK_STATUS: &str = "task_status";

    /// Node-related labels
    pub const NODE_ID: &str = "node_id";
    pub const NODE_TYPE: &str = "node_type";
    pub const NODE_REGION: &str = "region";
    pub const NODE_ZONE: &str = "zone";

    /// Request-related labels
    pub const METHOD: &str = "method";
    pub const PATH: &str = "path";
    pub const STATUS_CODE: &str = "status_code";

    /// Account-related labels
    pub const ACCOUNT_ID: &str = "account_id";
    pub const TENANT_ID: &str = "tenant_id";

    /// Resource-related labels
    pub const RESOURCE_TYPE: &str = "resource_type";

    /// Error-related labels
    pub const ERROR_TYPE: &str = "error_type";
    pub const ERROR_CODE: &str = "error_code";
}

/// Label set pool for reusing common label combinations
#[derive(Debug)]
pub struct LabelPool {
    /// Cached label sets
    cache: RwLock<HashMap<u64, Arc<MetricLabels>>>,
    /// Maximum cache size
    max_size: usize,
    /// Hit counter
    hits: AtomicU64,
    /// Miss counter
    misses: AtomicU64,
}

impl LabelPool {
    /// Create a new label pool
    pub fn new(max_size: usize) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            max_size,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Get or create a label set
    pub fn get_or_create(&self, labels: &MetricLabels) -> Arc<MetricLabels> {
        // Try cache first
        {
            let cache = self.cache.read();
            if let Some(cached) = cache.get(&labels.hash) {
                if cached.as_ref() == labels {
                    self.hits.fetch_add(1, Ordering::Relaxed);
                    return cached.clone();
                }
            }
        }

        self.misses.fetch_add(1, Ordering::Relaxed);

        // Insert into cache
        let mut cache = self.cache.write();

        // Evict if full
        if cache.len() >= self.max_size {
            // Simple eviction: remove first entry
            if let Some(&key) = cache.keys().next() {
                cache.remove(&key);
            }
        }

        let arc = Arc::new(labels.clone());
        cache.insert(labels.hash, arc.clone());
        arc
    }

    /// Get cache statistics
    pub fn stats(&self) -> LabelPoolStats {
        let cache = self.cache.read();
        LabelPoolStats {
            size: cache.len(),
            max_size: self.max_size,
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
        }
    }

    /// Clear the cache
    pub fn clear(&self) {
        self.cache.write().clear();
    }
}

impl Default for LabelPool {
    fn default() -> Self {
        Self::new(10000)
    }
}

/// Statistics for the label pool
#[derive(Debug, Clone)]
pub struct LabelPoolStats {
    /// Current cache size
    pub size: usize,
    /// Maximum cache size
    pub max_size: usize,
    /// Cache hits
    pub hits: u64,
    /// Cache misses
    pub misses: u64,
}

impl LabelPoolStats {
    /// Calculate hit ratio
    pub fn hit_ratio(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metric_labels_new() {
        let labels = MetricLabels::new(&[("method", "GET"), ("path", "/api/v1")]);
        assert_eq!(labels.len(), 2);
        assert_eq!(labels.get("method"), Some("GET"));
        assert_eq!(labels.get("path"), Some("/api/v1"));
    }

    #[test]
    fn test_metric_labels_sorted() {
        // Labels should be sorted by key
        let labels = MetricLabels::new(&[("z", "3"), ("a", "1"), ("m", "2")]);
        let pairs: Vec<_> = labels.iter().collect();
        assert_eq!(pairs[0], ("a", "1"));
        assert_eq!(pairs[1], ("m", "2"));
        assert_eq!(pairs[2], ("z", "3"));
    }

    #[test]
    fn test_metric_labels_prometheus_format() {
        let labels = MetricLabels::new(&[("method", "GET"), ("status", "200")]);
        let formatted = labels.prometheus_format();
        assert_eq!(formatted, "{method=\"GET\",status=\"200\"}");
    }

    #[test]
    fn test_metric_labels_prometheus_escaping() {
        let labels = MetricLabels::new(&[("path", "/api/v1?query=\"test\"")]);
        let formatted = labels.prometheus_format();
        assert!(formatted.contains("\\\"test\\\""));
    }

    #[test]
    fn test_metric_labels_with() {
        let labels = MetricLabels::new(&[("a", "1")]);
        let extended = labels.with("b", "2");
        assert_eq!(extended.len(), 2);
        assert_eq!(extended.get("a"), Some("1"));
        assert_eq!(extended.get("b"), Some("2"));
    }

    #[test]
    fn test_metric_labels_without() {
        let labels = MetricLabels::new(&[("a", "1"), ("b", "2")]);
        let reduced = labels.without("a");
        assert_eq!(reduced.len(), 1);
        assert_eq!(reduced.get("a"), None);
        assert_eq!(reduced.get("b"), Some("2"));
    }

    #[test]
    fn test_label_builder() {
        let labels = MetricLabels::builder()
            .label("method", "GET")
            .label("path", "/api")
            .label_opt("user", Some("admin"))
            .label_opt("tenant", None::<&str>)
            .build();

        assert_eq!(labels.len(), 3);
        assert_eq!(labels.get("method"), Some("GET"));
        assert_eq!(labels.get("user"), Some("admin"));
        assert_eq!(labels.get("tenant"), None);
    }

    #[test]
    fn test_label_spec_validation() {
        let spec = LabelSpec::new("status")
            .required()
            .allowed_values(&["200", "400", "500"])
            .max_length(3);

        assert!(spec.validate("200").is_ok());
        assert!(spec.validate("400").is_ok());

        // Invalid value
        assert!(matches!(
            spec.validate("201"),
            Err(LabelValidationError::InvalidValue { .. })
        ));

        // Too long
        assert!(matches!(
            spec.validate("2000"),
            Err(LabelValidationError::ValueTooLong { .. })
        ));
    }

    #[test]
    fn test_label_schema_validation() {
        let schema = LabelSchema::new("http_requests")
            .label(
                LabelSpec::new("method")
                    .required()
                    .allowed_values(&["GET", "POST", "PUT", "DELETE"]),
            )
            .label(LabelSpec::new("status").required())
            .strict();

        // Valid labels
        let valid = MetricLabels::new(&[("method", "GET"), ("status", "200")]);
        assert!(schema.validate(&valid).is_ok());

        // Missing required
        let missing = MetricLabels::new(&[("method", "GET")]);
        let errors = schema.validate(&missing).unwrap_err();
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            errors[0],
            LabelValidationError::MissingRequired { .. }
        ));

        // Unknown label in strict mode
        let unknown = MetricLabels::new(&[("method", "GET"), ("status", "200"), ("extra", "value")]);
        let errors = schema.validate(&unknown).unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e, LabelValidationError::UnknownLabel { .. })));
    }

    #[test]
    fn test_label_pool() {
        let pool = LabelPool::new(100);

        let labels1 = MetricLabels::new(&[("a", "1")]);
        let labels2 = MetricLabels::new(&[("a", "1")]);

        let arc1 = pool.get_or_create(&labels1);
        let arc2 = pool.get_or_create(&labels2);

        // Should be the same Arc
        assert!(Arc::ptr_eq(&arc1, &arc2));

        let stats = pool.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn test_label_pool_eviction() {
        let pool = LabelPool::new(2);

        let labels1 = MetricLabels::new(&[("id", "1")]);
        let labels2 = MetricLabels::new(&[("id", "2")]);
        let labels3 = MetricLabels::new(&[("id", "3")]);

        pool.get_or_create(&labels1);
        pool.get_or_create(&labels2);
        assert_eq!(pool.stats().size, 2);

        // This should trigger eviction
        pool.get_or_create(&labels3);
        assert_eq!(pool.stats().size, 2);
    }

    #[test]
    fn test_labels_equality_and_hash() {
        let labels1 = MetricLabels::new(&[("a", "1"), ("b", "2")]);
        let labels2 = MetricLabels::new(&[("b", "2"), ("a", "1")]); // Different order
        let labels3 = MetricLabels::new(&[("a", "1"), ("b", "3")]); // Different value

        assert_eq!(labels1, labels2);
        assert_ne!(labels1, labels3);

        // Hash should be equal for equal labels
        use std::collections::hash_map::DefaultHasher;
        let hash1 = {
            let mut h = DefaultHasher::new();
            labels1.hash(&mut h);
            h.finish()
        };
        let hash2 = {
            let mut h = DefaultHasher::new();
            labels2.hash(&mut h);
            h.finish()
        };
        assert_eq!(hash1, hash2);
    }
}
