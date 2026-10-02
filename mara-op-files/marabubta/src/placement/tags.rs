// Marabunta - Licensed under the MIT License.
//! Tags system for node labeling and job placement
//!
//! Tags are key-value labels attached to nodes that enable flexible
//! job placement through a powerful expression language.

use chrono::{DateTime, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use thiserror::Error;

/// Error types for tag operations
#[derive(Error, Debug, Clone)]
pub enum TagError {
    #[error("Invalid tag key: {0}")]
    InvalidKey(String),
    #[error("Invalid tag value: {0}")]
    InvalidValue(String),
    #[error("Tag not found: {0}")]
    NotFound(String),
    #[error("Tag expired: {0}")]
    Expired(String),
    #[error("Conflict resolution failed: {0}")]
    ConflictResolution(String),
}

/// Error types for parsing tag expressions
#[derive(Error, Debug, Clone, PartialEq)]
pub enum ParseError {
    #[error("Unexpected end of input")]
    UnexpectedEnd,
    #[error("Unexpected token: {0}")]
    UnexpectedToken(String),
    #[error("Invalid operator: {0}")]
    InvalidOperator(String),
    #[error("Invalid value: {0}")]
    InvalidValue(String),
    #[error("Unclosed parenthesis")]
    UnclosedParen,
    #[error("Invalid regex pattern: {0}")]
    InvalidRegex(String),
    #[error("Empty expression")]
    EmptyExpression,
}

/// Namespace categorization for tags
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TagNamespace {
    /// Location-based tags (e.g., location:building-a, location:rack-12)
    Location,
    /// Hardware capability tags (e.g., hardware:gpu-nvidia-a100)
    Hardware,
    /// Organizational tags (e.g., org:physics-dept)
    Org,
    /// Ephemeral/temporary tags (e.g., ephemeral:idle)
    Ephemeral,
    /// Project-specific tags (e.g., project:collider-sim)
    Project,
    /// Custom namespace with arbitrary name
    Custom(String),
}

impl TagNamespace {
    /// Parse a namespace from a string prefix
    pub fn from_prefix(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "location" | "loc" => TagNamespace::Location,
            "hardware" | "hw" => TagNamespace::Hardware,
            "org" | "organization" => TagNamespace::Org,
            "ephemeral" | "eph" => TagNamespace::Ephemeral,
            "project" | "proj" => TagNamespace::Project,
            other => TagNamespace::Custom(other.to_string()),
        }
    }

    /// Get the canonical prefix string for this namespace
    pub fn prefix(&self) -> &str {
        match self {
            TagNamespace::Location => "location",
            TagNamespace::Hardware => "hardware",
            TagNamespace::Org => "org",
            TagNamespace::Ephemeral => "ephemeral",
            TagNamespace::Project => "project",
            TagNamespace::Custom(s) => s.as_str(),
        }
    }
}

impl fmt::Display for TagNamespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.prefix())
    }
}

/// Value types that a tag can hold
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TagValue {
    /// Tag exists with no value (presence-only)
    Present,
    /// String value
    String(String),
    /// Numeric value (floating point for flexibility)
    Number(f64),
    /// Boolean value
    Bool(bool),
    /// List of string values
    List(Vec<String>),
    /// Hierarchical value (e.g., location:us:east:building-a)
    Hierarchical(Vec<String>),
}

impl TagValue {
    /// Check if this value matches another for equality
    pub fn matches(&self, other: &TagValue) -> bool {
        match (self, other) {
            (TagValue::Present, TagValue::Present) => true,
            (TagValue::String(a), TagValue::String(b)) => a == b,
            (TagValue::Number(a), TagValue::Number(b)) => (a - b).abs() < f64::EPSILON,
            (TagValue::Bool(a), TagValue::Bool(b)) => a == b,
            (TagValue::List(a), TagValue::List(b)) => a == b,
            (TagValue::Hierarchical(a), TagValue::Hierarchical(b)) => a == b,
            _ => false,
        }
    }

    /// Get the numeric value if this is a Number
    pub fn as_number(&self) -> Option<f64> {
        match self {
            TagValue::Number(n) => Some(*n),
            _ => None,
        }
    }

    /// Get the string value
    pub fn as_string(&self) -> Option<&str> {
        match self {
            TagValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Get the hierarchical path
    pub fn as_hierarchy(&self) -> Option<&[String]> {
        match self {
            TagValue::Hierarchical(h) => Some(h),
            _ => None,
        }
    }

    /// Get the list values
    pub fn as_list(&self) -> Option<&[String]> {
        match self {
            TagValue::List(l) => Some(l),
            _ => None,
        }
    }

    /// Convert to a display string
    pub fn to_display_string(&self) -> String {
        match self {
            TagValue::Present => String::new(),
            TagValue::String(s) => s.clone(),
            TagValue::Number(n) => n.to_string(),
            TagValue::Bool(b) => b.to_string(),
            TagValue::List(l) => format!("[{}]", l.join(", ")),
            TagValue::Hierarchical(h) => h.join(":"),
        }
    }
}

impl fmt::Display for TagValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_display_string())
    }
}

impl From<&str> for TagValue {
    fn from(s: &str) -> Self {
        TagValue::String(s.to_string())
    }
}

impl From<String> for TagValue {
    fn from(s: String) -> Self {
        TagValue::String(s)
    }
}

impl From<f64> for TagValue {
    fn from(n: f64) -> Self {
        TagValue::Number(n)
    }
}

impl From<bool> for TagValue {
    fn from(b: bool) -> Self {
        TagValue::Bool(b)
    }
}

impl From<Vec<String>> for TagValue {
    fn from(v: Vec<String>) -> Self {
        TagValue::List(v)
    }
}

/// Source of a tag (who/what created it)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TagSource {
    /// Manually created by a user/principal
    Manual { principal_id: String },
    /// Auto-detected by the system
    AutoDetected { detector_id: String },
    /// Imported from an external source
    Imported { source: String },
}

impl Default for TagSource {
    fn default() -> Self {
        TagSource::Manual {
            principal_id: "system".to_string(),
        }
    }
}

/// Metadata associated with a tag
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TagMetadata {
    /// Source of this tag
    pub source: TagSource,
    /// When this tag was created
    pub created_at: DateTime<Utc>,
    /// Optional expiration time (for ephemeral tags)
    pub expires_at: Option<DateTime<Utc>>,
    /// Confidence level for auto-detected tags (0.0-1.0)
    pub confidence: f64,
}

impl Default for TagMetadata {
    fn default() -> Self {
        TagMetadata {
            source: TagSource::default(),
            created_at: Utc::now(),
            expires_at: None,
            confidence: 1.0,
        }
    }
}

impl TagMetadata {
    /// Create metadata for a manual tag
    pub fn manual(principal_id: impl Into<String>) -> Self {
        TagMetadata {
            source: TagSource::Manual {
                principal_id: principal_id.into(),
            },
            created_at: Utc::now(),
            expires_at: None,
            confidence: 1.0,
        }
    }

    /// Create metadata for an auto-detected tag
    pub fn auto_detected(detector_id: impl Into<String>, confidence: f64) -> Self {
        TagMetadata {
            source: TagSource::AutoDetected {
                detector_id: detector_id.into(),
            },
            created_at: Utc::now(),
            expires_at: None,
            confidence: confidence.clamp(0.0, 1.0),
        }
    }

    /// Create metadata for an imported tag
    pub fn imported(source: impl Into<String>) -> Self {
        TagMetadata {
            source: TagSource::Imported {
                source: source.into(),
            },
            created_at: Utc::now(),
            expires_at: None,
            confidence: 1.0,
        }
    }

    /// Set the expiration time
    pub fn with_expiry(mut self, expires_at: DateTime<Utc>) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    /// Check if the tag is expired
    pub fn is_expired(&self) -> bool {
        self.expires_at.map(|exp| Utc::now() > exp).unwrap_or(false)
    }
}

/// A tag with namespace, key, value, and metadata
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tag {
    /// Namespace categorization
    pub namespace: TagNamespace,
    /// Tag key (unique within namespace)
    pub key: String,
    /// Tag value
    pub value: TagValue,
    /// Tag metadata
    pub metadata: TagMetadata,
}

impl Tag {
    /// Create a new tag with default metadata
    pub fn new(
        namespace: TagNamespace,
        key: impl Into<String>,
        value: impl Into<TagValue>,
    ) -> Self {
        Tag {
            namespace,
            key: key.into(),
            value: value.into(),
            metadata: TagMetadata::default(),
        }
    }

    /// Create a new tag with custom metadata
    pub fn with_metadata(
        namespace: TagNamespace,
        key: impl Into<String>,
        value: impl Into<TagValue>,
        metadata: TagMetadata,
    ) -> Self {
        Tag {
            namespace,
            key: key.into(),
            value: value.into(),
            metadata,
        }
    }

    /// Create a presence-only tag
    pub fn presence(namespace: TagNamespace, key: impl Into<String>) -> Self {
        Tag::new(namespace, key, TagValue::Present)
    }

    /// Get the full qualified key (namespace:key)
    pub fn qualified_key(&self) -> String {
        format!("{}:{}", self.namespace, self.key)
    }

    /// Check if this tag is expired
    pub fn is_expired(&self) -> bool {
        self.metadata.is_expired()
    }

    /// Parse a tag from a string like "namespace:key=value"
    pub fn parse(s: &str) -> Result<Self, TagError> {
        let parts: Vec<&str> = s.splitn(2, ':').collect();
        if parts.len() < 2 {
            return Err(TagError::InvalidKey(format!(
                "Tag must have namespace:key format: {}",
                s
            )));
        }

        let namespace = TagNamespace::from_prefix(parts[0]);
        let rest = parts[1];

        // Check for key=value format
        if let Some(eq_pos) = rest.find('=') {
            let key = &rest[..eq_pos];
            let value_str = &rest[eq_pos + 1..];
            let value = parse_tag_value(value_str);
            Ok(Tag::new(namespace, key, value))
        } else {
            // Just key, presence-only
            Ok(Tag::presence(namespace, rest))
        }
    }
}

impl fmt::Display for Tag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.value {
            TagValue::Present => write!(f, "{}:{}", self.namespace, self.key),
            _ => write!(f, "{}:{}={}", self.namespace, self.key, self.value),
        }
    }
}

/// Parse a tag value from a string
fn parse_tag_value(s: &str) -> TagValue {
    // Try to parse as number
    if let Ok(n) = s.parse::<f64>() {
        return TagValue::Number(n);
    }

    // Try to parse as boolean
    match s.to_lowercase().as_str() {
        "true" | "yes" | "1" => return TagValue::Bool(true),
        "false" | "no" | "0" => return TagValue::Bool(false),
        _ => {}
    }

    // Check for list format [a, b, c]
    if s.starts_with('[') && s.ends_with(']') {
        let inner = &s[1..s.len() - 1];
        let items: Vec<String> = inner
            .split(',')
            .map(|item| item.trim().trim_matches('"').to_string())
            .filter(|item| !item.is_empty())
            .collect();
        return TagValue::List(items);
    }

    // Check for hierarchical format (contains colons)
    if s.contains(':') {
        let parts: Vec<String> = s.split(':').map(|p| p.to_string()).collect();
        return TagValue::Hierarchical(parts);
    }

    // Default to string
    TagValue::String(s.to_string())
}

/// Conflict resolution strategy for merging tag sets
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictResolution {
    /// Keep the existing tag
    KeepExisting,
    /// Replace with the new tag
    ReplaceWithNew,
    /// Keep the tag with higher confidence
    HigherConfidence,
    /// Keep the tag with more recent creation time
    MostRecent,
}

/// A collection of tags
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TagSet {
    /// Tags indexed by qualified key (namespace:key)
    tags: HashMap<String, Tag>,
}

impl TagSet {
    /// Create an empty tag set
    pub fn new() -> Self {
        TagSet {
            tags: HashMap::new(),
        }
    }

    /// Create a tag set from an iterator of tags
    pub fn from_iter<I: IntoIterator<Item = Tag>>(iter: I) -> Self {
        let mut set = TagSet::new();
        for tag in iter {
            set.insert(tag);
        }
        set
    }

    /// Insert a tag into the set
    pub fn insert(&mut self, tag: Tag) {
        let key = tag.qualified_key();
        self.tags.insert(key, tag);
    }

    /// Remove a tag by its qualified key
    pub fn remove(&mut self, key: &str) -> Option<Tag> {
        self.tags.remove(key)
    }

    /// Get a tag by its qualified key
    pub fn get(&self, key: &str) -> Option<&Tag> {
        self.tags.get(key)
    }

    /// Get a mutable reference to a tag by its qualified key
    pub fn get_mut(&mut self, key: &str) -> Option<&mut Tag> {
        self.tags.get_mut(key)
    }

    /// Check if a tag exists (by qualified key or just key)
    pub fn contains(&self, key: &str) -> bool {
        self.tags.contains_key(key) || self.tags.keys().any(|k| k.ends_with(&format!(":{}", key)))
    }

    /// Get a tag by just its key (without namespace), returns first match
    pub fn get_by_key(&self, key: &str) -> Option<&Tag> {
        // First try exact match
        if let Some(tag) = self.tags.get(key) {
            return Some(tag);
        }
        // Then try suffix match
        let suffix = format!(":{}", key);
        self.tags
            .iter()
            .find(|(k, _)| k.ends_with(&suffix))
            .map(|(_, v)| v)
    }

    /// Check if the tag set matches an expression
    pub fn matches(&self, expr: &TagExpr) -> bool {
        expr.evaluate(self)
    }

    /// Get the number of tags
    pub fn len(&self) -> usize {
        self.tags.len()
    }

    /// Check if the tag set is empty
    pub fn is_empty(&self) -> bool {
        self.tags.is_empty()
    }

    /// Iterate over all tags
    pub fn iter(&self) -> impl Iterator<Item = &Tag> {
        self.tags.values()
    }

    /// Iterate over all tags with their qualified keys
    pub fn iter_with_keys(&self) -> impl Iterator<Item = (&String, &Tag)> {
        self.tags.iter()
    }

    /// Merge another tag set into this one
    pub fn merge(&mut self, other: &TagSet, conflict: ConflictResolution) {
        for (key, new_tag) in &other.tags {
            if let Some(existing) = self.tags.get(key) {
                let should_replace = match conflict {
                    ConflictResolution::KeepExisting => false,
                    ConflictResolution::ReplaceWithNew => true,
                    ConflictResolution::HigherConfidence => {
                        new_tag.metadata.confidence > existing.metadata.confidence
                    }
                    ConflictResolution::MostRecent => {
                        new_tag.metadata.created_at > existing.metadata.created_at
                    }
                };
                if should_replace {
                    self.tags.insert(key.clone(), new_tag.clone());
                }
            } else {
                self.tags.insert(key.clone(), new_tag.clone());
            }
        }
    }

    /// Filter tags by namespace
    pub fn filter_by_namespace(&self, ns: &TagNamespace) -> TagSet {
        TagSet {
            tags: self
                .tags
                .iter()
                .filter(|(_, tag)| &tag.namespace == ns)
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        }
    }

    /// Get all expired tags
    pub fn expired_tags(&self) -> Vec<&Tag> {
        self.tags.values().filter(|tag| tag.is_expired()).collect()
    }

    /// Remove and return all expired tags
    pub fn remove_expired(&mut self) -> Vec<Tag> {
        let expired_keys: Vec<String> = self
            .tags
            .iter()
            .filter(|(_, tag)| tag.is_expired())
            .map(|(k, _)| k.clone())
            .collect();

        expired_keys
            .into_iter()
            .filter_map(|k| self.tags.remove(&k))
            .collect()
    }

    /// Get all tags as a vector
    pub fn to_vec(&self) -> Vec<Tag> {
        self.tags.values().cloned().collect()
    }
}

impl IntoIterator for TagSet {
    type Item = Tag;
    type IntoIter = std::collections::hash_map::IntoValues<String, Tag>;

    fn into_iter(self) -> Self::IntoIter {
        self.tags.into_values()
    }
}

impl<'a> IntoIterator for &'a TagSet {
    type Item = &'a Tag;
    type IntoIter = std::collections::hash_map::Values<'a, String, Tag>;

    fn into_iter(self) -> Self::IntoIter {
        self.tags.values()
    }
}

/// Expression language for matching tags
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TagExpr {
    /// Check if a tag exists (has key)
    Has(String),
    /// Check if tag equals a specific value
    Equals(String, TagValue),

    // Numeric comparisons
    /// Check if numeric tag is greater than value
    GreaterThan(String, f64),
    /// Check if numeric tag is less than value
    LessThan(String, f64),
    /// Check if numeric tag is between min and max (inclusive)
    Between(String, f64, f64),

    // String matching
    /// Check if string tag starts with prefix
    StartsWith(String, String),
    /// Check if string tag ends with suffix
    EndsWith(String, String),
    /// Check if string tag contains substring
    Contains(String, String),
    /// Check if string tag matches regex pattern
    Regex(String, String),

    // Hierarchy
    /// Check if hierarchical tag is under a given path
    Under(String, Vec<String>),

    // Set operations
    /// Check if tag value is in a set of values
    In(String, Vec<TagValue>),

    // Logical operators
    /// Logical AND of two expressions
    And(Box<TagExpr>, Box<TagExpr>),
    /// Logical OR of two expressions
    Or(Box<TagExpr>, Box<TagExpr>),
    /// Logical NOT of an expression
    Not(Box<TagExpr>),

    /// Always true
    True,
    /// Always false
    False,
}

impl TagExpr {
    /// Evaluate the expression against a tag set
    pub fn evaluate(&self, tags: &TagSet) -> bool {
        match self {
            TagExpr::Has(key) => tags.contains(key) || tags.get_by_key(key).is_some(),

            TagExpr::Equals(key, value) => {
                if let Some(tag) = tags.get(key).or_else(|| tags.get_by_key(key)) {
                    tag.value.matches(value)
                } else {
                    false
                }
            }

            TagExpr::GreaterThan(key, threshold) => {
                if let Some(tag) = tags.get(key).or_else(|| tags.get_by_key(key)) {
                    tag.value
                        .as_number()
                        .map(|n| n > *threshold)
                        .unwrap_or(false)
                } else {
                    false
                }
            }

            TagExpr::LessThan(key, threshold) => {
                if let Some(tag) = tags.get(key).or_else(|| tags.get_by_key(key)) {
                    tag.value
                        .as_number()
                        .map(|n| n < *threshold)
                        .unwrap_or(false)
                } else {
                    false
                }
            }

            TagExpr::Between(key, min, max) => {
                if let Some(tag) = tags.get(key).or_else(|| tags.get_by_key(key)) {
                    tag.value
                        .as_number()
                        .map(|n| n >= *min && n <= *max)
                        .unwrap_or(false)
                } else {
                    false
                }
            }

            TagExpr::StartsWith(key, prefix) => {
                if let Some(tag) = tags.get(key).or_else(|| tags.get_by_key(key)) {
                    match &tag.value {
                        TagValue::String(s) => s.starts_with(prefix),
                        TagValue::Hierarchical(h) => h.join(":").starts_with(prefix),
                        _ => false,
                    }
                } else {
                    false
                }
            }

            TagExpr::EndsWith(key, suffix) => {
                if let Some(tag) = tags.get(key).or_else(|| tags.get_by_key(key)) {
                    match &tag.value {
                        TagValue::String(s) => s.ends_with(suffix),
                        TagValue::Hierarchical(h) => h.join(":").ends_with(suffix),
                        _ => false,
                    }
                } else {
                    false
                }
            }

            TagExpr::Contains(key, substring) => {
                if let Some(tag) = tags.get(key).or_else(|| tags.get_by_key(key)) {
                    match &tag.value {
                        TagValue::String(s) => s.contains(substring),
                        TagValue::Hierarchical(h) => h.join(":").contains(substring),
                        TagValue::List(l) => l.iter().any(|item| item.contains(substring)),
                        _ => false,
                    }
                } else {
                    false
                }
            }

            TagExpr::Regex(key, pattern) => {
                if let Some(tag) = tags.get(key).or_else(|| tags.get_by_key(key)) {
                    if let Ok(re) = Regex::new(pattern) {
                        match &tag.value {
                            TagValue::String(s) => re.is_match(s),
                            TagValue::Hierarchical(h) => re.is_match(&h.join(":")),
                            _ => false,
                        }
                    } else {
                        false
                    }
                } else {
                    false
                }
            }

            TagExpr::Under(key, path) => {
                if let Some(tag) = tags.get(key).or_else(|| tags.get_by_key(key)) {
                    if let Some(hierarchy) = tag.value.as_hierarchy() {
                        // Check if the tag's hierarchy starts with the given path
                        if hierarchy.len() >= path.len() {
                            hierarchy.iter().zip(path.iter()).all(|(a, b)| a == b)
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                } else {
                    false
                }
            }

            TagExpr::In(key, values) => {
                if let Some(tag) = tags.get(key).or_else(|| tags.get_by_key(key)) {
                    values.iter().any(|v| tag.value.matches(v))
                } else {
                    false
                }
            }

            TagExpr::And(left, right) => left.evaluate(tags) && right.evaluate(tags),

            TagExpr::Or(left, right) => left.evaluate(tags) || right.evaluate(tags),

            TagExpr::Not(inner) => !inner.evaluate(tags),

            TagExpr::True => true,

            TagExpr::False => false,
        }
    }

    /// Parse a tag expression from a string
    pub fn parse(s: &str) -> Result<TagExpr, ParseError> {
        let s = s.trim();
        if s.is_empty() {
            return Err(ParseError::EmptyExpression);
        }
        let mut parser = ExprParser::new(s);
        parser.parse_expr()
    }

    /// Create an AND expression
    pub fn and(self, other: TagExpr) -> TagExpr {
        TagExpr::And(Box::new(self), Box::new(other))
    }

    /// Create an OR expression
    pub fn or(self, other: TagExpr) -> TagExpr {
        TagExpr::Or(Box::new(self), Box::new(other))
    }

    /// Create a NOT expression
    pub fn not(self) -> TagExpr {
        TagExpr::Not(Box::new(self))
    }
}

impl fmt::Display for TagExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TagExpr::Has(key) => write!(f, "has({})", key),
            TagExpr::Equals(key, value) => write!(f, "{} = {}", key, format_value(value)),
            TagExpr::GreaterThan(key, n) => write!(f, "{} > {}", key, n),
            TagExpr::LessThan(key, n) => write!(f, "{} < {}", key, n),
            TagExpr::Between(key, min, max) => write!(f, "{} between {} and {}", key, min, max),
            TagExpr::StartsWith(key, prefix) => write!(f, "{} starts_with \"{}\"", key, prefix),
            TagExpr::EndsWith(key, suffix) => write!(f, "{} ends_with \"{}\"", key, suffix),
            TagExpr::Contains(key, sub) => write!(f, "{} contains \"{}\"", key, sub),
            TagExpr::Regex(key, pattern) => write!(f, "{} matches \"{}\"", key, pattern),
            TagExpr::Under(key, path) => write!(f, "{} under {}", key, path.join(":")),
            TagExpr::In(key, values) => {
                let vals: Vec<String> = values.iter().map(format_value).collect();
                write!(f, "{} in [{}]", key, vals.join(", "))
            }
            TagExpr::And(left, right) => write!(f, "({} AND {})", left, right),
            TagExpr::Or(left, right) => write!(f, "({} OR {})", left, right),
            TagExpr::Not(inner) => write!(f, "NOT {}", inner),
            TagExpr::True => write!(f, "true"),
            TagExpr::False => write!(f, "false"),
        }
    }
}

fn format_value(v: &TagValue) -> String {
    match v {
        TagValue::Present => "present".to_string(),
        TagValue::String(s) => format!("\"{}\"", s),
        TagValue::Number(n) => n.to_string(),
        TagValue::Bool(b) => b.to_string(),
        TagValue::List(l) => format!("[{}]", l.join(", ")),
        TagValue::Hierarchical(h) => h.join(":"),
    }
}

impl FromStr for TagExpr {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        TagExpr::parse(s)
    }
}

/// Token types for the expression parser
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
enum Token {
    Ident(String),
    String(String),
    Number(f64),
    Bool(bool),
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    Colon,
    Eq,
    Neq,
    Gt,
    Lt,
    Gte,
    Lte,
    And,
    Or,
    Not,
    Has,
    In,
    Between,
    StartsWith,
    EndsWith,
    Contains,
    Matches,
    Under,
    True,
    False,
    Eof,
}

/// Lexer for tokenizing expression strings
struct Lexer<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Self {
        Lexer { input, pos: 0 }
    }

    fn peek_char(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn next_char(&mut self) -> Option<char> {
        let c = self.peek_char()?;
        self.pos += c.len_utf8();
        Some(c)
    }

    fn skip_whitespace(&mut self) {
        while let Some(c) = self.peek_char() {
            if c.is_whitespace() {
                self.next_char();
            } else {
                break;
            }
        }
    }

    fn read_string(&mut self, quote: char) -> Result<String, ParseError> {
        let mut s = String::new();
        loop {
            match self.next_char() {
                Some(c) if c == quote => return Ok(s),
                Some('\\') => {
                    if let Some(escaped) = self.next_char() {
                        match escaped {
                            'n' => s.push('\n'),
                            't' => s.push('\t'),
                            'r' => s.push('\r'),
                            '\\' => s.push('\\'),
                            c if c == quote => s.push(c),
                            _ => {
                                s.push('\\');
                                s.push(escaped);
                            }
                        }
                    }
                }
                Some(c) => s.push(c),
                None => return Err(ParseError::UnexpectedEnd),
            }
        }
    }

    fn read_ident(&mut self) -> String {
        let start = self.pos;
        while let Some(c) = self.peek_char() {
            if c.is_alphanumeric() || c == '_' || c == '-' || c == ':' || c == '.' {
                self.next_char();
            } else {
                break;
            }
        }
        self.input[start..self.pos].to_string()
    }

    fn read_number(&mut self) -> Result<f64, ParseError> {
        let start = self.pos;
        let mut has_dot = false;
        let mut has_exp = false;

        // Handle negative sign
        if self.peek_char() == Some('-') {
            self.next_char();
        }

        while let Some(c) = self.peek_char() {
            if c.is_ascii_digit() {
                self.next_char();
            } else if c == '.' && !has_dot && !has_exp {
                has_dot = true;
                self.next_char();
            } else if (c == 'e' || c == 'E') && !has_exp {
                has_exp = true;
                self.next_char();
                if self.peek_char() == Some('-') || self.peek_char() == Some('+') {
                    self.next_char();
                }
            } else {
                break;
            }
        }

        let num_str = &self.input[start..self.pos];
        num_str
            .parse()
            .map_err(|_| ParseError::InvalidValue(num_str.to_string()))
    }

    fn next_token(&mut self) -> Result<Token, ParseError> {
        self.skip_whitespace();

        match self.peek_char() {
            None => Ok(Token::Eof),
            Some('(') => {
                self.next_char();
                Ok(Token::LParen)
            }
            Some(')') => {
                self.next_char();
                Ok(Token::RParen)
            }
            Some('[') => {
                self.next_char();
                Ok(Token::LBracket)
            }
            Some(']') => {
                self.next_char();
                Ok(Token::RBracket)
            }
            Some(',') => {
                self.next_char();
                Ok(Token::Comma)
            }
            Some('=') => {
                self.next_char();
                if self.peek_char() == Some('=') {
                    self.next_char();
                }
                Ok(Token::Eq)
            }
            Some('!') => {
                self.next_char();
                if self.peek_char() == Some('=') {
                    self.next_char();
                    Ok(Token::Neq)
                } else {
                    Ok(Token::Not)
                }
            }
            Some('>') => {
                self.next_char();
                if self.peek_char() == Some('=') {
                    self.next_char();
                    Ok(Token::Gte)
                } else {
                    Ok(Token::Gt)
                }
            }
            Some('<') => {
                self.next_char();
                if self.peek_char() == Some('=') {
                    self.next_char();
                    Ok(Token::Lte)
                } else {
                    Ok(Token::Lt)
                }
            }
            Some('"') => {
                self.next_char();
                Ok(Token::String(self.read_string('"')?))
            }
            Some('\'') => {
                self.next_char();
                Ok(Token::String(self.read_string('\'')?))
            }
            Some(c)
                if c.is_ascii_digit()
                    || (c == '-'
                        && self.input[self.pos + 1..]
                            .starts_with(|c: char| c.is_ascii_digit())) =>
            {
                Ok(Token::Number(self.read_number()?))
            }
            Some(_) => {
                let ident = self.read_ident();
                match ident.to_uppercase().as_str() {
                    "AND" | "&&" => Ok(Token::And),
                    "OR" | "||" => Ok(Token::Or),
                    "NOT" => Ok(Token::Not),
                    "HAS" => Ok(Token::Has),
                    "IN" => Ok(Token::In),
                    "BETWEEN" => Ok(Token::Between),
                    "STARTS_WITH" | "STARTSWITH" => Ok(Token::StartsWith),
                    "ENDS_WITH" | "ENDSWITH" => Ok(Token::EndsWith),
                    "CONTAINS" => Ok(Token::Contains),
                    "MATCHES" | "REGEX" => Ok(Token::Matches),
                    "UNDER" => Ok(Token::Under),
                    "TRUE" => Ok(Token::True),
                    "FALSE" => Ok(Token::False),
                    _ => Ok(Token::Ident(ident)),
                }
            }
        }
    }
}

/// Parser for tag expressions
struct ExprParser<'a> {
    lexer: Lexer<'a>,
    current: Token,
}

impl<'a> ExprParser<'a> {
    fn new(input: &'a str) -> Self {
        let mut parser = ExprParser {
            lexer: Lexer::new(input),
            current: Token::Eof,
        };
        // Prime the parser with the first token
        let _ = parser.advance();
        parser
    }

    fn advance(&mut self) -> Result<Token, ParseError> {
        let prev = std::mem::replace(&mut self.current, self.lexer.next_token()?);
        Ok(prev)
    }

    fn expect(&mut self, expected: Token) -> Result<(), ParseError> {
        if std::mem::discriminant(&self.current) == std::mem::discriminant(&expected) {
            self.advance()?;
            Ok(())
        } else {
            Err(ParseError::UnexpectedToken(format!(
                "expected {:?}, got {:?}",
                expected, self.current
            )))
        }
    }

    fn parse_expr(&mut self) -> Result<TagExpr, ParseError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<TagExpr, ParseError> {
        let mut left = self.parse_and()?;
        while matches!(self.current, Token::Or) {
            self.advance()?;
            let right = self.parse_and()?;
            left = TagExpr::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<TagExpr, ParseError> {
        let mut left = self.parse_not()?;
        while matches!(self.current, Token::And) {
            self.advance()?;
            let right = self.parse_not()?;
            left = TagExpr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<TagExpr, ParseError> {
        if matches!(self.current, Token::Not) {
            self.advance()?;
            let inner = self.parse_not()?;
            Ok(TagExpr::Not(Box::new(inner)))
        } else {
            self.parse_primary()
        }
    }

    fn parse_primary(&mut self) -> Result<TagExpr, ParseError> {
        match &self.current {
            Token::LParen => {
                self.advance()?;
                let expr = self.parse_expr()?;
                self.expect(Token::RParen)?;
                Ok(expr)
            }
            Token::True => {
                self.advance()?;
                Ok(TagExpr::True)
            }
            Token::False => {
                self.advance()?;
                Ok(TagExpr::False)
            }
            Token::Has => {
                self.advance()?;
                self.expect(Token::LParen)?;
                let key = self.parse_ident()?;
                self.expect(Token::RParen)?;
                Ok(TagExpr::Has(key))
            }
            Token::Ident(_) => self.parse_comparison(),
            _ => Err(ParseError::UnexpectedToken(format!("{:?}", self.current))),
        }
    }

    fn parse_ident(&mut self) -> Result<String, ParseError> {
        match &self.current {
            Token::Ident(s) => {
                let s = s.clone();
                self.advance()?;
                Ok(s)
            }
            Token::String(s) => {
                let s = s.clone();
                self.advance()?;
                Ok(s)
            }
            _ => Err(ParseError::UnexpectedToken(format!(
                "expected identifier, got {:?}",
                self.current
            ))),
        }
    }

    fn parse_comparison(&mut self) -> Result<TagExpr, ParseError> {
        let key = self.parse_ident()?;

        match &self.current {
            Token::Eq => {
                self.advance()?;
                let value = self.parse_value()?;
                Ok(TagExpr::Equals(key, value))
            }
            Token::Neq => {
                self.advance()?;
                let value = self.parse_value()?;
                Ok(TagExpr::Not(Box::new(TagExpr::Equals(key, value))))
            }
            Token::Gt => {
                self.advance()?;
                let n = self.parse_number()?;
                Ok(TagExpr::GreaterThan(key, n))
            }
            Token::Lt => {
                self.advance()?;
                let n = self.parse_number()?;
                Ok(TagExpr::LessThan(key, n))
            }
            Token::Gte => {
                self.advance()?;
                let n = self.parse_number()?;
                // >= is NOT < for our purposes
                Ok(TagExpr::Not(Box::new(TagExpr::LessThan(key, n))))
            }
            Token::Lte => {
                self.advance()?;
                let n = self.parse_number()?;
                Ok(TagExpr::Not(Box::new(TagExpr::GreaterThan(key, n))))
            }
            Token::In => {
                self.advance()?;
                let values = self.parse_value_list()?;
                Ok(TagExpr::In(key, values))
            }
            Token::Between => {
                self.advance()?;
                let min = self.parse_number()?;
                // Expect "and" keyword (tokenized as Token::And)
                if self.current == Token::And {
                    self.advance()?;
                }
                let max = self.parse_number()?;
                Ok(TagExpr::Between(key, min, max))
            }
            Token::StartsWith => {
                self.advance()?;
                let prefix = self.parse_string()?;
                Ok(TagExpr::StartsWith(key, prefix))
            }
            Token::EndsWith => {
                self.advance()?;
                let suffix = self.parse_string()?;
                Ok(TagExpr::EndsWith(key, suffix))
            }
            Token::Contains => {
                self.advance()?;
                let sub = self.parse_string()?;
                Ok(TagExpr::Contains(key, sub))
            }
            Token::Matches => {
                self.advance()?;
                let pattern = self.parse_string()?;
                // Validate the regex
                if Regex::new(&pattern).is_err() {
                    return Err(ParseError::InvalidRegex(pattern));
                }
                Ok(TagExpr::Regex(key, pattern))
            }
            Token::Under => {
                self.advance()?;
                let path = self.parse_path()?;
                Ok(TagExpr::Under(key, path))
            }
            // Just the key means "has"
            _ => Ok(TagExpr::Has(key)),
        }
    }

    fn parse_value(&mut self) -> Result<TagValue, ParseError> {
        match &self.current {
            Token::String(s) => {
                let s = s.clone();
                self.advance()?;
                Ok(TagValue::String(s))
            }
            Token::Number(n) => {
                let n = *n;
                self.advance()?;
                Ok(TagValue::Number(n))
            }
            Token::Bool(b) => {
                let b = *b;
                self.advance()?;
                Ok(TagValue::Bool(b))
            }
            Token::True => {
                self.advance()?;
                Ok(TagValue::Bool(true))
            }
            Token::False => {
                self.advance()?;
                Ok(TagValue::Bool(false))
            }
            Token::LBracket => {
                let values = self.parse_string_list()?;
                Ok(TagValue::List(values))
            }
            Token::Ident(s) => {
                // Could be hierarchical or simple string
                let s = s.clone();
                self.advance()?;
                if s.contains(':') {
                    Ok(TagValue::Hierarchical(
                        s.split(':').map(String::from).collect(),
                    ))
                } else {
                    Ok(TagValue::String(s))
                }
            }
            _ => Err(ParseError::InvalidValue(format!("{:?}", self.current))),
        }
    }

    fn parse_number(&mut self) -> Result<f64, ParseError> {
        match &self.current {
            Token::Number(n) => {
                let n = *n;
                self.advance()?;
                Ok(n)
            }
            _ => Err(ParseError::InvalidValue(format!(
                "expected number, got {:?}",
                self.current
            ))),
        }
    }

    fn parse_string(&mut self) -> Result<String, ParseError> {
        match &self.current {
            Token::String(s) => {
                let s = s.clone();
                self.advance()?;
                Ok(s)
            }
            Token::Ident(s) => {
                let s = s.clone();
                self.advance()?;
                Ok(s)
            }
            _ => Err(ParseError::InvalidValue(format!(
                "expected string, got {:?}",
                self.current
            ))),
        }
    }

    fn parse_path(&mut self) -> Result<Vec<String>, ParseError> {
        let s = self.parse_string()?;
        Ok(s.split(':').map(String::from).collect())
    }

    fn parse_value_list(&mut self) -> Result<Vec<TagValue>, ParseError> {
        self.expect(Token::LBracket)?;
        let mut values = Vec::new();
        if !matches!(self.current, Token::RBracket) {
            values.push(self.parse_value()?);
            while matches!(self.current, Token::Comma) {
                self.advance()?;
                values.push(self.parse_value()?);
            }
        }
        self.expect(Token::RBracket)?;
        Ok(values)
    }

    fn parse_string_list(&mut self) -> Result<Vec<String>, ParseError> {
        self.expect(Token::LBracket)?;
        let mut values = Vec::new();
        if !matches!(self.current, Token::RBracket) {
            values.push(self.parse_string()?);
            while matches!(self.current, Token::Comma) {
                self.advance()?;
                values.push(self.parse_string()?);
            }
        }
        self.expect(Token::RBracket)?;
        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_tag_set() -> TagSet {
        let mut tags = TagSet::new();
        tags.insert(Tag::new(
            TagNamespace::Location,
            "building",
            TagValue::String("building-a".to_string()),
        ));
        tags.insert(Tag::new(
            TagNamespace::Location,
            "region",
            TagValue::Hierarchical(vec!["us".into(), "east".into(), "dc1".into()]),
        ));
        tags.insert(Tag::new(TagNamespace::Hardware, "gpu", TagValue::Present));
        tags.insert(Tag::new(
            TagNamespace::Hardware,
            "memory",
            TagValue::Number(128.0),
        ));
        tags.insert(Tag::new(
            TagNamespace::Hardware,
            "capabilities",
            TagValue::List(vec!["avx512".into(), "cuda".into()]),
        ));
        tags.insert(Tag::new(
            TagNamespace::Org,
            "department",
            TagValue::String("physics".to_string()),
        ));
        tags
    }

    #[test]
    fn test_tag_creation() {
        let tag = Tag::new(TagNamespace::Location, "building", "building-a");
        assert_eq!(tag.namespace, TagNamespace::Location);
        assert_eq!(tag.key, "building");
        assert_eq!(tag.qualified_key(), "location:building");
    }

    #[test]
    fn test_tag_parse() {
        let tag = Tag::parse("location:building=building-a").unwrap();
        assert_eq!(tag.namespace, TagNamespace::Location);
        assert_eq!(tag.key, "building");
        assert_eq!(tag.value, TagValue::String("building-a".to_string()));

        let presence_tag = Tag::parse("hardware:gpu").unwrap();
        assert_eq!(presence_tag.value, TagValue::Present);
    }

    #[test]
    fn test_tag_set_basic() {
        let mut tags = TagSet::new();
        let tag = Tag::new(TagNamespace::Location, "building", "building-a");
        tags.insert(tag.clone());

        assert!(tags.contains("location:building"));
        assert!(tags.get("location:building").is_some());
        assert_eq!(tags.len(), 1);

        let removed = tags.remove("location:building");
        assert!(removed.is_some());
        assert!(tags.is_empty());
    }

    #[test]
    fn test_expr_has() {
        let tags = make_tag_set();
        assert!(TagExpr::Has("hardware:gpu".into()).evaluate(&tags));
        assert!(TagExpr::Has("gpu".into()).evaluate(&tags));
        assert!(!TagExpr::Has("hardware:fpga".into()).evaluate(&tags));
    }

    #[test]
    fn test_expr_equals() {
        let tags = make_tag_set();
        assert!(TagExpr::Equals(
            "location:building".into(),
            TagValue::String("building-a".into())
        )
        .evaluate(&tags));
        assert!(!TagExpr::Equals(
            "location:building".into(),
            TagValue::String("building-b".into())
        )
        .evaluate(&tags));
    }

    #[test]
    fn test_expr_numeric() {
        let tags = make_tag_set();
        assert!(TagExpr::GreaterThan("hardware:memory".into(), 64.0).evaluate(&tags));
        assert!(!TagExpr::GreaterThan("hardware:memory".into(), 256.0).evaluate(&tags));
        assert!(TagExpr::LessThan("hardware:memory".into(), 256.0).evaluate(&tags));
        assert!(TagExpr::Between("hardware:memory".into(), 64.0, 256.0).evaluate(&tags));
        assert!(!TagExpr::Between("hardware:memory".into(), 256.0, 512.0).evaluate(&tags));
    }

    #[test]
    fn test_expr_string_ops() {
        let tags = make_tag_set();
        assert!(TagExpr::StartsWith("location:building".into(), "building".into()).evaluate(&tags));
        assert!(TagExpr::EndsWith("location:building".into(), "-a".into()).evaluate(&tags));
        assert!(TagExpr::Contains("location:building".into(), "uild".into()).evaluate(&tags));
        assert!(
            TagExpr::Regex("location:building".into(), "building-[a-z]".into()).evaluate(&tags)
        );
    }

    #[test]
    fn test_expr_under() {
        let tags = make_tag_set();
        assert!(TagExpr::Under("location:region".into(), vec!["us".into()]).evaluate(&tags));
        assert!(
            TagExpr::Under("location:region".into(), vec!["us".into(), "east".into()])
                .evaluate(&tags)
        );
        assert!(!TagExpr::Under("location:region".into(), vec!["eu".into()]).evaluate(&tags));
    }

    #[test]
    fn test_expr_in() {
        let tags = make_tag_set();
        assert!(TagExpr::In(
            "location:building".into(),
            vec![
                TagValue::String("building-a".into()),
                TagValue::String("building-b".into())
            ]
        )
        .evaluate(&tags));
        assert!(!TagExpr::In(
            "location:building".into(),
            vec![
                TagValue::String("building-c".into()),
                TagValue::String("building-d".into())
            ]
        )
        .evaluate(&tags));
    }

    #[test]
    fn test_expr_logical() {
        let tags = make_tag_set();

        let expr = TagExpr::And(
            Box::new(TagExpr::Has("hardware:gpu".into())),
            Box::new(TagExpr::GreaterThan("hardware:memory".into(), 64.0)),
        );
        assert!(expr.evaluate(&tags));

        let expr = TagExpr::Or(
            Box::new(TagExpr::Has("hardware:fpga".into())),
            Box::new(TagExpr::Has("hardware:gpu".into())),
        );
        assert!(expr.evaluate(&tags));

        let expr = TagExpr::Not(Box::new(TagExpr::Has("hardware:fpga".into())));
        assert!(expr.evaluate(&tags));
    }

    #[test]
    fn test_parse_simple() {
        let expr = TagExpr::parse("hardware:gpu").unwrap();
        assert_eq!(expr, TagExpr::Has("hardware:gpu".into()));

        let expr = TagExpr::parse("has(hardware:gpu)").unwrap();
        assert_eq!(expr, TagExpr::Has("hardware:gpu".into()));
    }

    #[test]
    fn test_parse_equals() {
        let expr = TagExpr::parse("location:building = \"building-a\"").unwrap();
        assert_eq!(
            expr,
            TagExpr::Equals(
                "location:building".into(),
                TagValue::String("building-a".into())
            )
        );
    }

    #[test]
    fn test_parse_numeric() {
        let expr = TagExpr::parse("hardware:memory > 64").unwrap();
        assert_eq!(expr, TagExpr::GreaterThan("hardware:memory".into(), 64.0));

        let expr = TagExpr::parse("hardware:memory < 256").unwrap();
        assert_eq!(expr, TagExpr::LessThan("hardware:memory".into(), 256.0));

        let expr = TagExpr::parse("hardware:memory between 64 and 256").unwrap();
        assert_eq!(
            expr,
            TagExpr::Between("hardware:memory".into(), 64.0, 256.0)
        );
    }

    #[test]
    fn test_parse_logical() {
        let expr = TagExpr::parse("hardware:gpu AND hardware:memory > 64").unwrap();
        assert_eq!(
            expr,
            TagExpr::And(
                Box::new(TagExpr::Has("hardware:gpu".into())),
                Box::new(TagExpr::GreaterThan("hardware:memory".into(), 64.0))
            )
        );

        let expr = TagExpr::parse("hardware:gpu OR hardware:fpga").unwrap();
        assert_eq!(
            expr,
            TagExpr::Or(
                Box::new(TagExpr::Has("hardware:gpu".into())),
                Box::new(TagExpr::Has("hardware:fpga".into()))
            )
        );

        let expr = TagExpr::parse("NOT hardware:fpga").unwrap();
        assert_eq!(
            expr,
            TagExpr::Not(Box::new(TagExpr::Has("hardware:fpga".into())))
        );
    }

    #[test]
    fn test_parse_complex() {
        let expr = TagExpr::parse(
            "(hardware:gpu AND hardware:memory > 64) OR location:building = \"building-a\"",
        )
        .unwrap();
        let tags = make_tag_set();
        assert!(expr.evaluate(&tags));
    }

    #[test]
    fn test_parse_in() {
        let expr = TagExpr::parse("location:building in [\"building-a\", \"building-b\"]").unwrap();
        let tags = make_tag_set();
        assert!(expr.evaluate(&tags));
    }

    #[test]
    fn test_parse_string_ops() {
        let tags = make_tag_set();

        let expr = TagExpr::parse("location:building starts_with \"building\"").unwrap();
        assert!(expr.evaluate(&tags));

        let expr = TagExpr::parse("location:building ends_with \"-a\"").unwrap();
        assert!(expr.evaluate(&tags));

        let expr = TagExpr::parse("location:building contains \"uild\"").unwrap();
        assert!(expr.evaluate(&tags));

        let expr = TagExpr::parse("location:building matches \"building-[a-z]\"").unwrap();
        assert!(expr.evaluate(&tags));
    }

    #[test]
    fn test_parse_under() {
        let expr = TagExpr::parse("location:region under us:east").unwrap();
        let tags = make_tag_set();
        assert!(expr.evaluate(&tags));
    }

    #[test]
    fn test_display_roundtrip() {
        let exprs = vec![
            TagExpr::Has("hardware:gpu".into()),
            TagExpr::Equals(
                "location:building".into(),
                TagValue::String("building-a".into()),
            ),
            TagExpr::GreaterThan("hardware:memory".into(), 64.0),
            TagExpr::And(
                Box::new(TagExpr::Has("hardware:gpu".into())),
                Box::new(TagExpr::GreaterThan("hardware:memory".into(), 64.0)),
            ),
        ];

        for expr in exprs {
            let s = expr.to_string();
            let parsed = TagExpr::parse(&s).unwrap();
            // Note: structure may differ but evaluation should be same
            let tags = make_tag_set();
            assert_eq!(expr.evaluate(&tags), parsed.evaluate(&tags));
        }
    }

    #[test]
    fn test_tag_set_merge() {
        let mut tags1 = TagSet::new();
        tags1.insert(Tag::with_metadata(
            TagNamespace::Location,
            "building",
            TagValue::String("building-a".into()),
            TagMetadata {
                confidence: 0.8,
                ..Default::default()
            },
        ));

        let mut tags2 = TagSet::new();
        tags2.insert(Tag::with_metadata(
            TagNamespace::Location,
            "building",
            TagValue::String("building-b".into()),
            TagMetadata {
                confidence: 0.9,
                ..Default::default()
            },
        ));
        tags2.insert(Tag::new(TagNamespace::Hardware, "gpu", TagValue::Present));

        // Merge with higher confidence wins
        tags1.merge(&tags2, ConflictResolution::HigherConfidence);

        let building_tag = tags1.get("location:building").unwrap();
        assert_eq!(building_tag.value, TagValue::String("building-b".into()));
        assert!(tags1.contains("hardware:gpu"));
    }

    #[test]
    fn test_tag_expiry() {
        use chrono::Duration;

        let mut tags = TagSet::new();
        let expired_tag = Tag::with_metadata(
            TagNamespace::Ephemeral,
            "idle",
            TagValue::Bool(true),
            TagMetadata::manual("admin").with_expiry(Utc::now() - Duration::hours(1)),
        );
        let valid_tag = Tag::with_metadata(
            TagNamespace::Ephemeral,
            "active",
            TagValue::Bool(true),
            TagMetadata::manual("admin").with_expiry(Utc::now() + Duration::hours(1)),
        );

        tags.insert(expired_tag);
        tags.insert(valid_tag);

        let expired = tags.expired_tags();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].key, "idle");

        let removed = tags.remove_expired();
        assert_eq!(removed.len(), 1);
        assert_eq!(tags.len(), 1);
    }

    #[test]
    fn test_filter_by_namespace() {
        let tags = make_tag_set();
        let hardware_tags = tags.filter_by_namespace(&TagNamespace::Hardware);

        assert!(hardware_tags.contains("hardware:gpu"));
        assert!(hardware_tags.contains("hardware:memory"));
        assert!(!hardware_tags.contains("location:building"));
    }

    #[test]
    fn test_true_false() {
        let tags = make_tag_set();
        assert!(TagExpr::True.evaluate(&tags));
        assert!(!TagExpr::False.evaluate(&tags));

        let expr = TagExpr::parse("true").unwrap();
        assert!(expr.evaluate(&tags));

        let expr = TagExpr::parse("false").unwrap();
        assert!(!expr.evaluate(&tags));
    }
}
