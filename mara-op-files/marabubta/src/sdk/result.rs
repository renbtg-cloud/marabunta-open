// Marabunta - Licensed under the MIT License.
//! Type-Safe Result Deserialization for the Marabunta SDK
//!
//! This module provides utilities for safely deserializing job results
//! with proper error handling and type conversion.
//!
//! # Example
//!
//! ```rust
//! use marabunta_compute::sdk::result::{TypedResult, ResultExt};
//! use serde::Deserialize;
//!
//! #[derive(Debug, Deserialize)]
//! struct ComputeResult {
//!     value: f64,
//!     iterations: u32,
//! }
//!
//! // Parse a typed result
//! let raw = serde_json::json!({"value": 3.14, "iterations": 100});
//! let result: TypedResult<ComputeResult> = TypedResult::from_json(raw).unwrap();
//!
//! println!("Value: {}", result.value().value);
//! ```

use std::marker::PhantomData;

use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;

/// Errors that can occur during result handling
#[derive(Error, Debug, Clone)]
pub enum ResultError {
    #[error("Failed to deserialize result: {0}")]
    DeserializationFailed(String),

    #[error("Result is empty")]
    Empty,

    #[error("Result type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    #[error("Missing field: {0}")]
    MissingField(String),

    #[error("Invalid field value for '{field}': {message}")]
    InvalidField { field: String, message: String },
}

/// A type-safe wrapper around job results
#[derive(Debug, Clone)]
pub struct TypedResult<T> {
    inner: T,
    raw: serde_json::Value,
}

impl<T> TypedResult<T> {
    /// Get a reference to the typed value
    pub fn value(&self) -> &T {
        &self.inner
    }

    /// Get the raw JSON value
    pub fn raw(&self) -> &serde_json::Value {
        &self.raw
    }

    /// Consume and return the inner value
    pub fn into_inner(self) -> T {
        self.inner
    }

    /// Consume and return both inner value and raw JSON
    pub fn into_parts(self) -> (T, serde_json::Value) {
        (self.inner, self.raw)
    }
}

impl<T: DeserializeOwned> TypedResult<T> {
    /// Create a typed result from JSON value
    pub fn from_json(json: serde_json::Value) -> Result<Self, ResultError> {
        let inner: T = serde_json::from_value(json.clone())
            .map_err(|e| ResultError::DeserializationFailed(e.to_string()))?;

        Ok(Self { inner, raw: json })
    }

    /// Create a typed result from a JSON string
    pub fn from_json_str(json_str: &str) -> Result<Self, ResultError> {
        let json: serde_json::Value = serde_json::from_str(json_str)
            .map_err(|e| ResultError::DeserializationFailed(e.to_string()))?;
        Self::from_json(json)
    }

    /// Create a typed result from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ResultError> {
        let json: serde_json::Value = serde_json::from_slice(bytes)
            .map_err(|e| ResultError::DeserializationFailed(e.to_string()))?;
        Self::from_json(json)
    }
}

impl<T: Serialize> TypedResult<T> {
    /// Create a typed result from a value (for creating results)
    pub fn from_value(value: T) -> Result<Self, ResultError> {
        let raw = serde_json::to_value(&value)
            .map_err(|e| ResultError::DeserializationFailed(e.to_string()))?;
        Ok(Self { inner: value, raw })
    }
}

/// Extension trait for working with optional results
pub trait ResultExt<T> {
    /// Try to extract a typed value, returning None if deserialization fails
    fn try_typed<U: DeserializeOwned>(&self) -> Option<U>;

    /// Get a field from the result
    fn get_field<U: DeserializeOwned>(&self, field: &str) -> Result<U, ResultError>;

    /// Check if a field exists
    fn has_field(&self, field: &str) -> bool;
}

impl ResultExt<serde_json::Value> for serde_json::Value {
    fn try_typed<U: DeserializeOwned>(&self) -> Option<U> {
        serde_json::from_value(self.clone()).ok()
    }

    fn get_field<U: DeserializeOwned>(&self, field: &str) -> Result<U, ResultError> {
        let value = self.get(field).ok_or_else(|| ResultError::MissingField(field.to_string()))?;
        serde_json::from_value(value.clone()).map_err(|e| ResultError::InvalidField {
            field: field.to_string(),
            message: e.to_string(),
        })
    }

    fn has_field(&self, field: &str) -> bool {
        self.get(field).is_some()
    }
}

/// A result that can be one of several types
#[derive(Debug, Clone)]
pub enum MultiTypeResult {
    /// String result
    String(String),
    /// Integer result
    Integer(i64),
    /// Float result
    Float(f64),
    /// Boolean result
    Boolean(bool),
    /// Array result
    Array(Vec<serde_json::Value>),
    /// Object result
    Object(serde_json::Map<String, serde_json::Value>),
    /// Null result
    Null,
}

impl MultiTypeResult {
    /// Create from a JSON value
    pub fn from_json(value: serde_json::Value) -> Self {
        match value {
            serde_json::Value::String(s) => MultiTypeResult::String(s),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    MultiTypeResult::Integer(i)
                } else if let Some(f) = n.as_f64() {
                    MultiTypeResult::Float(f)
                } else {
                    MultiTypeResult::Float(0.0)
                }
            }
            serde_json::Value::Bool(b) => MultiTypeResult::Boolean(b),
            serde_json::Value::Array(a) => MultiTypeResult::Array(a),
            serde_json::Value::Object(o) => MultiTypeResult::Object(o),
            serde_json::Value::Null => MultiTypeResult::Null,
        }
    }

    /// Try to get as a string
    pub fn as_string(&self) -> Option<&str> {
        match self {
            MultiTypeResult::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as an integer
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            MultiTypeResult::Integer(i) => Some(*i),
            MultiTypeResult::Float(f) => Some(*f as i64),
            _ => None,
        }
    }

    /// Try to get as a float
    pub fn as_float(&self) -> Option<f64> {
        match self {
            MultiTypeResult::Float(f) => Some(*f),
            MultiTypeResult::Integer(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Try to get as a boolean
    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            MultiTypeResult::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// Try to get as an array
    pub fn as_array(&self) -> Option<&Vec<serde_json::Value>> {
        match self {
            MultiTypeResult::Array(a) => Some(a),
            _ => None,
        }
    }

    /// Try to get as an object
    pub fn as_object(&self) -> Option<&serde_json::Map<String, serde_json::Value>> {
        match self {
            MultiTypeResult::Object(o) => Some(o),
            _ => None,
        }
    }

    /// Check if null
    pub fn is_null(&self) -> bool {
        matches!(self, MultiTypeResult::Null)
    }

    /// Get the type name
    pub fn type_name(&self) -> &'static str {
        match self {
            MultiTypeResult::String(_) => "string",
            MultiTypeResult::Integer(_) => "integer",
            MultiTypeResult::Float(_) => "float",
            MultiTypeResult::Boolean(_) => "boolean",
            MultiTypeResult::Array(_) => "array",
            MultiTypeResult::Object(_) => "object",
            MultiTypeResult::Null => "null",
        }
    }
}

/// A validated result with schema checking
#[derive(Debug, Clone)]
pub struct ValidatedResult<T> {
    inner: T,
    validation_errors: Vec<String>,
}

impl<T> ValidatedResult<T> {
    /// Create a new validated result
    pub fn new(value: T) -> Self {
        Self {
            inner: value,
            validation_errors: Vec::new(),
        }
    }

    /// Create with validation errors
    pub fn with_errors(value: T, errors: Vec<String>) -> Self {
        Self {
            inner: value,
            validation_errors: errors,
        }
    }

    /// Check if the result is valid (no errors)
    pub fn is_valid(&self) -> bool {
        self.validation_errors.is_empty()
    }

    /// Get validation errors
    pub fn errors(&self) -> &[String] {
        &self.validation_errors
    }

    /// Get the value
    pub fn value(&self) -> &T {
        &self.inner
    }

    /// Get value only if valid
    pub fn valid_value(&self) -> Option<&T> {
        if self.is_valid() {
            Some(&self.inner)
        } else {
            None
        }
    }

    /// Consume and return the inner value
    pub fn into_inner(self) -> T {
        self.inner
    }
}

/// Builder for extracting and transforming results
pub struct ResultExtractor<T> {
    json: serde_json::Value,
    _phantom: PhantomData<T>,
}

impl<T: DeserializeOwned> ResultExtractor<T> {
    /// Create a new result extractor
    pub fn new(json: serde_json::Value) -> Self {
        Self {
            json,
            _phantom: PhantomData,
        }
    }

    /// Extract the full result
    pub fn extract(self) -> Result<T, ResultError> {
        serde_json::from_value(self.json)
            .map_err(|e| ResultError::DeserializationFailed(e.to_string()))
    }

    /// Extract a specific field
    pub fn field<U: DeserializeOwned>(self, name: &str) -> Result<U, ResultError> {
        self.json.get_field(name)
    }

    /// Map the result with a transformation
    pub fn map<U, F>(self, f: F) -> Result<U, ResultError>
    where
        F: FnOnce(T) -> U,
    {
        let value: T = serde_json::from_value(self.json)
            .map_err(|e| ResultError::DeserializationFailed(e.to_string()))?;
        Ok(f(value))
    }

    /// Extract with a default value on failure
    pub fn extract_or(self, default: T) -> T {
        serde_json::from_value(self.json).unwrap_or(default)
    }

    /// Extract with a default provided by a function
    pub fn extract_or_else<F: FnOnce() -> T>(self, f: F) -> T {
        serde_json::from_value(self.json).unwrap_or_else(|_| f())
    }
}

/// Macro for easy typed result extraction
#[macro_export]
macro_rules! extract_result {
    ($json:expr, $type:ty) => {
        $crate::sdk::result::TypedResult::<$type>::from_json($json)
    };
    ($json:expr, $field:expr, $type:ty) => {
        $crate::sdk::result::ResultExt::get_field::<$type>(&$json, $field)
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use serde_json::json;

    #[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
    struct TestResult {
        value: i32,
        name: String,
    }

    #[test]
    fn test_typed_result_from_json() {
        let json = json!({"value": 42, "name": "test"});
        let result: TypedResult<TestResult> = TypedResult::from_json(json).unwrap();

        assert_eq!(result.value().value, 42);
        assert_eq!(result.value().name, "test");
    }

    #[test]
    fn test_typed_result_from_json_str() {
        let json_str = r#"{"value": 42, "name": "test"}"#;
        let result: TypedResult<TestResult> = TypedResult::from_json_str(json_str).unwrap();

        assert_eq!(result.value().value, 42);
    }

    #[test]
    fn test_typed_result_from_bytes() {
        let bytes = b"{\"value\": 42, \"name\": \"test\"}";
        let result: TypedResult<TestResult> = TypedResult::from_bytes(bytes).unwrap();

        assert_eq!(result.value().value, 42);
    }

    #[test]
    fn test_typed_result_from_value() {
        let value = TestResult {
            value: 42,
            name: "test".to_string(),
        };
        let result: TypedResult<TestResult> = TypedResult::from_value(value).unwrap();

        assert_eq!(result.value().value, 42);
        assert_eq!(result.raw()["value"], 42);
    }

    #[test]
    fn test_typed_result_into_inner() {
        let json = json!({"value": 42, "name": "test"});
        let result: TypedResult<TestResult> = TypedResult::from_json(json).unwrap();
        let inner = result.into_inner();

        assert_eq!(inner.value, 42);
    }

    #[test]
    fn test_typed_result_deserialization_error() {
        let json = json!({"wrong_field": 42});
        let result = TypedResult::<TestResult>::from_json(json);

        assert!(result.is_err());
        assert!(matches!(result, Err(ResultError::DeserializationFailed(_))));
    }

    #[test]
    fn test_result_ext_try_typed() {
        let json = json!({"value": 42, "name": "test"});
        let result: Option<TestResult> = json.try_typed();

        assert!(result.is_some());
        assert_eq!(result.unwrap().value, 42);
    }

    #[test]
    fn test_result_ext_get_field() {
        let json = json!({"value": 42, "name": "test"});
        let value: i32 = json.get_field("value").unwrap();

        assert_eq!(value, 42);
    }

    #[test]
    fn test_result_ext_missing_field() {
        let json = json!({"value": 42});
        let result: Result<String, _> = json.get_field("missing");

        assert!(matches!(result, Err(ResultError::MissingField(_))));
    }

    #[test]
    fn test_result_ext_has_field() {
        let json = json!({"value": 42, "name": "test"});

        assert!(json.has_field("value"));
        assert!(!json.has_field("missing"));
    }

    #[test]
    fn test_multi_type_result() {
        let string_result = MultiTypeResult::from_json(json!("hello"));
        assert_eq!(string_result.as_string(), Some("hello"));
        assert_eq!(string_result.type_name(), "string");

        let int_result = MultiTypeResult::from_json(json!(42));
        assert_eq!(int_result.as_integer(), Some(42));

        let float_result = MultiTypeResult::from_json(json!(3.14));
        assert_eq!(float_result.as_float(), Some(3.14));

        let bool_result = MultiTypeResult::from_json(json!(true));
        assert_eq!(bool_result.as_boolean(), Some(true));

        let null_result = MultiTypeResult::from_json(json!(null));
        assert!(null_result.is_null());
    }

    #[test]
    fn test_validated_result() {
        let result = ValidatedResult::new(42);
        assert!(result.is_valid());
        assert_eq!(*result.value(), 42);

        let result_with_errors = ValidatedResult::with_errors(42, vec!["error1".to_string()]);
        assert!(!result_with_errors.is_valid());
        assert!(result_with_errors.valid_value().is_none());
    }

    #[test]
    fn test_result_extractor() {
        let json = json!({"value": 42, "name": "test"});

        // Extract full result
        let extractor: ResultExtractor<TestResult> = ResultExtractor::new(json.clone());
        let result = extractor.extract().unwrap();
        assert_eq!(result.value, 42);

        // Extract field
        let extractor: ResultExtractor<TestResult> = ResultExtractor::new(json.clone());
        let name: String = extractor.field("name").unwrap();
        assert_eq!(name, "test");

        // Map result
        let extractor: ResultExtractor<TestResult> = ResultExtractor::new(json.clone());
        let doubled = extractor.map(|r| r.value * 2).unwrap();
        assert_eq!(doubled, 84);
    }

    #[test]
    fn test_result_extractor_with_default() {
        let json = json!({"wrong": "data"});

        let extractor: ResultExtractor<TestResult> = ResultExtractor::new(json);
        let result = extractor.extract_or(TestResult {
            value: 0,
            name: "default".to_string(),
        });

        assert_eq!(result.value, 0);
        assert_eq!(result.name, "default");
    }

    #[test]
    fn test_extract_result_macro() {
        let json = json!({"value": 42, "name": "test"});

        let result = extract_result!(json.clone(), TestResult);
        assert!(result.is_ok());

        let value: i32 = extract_result!(json, "value", i32).unwrap();
        assert_eq!(value, 42);
    }
}
