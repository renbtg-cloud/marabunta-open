// Marabunta - Licensed under the MIT License.
//! Log sanitization middleware for blind data (M-8 compliance).
//!
//! When audit events contain classified or sensitive fields (blind envelopes,
//! key material, raw input/output bytes), they must be sanitized before being
//! persisted to general-purpose audit stores. This module provides a
//! [`SanitizationPolicy`] that drives field-level hashing, masking, and
//! redaction so that audit trails remain useful for forensics without leaking
//! classified data.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

// ============================================================================
// SanitizationPolicy
// ============================================================================

/// Controls which fields are hashed, masked, or redacted when an audit
/// event passes through the sanitizer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SanitizationPolicy {
    /// Replace input byte fields with their SHA-256 hex digest.
    pub hash_inputs: bool,
    /// Replace output byte fields with their SHA-256 hex digest.
    pub hash_outputs: bool,
    /// Mask the trailing characters of node identifiers.
    pub mask_node_ids: bool,
    /// Field names that should be hashed if present in event payloads.
    pub sensitive_fields: Vec<String>,
}

impl Default for SanitizationPolicy {
    fn default() -> Self {
        Self {
            hash_inputs: true,
            hash_outputs: true,
            mask_node_ids: true,
            sensitive_fields: vec![
                "input_bytes".to_string(),
                "output_bytes".to_string(),
                "key_material".to_string(),
                "blind_envelope".to_string(),
                "key_capsule".to_string(),
                "wasm_binary".to_string(),
                "decrypted_payload".to_string(),
            ],
        }
    }
}

// ============================================================================
// SanitizedEvent
// ============================================================================

/// A wrapper around a sanitized JSON value, indicating the event has already
/// passed through the sanitization pipeline and is safe for general storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SanitizedEvent {
    /// The sanitized event payload.
    pub inner: serde_json::Value,
    /// Whether sanitization was actually applied (false if policy was no-op).
    pub was_sanitized: bool,
}

// ============================================================================
// Sanitization functions
// ============================================================================

/// Apply `policy` to `event`, returning a new JSON value with sensitive
/// fields replaced by their SHA-256 hex digests and node IDs masked.
///
/// The original value is never mutated.
pub fn sanitize_event(
    event: &serde_json::Value,
    policy: &SanitizationPolicy,
) -> serde_json::Value {
    let mut output = event.clone();

    if let Some(obj) = output.as_object_mut() {
        // Hash sensitive fields.
        for field in &policy.sensitive_fields {
            hash_field_if_present(obj, field);
        }

        // Conditionally hash inputs.
        if policy.hash_inputs {
            hash_field_if_present(obj, "input");
            hash_field_if_present(obj, "input_data");
        }

        // Conditionally hash outputs.
        if policy.hash_outputs {
            hash_field_if_present(obj, "output");
            hash_field_if_present(obj, "output_data");
        }

        // Mask node IDs.
        if policy.mask_node_ids {
            if let Some(node_val) = obj.get_mut("node_id") {
                if let Some(s) = node_val.as_str() {
                    *node_val = serde_json::Value::String(mask_node_id(s));
                }
            }
            if let Some(node_val) = obj.get_mut("submitter_node_id") {
                if let Some(s) = node_val.as_str() {
                    *node_val = serde_json::Value::String(mask_node_id(s));
                }
            }
            if let Some(node_val) = obj.get_mut("executor_node_id") {
                if let Some(s) = node_val.as_str() {
                    *node_val = serde_json::Value::String(mask_node_id(s));
                }
            }
        }
    }

    // Recurse into nested objects.
    if let Some(obj) = output.as_object_mut() {
        let keys: Vec<String> = obj.keys().cloned().collect();
        for key in keys {
            if let Some(child) = obj.get(&key) {
                if child.is_object() {
                    let sanitized_child = sanitize_event(child, policy);
                    obj.insert(key, sanitized_child);
                } else if child.is_array() {
                    let arr: Vec<serde_json::Value> = child
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|item| {
                            if item.is_object() {
                                sanitize_event(item, policy)
                            } else {
                                item.clone()
                            }
                        })
                        .collect();
                    obj.insert(key, serde_json::Value::Array(arr));
                }
            }
        }
    }

    output
}

/// Replace a field's value with the SHA-256 hex digest of its JSON
/// string representation. If the field is absent, this is a no-op.
pub fn hash_field_if_present(
    obj: &mut serde_json::Map<String, serde_json::Value>,
    field: &str,
) {
    if let Some(val) = obj.get(field) {
        let raw = match val {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        let mut hasher = Sha256::new();
        hasher.update(raw.as_bytes());
        let hash = hasher.finalize();
        let hex_str = hex::encode(hash);
        obj.insert(
            field.to_string(),
            serde_json::Value::String(format!("sha256:{}", hex_str)),
        );
    }
}

/// Mask a node identifier by replacing the last 8 characters with
/// asterisks. If the ID is shorter than 8 characters, the entire
/// string is masked.
pub fn mask_node_id(node_id: &str) -> String {
    if node_id.len() <= 8 {
        "*".repeat(node_id.len())
    } else {
        let visible = &node_id[..node_id.len() - 8];
        format!("{}********", visible)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_default_policy_sensitive_fields() {
        let policy = SanitizationPolicy::default();
        assert!(policy.hash_inputs);
        assert!(policy.hash_outputs);
        assert!(policy.mask_node_ids);
        assert_eq!(policy.sensitive_fields.len(), 7);
        assert!(policy.sensitive_fields.contains(&"key_material".to_string()));
        assert!(policy.sensitive_fields.contains(&"blind_envelope".to_string()));
        assert!(policy.sensitive_fields.contains(&"wasm_binary".to_string()));
        assert!(policy.sensitive_fields.contains(&"decrypted_payload".to_string()));
    }

    #[test]
    fn test_mask_node_id_long() {
        let masked = mask_node_id("abcdefghijklmnop");
        assert_eq!(masked, "abcdefgh********");
    }

    #[test]
    fn test_mask_node_id_short() {
        let masked = mask_node_id("abc");
        assert_eq!(masked, "***");
    }

    #[test]
    fn test_mask_node_id_exactly_eight() {
        let masked = mask_node_id("12345678");
        assert_eq!(masked, "********");
    }

    #[test]
    fn test_hash_field_if_present_string() {
        let mut map = serde_json::Map::new();
        map.insert("key_material".to_string(), json!("secret-key-data"));
        hash_field_if_present(&mut map, "key_material");

        let val = map.get("key_material").unwrap().as_str().unwrap();
        assert!(val.starts_with("sha256:"));
        assert_eq!(val.len(), 7 + 64); // "sha256:" + 64 hex chars
    }

    #[test]
    fn test_hash_field_if_present_missing_field() {
        let mut map = serde_json::Map::new();
        map.insert("other".to_string(), json!("value"));
        hash_field_if_present(&mut map, "key_material");
        // No panic, no change
        assert_eq!(map.len(), 1);
        assert_eq!(map.get("other").unwrap(), &json!("value"));
    }

    #[test]
    fn test_hash_field_if_present_numeric() {
        let mut map = serde_json::Map::new();
        map.insert("input_bytes".to_string(), json!(42));
        hash_field_if_present(&mut map, "input_bytes");
        let val = map.get("input_bytes").unwrap().as_str().unwrap();
        assert!(val.starts_with("sha256:"));
    }

    #[test]
    fn test_sanitize_event_hashes_sensitive_fields() {
        let policy = SanitizationPolicy::default();
        let event = json!({
            "event_type": "job_completed",
            "key_material": "top-secret-key",
            "blind_envelope": "encrypted-data-here",
        });

        let sanitized = sanitize_event(&event, &policy);
        let key = sanitized["key_material"].as_str().unwrap();
        assert!(key.starts_with("sha256:"));
        let envelope = sanitized["blind_envelope"].as_str().unwrap();
        assert!(envelope.starts_with("sha256:"));
        // Non-sensitive field unchanged
        assert_eq!(sanitized["event_type"], "job_completed");
    }

    #[test]
    fn test_sanitize_event_masks_node_ids() {
        let policy = SanitizationPolicy::default();
        let event = json!({
            "node_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "event_type": "heartbeat",
        });

        let sanitized = sanitize_event(&event, &policy);
        let node_id = sanitized["node_id"].as_str().unwrap();
        assert!(node_id.ends_with("********"));
        assert_ne!(node_id, "a1b2c3d4-e5f6-7890-abcd-ef1234567890");
    }

    #[test]
    fn test_sanitize_event_no_masking_when_disabled() {
        let policy = SanitizationPolicy {
            hash_inputs: false,
            hash_outputs: false,
            mask_node_ids: false,
            sensitive_fields: vec![],
        };
        let event = json!({
            "node_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "input": "raw-data",
        });

        let sanitized = sanitize_event(&event, &policy);
        assert_eq!(sanitized["node_id"], "a1b2c3d4-e5f6-7890-abcd-ef1234567890");
        assert_eq!(sanitized["input"], "raw-data");
    }

    #[test]
    fn test_sanitize_event_nested_objects() {
        let policy = SanitizationPolicy::default();
        let event = json!({
            "event_type": "blind_result",
            "payload": {
                "key_material": "nested-secret",
                "node_id": "abcdefghijklmnopqrstuvwxyz123456",
            },
        });

        let sanitized = sanitize_event(&event, &policy);
        let nested = &sanitized["payload"];
        let key = nested["key_material"].as_str().unwrap();
        assert!(key.starts_with("sha256:"));
        let node_id = nested["node_id"].as_str().unwrap();
        assert!(node_id.ends_with("********"));
    }

    #[test]
    fn test_sanitized_event_wrapper() {
        let inner = json!({"event": "test"});
        let se = SanitizedEvent {
            inner: inner.clone(),
            was_sanitized: true,
        };
        let json_str = serde_json::to_string(&se).expect("serialize");
        let deserialized: SanitizedEvent = serde_json::from_str(&json_str).expect("deserialize");
        assert!(deserialized.was_sanitized);
        assert_eq!(deserialized.inner, inner);
    }

    #[test]
    fn test_policy_serialization_roundtrip() {
        let policy = SanitizationPolicy::default();
        let json_str = serde_json::to_string(&policy).expect("serialize");
        let deserialized: SanitizationPolicy =
            serde_json::from_str(&json_str).expect("deserialize");
        assert_eq!(deserialized.hash_inputs, policy.hash_inputs);
        assert_eq!(deserialized.sensitive_fields.len(), policy.sensitive_fields.len());
    }
}
