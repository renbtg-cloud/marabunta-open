// Marabunta - Licensed under the MIT License.
//! Durable timer service for workflow scheduled transitions (W3C).
//!
//! Provides `TimerService`, an in-memory timer store backed by `DashMap`
//! that can schedule, cancel, and poll for expired timers. Timer
//! durations support multiple formats:
//!
//! - Simple units: `"48h"`, `"30m"`, `"3600s"`, `"7d"`
//! - Compound: `"2d12h"`, `"1h30m"`, `"2d6h30m"`
//! - Context reference: `"at deadline"` reads an RFC 3339 datetime from
//!   the workflow context field named `deadline`
//!
//! All timer state is stored in the `ActiveTimer` and `TimerStatus` types
//! defined in `super::types`, which are gossip-propagated for durability.

use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde_json::Value;
use tracing::info;
use uuid::Uuid;

use super::types::{ActiveTimer, TimerStatus};

// ============================================================================
// Duration Parsing
// ============================================================================

/// Parse a timer duration specification into an absolute `DateTime<Utc>`.
///
/// Supported formats:
/// - `"48h"` — 48 hours from now
/// - `"30m"` — 30 minutes from now
/// - `"3600s"` — 3600 seconds from now
/// - `"7d"` — 7 days from now
/// - `"2d12h"` — compound: 2 days and 12 hours from now
/// - `"at field_name"` — read an RFC 3339 datetime from `context[field_name]`
///
/// Returns an error if the spec is empty, produces zero duration, or
/// references a context field that doesn't exist or isn't a valid datetime.
pub fn parse_timer_duration(spec: &str, context: &Value) -> Result<DateTime<Utc>> {
    let spec = spec.trim();

    // Handle "at {field_name}" context references.
    if spec.starts_with("at ") {
        let field_name = spec[3..].trim();
        if field_name.is_empty() {
            bail!("empty field name in 'at' timer spec");
        }
        let field_value = context
            .get(field_name)
            .ok_or_else(|| anyhow!("context field '{}' not found", field_name))?;
        let datetime_str = field_value
            .as_str()
            .ok_or_else(|| anyhow!("context field '{}' is not a string", field_name))?;
        let dt = DateTime::parse_from_rfc3339(datetime_str)
            .map_err(|e| anyhow!("invalid RFC 3339 datetime in '{}': {}", field_name, e))?;
        return Ok(dt.with_timezone(&Utc));
    }

    // Parse compound duration: "2d12h30m45s"
    let total_secs = parse_compound_duration(spec)?;
    if total_secs == 0 {
        bail!("timer duration must be greater than zero");
    }

    let duration = chrono::Duration::seconds(total_secs as i64);
    Ok(Utc::now() + duration)
}

/// Parse a compound duration string like `"2d12h30m45s"` into total seconds.
///
/// Supported units: `d` (days), `h` (hours), `m` (minutes), `s` (seconds).
/// Components can appear in any order and are additive.
fn parse_compound_duration(spec: &str) -> Result<u64> {
    if spec.is_empty() {
        bail!("empty timer duration specification");
    }

    let mut total_secs: u64 = 0;
    let mut num_buf = String::new();
    let mut found_any_unit = false;

    for ch in spec.chars() {
        if ch.is_ascii_digit() {
            num_buf.push(ch);
        } else {
            if num_buf.is_empty() {
                bail!("unexpected character '{}' without preceding number in '{}'", ch, spec);
            }
            let n: u64 = num_buf
                .parse()
                .map_err(|e| anyhow!("invalid number in duration '{}': {}", spec, e))?;
            num_buf.clear();

            let multiplier = match ch {
                'd' => 86400,
                'h' => 3600,
                'm' => 60,
                's' => 1,
                _ => bail!("unknown duration unit '{}' in '{}'", ch, spec),
            };
            total_secs += n * multiplier;
            found_any_unit = true;
        }
    }

    // Handle trailing number without unit (treat as error for clarity).
    if !num_buf.is_empty() {
        bail!(
            "duration '{}' has a number without a unit suffix (use d/h/m/s)",
            spec
        );
    }

    if !found_any_unit {
        bail!("no valid duration components found in '{}'", spec);
    }

    Ok(total_secs)
}

// ============================================================================
// TimerService
// ============================================================================

/// In-memory timer service for workflow scheduled transitions.
///
/// Timers are stored in a concurrent `DashMap` keyed by their `timer_id`.
/// The service supports scheduling, cancellation, expiry checking, and
/// per-instance queries.
pub struct TimerService {
    /// All timers, keyed by timer_id.
    timers: Arc<DashMap<Uuid, ActiveTimer>>,
    /// Node ID that created these timers (stamped into `created_by_node`).
    node_id: String,
}

impl TimerService {
    /// Create a new timer service for the given node.
    pub fn new(node_id: String) -> Self {
        Self {
            timers: Arc::new(DashMap::new()),
            node_id,
        }
    }

    /// Schedule a new timer.
    ///
    /// Parses `duration_spec` to compute the absolute fire time, creates
    /// an `ActiveTimer` in `Pending` status, and stores it in the map.
    pub fn schedule_timer(
        &self,
        instance_id: Uuid,
        trigger_name: &str,
        transition: &str,
        duration_spec: &str,
        context: &Value,
    ) -> Result<ActiveTimer> {
        let fire_at = parse_timer_duration(duration_spec, context)?;
        let now = Utc::now();
        let timer = ActiveTimer {
            timer_id: Uuid::new_v4(),
            instance_id,
            trigger_name: trigger_name.to_string(),
            transition: transition.to_string(),
            fire_at,
            created_at: now,
            created_by_node: self.node_id.clone(),
            status: TimerStatus::Pending,
        };

        info!(
            timer_id = %timer.timer_id,
            instance_id = %instance_id,
            trigger = trigger_name,
            fire_at = %fire_at,
            "scheduled timer"
        );

        self.timers.insert(timer.timer_id, timer.clone());
        Ok(timer)
    }

    /// Cancel a single timer by ID.
    ///
    /// Sets the timer's status to `Cancelled`. Returns an error if the
    /// timer does not exist. Cancelling an already-cancelled timer is
    /// a no-op (idempotent).
    pub fn cancel_timer(&self, timer_id: Uuid) -> Result<()> {
        let mut entry = self
            .timers
            .get_mut(&timer_id)
            .ok_or_else(|| anyhow!("timer {} not found", timer_id))?;

        if entry.status != TimerStatus::Cancelled {
            entry.status = TimerStatus::Cancelled;
            info!(timer_id = %timer_id, "cancelled timer");
        }
        Ok(())
    }

    /// Cancel all pending timers for a given workflow instance.
    ///
    /// Returns the number of timers that were actually cancelled
    /// (skips already-cancelled or fired timers).
    pub fn cancel_all_for_instance(&self, instance_id: Uuid) -> Result<usize> {
        let mut cancelled = 0;
        for mut entry in self.timers.iter_mut() {
            if entry.instance_id == instance_id && entry.status == TimerStatus::Pending {
                entry.status = TimerStatus::Cancelled;
                cancelled += 1;
            }
        }
        if cancelled > 0 {
            info!(
                instance_id = %instance_id,
                count = cancelled,
                "cancelled all timers for instance"
            );
        }
        Ok(cancelled)
    }

    /// Check for expired timers and mark them as fired.
    ///
    /// Scans all `Pending` timers whose `fire_at` is at or before `now`.
    /// Each expired timer's status is set to `Fired` and a clone is
    /// returned to the caller for transition execution.
    pub fn check_expired_timers(&self) -> Result<Vec<ActiveTimer>> {
        let now = Utc::now();
        let mut expired = Vec::new();

        for mut entry in self.timers.iter_mut() {
            if entry.status == TimerStatus::Pending && entry.fire_at <= now {
                entry.status = TimerStatus::Fired;
                expired.push(entry.clone());
            }
        }

        if !expired.is_empty() {
            info!(count = expired.len(), "found expired timers");
        }
        Ok(expired)
    }

    /// Get all pending timers for a given workflow instance.
    pub fn get_pending_for_instance(&self, instance_id: Uuid) -> Vec<ActiveTimer> {
        self.timers
            .iter()
            .filter(|entry| entry.instance_id == instance_id && entry.status == TimerStatus::Pending)
            .map(|entry| entry.value().clone())
            .collect()
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
    fn test_parse_duration_hours() {
        let ctx = json!({});
        let result = parse_timer_duration("48h", &ctx).unwrap();
        let expected_approx = Utc::now() + chrono::Duration::hours(48);
        // Allow 2 second tolerance for test execution time.
        let diff = (result - expected_approx).num_seconds().unsigned_abs();
        assert!(diff < 2, "expected ~48h from now, got diff={}s", diff);
    }

    #[test]
    fn test_parse_duration_compound() {
        let ctx = json!({});
        let result = parse_timer_duration("2d12h", &ctx).unwrap();
        let expected_secs = 2 * 86400 + 12 * 3600;
        let expected_approx = Utc::now() + chrono::Duration::seconds(expected_secs);
        let diff = (result - expected_approx).num_seconds().unsigned_abs();
        assert!(diff < 2, "expected ~2d12h from now, got diff={}s", diff);
    }

    #[test]
    fn test_parse_duration_minutes() {
        let ctx = json!({});
        let result = parse_timer_duration("30m", &ctx).unwrap();
        let expected_approx = Utc::now() + chrono::Duration::minutes(30);
        let diff = (result - expected_approx).num_seconds().unsigned_abs();
        assert!(diff < 2, "expected ~30m from now, got diff={}s", diff);
    }

    #[test]
    fn test_parse_duration_seconds() {
        let ctx = json!({});
        let result = parse_timer_duration("3600s", &ctx).unwrap();
        let expected_approx = Utc::now() + chrono::Duration::seconds(3600);
        let diff = (result - expected_approx).num_seconds().unsigned_abs();
        assert!(diff < 2, "expected ~3600s from now, got diff={}s", diff);
    }

    #[test]
    fn test_parse_duration_days() {
        let ctx = json!({});
        let result = parse_timer_duration("7d", &ctx).unwrap();
        let expected_approx = Utc::now() + chrono::Duration::days(7);
        let diff = (result - expected_approx).num_seconds().unsigned_abs();
        assert!(diff < 2, "expected ~7d from now, got diff={}s", diff);
    }

    #[test]
    fn test_parse_duration_at_reference() {
        let target = "2030-06-15T12:00:00Z";
        let ctx = json!({"deadline": target});
        let result = parse_timer_duration("at deadline", &ctx).unwrap();
        let expected = DateTime::parse_from_rfc3339(target)
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_duration_zero_error() {
        let ctx = json!({});
        let result = parse_timer_duration("0h", &ctx);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("greater than zero"),
            "unexpected error: {}",
            err_msg
        );
    }

    #[test]
    fn test_timer_status_transitions() {
        let svc = TimerService::new("test-node".to_string());
        let instance_id = Uuid::new_v4();
        let ctx = json!({});

        // Schedule a timer that fires 1 second from now.
        let timer = svc
            .schedule_timer(instance_id, "review_deadline", "escalate", "1s", &ctx)
            .unwrap();
        assert_eq!(timer.status, TimerStatus::Pending);

        // Immediately check — should not be expired yet (fires ~1s from now).
        // But due to timing, let's just verify the check doesn't crash.
        let _expired = svc.check_expired_timers().unwrap();

        // Cancel the timer.
        svc.cancel_timer(timer.timer_id).unwrap();
        let entry = svc.timers.get(&timer.timer_id).unwrap();
        assert_eq!(entry.status, TimerStatus::Cancelled);
    }

    #[test]
    fn test_timer_serde_roundtrip() {
        let timer = ActiveTimer {
            timer_id: Uuid::new_v4(),
            instance_id: Uuid::new_v4(),
            trigger_name: "deadline".to_string(),
            transition: "escalate".to_string(),
            fire_at: Utc::now() + chrono::Duration::hours(24),
            created_at: Utc::now(),
            created_by_node: "node-1".to_string(),
            status: TimerStatus::Pending,
        };

        let json_str = serde_json::to_string(&timer).unwrap();
        let deserialized: ActiveTimer = serde_json::from_str(&json_str).unwrap();
        assert_eq!(timer.timer_id, deserialized.timer_id);
        assert_eq!(timer.instance_id, deserialized.instance_id);
        assert_eq!(timer.trigger_name, deserialized.trigger_name);
        assert_eq!(timer.transition, deserialized.transition);
        assert_eq!(timer.status, deserialized.status);
    }

    #[test]
    fn test_timer_cancel_idempotent() {
        let svc = TimerService::new("test-node".to_string());
        let instance_id = Uuid::new_v4();
        let ctx = json!({});

        let timer = svc
            .schedule_timer(instance_id, "reminder", "nudge", "1h", &ctx)
            .unwrap();

        // Cancel twice — should not error.
        svc.cancel_timer(timer.timer_id).unwrap();
        svc.cancel_timer(timer.timer_id).unwrap();

        let entry = svc.timers.get(&timer.timer_id).unwrap();
        assert_eq!(entry.status, TimerStatus::Cancelled);
    }
}
