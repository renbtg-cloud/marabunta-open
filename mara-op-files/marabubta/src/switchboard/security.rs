// Marabunta - Licensed under the MIT License.
//! Rate limiting and input sanitization for the Switchboard.
//!
//! Provides sliding-window rate limiting (per-minute and per-hour) and input
//! sanitization for LaTeX, plain text, and image payloads. Dangerous LaTeX
//! commands are blocked to prevent file-system access or shell execution via
//! TeX engines.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use once_cell::sync::Lazy;
use regex::Regex;

use super::config::{MAX_IMAGE_SIZE, MAX_INPUT_SIZE};
use super::errors::{SwitchboardError, SwitchboardResult};

// ============================================================================
// Regex patterns (compiled once)
// ============================================================================

/// Matches dangerous LaTeX commands that could perform file I/O or shell execution.
///
/// Covers: `\input`, `\include`, `\write18`, `\immediate`, `\csname`, `\def`,
/// `\newcommand`. The pattern matches the backslash-command whether or not it is
/// followed by braces.
static DANGEROUS_LATEX_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"\\(input|include|write18|immediate|csname|def|newcommand)\b"
    )
    .expect("dangerous LaTeX regex must compile")
});

/// Matches shell escape sequences commonly used to break out of sandboxed
/// rendering (e.g., backtick execution, $(...) subshells).
static SHELL_ESCAPE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"`[^`]+`|\$\([^)]+\)")
        .expect("shell escape regex must compile")
});

/// Matches ASCII control characters (0x00-0x1F) except newline (0x0A).
static CONTROL_CHARS_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"[\x00-\x09\x0B-\x1F]")
        .expect("control chars regex must compile")
});

// ============================================================================
// Rate limiter
// ============================================================================

/// Distinguishes the two sliding-window buckets tracked per user.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RateLimitKind {
    /// Requests per minute window.
    PerMinute,
    /// Requests per hour window.
    PerHour,
}

impl RateLimitKind {
    /// Returns the duration of the sliding window for this kind.
    fn window(&self) -> Duration {
        match self {
            Self::PerMinute => Duration::from_secs(60),
            Self::PerHour => Duration::from_secs(3600),
        }
    }
}

/// Sliding-window rate limiter backed by a concurrent `DashMap`.
///
/// Each `(user_id, RateLimitKind)` pair tracks a `VecDeque<Instant>` of
/// request timestamps. Expired entries are pruned lazily during `check_and_record`
/// and eagerly via `cleanup`.
pub struct RateLimiter {
    /// Maximum requests allowed in the per-minute window.
    per_minute: u32,
    /// Maximum requests allowed in the per-hour window.
    per_hour: u32,
    /// Sliding-window storage: `(user_id, kind) -> timestamps`.
    windows: DashMap<(String, RateLimitKind), VecDeque<Instant>>,
}

impl RateLimiter {
    /// Creates a new rate limiter with the given per-minute and per-hour caps.
    pub fn new(per_minute: u32, per_hour: u32) -> Self {
        Self {
            per_minute,
            per_hour,
            windows: DashMap::new(),
        }
    }

    /// Checks whether `user_id` is within both rate limits. If so, records the
    /// current timestamp and returns `Ok(())`. If either limit is exceeded,
    /// returns `Err(SwitchboardError::RateLimited)`.
    pub fn check_and_record(&self, user_id: &str) -> SwitchboardResult<()> {
        let now = Instant::now();

        // Check per-minute first.
        self.check_window(user_id, RateLimitKind::PerMinute, self.per_minute, now)?;
        // Check per-hour.
        self.check_window(user_id, RateLimitKind::PerHour, self.per_hour, now)?;

        // Both checks passed — record in both windows.
        self.record(user_id, RateLimitKind::PerMinute, now);
        self.record(user_id, RateLimitKind::PerHour, now);

        Ok(())
    }

    /// Removes all expired timestamps from every window. Intended to be called
    /// periodically from a background task.
    pub fn cleanup(&self) {
        let now = Instant::now();
        // Collect keys that need cleaning to avoid holding multiple guards.
        let keys: Vec<(String, RateLimitKind)> = self
            .windows
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

        for key in keys {
            let window = key.1.window();
            let mut remove_entry = false;

            if let Some(mut entry) = self.windows.get_mut(&key) {
                let deque = entry.value_mut();
                while let Some(&front) = deque.front() {
                    if now.duration_since(front) > window {
                        deque.pop_front();
                    } else {
                        break;
                    }
                }
                if deque.is_empty() {
                    remove_entry = true;
                }
            }

            if remove_entry {
                self.windows.remove(&key);
            }
        }
    }

    // -- private helpers -----------------------------------------------------

    /// Prunes expired entries from a single window and checks whether the
    /// current count exceeds `limit`.
    fn check_window(
        &self,
        user_id: &str,
        kind: RateLimitKind,
        limit: u32,
        now: Instant,
    ) -> SwitchboardResult<()> {
        let key = (user_id.to_string(), kind);
        let window = kind.window();

        if let Some(mut entry) = self.windows.get_mut(&key) {
            let deque = entry.value_mut();
            // Prune expired timestamps from the front.
            while let Some(&front) = deque.front() {
                if now.duration_since(front) > window {
                    deque.pop_front();
                } else {
                    break;
                }
            }
            if deque.len() >= limit as usize {
                return Err(SwitchboardError::RateLimited(format!(
                    "user '{}' exceeded {:?} limit ({} requests)",
                    user_id, kind, limit
                )));
            }
        }

        Ok(())
    }

    /// Appends a timestamp to the given window, creating the entry if absent.
    fn record(&self, user_id: &str, kind: RateLimitKind, now: Instant) {
        let key = (user_id.to_string(), kind);
        self.windows
            .entry(key)
            .or_default()
            .push_back(now);
    }
}

// ============================================================================
// Sanitization functions
// ============================================================================

/// Sanitizes a LaTeX input string.
///
/// 1. Rejects inputs exceeding [`MAX_INPUT_SIZE`].
/// 2. Rejects inputs containing dangerous LaTeX commands (file I/O, shell exec).
/// 3. Rejects inputs containing shell escape sequences.
/// 4. Strips ASCII control characters (except newline).
///
/// Returns the sanitized string on success.
pub fn sanitize_latex(input: &str) -> SwitchboardResult<String> {
    // Size check.
    if input.len() > MAX_INPUT_SIZE {
        return Err(SwitchboardError::InputTooLarge {
            size: input.len(),
            max: MAX_INPUT_SIZE,
        });
    }

    // Dangerous LaTeX commands.
    if let Some(m) = DANGEROUS_LATEX_RE.find(input) {
        return Err(SwitchboardError::SanitizationRejected(format!(
            "dangerous LaTeX command: '{}'",
            m.as_str()
        )));
    }

    // Shell escape sequences.
    if let Some(m) = SHELL_ESCAPE_RE.find(input) {
        return Err(SwitchboardError::SanitizationRejected(format!(
            "shell escape sequence detected: '{}'",
            m.as_str()
        )));
    }

    // Strip control characters.
    let sanitized = CONTROL_CHARS_RE.replace_all(input, "").into_owned();

    Ok(sanitized)
}

/// Validates raw image bytes.
///
/// 1. Rejects images exceeding [`MAX_IMAGE_SIZE`].
/// 2. Checks magic bytes for PNG, JPEG, or WebP.
/// 3. Rejects unknown formats.
pub fn validate_image(data: &[u8]) -> SwitchboardResult<()> {
    // Size check.
    if data.len() > MAX_IMAGE_SIZE {
        return Err(SwitchboardError::InputTooLarge {
            size: data.len(),
            max: MAX_IMAGE_SIZE,
        });
    }

    // Magic bytes check.
    if is_png(data) || is_jpeg(data) || is_webp(data) {
        return Ok(());
    }

    Err(SwitchboardError::ImageError(
        "unknown image format: expected PNG, JPEG, or WebP magic bytes".to_string(),
    ))
}

/// Sanitizes a plain-text input string.
///
/// 1. Rejects inputs exceeding [`MAX_INPUT_SIZE`].
/// 2. Strips ASCII control characters (except newline).
///
/// Returns the sanitized string on success.
pub fn sanitize_plain_text(input: &str) -> SwitchboardResult<String> {
    if input.len() > MAX_INPUT_SIZE {
        return Err(SwitchboardError::InputTooLarge {
            size: input.len(),
            max: MAX_INPUT_SIZE,
        });
    }

    let sanitized = CONTROL_CHARS_RE.replace_all(input, "").into_owned();
    Ok(sanitized)
}

// ============================================================================
// Magic-byte helpers
// ============================================================================

/// PNG magic: `89 50 4E 47` (first 4 bytes).
fn is_png(data: &[u8]) -> bool {
    data.len() >= 4 && data[0] == 0x89 && data[1] == 0x50 && data[2] == 0x4E && data[3] == 0x47
}

/// JPEG magic: `FF D8 FF` (first 3 bytes).
fn is_jpeg(data: &[u8]) -> bool {
    data.len() >= 3 && data[0] == 0xFF && data[1] == 0xD8 && data[2] == 0xFF
}

/// WebP magic: bytes 0-3 = `RIFF`, bytes 8-11 = `WEBP`.
fn is_webp(data: &[u8]) -> bool {
    data.len() >= 12
        && &data[0..4] == b"RIFF"
        && &data[8..12] == b"WEBP"
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -- RateLimiter tests ---------------------------------------------------

    #[test]
    fn test_rate_limiter_allows_within_limit() {
        let limiter = RateLimiter::new(5, 100);
        for _ in 0..5 {
            assert!(limiter.check_and_record("alice").is_ok());
        }
    }

    #[test]
    fn test_rate_limiter_rejects_over_per_minute() {
        let limiter = RateLimiter::new(3, 100);
        for _ in 0..3 {
            limiter.check_and_record("bob").unwrap();
        }
        let err = limiter.check_and_record("bob").unwrap_err();
        match err {
            SwitchboardError::RateLimited(msg) => {
                assert!(msg.contains("bob"));
                assert!(msg.contains("PerMinute"));
            }
            other => panic!("expected RateLimited, got: {:?}", other),
        }
    }

    #[test]
    fn test_rate_limiter_per_hour_limit() {
        // Set per-minute high enough that it won't trigger, but per-hour low.
        let limiter = RateLimiter::new(100, 5);
        for _ in 0..5 {
            limiter.check_and_record("carol").unwrap();
        }
        let err = limiter.check_and_record("carol").unwrap_err();
        match err {
            SwitchboardError::RateLimited(msg) => {
                assert!(msg.contains("carol"));
                assert!(msg.contains("PerHour"));
            }
            other => panic!("expected RateLimited, got: {:?}", other),
        }
    }

    #[test]
    fn test_rate_limiter_independent_users() {
        let limiter = RateLimiter::new(2, 100);
        limiter.check_and_record("user_a").unwrap();
        limiter.check_and_record("user_a").unwrap();
        // user_a is at limit, but user_b should be fine.
        assert!(limiter.check_and_record("user_b").is_ok());
        assert!(limiter.check_and_record("user_a").is_err());
    }

    #[test]
    fn test_rate_limiter_cleanup_removes_expired() {
        let limiter = RateLimiter::new(1000, 1000);
        // Insert a synthetic expired entry by manipulating the internal map.
        let key = ("cleanup_user".to_string(), RateLimitKind::PerMinute);
        let mut deque = VecDeque::new();
        // Push a timestamp 120 seconds in the past (well beyond the 60s window).
        deque.push_back(Instant::now() - Duration::from_secs(120));
        limiter.windows.insert(key.clone(), deque);

        assert!(limiter.windows.contains_key(&key));
        limiter.cleanup();
        // The expired entry should have been removed.
        assert!(!limiter.windows.contains_key(&key));
    }

    #[test]
    fn test_rate_limiter_cleanup_keeps_valid() {
        let limiter = RateLimiter::new(1000, 1000);
        limiter.check_and_record("valid_user").unwrap();

        let key = ("valid_user".to_string(), RateLimitKind::PerMinute);
        assert!(limiter.windows.contains_key(&key));
        limiter.cleanup();
        // Recent entry should survive cleanup.
        assert!(limiter.windows.contains_key(&key));
    }

    // -- sanitize_latex tests ------------------------------------------------

    #[test]
    fn test_sanitize_latex_rejects_dangerous_commands() {
        let dangerous_inputs = [
            r"\input{secrets.tex}",
            r"\include{malicious}",
            r"\write18{rm -rf /}",
            r"\immediate\write18{cmd}",
            r"\csname endcsname",
            r"\def\foo{bar}",
            r"\newcommand{\x}{y}",
        ];
        for input in &dangerous_inputs {
            let result = sanitize_latex(input);
            assert!(
                result.is_err(),
                "expected rejection for '{}', got Ok",
                input
            );
            match result.unwrap_err() {
                SwitchboardError::SanitizationRejected(_) => {}
                other => panic!("expected SanitizationRejected, got: {:?}", other),
            }
        }
    }

    #[test]
    fn test_sanitize_latex_rejects_shell_escapes() {
        let result = sanitize_latex(r"`whoami`");
        assert!(result.is_err());
        match result.unwrap_err() {
            SwitchboardError::SanitizationRejected(msg) => {
                assert!(msg.contains("shell escape"));
            }
            other => panic!("expected SanitizationRejected, got: {:?}", other),
        }

        let result2 = sanitize_latex(r"$(cat /etc/passwd)");
        assert!(result2.is_err());
    }

    #[test]
    fn test_sanitize_latex_accepts_valid_input() {
        let valid = r"\frac{1}{2} + \int_0^1 x\,dx = \sum_{n=0}^{\infty} a_n";
        let result = sanitize_latex(valid).unwrap();
        assert_eq!(result, valid);
    }

    #[test]
    fn test_sanitize_latex_strips_control_chars() {
        let input = "hello\x00world\x07test\nnewline";
        let result = sanitize_latex(input).unwrap();
        assert_eq!(result, "helloworldtest\nnewline");
    }

    #[test]
    fn test_sanitize_latex_rejects_oversized_input() {
        let big = "x".repeat(MAX_INPUT_SIZE + 1);
        let result = sanitize_latex(&big);
        match result.unwrap_err() {
            SwitchboardError::InputTooLarge { size, max } => {
                assert_eq!(size, MAX_INPUT_SIZE + 1);
                assert_eq!(max, MAX_INPUT_SIZE);
            }
            other => panic!("expected InputTooLarge, got: {:?}", other),
        }
    }

    // -- validate_image tests ------------------------------------------------

    #[test]
    fn test_validate_image_accepts_png() {
        let mut png = vec![0x89, 0x50, 0x4E, 0x47]; // PNG magic
        png.extend_from_slice(&[0; 100]); // some body bytes
        assert!(validate_image(&png).is_ok());
    }

    #[test]
    fn test_validate_image_accepts_jpeg() {
        let mut jpeg = vec![0xFF, 0xD8, 0xFF]; // JPEG magic
        jpeg.extend_from_slice(&[0; 100]);
        assert!(validate_image(&jpeg).is_ok());
    }

    #[test]
    fn test_validate_image_accepts_webp() {
        let mut webp = vec![0; 12];
        webp[0..4].copy_from_slice(b"RIFF");
        webp[8..12].copy_from_slice(b"WEBP");
        assert!(validate_image(&webp).is_ok());
    }

    #[test]
    fn test_validate_image_rejects_unknown_format() {
        let garbage = vec![0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07];
        let err = validate_image(&garbage).unwrap_err();
        match err {
            SwitchboardError::ImageError(msg) => {
                assert!(msg.contains("unknown image format"));
            }
            other => panic!("expected ImageError, got: {:?}", other),
        }
    }

    #[test]
    fn test_validate_image_rejects_oversized() {
        let big = vec![0xFF, 0xD8, 0xFF]; // valid JPEG header ...
        let mut data = big;
        data.resize(MAX_IMAGE_SIZE + 1, 0x00); // ... but too large
        let err = validate_image(&data).unwrap_err();
        match err {
            SwitchboardError::InputTooLarge { size, max } => {
                assert_eq!(size, MAX_IMAGE_SIZE + 1);
                assert_eq!(max, MAX_IMAGE_SIZE);
            }
            other => panic!("expected InputTooLarge, got: {:?}", other),
        }
    }

    // -- sanitize_plain_text tests -------------------------------------------

    #[test]
    fn test_sanitize_plain_text_basic() {
        let input = "Hello, world!\nSecond line.";
        let result = sanitize_plain_text(input).unwrap();
        assert_eq!(result, input);
    }

    #[test]
    fn test_sanitize_plain_text_strips_control_chars() {
        let input = "abc\x00def\x1Fghi\njkl";
        let result = sanitize_plain_text(input).unwrap();
        assert_eq!(result, "abcdefghi\njkl");
    }

    #[test]
    fn test_sanitize_plain_text_rejects_oversized() {
        let big = "a".repeat(MAX_INPUT_SIZE + 1);
        let result = sanitize_plain_text(&big);
        assert!(result.is_err());
        match result.unwrap_err() {
            SwitchboardError::InputTooLarge { size, max } => {
                assert_eq!(size, MAX_INPUT_SIZE + 1);
                assert_eq!(max, MAX_INPUT_SIZE);
            }
            other => panic!("expected InputTooLarge, got: {:?}", other),
        }
    }
}
