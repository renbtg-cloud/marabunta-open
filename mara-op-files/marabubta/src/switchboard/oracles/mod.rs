// Marabunta - Licensed under the MIT License.
//! Oracle trait and cascade solver for mathematical expression evaluation.
//!
//! Defines the `MathOracle` trait that compute backends implement to solve
//! mathematical expressions, and provides `cascade_solve` which tries oracles
//! in priority order with per-call timeouts and failure tracking.

pub mod sympy;
pub mod wolfram;

use std::fmt;
use std::time::{Duration, Instant};

use crate::switchboard::failure::{FailureContext, FailureLevel};
use crate::switchboard::types::{Category, NormalizedExpression, Operation};

// ============================================================================
// OracleError
// ============================================================================

/// Error returned by an oracle backend when it cannot solve an expression.
#[derive(Debug, Clone)]
pub struct OracleError {
    /// Which backend produced the error (e.g. "sympy", "wolfram").
    pub backend: String,
    /// Human-readable error description.
    pub message: String,
    /// Whether this error is transient and the caller should retry.
    pub is_transient: bool,
}

impl OracleError {
    /// Creates a new permanent (non-transient) oracle error.
    pub fn permanent(backend: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            backend: backend.into(),
            message: message.into(),
            is_transient: false,
        }
    }

    /// Creates a new transient oracle error (eligible for retry).
    pub fn transient(backend: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            backend: backend.into(),
            message: message.into(),
            is_transient: true,
        }
    }
}

impl fmt::Display for OracleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "oracle error [{}]{}: {}",
            self.backend,
            if self.is_transient { " (transient)" } else { "" },
            self.message
        )
    }
}

impl std::error::Error for OracleError {}

// ============================================================================
// OracleResult
// ============================================================================

/// Successful result from an oracle backend.
#[derive(Debug, Clone)]
pub struct OracleResult {
    /// LaTeX-formatted result, if available.
    pub latex: Option<String>,
    /// Plain-text result, if available.
    pub plain_text: Option<String>,
    /// Step-by-step solution strings.
    pub steps: Vec<String>,
    /// Which backend produced this result.
    pub backend: String,
    /// Confidence score in [0.0, 1.0].
    pub confidence: f32,
}

impl OracleResult {
    /// Creates a new oracle result from the given backend.
    pub fn new(backend: impl Into<String>) -> Self {
        Self {
            latex: None,
            plain_text: None,
            steps: Vec::new(),
            backend: backend.into(),
            confidence: 1.0,
        }
    }

    pub fn with_latex(mut self, latex: impl Into<String>) -> Self {
        self.latex = Some(latex.into());
        self
    }

    pub fn with_plain_text(mut self, text: impl Into<String>) -> Self {
        self.plain_text = Some(text.into());
        self
    }

    pub fn with_steps(mut self, steps: Vec<String>) -> Self {
        self.steps = steps;
        self
    }

    pub fn with_confidence(mut self, confidence: f32) -> Self {
        self.confidence = confidence.clamp(0.0, 1.0);
        self
    }
}

// ============================================================================
// MathOracle trait
// ============================================================================

/// Trait for mathematical computation backends.
///
/// Each oracle declares which categories and operations it supports, and
/// implements an async `solve` method. The `priority` method controls cascade
/// ordering (lower value = tried first).
#[async_trait::async_trait]
pub trait MathOracle: Send + Sync {
    /// Human-readable name of this oracle (e.g. "SymPy", "Wolfram Alpha").
    fn name(&self) -> &str;

    /// Mathematical categories this oracle can handle.
    fn supported_categories(&self) -> Vec<Category>;

    /// Operations this oracle can perform.
    fn supported_operations(&self) -> Vec<Operation>;

    /// Attempt to solve the expression with the given operation.
    async fn solve(
        &self,
        expr: &NormalizedExpression,
        operation: &Operation,
    ) -> Result<OracleResult, OracleError>;

    /// Priority for cascade ordering. Lower values are tried first.
    fn priority(&self) -> u32;
}

// ============================================================================
// select_oracles helper
// ============================================================================

/// Filters oracles that support the given category and sorts by priority (ascending).
///
/// Oracles that do not list `category` in their `supported_categories()` are excluded.
/// The returned indices reference the original slice positions.
pub fn select_oracles(
    oracles: &[Box<dyn MathOracle>],
    category: &Category,
) -> Vec<usize> {
    let mut candidates: Vec<(usize, u32)> = oracles
        .iter()
        .enumerate()
        .filter(|(_, o)| o.supported_categories().contains(category))
        .map(|(i, o)| (i, o.priority()))
        .collect();

    candidates.sort_by_key(|&(_, prio)| prio);
    candidates.into_iter().map(|(i, _)| i).collect()
}

// ============================================================================
// cascade_solve
// ============================================================================

/// Tries oracles in priority order until one succeeds or all fail.
///
/// For each oracle:
/// 1. Skips oracles that do not support the expression's category (determined
///    by the caller — here we use the structural hints on `NormalizedExpression`
///    to infer a category, but the primary filter is done via `select_oracles`
///    which the caller can invoke beforehand).
/// 2. Wraps each `solve` call in `tokio::time::timeout`.
/// 3. Records failures in the returned `FailureContext`.
/// 4. Returns the first successful `OracleResult`, or `None` if all fail.
pub async fn cascade_solve(
    oracles: &[Box<dyn MathOracle>],
    expr: &NormalizedExpression,
    operation: &Operation,
    timeout: Duration,
) -> (Option<OracleResult>, FailureContext) {
    let mut failure_ctx = FailureContext::new();

    // Infer a category from the expression's structural flags for filtering.
    let category = infer_category(expr);

    // Get oracles sorted by priority, filtered by category support.
    let ordered = select_oracles(oracles, &category);

    if ordered.is_empty() {
        tracing::warn!(
            category = %category,
            operation = %operation,
            "no oracles support category"
        );
        failure_ctx.record_with_level(
            "cascade".to_string(),
            format!("no oracles support category {}", category),
            0,
            FailureLevel::L6,
        );
        return (None, failure_ctx);
    }

    for idx in ordered {
        let oracle = &oracles[idx];
        let oracle_name = oracle.name().to_string();
        let start = Instant::now();

        tracing::debug!(
            oracle = %oracle_name,
            operation = %operation,
            "attempting oracle"
        );

        let result = tokio::time::timeout(timeout, oracle.solve(expr, operation)).await;
        let elapsed_ms = start.elapsed().as_millis() as u64;

        match result {
            Ok(Ok(oracle_result)) => {
                tracing::info!(
                    oracle = %oracle_name,
                    elapsed_ms = elapsed_ms,
                    confidence = oracle_result.confidence,
                    "oracle succeeded"
                );
                return (Some(oracle_result), failure_ctx);
            }
            Ok(Err(oracle_err)) => {
                let level = if oracle_err.is_transient {
                    FailureLevel::L5
                } else {
                    FailureLevel::L6
                };
                tracing::warn!(
                    oracle = %oracle_name,
                    error = %oracle_err,
                    transient = oracle_err.is_transient,
                    elapsed_ms = elapsed_ms,
                    "oracle failed"
                );
                failure_ctx.record_with_level(
                    oracle_name,
                    oracle_err.message,
                    elapsed_ms,
                    level,
                );
            }
            Err(_elapsed) => {
                tracing::warn!(
                    oracle = %oracle_name,
                    timeout_ms = timeout.as_millis() as u64,
                    "oracle timed out"
                );
                failure_ctx.record_with_level(
                    oracle_name,
                    format!("timeout after {}ms", timeout.as_millis()),
                    elapsed_ms,
                    FailureLevel::L5,
                );
            }
        }
    }

    (None, failure_ctx)
}

/// Infers a `Category` from the structural flags on a `NormalizedExpression`.
fn infer_category(expr: &NormalizedExpression) -> Category {
    if expr.has_matrix {
        Category::LinearAlgebra
    } else if expr.has_integral || expr.has_derivative || expr.has_limit {
        Category::Calculus
    } else if expr.has_summation {
        Category::Combinatorics
    } else if expr.is_equation || expr.is_inequality {
        Category::Algebra
    } else {
        Category::Algebra // default
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    // -----------------------------------------------------------------------
    // Mock oracle for testing
    // -----------------------------------------------------------------------

    struct MockOracle {
        oracle_name: String,
        categories: Vec<Category>,
        operations: Vec<Operation>,
        prio: u32,
        /// If Some(err), solve returns Err. If None, solve returns Ok.
        fail_with: Option<OracleError>,
        /// Artificial delay before returning.
        delay: Duration,
        /// Tracks how many times solve was called.
        call_count: Arc<AtomicU32>,
    }

    impl MockOracle {
        fn success(name: &str, prio: u32, categories: Vec<Category>) -> Self {
            Self {
                oracle_name: name.to_string(),
                categories,
                operations: vec![Operation::Solve, Operation::Simplify],
                prio,
                fail_with: None,
                delay: Duration::ZERO,
                call_count: Arc::new(AtomicU32::new(0)),
            }
        }

        fn failing(name: &str, prio: u32, categories: Vec<Category>, err: OracleError) -> Self {
            Self {
                oracle_name: name.to_string(),
                categories,
                operations: vec![Operation::Solve],
                prio,
                fail_with: Some(err),
                delay: Duration::ZERO,
                call_count: Arc::new(AtomicU32::new(0)),
            }
        }

        fn slow(name: &str, prio: u32, categories: Vec<Category>, delay: Duration) -> Self {
            Self {
                oracle_name: name.to_string(),
                categories,
                operations: vec![Operation::Solve],
                prio,
                fail_with: None,
                delay,
                call_count: Arc::new(AtomicU32::new(0)),
            }
        }

        fn calls(&self) -> u32 {
            self.call_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl MathOracle for MockOracle {
        fn name(&self) -> &str {
            &self.oracle_name
        }

        fn supported_categories(&self) -> Vec<Category> {
            self.categories.clone()
        }

        fn supported_operations(&self) -> Vec<Operation> {
            self.operations.clone()
        }

        async fn solve(
            &self,
            _expr: &NormalizedExpression,
            _operation: &Operation,
        ) -> Result<OracleResult, OracleError> {
            self.call_count.fetch_add(1, Ordering::SeqCst);

            if self.delay > Duration::ZERO {
                tokio::time::sleep(self.delay).await;
            }

            match &self.fail_with {
                Some(err) => Err(err.clone()),
                None => Ok(OracleResult::new(&self.oracle_name)
                    .with_plain_text("x = 42")
                    .with_latex("x = 42")
                    .with_confidence(0.95)),
            }
        }

        fn priority(&self) -> u32 {
            self.prio
        }
    }

    fn make_algebra_expr() -> NormalizedExpression {
        let mut expr = NormalizedExpression::new("x^2 + 1 = 0".to_string(), "x^2 + 1 = 0".to_string());
        expr.is_equation = true;
        expr.variable_count = 1;
        expr
    }

    fn make_calculus_expr() -> NormalizedExpression {
        let mut expr = NormalizedExpression::new(
            "\\int x^2 dx".to_string(),
            "integral of x^2 dx".to_string(),
        );
        expr.has_integral = true;
        expr.variable_count = 1;
        expr
    }

    // -----------------------------------------------------------------------
    // Test: cascade_solve returns first successful result
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_cascade_returns_first_success() {
        let oracles: Vec<Box<dyn MathOracle>> = vec![
            Box::new(MockOracle::success("oracle_a", 1, vec![Category::Algebra])),
            Box::new(MockOracle::success("oracle_b", 2, vec![Category::Algebra])),
        ];

        let expr = make_algebra_expr();
        let (result, ctx) = cascade_solve(&oracles, &expr, &Operation::Solve, Duration::from_secs(5)).await;

        assert!(result.is_some());
        let r = result.unwrap();
        assert_eq!(r.backend, "oracle_a");
        assert!(ctx.attempts.is_empty(), "no failures should be recorded on success");
    }

    // -----------------------------------------------------------------------
    // Test: cascade_solve skips unsupported categories
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_cascade_skips_unsupported_categories() {
        let calculus_oracle = MockOracle::success("calculus_only", 1, vec![Category::Calculus]);
        let algebra_oracle = MockOracle::success("algebra_oracle", 2, vec![Category::Algebra]);
        let calc_calls = calculus_oracle.call_count.clone();

        let oracles: Vec<Box<dyn MathOracle>> = vec![
            Box::new(calculus_oracle),
            Box::new(algebra_oracle),
        ];

        // Expression is algebra (is_equation = true), so calculus_only should be skipped.
        let expr = make_algebra_expr();
        let (result, _ctx) = cascade_solve(&oracles, &expr, &Operation::Solve, Duration::from_secs(5)).await;

        assert!(result.is_some());
        assert_eq!(result.unwrap().backend, "algebra_oracle");
        assert_eq!(calc_calls.load(Ordering::SeqCst), 0, "calculus oracle should not be called");
    }

    // -----------------------------------------------------------------------
    // Test: cascade_solve records failures and falls through
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_cascade_records_failures() {
        let oracles: Vec<Box<dyn MathOracle>> = vec![
            Box::new(MockOracle::failing(
                "bad_oracle",
                1,
                vec![Category::Algebra],
                OracleError::permanent("bad_oracle", "unsolvable"),
            )),
            Box::new(MockOracle::success("good_oracle", 2, vec![Category::Algebra])),
        ];

        let expr = make_algebra_expr();
        let (result, ctx) = cascade_solve(&oracles, &expr, &Operation::Solve, Duration::from_secs(5)).await;

        assert!(result.is_some());
        assert_eq!(result.unwrap().backend, "good_oracle");
        assert_eq!(ctx.attempts.len(), 1);
        assert_eq!(ctx.attempts[0].backend, "bad_oracle");
        assert_eq!(ctx.attempts[0].error, "unsolvable");
    }

    // -----------------------------------------------------------------------
    // Test: cascade_solve respects timeout
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_cascade_respects_timeout() {
        let oracles: Vec<Box<dyn MathOracle>> = vec![
            Box::new(MockOracle::slow(
                "slow_oracle",
                1,
                vec![Category::Algebra],
                Duration::from_secs(10),
            )),
            Box::new(MockOracle::success("fast_oracle", 2, vec![Category::Algebra])),
        ];

        let expr = make_algebra_expr();
        let timeout = Duration::from_millis(50);
        let (result, ctx) = cascade_solve(&oracles, &expr, &Operation::Solve, timeout).await;

        // The slow oracle times out, the fast oracle succeeds.
        assert!(result.is_some());
        assert_eq!(result.unwrap().backend, "fast_oracle");
        assert_eq!(ctx.attempts.len(), 1);
        assert!(ctx.attempts[0].error.contains("timeout"));
    }

    // -----------------------------------------------------------------------
    // Test: select_oracles filters and sorts correctly
    // -----------------------------------------------------------------------

    #[test]
    fn test_select_oracles_filters_and_sorts() {
        let oracles: Vec<Box<dyn MathOracle>> = vec![
            Box::new(MockOracle::success("high_prio_calc", 1, vec![Category::Calculus])),
            Box::new(MockOracle::success("low_prio_alg", 10, vec![Category::Algebra])),
            Box::new(MockOracle::success("mid_prio_alg", 5, vec![Category::Algebra])),
            Box::new(MockOracle::success("high_prio_alg", 2, vec![Category::Algebra, Category::Calculus])),
        ];

        let algebra_indices = select_oracles(&oracles, &Category::Algebra);
        // Should include indices 1, 2, 3 (algebra-supporting), sorted by priority: 3(2), 2(5), 1(10)
        assert_eq!(algebra_indices, vec![3, 2, 1]);

        let calculus_indices = select_oracles(&oracles, &Category::Calculus);
        // Should include indices 0(1) and 3(2)
        assert_eq!(calculus_indices, vec![0, 3]);

        let stats_indices = select_oracles(&oracles, &Category::Statistics);
        assert!(stats_indices.is_empty());
    }

    // -----------------------------------------------------------------------
    // Test: OracleResult construction
    // -----------------------------------------------------------------------

    #[test]
    fn test_oracle_result_construction() {
        let result = OracleResult::new("test_backend")
            .with_latex("x = \\pm 1")
            .with_plain_text("x = +/- 1")
            .with_steps(vec![
                "Move constant to RHS".to_string(),
                "Take square root".to_string(),
            ])
            .with_confidence(0.92);

        assert_eq!(result.backend, "test_backend");
        assert_eq!(result.latex, Some("x = \\pm 1".to_string()));
        assert_eq!(result.plain_text, Some("x = +/- 1".to_string()));
        assert_eq!(result.steps.len(), 2);
        assert!((result.confidence - 0.92).abs() < f32::EPSILON);

        // Test confidence clamping
        let clamped = OracleResult::new("b").with_confidence(1.5);
        assert!((clamped.confidence - 1.0).abs() < f32::EPSILON);

        let clamped_low = OracleResult::new("b").with_confidence(-0.3);
        assert!((clamped_low.confidence - 0.0).abs() < f32::EPSILON);
    }

    // -----------------------------------------------------------------------
    // Test: OracleError construction
    // -----------------------------------------------------------------------

    #[test]
    fn test_oracle_error_construction() {
        let perm = OracleError::permanent("sympy", "expression too complex");
        assert_eq!(perm.backend, "sympy");
        assert_eq!(perm.message, "expression too complex");
        assert!(!perm.is_transient);
        assert!(perm.to_string().contains("sympy"));
        assert!(!perm.to_string().contains("transient"));

        let trans = OracleError::transient("wolfram", "rate limited");
        assert_eq!(trans.backend, "wolfram");
        assert_eq!(trans.message, "rate limited");
        assert!(trans.is_transient);
        assert!(trans.to_string().contains("(transient)"));
    }

    // -----------------------------------------------------------------------
    // Test: cascade_solve with all failures returns None
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_cascade_all_failures_returns_none() {
        let oracles: Vec<Box<dyn MathOracle>> = vec![
            Box::new(MockOracle::failing(
                "oracle_a",
                1,
                vec![Category::Algebra],
                OracleError::permanent("oracle_a", "cannot solve"),
            )),
            Box::new(MockOracle::failing(
                "oracle_b",
                2,
                vec![Category::Algebra],
                OracleError::transient("oracle_b", "service unavailable"),
            )),
        ];

        let expr = make_algebra_expr();
        let (result, ctx) = cascade_solve(&oracles, &expr, &Operation::Solve, Duration::from_secs(5)).await;

        assert!(result.is_none());
        assert_eq!(ctx.attempts.len(), 2);
        assert_eq!(ctx.attempts[0].backend, "oracle_a");
        assert_eq!(ctx.attempts[0].level, FailureLevel::L6); // permanent
        assert_eq!(ctx.attempts[1].backend, "oracle_b");
        assert_eq!(ctx.attempts[1].level, FailureLevel::L5); // transient
    }
}
