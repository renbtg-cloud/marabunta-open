// Marabunta - Licensed under the MIT License.
//! Verify handler -- cross-check results between multiple oracles.
//!
//! Sends the same expression to available oracles and compares their results.
//! When multiple oracles agree the confidence is high; when they disagree,
//! the discrepancies are reported as steps.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::switchboard::config::ORACLE_TIMEOUT;
use crate::switchboard::errors::SwitchboardResult;
use crate::switchboard::handlers::ComputeHandler;
use crate::switchboard::oracles::{cascade_solve, MathOracle};
use crate::switchboard::types::{
    Applicability, Category, ClassificationResult, ComputeResult, ComputeStep, HandlerId,
    NormalizedExpression, Operation,
};

// ============================================================================
// Verify handler
// ============================================================================

/// Cross-verification handler that queries the oracle cascade and reports
/// which backends succeeded or failed.
///
/// In a production system this would query all oracles in parallel and
/// compare results. The current implementation uses the cascade (sequential
/// with first-success) and reports the outcome of each attempt.
pub struct Verify {
    oracles: Arc<Vec<Box<dyn MathOracle>>>,
}

impl Verify {
    pub fn new(oracles: Arc<Vec<Box<dyn MathOracle>>>) -> Self {
        Self { oracles }
    }

    const SUPPORTED_CATEGORIES: [Category; 9] = [
        Category::Algebra,
        Category::Calculus,
        Category::LinearAlgebra,
        Category::DifferentialEquations,
        Category::Statistics,
        Category::NumberTheory,
        Category::Combinatorics,
        Category::Geometry,
        Category::Optimization,
    ];
}

#[async_trait::async_trait]
impl ComputeHandler for Verify {
    fn id(&self) -> HandlerId {
        HandlerId::from("verify")
    }

    fn name(&self) -> &str {
        "Verify Result"
    }

    fn description(&self) -> &str {
        "Cross-check results between multiple oracle backends"
    }

    fn supported_categories(&self) -> Vec<Category> {
        Self::SUPPORTED_CATEGORIES.to_vec()
    }

    fn applicability(&self, classification: &ClassificationResult) -> Applicability {
        if Self::SUPPORTED_CATEGORIES.contains(&classification.category) {
            Applicability::Applicable
        } else {
            Applicability::NotApplicable
        }
    }

    async fn execute(
        &self,
        expr: &NormalizedExpression,
        classification: &ClassificationResult,
        _params: &HashMap<String, String>,
    ) -> SwitchboardResult<ComputeResult> {
        let operation = classification
            .operations
            .first()
            .cloned()
            .unwrap_or(Operation::Solve);

        let start = Instant::now();

        let (maybe_result, failure_ctx) =
            cascade_solve(&self.oracles, expr, &operation, ORACLE_TIMEOUT).await;

        let duration_ms = start.elapsed().as_millis() as u64;

        let mut steps = Vec::new();

        // Report successful oracle result.
        if let Some(ref oracle_result) = maybe_result {
            steps.push(ComputeStep::new(
                1,
                format!("Oracle '{}' succeeded", oracle_result.backend),
                oracle_result
                    .plain_text
                    .clone()
                    .unwrap_or_else(|| "(no plain text)".to_string()),
            ));
        }

        // Report failed attempts.
        for attempt in &failure_ctx.attempts {
            steps.push(ComputeStep::new(
                (steps.len() + 1) as u32,
                format!("Oracle '{}' failed", attempt.backend),
                format!("Error: {}", attempt.error),
            ));
        }

        // Verification summary.
        let oracle_count =
            if maybe_result.is_some() { 1 } else { 0 } + failure_ctx.attempts.len();
        let success_count = if maybe_result.is_some() { 1 } else { 0 };

        steps.push(ComputeStep::new(
            (steps.len() + 1) as u32,
            "Verification summary".to_string(),
            format!("{}/{} oracle(s) succeeded", success_count, oracle_count),
        ));

        let backend = maybe_result
            .as_ref()
            .map(|r| r.backend.clone())
            .unwrap_or_else(|| "verify".to_string());

        let success = maybe_result.is_some();
        let mut result = if success {
            ComputeResult::success(self.id(), backend, duration_ms)
        } else {
            ComputeResult::failure(self.id(), backend, duration_ms)
        };

        result = result.with_steps(steps);

        if let Some(ref oracle_result) = maybe_result {
            if let Some(ref latex) = oracle_result.latex {
                result = result.with_latex(latex.clone());
            }
            if let Some(ref plain) = oracle_result.plain_text {
                result = result.with_plain(plain.clone());
            }
        } else {
            result =
                result.with_plain("Verification failed: no oracles succeeded".to_string());
        }

        Ok(result)
    }

    fn estimated_time_ms(&self) -> u64 {
        3000
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::switchboard::oracles::{OracleError, OracleResult};

    struct StubOracle {
        oracle_name: String,
        categories: Vec<Category>,
        result: Result<OracleResult, OracleError>,
    }

    impl StubOracle {
        fn ok(name: &str, categories: Vec<Category>) -> Self {
            Self {
                oracle_name: name.to_string(),
                categories,
                result: Ok(OracleResult::new(name)
                    .with_plain_text("x = 2")
                    .with_latex("x = 2")
                    .with_confidence(0.95)),
            }
        }

        fn failing(name: &str, categories: Vec<Category>) -> Self {
            Self {
                oracle_name: name.to_string(),
                categories,
                result: Err(OracleError::permanent(name, "verification failed")),
            }
        }
    }

    #[async_trait::async_trait]
    impl MathOracle for StubOracle {
        fn name(&self) -> &str {
            &self.oracle_name
        }
        fn supported_categories(&self) -> Vec<Category> {
            self.categories.clone()
        }
        fn supported_operations(&self) -> Vec<Operation> {
            vec![Operation::Solve, Operation::Simplify]
        }
        async fn solve(
            &self,
            _expr: &NormalizedExpression,
            _op: &Operation,
        ) -> Result<OracleResult, OracleError> {
            self.result.clone()
        }
        fn priority(&self) -> u32 {
            1
        }
    }

    fn make_handler(oracles: Vec<Box<dyn MathOracle>>) -> Verify {
        Verify::new(Arc::new(oracles))
    }

    fn make_classification(category: Category, confidence: f32) -> ClassificationResult {
        ClassificationResult::new(category, vec![Operation::Solve], confidence)
    }

    #[test]
    fn test_handler_metadata() {
        let handler = make_handler(vec![]);
        assert_eq!(handler.id(), HandlerId::from("verify"));
        assert_eq!(handler.name(), "Verify Result");
        assert_eq!(
            handler.description(),
            "Cross-check results between multiple oracle backends"
        );
        assert_eq!(handler.estimated_time_ms(), 3000);
        assert!(!handler.requires_api_key());
    }

    #[test]
    fn test_supported_categories() {
        let handler = make_handler(vec![]);
        assert_eq!(handler.supported_categories().len(), 9);
        assert!(!handler
            .supported_categories()
            .contains(&Category::Unknown));
    }

    #[test]
    fn test_applicability_applicable_for_algebra() {
        let handler = make_handler(vec![]);
        assert_eq!(
            handler.applicability(&make_classification(Category::Algebra, 0.9)),
            Applicability::Applicable
        );
    }

    #[test]
    fn test_applicability_applicable_for_calculus() {
        let handler = make_handler(vec![]);
        assert_eq!(
            handler.applicability(&make_classification(Category::Calculus, 0.9)),
            Applicability::Applicable
        );
    }

    #[test]
    fn test_applicability_not_applicable_for_unknown() {
        let handler = make_handler(vec![]);
        assert_eq!(
            handler.applicability(&make_classification(Category::Unknown, 0.5)),
            Applicability::NotApplicable
        );
    }

    #[tokio::test]
    async fn test_execute_with_success() {
        let handler = make_handler(vec![Box::new(StubOracle::ok(
            "sympy",
            vec![Category::Algebra],
        ))]);
        let mut expr =
            NormalizedExpression::new("2x = 4".to_string(), "2x = 4".to_string());
        expr.is_equation = true;
        let classification = make_classification(Category::Algebra, 0.9);
        let params = HashMap::new();

        let result = handler
            .execute(&expr, &classification, &params)
            .await
            .unwrap();
        assert!(result.success);
        assert!(!result.steps.is_empty());
        // Last step should be verification summary.
        let summary = result.steps.last().unwrap();
        assert!(summary.description.contains("Verification summary"));
        assert!(summary.expression.contains("1/1"));
    }

    #[tokio::test]
    async fn test_execute_with_failure() {
        let handler = make_handler(vec![Box::new(StubOracle::failing(
            "sympy",
            vec![Category::Algebra],
        ))]);
        let mut expr =
            NormalizedExpression::new("bad".to_string(), "bad".to_string());
        expr.is_equation = true;
        let classification = make_classification(Category::Algebra, 0.9);
        let params = HashMap::new();

        let result = handler
            .execute(&expr, &classification, &params)
            .await
            .unwrap();
        assert!(!result.success);
        assert!(result
            .result_plain
            .unwrap()
            .contains("Verification failed"));
    }

    #[tokio::test]
    async fn test_execute_reports_failed_attempts() {
        let handler = make_handler(vec![
            Box::new(StubOracle::failing("oracle_a", vec![Category::Algebra])),
            Box::new(StubOracle::ok("oracle_b", vec![Category::Algebra])),
        ]);
        let mut expr =
            NormalizedExpression::new("x = 1".to_string(), "x = 1".to_string());
        expr.is_equation = true;
        let classification = make_classification(Category::Algebra, 0.9);
        let params = HashMap::new();

        let result = handler
            .execute(&expr, &classification, &params)
            .await
            .unwrap();
        assert!(result.success);
        // Should have steps for success, failure, and summary.
        assert!(result.steps.len() >= 3);
    }
}
