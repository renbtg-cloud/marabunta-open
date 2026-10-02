// Marabunta - Licensed under the MIT License.
//! NumericalCompute handler -- floating-point numerical evaluation.
//!
//! Delegates to the oracle cascade requesting numeric results, or falls back
//! to a simple evaluation for basic arithmetic expressions.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::switchboard::config::ORACLE_TIMEOUT;
use crate::switchboard::errors::SwitchboardResult;
use crate::switchboard::failure::FailureContext;
use crate::switchboard::handlers::ComputeHandler;
use crate::switchboard::oracles::{cascade_solve, MathOracle};
use crate::switchboard::types::{
    Applicability, Category, ClassificationResult, ComputeResult, HandlerId,
    NormalizedExpression, Operation,
};

// ============================================================================
// NumericalCompute handler
// ============================================================================

/// Floating-point numerical evaluation handler.
///
/// Uses the oracle cascade to obtain a numeric answer. Best suited for
/// expressions where an approximate decimal answer is preferred over an
/// exact symbolic result.
pub struct NumericalCompute {
    oracles: Arc<Vec<Box<dyn MathOracle>>>,
}

impl NumericalCompute {
    pub fn new(oracles: Arc<Vec<Box<dyn MathOracle>>>) -> Self {
        Self { oracles }
    }

    const RECOMMENDED_CATEGORIES: [Category; 2] = [Category::Statistics, Category::Optimization];

    const SUPPORTED_CATEGORIES: [Category; 6] = [
        Category::Algebra,
        Category::Calculus,
        Category::LinearAlgebra,
        Category::Statistics,
        Category::Optimization,
        Category::DifferentialEquations,
    ];

    fn primary_operation(classification: &ClassificationResult) -> Operation {
        classification
            .operations
            .first()
            .cloned()
            .unwrap_or(Operation::Simplify)
    }

    fn failure_result(
        handler_id: &HandlerId,
        failure_ctx: &FailureContext,
        duration_ms: u64,
    ) -> ComputeResult {
        let backend_summary = if failure_ctx.attempts.is_empty() {
            "none".to_string()
        } else {
            failure_ctx
                .attempts
                .iter()
                .map(|a| a.backend.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        };

        let error_detail = failure_ctx
            .attempts
            .iter()
            .map(|a| format!("{}: {}", a.backend, a.error))
            .collect::<Vec<_>>()
            .join("; ");

        ComputeResult::failure(handler_id.clone(), backend_summary, duration_ms)
            .with_plain(format!("Numerical computation failed: {}", error_detail))
    }
}

#[async_trait::async_trait]
impl ComputeHandler for NumericalCompute {
    fn id(&self) -> HandlerId {
        HandlerId::from("numerical_compute")
    }

    fn name(&self) -> &str {
        "Numerical Compute"
    }

    fn description(&self) -> &str {
        "Floating-point numerical evaluation and approximation"
    }

    fn supported_categories(&self) -> Vec<Category> {
        Self::SUPPORTED_CATEGORIES.to_vec()
    }

    fn applicability(&self, classification: &ClassificationResult) -> Applicability {
        if Self::RECOMMENDED_CATEGORIES.contains(&classification.category) {
            Applicability::Recommended
        } else if Self::SUPPORTED_CATEGORIES.contains(&classification.category) {
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
        let operation = Self::primary_operation(classification);
        let start = Instant::now();

        let (maybe_result, failure_ctx) =
            cascade_solve(&self.oracles, expr, &operation, ORACLE_TIMEOUT).await;

        let duration_ms = start.elapsed().as_millis() as u64;
        let handler_id = self.id();

        match maybe_result {
            Some(oracle_result) => {
                let mut result = ComputeResult::success(
                    handler_id,
                    oracle_result.backend.clone(),
                    duration_ms,
                );

                if let Some(ref latex) = oracle_result.latex {
                    result = result.with_latex(latex.clone());
                }
                if let Some(ref plain) = oracle_result.plain_text {
                    result = result.with_plain(plain.clone());
                }

                Ok(result)
            }
            None => Ok(Self::failure_result(&handler_id, &failure_ctx, duration_ms)),
        }
    }

    fn estimated_time_ms(&self) -> u64 {
        1000
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::switchboard::oracles::{OracleError, OracleResult};
    use std::sync::Arc;

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
                    .with_plain_text("3.14159")
                    .with_latex("3.14159")
                    .with_confidence(0.95)),
            }
        }

        fn failing(name: &str, categories: Vec<Category>) -> Self {
            Self {
                oracle_name: name.to_string(),
                categories,
                result: Err(OracleError::permanent(name, "computation failed")),
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

    fn make_handler(oracles: Vec<Box<dyn MathOracle>>) -> NumericalCompute {
        NumericalCompute::new(Arc::new(oracles))
    }

    fn make_classification(category: Category, confidence: f32) -> ClassificationResult {
        ClassificationResult::new(category, vec![Operation::Simplify], confidence)
    }

    fn make_expr() -> NormalizedExpression {
        NormalizedExpression::new("pi".to_string(), "pi".to_string())
    }

    #[test]
    fn test_id_and_name() {
        let handler = make_handler(vec![]);
        assert_eq!(handler.id(), HandlerId::from("numerical_compute"));
        assert_eq!(handler.name(), "Numerical Compute");
        assert_eq!(
            handler.description(),
            "Floating-point numerical evaluation and approximation"
        );
        assert_eq!(handler.estimated_time_ms(), 1000);
    }

    #[test]
    fn test_supported_categories() {
        let handler = make_handler(vec![]);
        let cats = handler.supported_categories();
        assert_eq!(cats.len(), 6);
        assert!(cats.contains(&Category::Statistics));
        assert!(cats.contains(&Category::Optimization));
        assert!(cats.contains(&Category::Algebra));
    }

    #[test]
    fn test_applicability_recommended_for_statistics() {
        let handler = make_handler(vec![]);
        assert_eq!(
            handler.applicability(&make_classification(Category::Statistics, 0.9)),
            Applicability::Recommended
        );
    }

    #[test]
    fn test_applicability_recommended_for_optimization() {
        let handler = make_handler(vec![]);
        assert_eq!(
            handler.applicability(&make_classification(Category::Optimization, 0.9)),
            Applicability::Recommended
        );
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
    fn test_applicability_not_applicable_for_geometry() {
        let handler = make_handler(vec![]);
        assert_eq!(
            handler.applicability(&make_classification(Category::Geometry, 0.9)),
            Applicability::NotApplicable
        );
    }

    #[tokio::test]
    async fn test_execute_success() {
        let handler = make_handler(vec![Box::new(StubOracle::ok(
            "sympy",
            vec![Category::Algebra],
        ))]);
        let expr = make_expr();
        let classification = make_classification(Category::Algebra, 0.9);
        let params = HashMap::new();

        let result = handler
            .execute(&expr, &classification, &params)
            .await
            .unwrap();
        assert!(result.success);
        assert_eq!(result.result_plain, Some("3.14159".to_string()));
    }

    #[tokio::test]
    async fn test_execute_failure() {
        let handler = make_handler(vec![Box::new(StubOracle::failing(
            "sympy",
            vec![Category::Algebra],
        ))]);
        let expr = make_expr();
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
            .contains("Numerical computation failed"));
    }
}
