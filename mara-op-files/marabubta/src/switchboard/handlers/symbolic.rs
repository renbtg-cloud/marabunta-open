// Marabunta - Licensed under the MIT License.
//! SymbolicSolve handler -- exact symbolic solution via CAS oracle cascade.
//!
//! Delegates to [`cascade_solve`] with all registered oracles, converting the
//! resulting [`OracleResult`] into a [`ComputeResult`] that the switchboard
//! pipeline can present to the user.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::switchboard::config::ORACLE_TIMEOUT;
use crate::switchboard::errors::SwitchboardResult;
use crate::switchboard::failure::FailureContext;
use crate::switchboard::handlers::ComputeHandler;
use crate::switchboard::oracles::{cascade_solve, MathOracle, OracleResult};
use crate::switchboard::types::{
    Applicability, Category, ClassificationResult, ComputeResult, ComputeStep, HandlerId,
    NormalizedExpression, Operation,
};

// ============================================================================
// SymbolicSolve handler
// ============================================================================

/// Exact symbolic computation handler backed by a cascade of CAS oracles
/// (e.g. SymPy local subprocess, Wolfram Alpha API).
///
/// The handler iterates through its oracle list in priority order, returning
/// the first successful result. Failures are tracked in a [`FailureContext`]
/// and surfaced in the returned [`ComputeResult`] when no oracle succeeds.
pub struct SymbolicSolve {
    oracles: Arc<Vec<Box<dyn MathOracle>>>,
}

impl SymbolicSolve {
    /// Creates a new `SymbolicSolve` handler with the given oracle list.
    pub fn new(oracles: Arc<Vec<Box<dyn MathOracle>>>) -> Self {
        Self { oracles }
    }

    /// Categories for which this handler is strongly recommended.
    const RECOMMENDED_CATEGORIES: [Category; 2] = [Category::Algebra, Category::Calculus];

    /// Full set of categories this handler can operate on.
    const SUPPORTED_CATEGORIES: [Category; 4] = [
        Category::Algebra,
        Category::Calculus,
        Category::DifferentialEquations,
        Category::LinearAlgebra,
    ];

    /// Determines the primary operation to pass to the oracle cascade based on
    /// the classification result. Falls back to `Operation::Solve` when the
    /// classification carries no operations.
    fn primary_operation(classification: &ClassificationResult) -> Operation {
        classification
            .operations
            .first()
            .cloned()
            .unwrap_or(Operation::Solve)
    }

    /// Converts a successful [`OracleResult`] into a [`ComputeResult`].
    fn oracle_result_to_compute(
        oracle_result: &OracleResult,
        handler_id: &HandlerId,
        duration_ms: u64,
    ) -> ComputeResult {
        let mut result =
            ComputeResult::success(handler_id.clone(), oracle_result.backend.clone(), duration_ms);

        if let Some(ref latex) = oracle_result.latex {
            result = result.with_latex(latex.clone());
        }
        if let Some(ref plain) = oracle_result.plain_text {
            result = result.with_plain(plain.clone());
        }

        if !oracle_result.steps.is_empty() {
            let steps: Vec<ComputeStep> = oracle_result
                .steps
                .iter()
                .enumerate()
                .map(|(i, desc)| {
                    ComputeStep::new(
                        (i + 1) as u32,
                        desc.clone(),
                        String::new(), // expression detail not provided by oracle
                    )
                })
                .collect();
            result = result.with_steps(steps);
        }

        result
    }

    /// Builds a failure [`ComputeResult`] from the cascade's failure context.
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
            .with_plain(format!("All oracles failed: {}", error_detail))
    }
}

#[async_trait::async_trait]
impl ComputeHandler for SymbolicSolve {
    fn id(&self) -> HandlerId {
        HandlerId::from("symbolic_solve")
    }

    fn name(&self) -> &str {
        "Symbolic Solve"
    }

    fn description(&self) -> &str {
        "Exact symbolic solution using CAS backends (SymPy, Wolfram)"
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
                Ok(Self::oracle_result_to_compute(&oracle_result, &handler_id, duration_ms))
            }
            None => Ok(Self::failure_result(&handler_id, &failure_ctx, duration_ms)),
        }
    }

    fn estimated_time_ms(&self) -> u64 {
        1500
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::switchboard::oracles::OracleError;
    use std::sync::Arc;
    use std::time::Duration;

    // -----------------------------------------------------------------------
    // Mock oracle for handler-level tests
    // -----------------------------------------------------------------------

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
                    .with_latex("x = 1")
                    .with_plain_text("x = 1")
                    .with_steps(vec![
                        "Rearrange equation".to_string(),
                        "Solve for x".to_string(),
                    ])
                    .with_confidence(0.95)),
            }
        }

        fn failing(name: &str, categories: Vec<Category>) -> Self {
            Self {
                oracle_name: name.to_string(),
                categories,
                result: Err(OracleError::permanent(name, "not supported")),
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
            _operation: &Operation,
        ) -> Result<OracleResult, OracleError> {
            self.result.clone()
        }

        fn priority(&self) -> u32 {
            1
        }
    }

    fn make_handler(oracles: Vec<Box<dyn MathOracle>>) -> SymbolicSolve {
        SymbolicSolve::new(Arc::new(oracles))
    }

    fn make_classification(category: Category, confidence: f32) -> ClassificationResult {
        ClassificationResult::new(category, vec![Operation::Solve], confidence)
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_id_and_name() {
        let handler = make_handler(vec![]);
        assert_eq!(handler.id(), HandlerId::from("symbolic_solve"));
        assert_eq!(handler.name(), "Symbolic Solve");
        assert_eq!(
            handler.description(),
            "Exact symbolic solution using CAS backends (SymPy, Wolfram)"
        );
    }

    #[test]
    fn test_estimated_time() {
        let handler = make_handler(vec![]);
        assert_eq!(handler.estimated_time_ms(), 1500);
    }

    #[test]
    fn test_supported_categories() {
        let handler = make_handler(vec![]);
        let cats = handler.supported_categories();
        assert!(cats.contains(&Category::Algebra));
        assert!(cats.contains(&Category::Calculus));
        assert!(cats.contains(&Category::DifferentialEquations));
        assert!(cats.contains(&Category::LinearAlgebra));
        assert!(!cats.contains(&Category::Statistics));
    }

    #[test]
    fn test_applicability_recommended_for_algebra() {
        let handler = make_handler(vec![]);
        let classification = make_classification(Category::Algebra, 0.9);
        assert_eq!(handler.applicability(&classification), Applicability::Recommended);
    }

    #[test]
    fn test_applicability_recommended_for_calculus() {
        let handler = make_handler(vec![]);
        let classification = make_classification(Category::Calculus, 0.85);
        assert_eq!(handler.applicability(&classification), Applicability::Recommended);
    }

    #[test]
    fn test_applicability_applicable_for_diff_eq() {
        let handler = make_handler(vec![]);
        let classification = make_classification(Category::DifferentialEquations, 0.9);
        assert_eq!(handler.applicability(&classification), Applicability::Applicable);
    }

    #[test]
    fn test_applicability_applicable_for_linear_algebra() {
        let handler = make_handler(vec![]);
        let classification = make_classification(Category::LinearAlgebra, 0.75);
        assert_eq!(handler.applicability(&classification), Applicability::Applicable);
    }

    #[test]
    fn test_applicability_not_applicable_for_statistics() {
        let handler = make_handler(vec![]);
        let classification = make_classification(Category::Statistics, 0.95);
        assert_eq!(handler.applicability(&classification), Applicability::NotApplicable);
    }

    #[test]
    fn test_applicability_not_applicable_for_unknown() {
        let handler = make_handler(vec![]);
        let classification = make_classification(Category::Unknown, 0.5);
        assert_eq!(handler.applicability(&classification), Applicability::NotApplicable);
    }

    #[tokio::test]
    async fn test_execute_success() {
        let handler = make_handler(vec![
            Box::new(StubOracle::ok("sympy", vec![Category::Algebra])),
        ]);

        let expr = {
            let mut e = NormalizedExpression::new("x^2 - 1 = 0".to_string(), "x^2 - 1 = 0".to_string());
            e.is_equation = true;
            e.variable_count = 1;
            e
        };
        let classification = make_classification(Category::Algebra, 0.9);
        let params = HashMap::new();

        let result = handler.execute(&expr, &classification, &params).await.unwrap();
        assert!(result.success);
        assert_eq!(result.result_latex, Some("x = 1".to_string()));
        assert_eq!(result.result_plain, Some("x = 1".to_string()));
        assert_eq!(result.steps.len(), 2);
        assert_eq!(result.steps[0].step_number, 1);
        assert_eq!(result.steps[0].description, "Rearrange equation");
    }

    #[tokio::test]
    async fn test_execute_all_oracles_fail() {
        let handler = make_handler(vec![
            Box::new(StubOracle::failing("sympy", vec![Category::Algebra])),
        ]);

        let expr = {
            let mut e = NormalizedExpression::new("x^2 - 1 = 0".to_string(), "x^2 - 1 = 0".to_string());
            e.is_equation = true;
            e
        };
        let classification = make_classification(Category::Algebra, 0.9);
        let params = HashMap::new();

        let result = handler.execute(&expr, &classification, &params).await.unwrap();
        assert!(!result.success);
        assert!(result.result_plain.is_some());
        assert!(result.result_plain.unwrap().contains("All oracles failed"));
    }
}
