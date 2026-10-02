// Marabunta - Licensed under the MIT License.
//! StepByStep handler -- detailed step-by-step solution walkthrough.
//!
//! Delegates to [`cascade_solve`] and then formats the oracle's output as a
//! numbered list of [`ComputeStep`]s. When the oracle provides its own steps
//! they are used directly; otherwise a minimal "Input -> Process -> Result"
//! sequence is synthesised.

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
// StepByStep handler
// ============================================================================

/// Generates a detailed, numbered step-by-step walkthrough of the solution
/// process. This is the "learning mode" handler: users who want to understand
/// *how* the answer was reached rather than just seeing the final result.
pub struct StepByStep {
    oracles: Arc<Vec<Box<dyn MathOracle>>>,
}

impl StepByStep {
    /// Creates a new `StepByStep` handler with the given oracle list.
    pub fn new(oracles: Arc<Vec<Box<dyn MathOracle>>>) -> Self {
        Self { oracles }
    }

    /// All categories this handler supports (everything except `Unknown`).
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

    /// Categories for which step-by-step is most valuable.
    const RECOMMENDED_CATEGORIES: [Category; 2] =
        [Category::Calculus, Category::DifferentialEquations];

    /// Categories that benefit from step-by-step but are not the primary use-case.
    const APPLICABLE_CATEGORIES: [Category; 2] = [Category::Algebra, Category::LinearAlgebra];

    /// Selects the primary operation from the classification, defaulting to
    /// `Solve` when none is present.
    fn primary_operation(classification: &ClassificationResult) -> Operation {
        classification
            .operations
            .first()
            .cloned()
            .unwrap_or(Operation::Solve)
    }

    /// Converts oracle steps into [`ComputeStep`] entries. If the oracle
    /// provided steps they are used as-is; otherwise a basic three-step
    /// "Input -> Process -> Result" sequence is generated.
    fn build_steps(
        oracle_result: &OracleResult,
        expr: &NormalizedExpression,
    ) -> Vec<ComputeStep> {
        if !oracle_result.steps.is_empty() {
            // Use oracle-provided steps directly.
            oracle_result
                .steps
                .iter()
                .enumerate()
                .map(|(i, desc)| {
                    ComputeStep::new(
                        (i + 1) as u32,
                        desc.clone(),
                        String::new(),
                    )
                })
                .collect()
        } else {
            // Synthesise minimal Input -> Process -> Result steps.
            let result_expr = oracle_result
                .plain_text
                .clone()
                .or_else(|| oracle_result.latex.clone())
                .unwrap_or_else(|| "(no result)".to_string());

            vec![
                ComputeStep::new(
                    1,
                    "Input expression".to_string(),
                    expr.plain_text.clone(),
                ),
                ComputeStep::new(
                    2,
                    "Process with solver".to_string(),
                    format!("Backend: {}", oracle_result.backend),
                ),
                ComputeStep::new(
                    3,
                    "Result".to_string(),
                    result_expr,
                ),
            ]
        }
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
            .with_plain(format!("Step-by-step generation failed: {}", error_detail))
    }
}

#[async_trait::async_trait]
impl ComputeHandler for StepByStep {
    fn id(&self) -> HandlerId {
        HandlerId::from("step_by_step")
    }

    fn name(&self) -> &str {
        "Step-by-Step Solution"
    }

    fn description(&self) -> &str {
        "Detailed step-by-step walkthrough of the solution process"
    }

    fn supported_categories(&self) -> Vec<Category> {
        Self::SUPPORTED_CATEGORIES.to_vec()
    }

    fn applicability(&self, classification: &ClassificationResult) -> Applicability {
        if Self::RECOMMENDED_CATEGORIES.contains(&classification.category) {
            Applicability::Recommended
        } else if Self::APPLICABLE_CATEGORIES.contains(&classification.category) {
            Applicability::Applicable
        } else if Self::SUPPORTED_CATEGORIES.contains(&classification.category) {
            Applicability::Marginal
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
                let steps = Self::build_steps(&oracle_result, expr);

                let mut result = ComputeResult::success(
                    handler_id,
                    oracle_result.backend.clone(),
                    duration_ms,
                )
                .with_steps(steps);

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
        2000
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

    // -----------------------------------------------------------------------
    // Stub oracle for handler-level tests
    // -----------------------------------------------------------------------

    struct StubOracle {
        oracle_name: String,
        categories: Vec<Category>,
        result: Result<OracleResult, OracleError>,
    }

    impl StubOracle {
        fn with_steps(name: &str, categories: Vec<Category>) -> Self {
            Self {
                oracle_name: name.to_string(),
                categories,
                result: Ok(OracleResult::new(name)
                    .with_latex("x = 2")
                    .with_plain_text("x = 2")
                    .with_steps(vec![
                        "Subtract 3 from both sides".to_string(),
                        "Divide by 2".to_string(),
                        "x = 2".to_string(),
                    ])
                    .with_confidence(0.9)),
            }
        }

        fn without_steps(name: &str, categories: Vec<Category>) -> Self {
            Self {
                oracle_name: name.to_string(),
                categories,
                result: Ok(OracleResult::new(name)
                    .with_latex("x = 2")
                    .with_plain_text("x = 2")
                    .with_confidence(0.9)),
            }
        }

        fn failing(name: &str, categories: Vec<Category>) -> Self {
            Self {
                oracle_name: name.to_string(),
                categories,
                result: Err(OracleError::permanent(name, "cannot solve")),
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

    fn make_handler(oracles: Vec<Box<dyn MathOracle>>) -> StepByStep {
        StepByStep::new(Arc::new(oracles))
    }

    fn make_classification(category: Category, confidence: f32) -> ClassificationResult {
        ClassificationResult::new(category, vec![Operation::Solve], confidence)
    }

    fn make_algebra_expr() -> NormalizedExpression {
        let mut e =
            NormalizedExpression::new("2x + 3 = 7".to_string(), "2x + 3 = 7".to_string());
        e.is_equation = true;
        e.variable_count = 1;
        e
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_id_and_name() {
        let handler = make_handler(vec![]);
        assert_eq!(handler.id(), HandlerId::from("step_by_step"));
        assert_eq!(handler.name(), "Step-by-Step Solution");
        assert_eq!(
            handler.description(),
            "Detailed step-by-step walkthrough of the solution process"
        );
        assert_eq!(handler.estimated_time_ms(), 2000);
    }

    #[test]
    fn test_supported_categories_exclude_unknown() {
        let handler = make_handler(vec![]);
        let cats = handler.supported_categories();
        assert!(!cats.contains(&Category::Unknown));
        assert_eq!(cats.len(), 9);
        assert!(cats.contains(&Category::Algebra));
        assert!(cats.contains(&Category::Calculus));
        assert!(cats.contains(&Category::Statistics));
        assert!(cats.contains(&Category::Geometry));
    }

    #[test]
    fn test_applicability_recommended_for_calculus() {
        let handler = make_handler(vec![]);
        assert_eq!(
            handler.applicability(&make_classification(Category::Calculus, 0.9)),
            Applicability::Recommended
        );
    }

    #[test]
    fn test_applicability_recommended_for_diff_eq() {
        let handler = make_handler(vec![]);
        assert_eq!(
            handler.applicability(&make_classification(Category::DifferentialEquations, 0.85)),
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
    fn test_applicability_applicable_for_linear_algebra() {
        let handler = make_handler(vec![]);
        assert_eq!(
            handler.applicability(&make_classification(Category::LinearAlgebra, 0.9)),
            Applicability::Applicable
        );
    }

    #[test]
    fn test_applicability_marginal_for_statistics() {
        let handler = make_handler(vec![]);
        assert_eq!(
            handler.applicability(&make_classification(Category::Statistics, 0.9)),
            Applicability::Marginal
        );
    }

    #[test]
    fn test_applicability_marginal_for_geometry() {
        let handler = make_handler(vec![]);
        assert_eq!(
            handler.applicability(&make_classification(Category::Geometry, 0.8)),
            Applicability::Marginal
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
    async fn test_execute_with_oracle_steps() {
        let handler = make_handler(vec![Box::new(StubOracle::with_steps(
            "sympy",
            vec![Category::Algebra],
        ))]);

        let expr = make_algebra_expr();
        let classification = make_classification(Category::Algebra, 0.9);
        let params = HashMap::new();

        let result = handler.execute(&expr, &classification, &params).await.unwrap();
        assert!(result.success);
        assert_eq!(result.steps.len(), 3);
        assert_eq!(result.steps[0].step_number, 1);
        assert_eq!(result.steps[0].description, "Subtract 3 from both sides");
        assert_eq!(result.steps[2].description, "x = 2");
        assert_eq!(result.result_latex, Some("x = 2".to_string()));
    }

    #[tokio::test]
    async fn test_execute_without_oracle_steps_generates_basic() {
        let handler = make_handler(vec![Box::new(StubOracle::without_steps(
            "wolfram",
            vec![Category::Algebra],
        ))]);

        let expr = make_algebra_expr();
        let classification = make_classification(Category::Algebra, 0.9);
        let params = HashMap::new();

        let result = handler.execute(&expr, &classification, &params).await.unwrap();
        assert!(result.success);
        // Should have synthesised 3 basic steps
        assert_eq!(result.steps.len(), 3);
        assert_eq!(result.steps[0].description, "Input expression");
        assert_eq!(result.steps[0].expression, "2x + 3 = 7");
        assert_eq!(result.steps[1].description, "Process with solver");
        assert!(result.steps[1].expression.contains("wolfram"));
        assert_eq!(result.steps[2].description, "Result");
        assert_eq!(result.steps[2].expression, "x = 2");
    }

    #[tokio::test]
    async fn test_execute_failure() {
        let handler = make_handler(vec![Box::new(StubOracle::failing(
            "sympy",
            vec![Category::Algebra],
        ))]);

        let expr = make_algebra_expr();
        let classification = make_classification(Category::Algebra, 0.9);
        let params = HashMap::new();

        let result = handler.execute(&expr, &classification, &params).await.unwrap();
        assert!(!result.success);
        assert!(result.result_plain.is_some());
        assert!(result
            .result_plain
            .unwrap()
            .contains("Step-by-step generation failed"));
    }
}
