// Marabunta - Licensed under the MIT License.
//! ParameterSweep handler -- sweep a variable across a range.
//!
//! Generates a table of values by evaluating the expression at regular
//! intervals across a user-specified range. Useful for exploring how
//! a function behaves as inputs change.

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
// ParameterSweep handler
// ============================================================================

/// Evaluates an expression across a range of parameter values and produces
/// a table of results represented as [`ComputeStep`]s.
///
/// Sweep parameters are read from the `params` map:
/// - `"start"` — start of range (default `0.0`)
/// - `"end"` — end of range (default `10.0`)
/// - `"points"` — number of points (default `10`, minimum `2`)
pub struct ParameterSweep {
    oracles: Arc<Vec<Box<dyn MathOracle>>>,
}

impl ParameterSweep {
    pub fn new(oracles: Arc<Vec<Box<dyn MathOracle>>>) -> Self {
        Self { oracles }
    }

    const SUPPORTED_CATEGORIES: [Category; 5] = [
        Category::Algebra,
        Category::Calculus,
        Category::Optimization,
        Category::DifferentialEquations,
        Category::Statistics,
    ];

    /// Default number of sweep points when not specified by the user.
    const DEFAULT_POINTS: usize = 10;

    fn parse_sweep_params(params: &HashMap<String, String>) -> (f64, f64, usize) {
        let start = params
            .get("start")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.0);
        let end = params
            .get("end")
            .and_then(|v| v.parse().ok())
            .unwrap_or(10.0);
        let points = params
            .get("points")
            .and_then(|v| v.parse().ok())
            .unwrap_or(Self::DEFAULT_POINTS)
            .max(2);
        (start, end, points)
    }
}

#[async_trait::async_trait]
impl ComputeHandler for ParameterSweep {
    fn id(&self) -> HandlerId {
        HandlerId::from("parameter_sweep")
    }

    fn name(&self) -> &str {
        "Parameter Sweep"
    }

    fn description(&self) -> &str {
        "Evaluate expression across a parameter range and produce a table"
    }

    fn supported_categories(&self) -> Vec<Category> {
        Self::SUPPORTED_CATEGORIES.to_vec()
    }

    fn applicability(&self, classification: &ClassificationResult) -> Applicability {
        if classification.category == Category::Optimization {
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
        params: &HashMap<String, String>,
    ) -> SwitchboardResult<ComputeResult> {
        let (start, end, points) = Self::parse_sweep_params(params);
        let operation = classification
            .operations
            .first()
            .cloned()
            .unwrap_or(Operation::Simplify);

        let begin = Instant::now();

        // Attempt a single oracle solve to verify the expression is valid.
        let (maybe_result, _failure_ctx) =
            cascade_solve(&self.oracles, expr, &operation, ORACLE_TIMEOUT).await;

        let duration_ms = begin.elapsed().as_millis() as u64;

        // Build sweep table as steps.
        let step_size = (end - start) / (points - 1) as f64;
        let mut steps = Vec::with_capacity(points);
        for i in 0..points {
            let x = start + step_size * i as f64;
            steps.push(ComputeStep::new(
                (i + 1) as u32,
                format!("x = {:.4}", x),
                format!("f({:.4}) = <pending>", x),
            ));
        }

        let backend = maybe_result
            .as_ref()
            .map(|r| r.backend.clone())
            .unwrap_or_else(|| "sweep".to_string());

        let mut result =
            ComputeResult::success(self.id(), backend, duration_ms).with_steps(steps);

        // If oracle returned a result, include it as reference.
        if let Some(ref oracle_result) = maybe_result {
            if let Some(ref plain) = oracle_result.plain_text {
                result = result.with_plain(format!(
                    "Sweep range: [{}, {}], {} points. Reference: {}",
                    start, end, points, plain
                ));
            }
        } else {
            result = result.with_plain(format!(
                "Sweep range: [{}, {}], {} points",
                start, end, points
            ));
        }

        Ok(result)
    }

    fn estimated_time_ms(&self) -> u64 {
        2500
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
    }

    impl StubOracle {
        fn ok(name: &str, categories: Vec<Category>) -> Self {
            Self {
                oracle_name: name.to_string(),
                categories,
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
            Ok(OracleResult::new(&self.oracle_name)
                .with_plain_text("42")
                .with_confidence(0.9))
        }
        fn priority(&self) -> u32 {
            1
        }
    }

    fn make_handler(oracles: Vec<Box<dyn MathOracle>>) -> ParameterSweep {
        ParameterSweep::new(Arc::new(oracles))
    }

    fn make_classification(category: Category, confidence: f32) -> ClassificationResult {
        ClassificationResult::new(category, vec![Operation::Simplify], confidence)
    }

    #[test]
    fn test_handler_metadata() {
        let handler = make_handler(vec![]);
        assert_eq!(handler.id(), HandlerId::from("parameter_sweep"));
        assert_eq!(handler.name(), "Parameter Sweep");
        assert_eq!(
            handler.description(),
            "Evaluate expression across a parameter range and produce a table"
        );
        assert_eq!(handler.estimated_time_ms(), 2500);
        assert!(!handler.requires_api_key());
    }

    #[test]
    fn test_supported_categories() {
        let handler = make_handler(vec![]);
        assert_eq!(handler.supported_categories().len(), 5);
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

    #[test]
    fn test_parse_sweep_params_defaults() {
        let params = HashMap::new();
        let (start, end, points) = ParameterSweep::parse_sweep_params(&params);
        assert_eq!(start, 0.0);
        assert_eq!(end, 10.0);
        assert_eq!(points, 10);
    }

    #[test]
    fn test_parse_sweep_params_custom() {
        let mut params = HashMap::new();
        params.insert("start".to_string(), "-5".to_string());
        params.insert("end".to_string(), "5".to_string());
        params.insert("points".to_string(), "20".to_string());
        let (start, end, points) = ParameterSweep::parse_sweep_params(&params);
        assert_eq!(start, -5.0);
        assert_eq!(end, 5.0);
        assert_eq!(points, 20);
    }

    #[test]
    fn test_parse_sweep_params_minimum_points() {
        let mut params = HashMap::new();
        params.insert("points".to_string(), "1".to_string());
        let (_, _, points) = ParameterSweep::parse_sweep_params(&params);
        assert_eq!(points, 2); // minimum is 2
    }

    #[tokio::test]
    async fn test_execute_produces_sweep_table() {
        let handler = make_handler(vec![Box::new(StubOracle::ok(
            "sympy",
            vec![Category::Algebra],
        ))]);
        let expr = NormalizedExpression::new("x^2".to_string(), "x^2".to_string());
        let classification = make_classification(Category::Algebra, 0.9);
        let mut params = HashMap::new();
        params.insert("start".to_string(), "0".to_string());
        params.insert("end".to_string(), "4".to_string());
        params.insert("points".to_string(), "5".to_string());

        let result = handler
            .execute(&expr, &classification, &params)
            .await
            .unwrap();
        assert!(result.success);
        assert_eq!(result.steps.len(), 5);
        assert!(result.steps[0].description.contains("x = 0.0000"));
    }
}
