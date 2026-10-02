// Marabunta - Licensed under the MIT License.
//! MonteCarlo handler -- probabilistic estimation via random sampling.
//!
//! Uses random sampling to estimate values such as probabilities, expected
//! values, or area under curves. The number of samples is configurable via
//! the `params` map.

use std::collections::HashMap;

use crate::switchboard::errors::SwitchboardResult;
use crate::switchboard::handlers::ComputeHandler;
use crate::switchboard::types::{
    Applicability, Category, ClassificationResult, ComputeResult, ComputeStep, HandlerId,
    NormalizedExpression,
};

// ============================================================================
// MonteCarlo handler
// ============================================================================

/// Probabilistic estimation handler using random sampling.
///
/// Generates a simulation plan described as [`ComputeStep`]s. The actual
/// random sampling would be executed by an oracle or swarm node; this
/// handler produces the specification and aggregation logic.
///
/// Reads `"samples"` from the params map (default: 10,000, minimum: 100).
pub struct MonteCarlo;

impl MonteCarlo {
    pub fn new() -> Self {
        Self
    }

    const RECOMMENDED_CATEGORIES: [Category; 2] =
        [Category::Statistics, Category::Combinatorics];

    const SUPPORTED_CATEGORIES: [Category; 5] = [
        Category::Statistics,
        Category::Combinatorics,
        Category::Optimization,
        Category::Calculus,
        Category::Algebra,
    ];

    /// Default number of samples.
    const DEFAULT_SAMPLES: u64 = 10_000;

    fn parse_samples(params: &HashMap<String, String>) -> u64 {
        params
            .get("samples")
            .and_then(|v| v.parse().ok())
            .unwrap_or(Self::DEFAULT_SAMPLES)
            .max(100)
    }
}

impl Default for MonteCarlo {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl ComputeHandler for MonteCarlo {
    fn id(&self) -> HandlerId {
        HandlerId::from("monte_carlo")
    }

    fn name(&self) -> &str {
        "Monte Carlo Estimation"
    }

    fn description(&self) -> &str {
        "Probabilistic estimation via random sampling"
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
        _classification: &ClassificationResult,
        params: &HashMap<String, String>,
    ) -> SwitchboardResult<ComputeResult> {
        let samples = Self::parse_samples(params);

        // Build a description of what the Monte Carlo simulation would do.
        let steps = vec![
            ComputeStep::new(
                1,
                "Parse expression".to_string(),
                expr.plain_text.clone(),
            ),
            ComputeStep::new(
                2,
                "Configure sampling".to_string(),
                format!("{} random samples", samples),
            ),
            ComputeStep::new(
                3,
                "Execute simulation".to_string(),
                format!("Monte Carlo estimation for: {}", expr.plain_text),
            ),
            ComputeStep::new(
                4,
                "Aggregate results".to_string(),
                "Compute mean, variance, confidence interval".to_string(),
            ),
        ];

        let result = ComputeResult::success(
            self.id(),
            "monte_carlo".to_string(),
            self.estimated_time_ms(),
        )
        .with_steps(steps)
        .with_plain(format!(
            "Monte Carlo simulation configured: {} samples for expression '{}'",
            samples, expr.plain_text
        ));

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
    use crate::switchboard::types::Operation;

    fn make_classification(category: Category, confidence: f32) -> ClassificationResult {
        ClassificationResult::new(category, vec![Operation::Probability], confidence)
    }

    #[test]
    fn test_handler_metadata() {
        let handler = MonteCarlo::new();
        assert_eq!(handler.id(), HandlerId::from("monte_carlo"));
        assert_eq!(handler.name(), "Monte Carlo Estimation");
        assert_eq!(
            handler.description(),
            "Probabilistic estimation via random sampling"
        );
        assert_eq!(handler.estimated_time_ms(), 3000);
        assert!(!handler.requires_api_key());
    }

    #[test]
    fn test_supported_categories() {
        let handler = MonteCarlo::new();
        assert_eq!(handler.supported_categories().len(), 5);
        assert!(handler
            .supported_categories()
            .contains(&Category::Statistics));
        assert!(handler
            .supported_categories()
            .contains(&Category::Combinatorics));
    }

    #[test]
    fn test_applicability_recommended_for_statistics() {
        let handler = MonteCarlo::new();
        assert_eq!(
            handler.applicability(&make_classification(Category::Statistics, 0.9)),
            Applicability::Recommended
        );
    }

    #[test]
    fn test_applicability_recommended_for_combinatorics() {
        let handler = MonteCarlo::new();
        assert_eq!(
            handler.applicability(&make_classification(Category::Combinatorics, 0.9)),
            Applicability::Recommended
        );
    }

    #[test]
    fn test_applicability_applicable_for_optimization() {
        let handler = MonteCarlo::new();
        assert_eq!(
            handler.applicability(&make_classification(Category::Optimization, 0.9)),
            Applicability::Applicable
        );
    }

    #[test]
    fn test_applicability_applicable_for_calculus() {
        let handler = MonteCarlo::new();
        assert_eq!(
            handler.applicability(&make_classification(Category::Calculus, 0.9)),
            Applicability::Applicable
        );
    }

    #[test]
    fn test_applicability_not_applicable_for_geometry() {
        let handler = MonteCarlo::new();
        assert_eq!(
            handler.applicability(&make_classification(Category::Geometry, 0.9)),
            Applicability::NotApplicable
        );
    }

    #[test]
    fn test_parse_samples_default() {
        let params = HashMap::new();
        assert_eq!(MonteCarlo::parse_samples(&params), 10_000);
    }

    #[test]
    fn test_parse_samples_custom() {
        let mut params = HashMap::new();
        params.insert("samples".to_string(), "50000".to_string());
        assert_eq!(MonteCarlo::parse_samples(&params), 50_000);
    }

    #[test]
    fn test_parse_samples_minimum() {
        let mut params = HashMap::new();
        params.insert("samples".to_string(), "10".to_string());
        assert_eq!(MonteCarlo::parse_samples(&params), 100);
    }

    #[tokio::test]
    async fn test_execute_produces_steps() {
        let handler = MonteCarlo::new();
        let expr =
            NormalizedExpression::new("P(X > 3)".to_string(), "P(X > 3)".to_string());
        let classification = make_classification(Category::Statistics, 0.9);
        let params = HashMap::new();

        let result = handler
            .execute(&expr, &classification, &params)
            .await
            .unwrap();
        assert!(result.success);
        assert_eq!(result.steps.len(), 4);
        assert!(result.steps[0].description.contains("Parse"));
        assert!(result.steps[1].expression.contains("10000"));
        assert!(result.result_plain.unwrap().contains("10000"));
    }

    #[tokio::test]
    async fn test_execute_with_custom_samples() {
        let handler = MonteCarlo::new();
        let expr =
            NormalizedExpression::new("E[X^2]".to_string(), "E[X^2]".to_string());
        let classification = make_classification(Category::Statistics, 0.9);
        let mut params = HashMap::new();
        params.insert("samples".to_string(), "50000".to_string());

        let result = handler
            .execute(&expr, &classification, &params)
            .await
            .unwrap();
        assert!(result.success);
        assert!(result.steps[1].expression.contains("50000"));
    }
}
