// Marabunta - Licensed under the MIT License.
//! Visualize handler -- generates plotting specifications for expressions.
//!
//! Produces JSON plot descriptions that downstream renderers can turn into
//! actual graphs. Supports line plots, geometric diagrams, histograms, and
//! vector field visualizations depending on the expression category.

use std::collections::HashMap;

use crate::switchboard::errors::SwitchboardResult;
use crate::switchboard::handlers::ComputeHandler;
use crate::switchboard::types::{
    Applicability, Category, ClassificationResult, ComputeResult, HandlerId,
    NormalizedExpression,
};

// ============================================================================
// Visualize handler
// ============================================================================

/// Generates a plot specification from the expression and its classification.
///
/// The handler is entirely local — no external API calls are needed. It
/// produces a JSON description of the desired visualization which a
/// rendering layer (frontend, SVG generator, matplotlib, etc.) can consume.
pub struct Visualize;

impl Visualize {
    pub fn new() -> Self {
        Self
    }

    const RECOMMENDED_CATEGORIES: [Category; 2] = [Category::Geometry, Category::Calculus];

    const SUPPORTED_CATEGORIES: [Category; 7] = [
        Category::Algebra,
        Category::Calculus,
        Category::LinearAlgebra,
        Category::Statistics,
        Category::Geometry,
        Category::Optimization,
        Category::DifferentialEquations,
    ];
}

impl Default for Visualize {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl ComputeHandler for Visualize {
    fn id(&self) -> HandlerId {
        HandlerId::from("visualize")
    }

    fn name(&self) -> &str {
        "Visualize"
    }

    fn description(&self) -> &str {
        "Generate a plot or geometric visualization of the expression"
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
        params: &HashMap<String, String>,
    ) -> SwitchboardResult<ComputeResult> {
        let plot_spec = generate_plot_spec(expr, classification, params);

        let result = ComputeResult::success(
            self.id(),
            "visualize".to_string(),
            self.estimated_time_ms(),
        )
        .with_plain(plot_spec);

        Ok(result)
    }

    fn estimated_time_ms(&self) -> u64 {
        1200
    }
}

// ============================================================================
// Plot specification generation
// ============================================================================

/// Generates a JSON plot specification from the expression.
fn generate_plot_spec(
    expr: &NormalizedExpression,
    classification: &ClassificationResult,
    params: &HashMap<String, String>,
) -> String {
    let x_min = params
        .get("x_min")
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(-10.0);
    let x_max = params
        .get("x_max")
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(10.0);
    let points = params
        .get("points")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(200);

    let plot_type = match classification.category {
        Category::Geometry => "geometric",
        Category::Statistics => "histogram",
        Category::LinearAlgebra => "vector_field",
        _ => "line",
    };

    format!(
        r#"{{"type":"{}","expression":"{}","domain":{{"x_min":{},"x_max":{}}},"points":{},"category":"{}","variables":{:?}}}"#,
        plot_type,
        expr.plain_text,
        x_min,
        x_max,
        points,
        classification.category,
        expr.symbols,
    )
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::switchboard::types::Operation;

    fn make_expr(text: &str) -> NormalizedExpression {
        NormalizedExpression::new(text.to_string(), text.to_string())
    }

    fn make_classification(category: Category, confidence: f32) -> ClassificationResult {
        ClassificationResult::new(category, vec![Operation::Solve], confidence)
    }

    #[test]
    fn test_handler_metadata() {
        let handler = Visualize::new();
        assert_eq!(handler.id(), HandlerId::from("visualize"));
        assert_eq!(handler.name(), "Visualize");
        assert_eq!(
            handler.description(),
            "Generate a plot or geometric visualization of the expression"
        );
        assert_eq!(handler.estimated_time_ms(), 1200);
        assert!(!handler.requires_api_key());
    }

    #[test]
    fn test_supported_categories() {
        let handler = Visualize::new();
        assert_eq!(handler.supported_categories().len(), 7);
    }

    #[test]
    fn test_applicability_recommended_for_geometry() {
        let handler = Visualize::new();
        assert_eq!(
            handler.applicability(&make_classification(Category::Geometry, 0.9)),
            Applicability::Recommended
        );
    }

    #[test]
    fn test_applicability_recommended_for_calculus() {
        let handler = Visualize::new();
        assert_eq!(
            handler.applicability(&make_classification(Category::Calculus, 0.9)),
            Applicability::Recommended
        );
    }

    #[test]
    fn test_applicability_applicable_for_algebra() {
        let handler = Visualize::new();
        assert_eq!(
            handler.applicability(&make_classification(Category::Algebra, 0.9)),
            Applicability::Applicable
        );
    }

    #[test]
    fn test_applicability_not_applicable_for_number_theory() {
        let handler = Visualize::new();
        assert_eq!(
            handler.applicability(&make_classification(Category::NumberTheory, 0.9)),
            Applicability::NotApplicable
        );
    }

    #[test]
    fn test_applicability_not_applicable_for_unknown() {
        let handler = Visualize::new();
        assert_eq!(
            handler.applicability(&make_classification(Category::Unknown, 0.5)),
            Applicability::NotApplicable
        );
    }

    #[tokio::test]
    async fn test_execute_generates_plot_spec() {
        let handler = Visualize::new();
        let expr = make_expr("x^2 + 1");
        let classification = make_classification(Category::Algebra, 0.9);
        let params = HashMap::new();

        let result = handler
            .execute(&expr, &classification, &params)
            .await
            .unwrap();
        assert!(result.success);
        let spec = result.result_plain.unwrap();
        assert!(spec.contains("\"type\":\"line\""));
        assert!(spec.contains("x^2 + 1"));
    }

    #[tokio::test]
    async fn test_execute_geometry_produces_geometric_type() {
        let handler = Visualize::new();
        let expr = make_expr("circle r=5");
        let classification = make_classification(Category::Geometry, 0.9);
        let params = HashMap::new();

        let result = handler
            .execute(&expr, &classification, &params)
            .await
            .unwrap();
        let spec = result.result_plain.unwrap();
        assert!(spec.contains("\"type\":\"geometric\""));
    }

    #[tokio::test]
    async fn test_execute_with_custom_domain() {
        let handler = Visualize::new();
        let expr = make_expr("sin(x)");
        let classification = make_classification(Category::Calculus, 0.9);
        let mut params = HashMap::new();
        params.insert("x_min".to_string(), "-3.14".to_string());
        params.insert("x_max".to_string(), "3.14".to_string());
        params.insert("points".to_string(), "100".to_string());

        let result = handler
            .execute(&expr, &classification, &params)
            .await
            .unwrap();
        let spec = result.result_plain.unwrap();
        assert!(spec.contains("-3.14"));
        assert!(spec.contains("3.14"));
        assert!(spec.contains("100"));
    }
}
