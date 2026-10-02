// Marabunta - Licensed under the MIT License.
//! Expression explanation handler for the Switchboard pipeline.
//!
//! Generates a plain-language explanation of what a mathematical expression
//! means, what its components are, and what operations are involved. Useful
//! as a first step before diving into computation.

use std::collections::HashMap;

use crate::switchboard::errors::SwitchboardResult;
use crate::switchboard::handlers::ComputeHandler;
use crate::switchboard::types::{
    Applicability, Category, ClassificationResult, ComputeResult, HandlerId,
    NormalizedExpression,
};

// ============================================================================
// Explain handler
// ============================================================================

/// Handler that produces plain-language explanations of expressions.
///
/// This handler has no external dependencies and works entirely from the
/// structural information already present in the `NormalizedExpression` and
/// `ClassificationResult`.
pub struct Explain;

impl Explain {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Explain {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl ComputeHandler for Explain {
    fn id(&self) -> HandlerId {
        HandlerId::from("explain")
    }

    fn name(&self) -> &str {
        "Explain Expression"
    }

    fn description(&self) -> &str {
        "Plain-language explanation of what the expression means and its components"
    }

    fn supported_categories(&self) -> Vec<Category> {
        vec![
            Category::Algebra,
            Category::Calculus,
            Category::LinearAlgebra,
            Category::DifferentialEquations,
            Category::Statistics,
            Category::NumberTheory,
            Category::Combinatorics,
            Category::Geometry,
            Category::Optimization,
            Category::Unknown,
        ]
    }

    fn applicability(&self, classification: &ClassificationResult) -> Applicability {
        match classification.category {
            Category::Unknown => Applicability::Recommended,
            _ => Applicability::Applicable,
        }
    }

    async fn execute(
        &self,
        expr: &NormalizedExpression,
        classification: &ClassificationResult,
        _params: &HashMap<String, String>,
    ) -> SwitchboardResult<ComputeResult> {
        let explanation = explain_expression(expr, classification);

        let result = ComputeResult::success(
            self.id(),
            "explain".to_string(),
            self.estimated_time_ms(),
        )
        .with_plain(explanation);

        Ok(result)
    }

    fn estimated_time_ms(&self) -> u64 {
        800
    }
}

// ============================================================================
// Explanation generation
// ============================================================================

/// Build a plain-language explanation from the expression and its classification.
///
/// The explanation covers:
/// 1. The category and what area of mathematics it belongs to.
/// 2. Structural features (integrals, derivatives, matrices, etc.).
/// 3. The detected operations that could be applied.
/// 4. Any symbols found in the expression.
pub fn explain_expression(
    expr: &NormalizedExpression,
    classification: &ClassificationResult,
) -> String {
    let mut parts: Vec<String> = Vec::new();

    // Opening summary
    parts.push(format!(
        "This expression belongs to the field of {}.",
        category_description(&classification.category),
    ));

    // Expression text
    parts.push(format!(
        "The expression is: {}",
        expr.plain_text,
    ));

    // Structural features
    let features = describe_features(expr);
    if !features.is_empty() {
        parts.push(format!("Structural features: {}.", features.join(", ")));
    }

    // Symbols
    if !expr.symbols.is_empty() {
        parts.push(format!(
            "Symbols involved: {}.",
            expr.symbols.join(", "),
        ));
    }

    // Operations
    if !classification.operations.is_empty() {
        let op_names: Vec<String> = classification
            .operations
            .iter()
            .map(|op| op.to_string())
            .collect();
        parts.push(format!(
            "Applicable operations: {}.",
            op_names.join(", "),
        ));
    }

    // Confidence note
    if classification.confidence < 0.5 {
        parts.push(
            "Note: classification confidence is low; the expression may be ambiguous.".to_string(),
        );
    }

    // Ambiguities
    if !classification.ambiguities.is_empty() {
        let amb_descriptions: Vec<String> = classification
            .ambiguities
            .iter()
            .map(|a| a.description.clone())
            .collect();
        parts.push(format!(
            "Potential ambiguities detected: {}.",
            amb_descriptions.join("; "),
        ));
    }

    parts.join("\n\n")
}

/// Map a category to a human-readable description of the mathematical field.
fn category_description(category: &Category) -> &str {
    match category {
        Category::Algebra => "algebra (equations, polynomials, and symbolic manipulation)",
        Category::Calculus => "calculus (limits, derivatives, integrals, and series)",
        Category::LinearAlgebra => "linear algebra (matrices, vectors, and linear transformations)",
        Category::DifferentialEquations => {
            "differential equations (equations involving derivatives of functions)"
        }
        Category::Statistics => "statistics and probability",
        Category::NumberTheory => "number theory (properties and relationships of integers)",
        Category::Combinatorics => "combinatorics (counting, permutations, and combinations)",
        Category::Geometry => "geometry (shapes, angles, and spatial relationships)",
        Category::Optimization => "optimization (finding extrema subject to constraints)",
        Category::Unknown => "an unclassified mathematical domain",
    }
}

/// Collect human-readable descriptions of the structural features present.
fn describe_features(expr: &NormalizedExpression) -> Vec<String> {
    let mut features = Vec::new();

    if expr.is_equation {
        features.push("contains an equality".to_string());
    }
    if expr.is_inequality {
        features.push("contains an inequality".to_string());
    }
    if expr.has_integral {
        features.push("involves integration".to_string());
    }
    if expr.has_derivative {
        features.push("involves differentiation".to_string());
    }
    if expr.has_summation {
        features.push("includes a summation".to_string());
    }
    if expr.has_limit {
        features.push("includes a limit".to_string());
    }
    if expr.has_matrix {
        features.push("contains a matrix".to_string());
    }

    features
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::switchboard::types::{Ambiguity, Operation};

    fn make_expr(text: &str) -> NormalizedExpression {
        NormalizedExpression::new(text.to_string(), text.to_string())
    }

    fn make_classification(category: Category, confidence: f32) -> ClassificationResult {
        ClassificationResult::new(category, vec![Operation::Solve, Operation::Simplify], confidence)
    }

    #[tokio::test]
    async fn test_explanation_generation() {
        let handler = Explain::new();
        let mut expr = make_expr("x^2 + 3x + 2 = 0");
        expr.is_equation = true;
        expr.symbols = vec!["x".to_string()];
        let classification = make_classification(Category::Algebra, 0.9);
        let params = HashMap::new();

        let result = handler.execute(&expr, &classification, &params).await.unwrap();
        assert!(result.success);

        let explanation = result.result_plain.unwrap();
        assert!(explanation.contains("algebra"));
        assert!(explanation.contains("x^2 + 3x + 2 = 0"));
        assert!(explanation.contains("contains an equality"));
        assert!(explanation.contains("solve"));
    }

    #[test]
    fn test_applicability_unknown_is_recommended() {
        let handler = Explain::new();
        let classification = make_classification(Category::Unknown, 0.3);
        assert_eq!(handler.applicability(&classification), Applicability::Recommended);
    }

    #[test]
    fn test_applicability_known_is_applicable() {
        let handler = Explain::new();
        let classification = make_classification(Category::Calculus, 0.9);
        assert_eq!(handler.applicability(&classification), Applicability::Applicable);
    }

    #[test]
    fn test_handler_metadata() {
        let handler = Explain::new();
        assert_eq!(handler.id(), HandlerId::from("explain"));
        assert_eq!(handler.name(), "Explain Expression");
        assert_eq!(handler.estimated_time_ms(), 800);
        assert!(!handler.requires_api_key());
        assert_eq!(handler.supported_categories().len(), 10);
        assert_eq!(
            handler.description(),
            "Plain-language explanation of what the expression means and its components"
        );
    }
}
