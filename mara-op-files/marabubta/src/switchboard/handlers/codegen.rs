// Marabunta - Licensed under the MIT License.
//! Template-based code generation handler for the Switchboard pipeline.
//!
//! Generates executable code in Python, Rust, C++, Julia, or MATLAB from
//! a normalized mathematical expression. Each language template uses
//! idiomatic libraries (e.g., sympy for Python, nalgebra concepts for Rust).

use std::collections::HashMap;

use crate::switchboard::errors::SwitchboardResult;
use crate::switchboard::handlers::ComputeHandler;
use crate::switchboard::types::{
    Applicability, Category, ClassificationResult, ComputeResult, HandlerId,
    NormalizedExpression,
};

// ============================================================================
// TargetLanguage
// ============================================================================

/// Supported target languages for code generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TargetLanguage {
    Python,
    Rust,
    Cpp,
    Julia,
    Matlab,
}

impl TargetLanguage {
    /// Parse a language string (case-insensitive) into a `TargetLanguage`.
    /// Returns `None` for unrecognized strings.
    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "python" | "py" => Some(Self::Python),
            "rust" | "rs" => Some(Self::Rust),
            "cpp" | "c++" => Some(Self::Cpp),
            "julia" | "jl" => Some(Self::Julia),
            "matlab" | "m" => Some(Self::Matlab),
            _ => None,
        }
    }

    /// Returns the display name used in generated comments.
    pub fn display_name(&self) -> &str {
        match self {
            Self::Python => "Python",
            Self::Rust => "Rust",
            Self::Cpp => "C++",
            Self::Julia => "Julia",
            Self::Matlab => "MATLAB",
        }
    }
}

// ============================================================================
// GenerateCode handler
// ============================================================================

/// Code generation handler that produces executable templates.
///
/// Reads an optional `"language"` key from the params map. If absent or
/// unrecognized, falls back to `default_language`.
pub struct GenerateCode {
    pub default_language: TargetLanguage,
}

impl GenerateCode {
    /// Create a new `GenerateCode` handler with the given default language.
    pub fn new(default_language: TargetLanguage) -> Self {
        Self { default_language }
    }
}

impl Default for GenerateCode {
    fn default() -> Self {
        Self::new(TargetLanguage::Python)
    }
}

#[async_trait::async_trait]
impl ComputeHandler for GenerateCode {
    fn id(&self) -> HandlerId {
        HandlerId::from("generate_code")
    }

    fn name(&self) -> &str {
        "Generate Code"
    }

    fn description(&self) -> &str {
        "Generate executable code in Python, Rust, C++, Julia, or MATLAB"
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
            Category::Unknown => Applicability::Marginal,
            _ => Applicability::Applicable,
        }
    }

    async fn execute(
        &self,
        expr: &NormalizedExpression,
        classification: &ClassificationResult,
        params: &HashMap<String, String>,
    ) -> SwitchboardResult<ComputeResult> {
        let language = params
            .get("language")
            .and_then(|l| TargetLanguage::from_str_loose(l))
            .unwrap_or(self.default_language);

        let code = generate_template(&expr.plain_text, &language, &classification.category);

        let result = ComputeResult::success(
            self.id(),
            format!("codegen_{}", language.display_name().to_lowercase()),
            self.estimated_time_ms(),
        )
        .with_plain(code);

        Ok(result)
    }

    fn estimated_time_ms(&self) -> u64 {
        500
    }
}

// ============================================================================
// Template generation
// ============================================================================

/// Generate a code template for the given expression, language, and category.
///
/// The templates are intentionally simple and use well-known libraries
/// for each target language.
pub fn generate_template(expr: &str, language: &TargetLanguage, category: &Category) -> String {
    match language {
        TargetLanguage::Python => generate_python(expr, category),
        TargetLanguage::Rust => generate_rust(expr, category),
        TargetLanguage::Cpp => generate_cpp(expr, category),
        TargetLanguage::Julia => generate_julia(expr, category),
        TargetLanguage::Matlab => generate_matlab(expr, category),
    }
}

fn generate_python(expr: &str, category: &Category) -> String {
    let category_comment = format!("# Category: {}", category);
    format!(
        r#"#!/usr/bin/env python3
"""Auto-generated code for: {expr}"""
{category_comment}
from sympy import symbols, solve, simplify, integrate, diff, Matrix

x, y, z = symbols('x y z')

expr = {expr}
result = simplify(expr)
print(f"Result: {{result}}")
"#,
        expr = expr,
        category_comment = category_comment,
    )
}

fn generate_rust(expr: &str, category: &Category) -> String {
    let category_comment = format!("// Category: {}", category);
    
    // Extremely basic translation of standard math strings to valid Rust syntax
    // e.g. "x^2 + 2*x" -> "x.powi(2) + 2.0 * x"
    let mut safe_expr = expr.to_string();
    safe_expr = safe_expr.replace("^2", ".powi(2)");
    safe_expr = safe_expr.replace("^3", ".powi(3)");
    safe_expr = safe_expr.replace("sin(", "x.sin(");
    safe_expr = safe_expr.replace("cos(", "x.cos(");
    safe_expr = safe_expr.replace("sqrt(", "x.sqrt(");
    
    // Ensure numeric literals are floats if they aren't already
    safe_expr = safe_expr.replace(" 2 ", " 2.0 ");
    safe_expr = safe_expr.replace(" 3 ", " 3.0 ");
    safe_expr = safe_expr.replace(" 4 ", " 4.0 ");
    
    if safe_expr.trim() == expr.trim() && !safe_expr.contains("x") {
        safe_expr = "0.0 /* Failed to translate complex expression to Rust */".to_string();
    }

    format!(
        r#"//! Auto-generated code for: {expr}
{category_comment}

fn main() {{
    let x: f64 = std::env::args().nth(1).and_then(|a| a.parse().ok()).unwrap_or(1.0);
    let result = compute(x);
    println!("Result: {{}}", result);
}}

fn compute(x: f64) -> f64 {{
    {safe_expr}
}}
"#,
        expr = expr,
        category_comment = category_comment,
        safe_expr = safe_expr
    )
}

fn generate_cpp(expr: &str, category: &Category) -> String {
    let category_comment = format!("// Category: {}", category);
    
    // Basic C++ transpilation
    let mut safe_expr = expr.to_string();
    safe_expr = safe_expr.replace("^2", " * x");
    safe_expr = safe_expr.replace("^3", " * x * x");

    format!(
        r#"// Auto-generated code for: {expr}
{category_comment}
#include <iostream>
#include <cmath>
#include <cstdlib>

double compute(double x) {{
    return {safe_expr};
}}

int main(int argc, char** argv) {{
    double x = (argc > 1) ? std::atof(argv[1]) : 1.0;
    std::cout << "Result: " << compute(x) << std::endl;
    return 0;
}}
"#,
        expr = expr,
        category_comment = category_comment,
        safe_expr = safe_expr
    )
}

fn generate_julia(expr: &str, category: &Category) -> String {
    let category_comment = format!("# Category: {}", category);
    format!(
        r#"# Auto-generated code for: {expr}
{category_comment}

x = length(ARGS) > 0 ? parse(Float64, ARGS[1]) : 1.0

# Expression: {expr}
result = {expr}
println("Result: $result")
"#,
        expr = expr,
        category_comment = category_comment,
    )
}

fn generate_matlab(expr: &str, category: &Category) -> String {
    let category_comment = format!("%% Category: {}", category);
    format!(
        r#"%% Auto-generated code for: {expr}
{category_comment}
syms x y z;

expr = {expr};
result = simplify(expr);
disp(result);
"#,
        expr = expr,
        category_comment = category_comment,
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
        ClassificationResult::new(category, vec![Operation::Simplify], confidence)
    }

    #[tokio::test]
    async fn test_python_template_generation() {
        let handler = GenerateCode::default();
        let expr = make_expr("x^2 + 1");
        let classification = make_classification(Category::Algebra, 0.9);
        let params = HashMap::new();

        let result = handler.execute(&expr, &classification, &params).await.unwrap();
        assert!(result.success);
        let code = result.result_plain.unwrap();
        assert!(code.contains("sympy"));
        assert!(code.contains("x^2 + 1"));
        assert!(code.contains("# Category: algebra"));
    }

    #[tokio::test]
    async fn test_language_selection_from_params() {
        let handler = GenerateCode::default();
        let expr = make_expr("x^2 + 1");
        let classification = make_classification(Category::Algebra, 0.9);
        let mut params = HashMap::new();
        params.insert("language".to_string(), "rust".to_string());

        let result = handler.execute(&expr, &classification, &params).await.unwrap();
        let code = result.result_plain.unwrap();
        assert!(code.contains("fn main()"));
        assert!(code.contains("f64"));
    }

    #[tokio::test]
    async fn test_default_language_fallback() {
        let handler = GenerateCode::new(TargetLanguage::Julia);
        let expr = make_expr("sin(x)");
        let classification = make_classification(Category::Calculus, 0.8);
        let params = HashMap::new(); // no language param

        let result = handler.execute(&expr, &classification, &params).await.unwrap();
        let code = result.result_plain.unwrap();
        assert!(code.contains("SymbolicUtils"));
    }

    #[test]
    fn test_applicability_known_category() {
        let handler = GenerateCode::default();
        let classification = make_classification(Category::Calculus, 0.9);
        assert_eq!(handler.applicability(&classification), Applicability::Applicable);
    }

    #[test]
    fn test_applicability_unknown_category() {
        let handler = GenerateCode::default();
        let classification = make_classification(Category::Unknown, 0.3);
        assert_eq!(handler.applicability(&classification), Applicability::Marginal);
    }

    #[test]
    fn test_handler_metadata() {
        let handler = GenerateCode::default();
        assert_eq!(handler.id(), HandlerId::from("generate_code"));
        assert_eq!(handler.name(), "Generate Code");
        assert_eq!(handler.estimated_time_ms(), 500);
        assert!(!handler.requires_api_key());
        assert_eq!(handler.supported_categories().len(), 10);
    }
}
