// Marabunta - Licensed under the MIT License.
use chrono::Utc;
use once_cell::sync::Lazy;
use regex::Regex;

use super::types::{Ambiguity, Category, ClassificationResult, NormalizedExpression, Operation};

// Regex patterns compiled once at startup
static RE_INTEGRAL: Lazy<Regex> = Lazy::new(|| Regex::new(r"\\int").unwrap());
static RE_DERIVATIVE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\\frac\{d\}|\\partial|\\nabla|\\frac\{d[a-z]\}|\\dot\{|'").unwrap()
});
static RE_SUMMATION: Lazy<Regex> = Lazy::new(|| Regex::new(r"\\sum").unwrap());
static RE_PRODUCT: Lazy<Regex> = Lazy::new(|| Regex::new(r"\\prod").unwrap());
static RE_LIMIT: Lazy<Regex> = Lazy::new(|| Regex::new(r"\\lim").unwrap());
static RE_MATRIX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\\begin\{(matrix|pmatrix|bmatrix|vmatrix)\}").unwrap());
static RE_EQUALS: Lazy<Regex> = Lazy::new(|| Regex::new(r"[^\\]=|^=").unwrap());
static RE_INEQUALITY: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"<|>|\\le|\\ge|\\leq|\\geq|\\ne|\\neq").unwrap());
static RE_FRACTION: Lazy<Regex> = Lazy::new(|| Regex::new(r"\\frac\{").unwrap());
static RE_EXPONENT: Lazy<Regex> = Lazy::new(|| Regex::new(r"\^").unwrap());
static RE_SUBSCRIPT: Lazy<Regex> = Lazy::new(|| Regex::new(r"_").unwrap());

// Greek letters
static RE_GREEK: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\\(alpha|beta|gamma|delta|epsilon|zeta|eta|theta|iota|kappa|lambda|mu|nu|xi|pi|rho|sigma|tau|upsilon|phi|chi|psi|omega|Gamma|Delta|Theta|Lambda|Xi|Pi|Sigma|Phi|Psi|Omega)(?:\b|[^a-zA-Z])").unwrap()
});

// Named functions
static RE_FUNCTION: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\\(sin|cos|tan|sec|csc|cot|sinh|cosh|tanh|arcsin|arccos|arctan|ln|log|exp|sqrt|min|max|gcd|lcm|det|tr)(?:\b|[^a-zA-Z])").unwrap()
});

// Variables (single letters not part of functions)
static RE_VARIABLE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?:^|[^a-zA-Z\\])([a-zA-Z])(?:[^a-zA-Z]|$)").unwrap());

// Statistical symbols
static RE_STATISTICAL: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\\mathbb\{E\}|\\text\{Var\}|\\mathbb\{P\}|\\sigma|\\mu").unwrap());

// Number theory patterns
static RE_NUMBER_THEORY: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\\mod|\\gcd|\\lcm|!|\\text\{mod\}").unwrap());

// Optimization patterns
static RE_OPTIMIZATION: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\\min|\\max|\\text\{subject to\}|\\text\{s\.t\.\}").unwrap());

// Implicit multiplication pattern
static RE_IMPLICIT_MULT: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"([a-zA-Z0-9])\s*([a-zA-Z])").unwrap());

// Function application pattern
static RE_FUNCTION_APP: Lazy<Regex> = Lazy::new(|| Regex::new(r"([a-zA-Z])\(").unwrap());

// Plus/minus terms
static RE_TERM: Lazy<Regex> = Lazy::new(|| Regex::new(r"[+\-]").unwrap());

/// Structural analysis of a mathematical expression
#[derive(Debug, Clone)]
pub struct ExpressionStructure {
    pub has_equals: bool,
    pub has_inequality: bool,
    pub has_integral: bool,
    pub has_derivative: bool,
    pub has_summation: bool,
    pub has_limit: bool,
    pub has_matrix: bool,
    pub has_fraction: bool,
    pub has_exponent: bool,
    pub has_subscript: bool,
    pub nesting_depth: usize,
    pub term_count: usize,
}

/// Main classification entry point
pub fn classify(expr: &NormalizedExpression) -> ClassificationResult {
    // Extract symbols if not already done
    let symbols = if expr.symbols.is_empty() {
        extract_symbols(&expr.latex)
    } else {
        expr.symbols.clone()
    };

    // Analyze structure
    let structure = analyze_structure(&expr.latex);

    // Determine category and confidence
    let (category, base_confidence) = determine_category(expr, &structure);

    // Determine applicable operations
    let operations = determine_operations(&category, expr, &structure);

    // Adjust confidence based on clarity of structure
    let confidence = adjust_confidence(base_confidence, &structure, &symbols);

    // Detect ambiguities
    let ambiguities = detect_ambiguities(expr, &structure);

    ClassificationResult {
        category,
        operations,
        confidence: confidence.clamp(0.0, 1.0),
        ambiguities,
        classified_at: Utc::now(),
    }
}

/// Extract mathematical symbols from LaTeX
pub fn extract_symbols(latex: &str) -> Vec<String> {
    let mut symbols = Vec::new();

    // Extract Greek letters
    for cap in RE_GREEK.captures_iter(latex) {
        if let Some(greek) = cap.get(1) {
            symbols.push(format!("\\{}", greek.as_str()));
        }
    }

    // Extract named functions
    for cap in RE_FUNCTION.captures_iter(latex) {
        if let Some(func) = cap.get(1) {
            symbols.push(format!("\\{}", func.as_str()));
        }
    }

    // Extract variables (single letters)
    // Exclude common function abbreviations and text macros
    let excluded_vars = ["e", "i", "d", "o", "s", "t", "a"];
    for cap in RE_VARIABLE.captures_iter(latex) {
        if let Some(var) = cap.get(1) {
            let var_str = var.as_str();
            // Only add if it's not surrounded by backslash (part of a macro)
            let pos = var.start();
            if pos > 0 && latex.chars().nth(pos - 1) == Some('\\') {
                continue;
            }
            if !excluded_vars.contains(&var_str) || latex.len() < 20 {
                symbols.push(var_str.to_string());
            }
        }
    }

    // Add integral/derivative operators
    if RE_INTEGRAL.is_match(latex) {
        symbols.push("\\int".to_string());
    }
    if RE_SUMMATION.is_match(latex) {
        symbols.push("\\sum".to_string());
    }
    if RE_PRODUCT.is_match(latex) {
        symbols.push("\\prod".to_string());
    }
    if RE_LIMIT.is_match(latex) {
        symbols.push("\\lim".to_string());
    }

    // Sort and deduplicate
    symbols.sort();
    symbols.dedup();

    symbols
}

/// Analyze the structural properties of a LaTeX expression
pub fn analyze_structure(latex: &str) -> ExpressionStructure {
    let has_equals = RE_EQUALS.is_match(latex);
    let has_inequality = RE_INEQUALITY.is_match(latex);
    let has_integral = RE_INTEGRAL.is_match(latex);
    let has_derivative = RE_DERIVATIVE.is_match(latex);
    let has_summation = RE_SUMMATION.is_match(latex);
    let has_limit = RE_LIMIT.is_match(latex);
    let has_matrix = RE_MATRIX.is_match(latex);
    let has_fraction = RE_FRACTION.is_match(latex);
    let has_exponent = RE_EXPONENT.is_match(latex);
    let has_subscript = RE_SUBSCRIPT.is_match(latex);

    // Calculate nesting depth based on braces
    let mut nesting_depth: usize = 0;
    let mut max_depth: usize = 0;
    for ch in latex.chars() {
        match ch {
            '{' => {
                nesting_depth += 1;
                max_depth = max_depth.max(nesting_depth);
            }
            '}' => {
                nesting_depth = nesting_depth.saturating_sub(1);
            }
            _ => {}
        }
    }

    // Count terms (rough estimate based on + and - at low nesting)
    let term_count = RE_TERM.find_iter(latex).count() + 1;

    ExpressionStructure {
        has_equals,
        has_inequality,
        has_integral,
        has_derivative,
        has_summation,
        has_limit,
        has_matrix,
        has_fraction,
        has_exponent,
        has_subscript,
        nesting_depth: max_depth,
        term_count,
    }
}

/// Determine the category and base confidence
fn determine_category(
    expr: &NormalizedExpression,
    structure: &ExpressionStructure,
) -> (Category, f32) {
    let latex = &expr.latex;

    // Priority 1: Matrix operations
    if structure.has_matrix {
        return (Category::LinearAlgebra, 0.9);
    }

    // Priority 2: Differential equations (derivative + equals — must check before pure calculus)
    if structure.has_derivative && structure.has_equals {
        return (Category::DifferentialEquations, 0.85);
    }

    // Priority 3: Calculus operations
    if structure.has_integral {
        return (Category::Calculus, 0.9);
    }
    if structure.has_derivative {
        return (Category::Calculus, 0.9);
    }
    if structure.has_limit {
        return (Category::Calculus, 0.85);
    }

    // Priority 4: Statistics
    if RE_STATISTICAL.is_match(latex) {
        return (Category::Statistics, 0.8);
    }

    // Priority 5: Number theory
    if RE_NUMBER_THEORY.is_match(latex) {
        return (Category::NumberTheory, 0.8);
    }

    // Priority 6: Optimization
    if RE_OPTIMIZATION.is_match(latex) && (structure.has_equals || structure.has_inequality) {
        return (Category::Optimization, 0.75);
    }

    // Priority 7: Combinatorics (summation with discrete indices)
    if structure.has_summation {
        let symbols = extract_symbols(latex);
        if symbols.iter().any(|s| s == "n" || s == "k" || s == "m") {
            // Check if it's a series (infinite) or combinatorics (discrete)
            if latex.contains("infty") || latex.contains("\\infty") {
                return (Category::Calculus, 0.7); // Infinite series
            } else {
                return (Category::Combinatorics, 0.7);
            }
        }
    }

    // Priority 8: Geometry (special symbols or functions)
    if latex.contains("\\angle")
        || latex.contains("\\triangle")
        || latex.contains("\\parallel")
        || latex.contains("\\perp")
    {
        return (Category::Geometry, 0.75);
    }

    // Priority 9: Algebra (equations with polynomials)
    if structure.has_equals {
        return (Category::Algebra, 0.85);
    }

    // Priority 10: Algebra (inequalities)
    if structure.has_inequality {
        return (Category::Algebra, 0.75);
    }

    // Default: Basic algebra
    (Category::Algebra, 0.5)
}

/// Determine applicable operations based on category and structure
fn determine_operations(
    category: &Category,
    _expr: &NormalizedExpression,
    structure: &ExpressionStructure,
) -> Vec<Operation> {
    let mut operations = Vec::new();

    match category {
        Category::Algebra => {
            if structure.has_equals {
                operations.push(Operation::Solve);
            }
            operations.push(Operation::Simplify);
            if structure.has_exponent && structure.term_count > 1 {
                operations.push(Operation::Factor);
            }
            operations.push(Operation::Expand);
        }
        Category::Calculus => {
            if structure.has_derivative || (!structure.has_integral && !structure.has_limit) {
                operations.push(Operation::Differentiate);
            }
            if structure.has_integral || (!structure.has_derivative && !structure.has_limit) {
                operations.push(Operation::Integrate);
            }
            if structure.has_limit {
                operations.push(Operation::Limit);
            }
            if structure.has_summation {
                operations.push(Operation::Series);
            }
            operations.push(Operation::Simplify);
        }
        Category::LinearAlgebra => {
            operations.push(Operation::EigenDecompose);
            operations.push(Operation::Determinant);
            operations.push(Operation::MatrixInverse);
            operations.push(Operation::Simplify);
        }
        Category::DifferentialEquations => {
            operations.push(Operation::Solve);
            operations.push(Operation::Simplify);
        }
        Category::Statistics => {
            operations.push(Operation::Probability);
            operations.push(Operation::Regression);
            operations.push(Operation::Simplify);
        }
        Category::NumberTheory => {
            operations.push(Operation::Simplify);
            if structure.has_equals {
                operations.push(Operation::Solve);
            }
        }
        Category::Combinatorics => {
            operations.push(Operation::Simplify);
            operations.push(Operation::Expand);
        }
        Category::Geometry => {
            operations.push(Operation::Solve);
            operations.push(Operation::Simplify);
        }
        Category::Optimization => {
            operations.push(Operation::Solve);
            operations.push(Operation::Simplify);
        }
        Category::Unknown => {
            operations.push(Operation::Simplify);
        }
    }

    operations
}

/// Adjust confidence based on expression clarity
fn adjust_confidence(
    base_confidence: f32,
    structure: &ExpressionStructure,
    symbols: &[String],
) -> f32 {
    let mut confidence = base_confidence;

    // Boost confidence for clear structural markers
    if structure.has_integral || structure.has_derivative || structure.has_matrix {
        confidence += 0.05;
    }

    // Reduce confidence for very simple expressions (might be ambiguous)
    // But not when clear structural markers are present
    let has_clear_markers = structure.has_integral || structure.has_derivative
        || structure.has_matrix || structure.has_limit || structure.has_summation;
    if symbols.len() <= 2 && structure.term_count <= 2 && structure.nesting_depth == 0
        && !has_clear_markers
    {
        confidence -= 0.1;
    }

    // Reduce confidence for very complex expressions (harder to classify)
    if structure.nesting_depth > 4 {
        confidence -= 0.05;
    }

    // Boost confidence for equations (clear intent)
    if structure.has_equals {
        confidence += 0.05;
    }

    confidence
}

/// Detect potential ambiguities in the expression
fn detect_ambiguities(
    expr: &NormalizedExpression,
    structure: &ExpressionStructure,
) -> Vec<Ambiguity> {
    let mut ambiguities = Vec::new();
    let latex = &expr.latex;

    // Detect implicit multiplication
    if RE_IMPLICIT_MULT.is_match(latex) && !latex.contains("\\cdot") && !latex.contains("\\times")
    {
        // Check if it's actually implicit multiplication
        for cap in RE_IMPLICIT_MULT.captures_iter(latex) {
            let full_match = cap.get(0).unwrap();
            let pos = full_match.start();

            // Skip if it's part of a function name or macro
            if pos > 0 && latex.chars().nth(pos - 1) == Some('\\') {
                continue;
            }

            // Skip if there's a clear operator
            let text = full_match.as_str();
            if !text.contains('+')
                && !text.contains('-')
                && !text.contains('=')
                && text.chars().filter(|c| c.is_alphabetic()).count() >= 2
            {
                ambiguities.push(
                    Ambiguity::new("Implicit multiplication detected".to_string())
                        .with_alternatives(vec![
                            "Product of variables".to_string(),
                            "Multi-letter variable name".to_string(),
                        ])
                        .with_position(pos),
                );
                break; // Only report once
            }
        }
    }

    // Detect function application ambiguity
    if RE_FUNCTION_APP.is_match(latex) {
        for cap in RE_FUNCTION_APP.captures_iter(latex) {
            let var = cap.get(1).unwrap().as_str();
            let pos = cap.get(0).unwrap().start();

            // Skip known functions
            if matches!(
                var,
                "f" | "g" | "h" | "p" | "q" | "r" | "F" | "G" | "H" | "P" | "Q"
            ) {
                // Could be function or multiplication
                ambiguities.push(
                    Ambiguity::new(format!("{}() could be function or multiplication", var))
                        .with_alternatives(vec![
                            format!("Function application {}(x)", var),
                            format!("Multiplication {}*(x)", var),
                        ])
                        .with_position(pos),
                );
                break; // Only report once
            }
        }
    }

    // Detect missing parentheses in fractions
    if structure.has_fraction && structure.term_count > 2 {
        ambiguities.push(Ambiguity::new(
            "Complex fraction may need explicit parentheses".to_string(),
        ));
    }

    // Detect variable name ambiguity
    if !structure.has_equals
        && !structure.has_inequality
        && structure.nesting_depth == 0
        && structure.term_count <= 1
    {
        let symbols = extract_symbols(latex);
        if symbols.len() == 1 && symbols[0].chars().all(|c| c.is_alphabetic()) {
            ambiguities.push(
                Ambiguity::new("Single symbol could be variable or expression".to_string())
                    .with_alternatives(vec![
                        "Variable name".to_string(),
                        "Expression to evaluate".to_string(),
                    ]),
            );
        }
    }

    ambiguities
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_expr(latex: &str) -> NormalizedExpression {
        let mut expr = NormalizedExpression::new(latex.to_string(), latex.to_string());
        expr.symbols = extract_symbols(latex);
        let structure = analyze_structure(latex);
        expr.is_equation = structure.has_equals;
        expr.is_inequality = structure.has_inequality;
        expr.has_integral = structure.has_integral;
        expr.has_derivative = structure.has_derivative;
        expr.has_summation = structure.has_summation;
        expr.has_limit = structure.has_limit;
        expr.has_matrix = structure.has_matrix;
        expr
    }

    #[test]
    fn test_classify_polynomial() {
        let expr = make_expr("x^2 + 3x + 2 = 0");
        let result = classify(&expr);
        assert_eq!(result.category, Category::Algebra);
        assert!(result.operations.contains(&Operation::Solve));
        assert!(result.confidence > 0.7);
    }

    #[test]
    fn test_classify_integral() {
        let expr = make_expr("\\int_0^1 x^2 dx");
        let result = classify(&expr);
        assert_eq!(result.category, Category::Calculus);
        assert!(result.operations.contains(&Operation::Integrate));
        assert!(result.confidence > 0.8);
    }

    #[test]
    fn test_classify_matrix() {
        let expr = make_expr("\\begin{matrix} 1 & 2 \\\\ 3 & 4 \\end{matrix}");
        let result = classify(&expr);
        assert_eq!(result.category, Category::LinearAlgebra);
        assert!(result.operations.contains(&Operation::EigenDecompose));
        assert!(result.operations.contains(&Operation::Determinant));
        assert!(result.confidence > 0.8);
    }

    #[test]
    fn test_classify_limit() {
        let expr = make_expr("\\lim_{x \\to 0} \\frac{\\sin x}{x}");
        let result = classify(&expr);
        assert_eq!(result.category, Category::Calculus);
        assert!(result.operations.contains(&Operation::Limit));
        assert!(result.confidence > 0.75);
    }

    #[test]
    fn test_classify_summation_combinatorics() {
        let expr = make_expr("\\sum_{k=1}^{n} k^2");
        let result = classify(&expr);
        assert_eq!(result.category, Category::Combinatorics);
        assert!(result.confidence > 0.6);
    }

    #[test]
    fn test_classify_summation_series() {
        let expr = make_expr("\\sum_{n=1}^{\\infty} \\frac{1}{n^2}");
        let result = classify(&expr);
        assert_eq!(result.category, Category::Calculus);
    }

    #[test]
    fn test_classify_derivative() {
        let expr = make_expr("\\frac{d}{dx} x^3");
        let result = classify(&expr);
        assert_eq!(result.category, Category::Calculus);
        assert!(result.operations.contains(&Operation::Differentiate));
    }

    #[test]
    fn test_classify_differential_equation() {
        let expr = make_expr("\\frac{dy}{dx} = 2x");
        let result = classify(&expr);
        assert_eq!(result.category, Category::DifferentialEquations);
        assert!(result.operations.contains(&Operation::Solve));
    }

    #[test]
    fn test_extract_symbols_simple() {
        let symbols = extract_symbols("x^2 + y");
        assert!(symbols.contains(&"x".to_string()));
        assert!(symbols.contains(&"y".to_string()));
    }

    #[test]
    fn test_extract_symbols_greek() {
        let symbols = extract_symbols("\\alpha + \\beta = \\gamma");
        assert!(symbols.contains(&"\\alpha".to_string()));
        assert!(symbols.contains(&"\\beta".to_string()));
        assert!(symbols.contains(&"\\gamma".to_string()));
    }

    #[test]
    fn test_extract_symbols_functions() {
        let symbols = extract_symbols("\\sin x + \\cos y");
        assert!(symbols.contains(&"\\sin".to_string()));
        assert!(symbols.contains(&"\\cos".to_string()));
    }

    #[test]
    fn test_extract_symbols_operators() {
        let symbols = extract_symbols("\\int x dx + \\sum_{i=1}^{n} i");
        assert!(symbols.contains(&"\\int".to_string()));
        assert!(symbols.contains(&"\\sum".to_string()));
    }

    #[test]
    fn test_analyze_structure_equation() {
        let structure = analyze_structure("x + 1 = 0");
        assert!(structure.has_equals);
        assert!(!structure.has_inequality);
        assert!(!structure.has_integral);
    }

    #[test]
    fn test_analyze_structure_complex() {
        let structure = analyze_structure("\\int_0^1 \\frac{x^2}{x+1} dx");
        assert!(structure.has_integral);
        assert!(structure.has_fraction);
        assert!(structure.has_exponent);
        assert!(structure.has_subscript);
        assert!(structure.nesting_depth > 0);
    }

    #[test]
    fn test_detect_ambiguities_implicit_mult() {
        let expr = make_expr("xy + z");
        let structure = analyze_structure(&expr.latex);
        let ambiguities = detect_ambiguities(&expr, &structure);
        assert!(ambiguities
            .iter()
            .any(|a| a.description.contains("Implicit multiplication")));
    }

    #[test]
    fn test_operations_algebra() {
        let expr = make_expr("x^2 - 1 = 0");
        let structure = analyze_structure(&expr.latex);
        let operations = determine_operations(&Category::Algebra, &expr, &structure);
        assert!(operations.contains(&Operation::Solve));
        assert!(operations.contains(&Operation::Simplify));
        assert!(operations.contains(&Operation::Factor));
    }

    #[test]
    fn test_confidence_adjustment() {
        let structure = analyze_structure("\\int_0^1 x^2 dx");
        let symbols = extract_symbols("\\int_0^1 x^2 dx");
        let adjusted = adjust_confidence(0.8, &structure, &symbols);
        assert!(adjusted >= 0.8); // Should boost or maintain
    }

    #[test]
    fn test_confidence_within_bounds() {
        let expr = make_expr("x^2 + 1 = 0");
        let result = classify(&expr);
        assert!(result.confidence >= 0.0 && result.confidence <= 1.0);
    }
}
