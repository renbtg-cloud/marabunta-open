// Marabunta - Licensed under the MIT License.
//! Policy Domain-Specific Language (DSL)
//!
//! Provides a human-readable language for defining placement policies that
//! compiles to the Policy IR for evaluation by the policy engine.
//!
//! # DSL Syntax
//!
//! The DSL uses a simple condition => effect syntax:
//!
//! ```text
//! condition => effect [; effect ...]
//! ```
//!
//! ## Conditions
//!
//! Conditions specify when a policy rule applies:
//!
//! ```text
//! # Comparison operators
//! job.priority > 5
//! job.name = "ml-training"
//! resource.gpu >= 2
//!
//! # String matching
//! job.name matches "ml-.*"
//!
//! # Collection operations
//! job.tags has "gpu"
//! submitter.domains contains "production"
//! job.type in ["monte_carlo", "parameter_sweep"]
//!
//! # Range checking
//! job.priority between 0 and 100
//!
//! # Time windows (cron expression)
//! time in "0 9-17 * * MON-FRI"
//!
//! # Logical operators
//! job.priority > 5 AND node.region = "us-east"
//! job.priority > 50 OR submitter.domains contains "vip"
//! NOT job.type = "batch"
//! ```
//!
//! ## Effects
//!
//! Effects specify what happens when the condition is met:
//!
//! ```text
//! # Prefer nodes matching selector (soft constraint)
//! prefer node.gpu = "true" weight 0.8
//! prefer tag(gpu = "true")
//! prefer group("ml-cluster")
//! prefer all
//!
//! # Require nodes matching selector (hard constraint)
//! require node.env = "production"
//!
//! # Exclude nodes matching selector (hard constraint)
//! exclude node.region = "eu-west"
//!
//! # Affinity/Anti-affinity
//! affinity job at node weight 0.5
//! anti_affinity tag(team = "ml") at region
//! ```
//!
//! ## Examples
//!
//! ```text
//! # High priority ML jobs go to GPU nodes
//! job.priority > 50 AND job.name matches "ml-.*" => prefer tag(gpu = "true") weight 0.9
//!
//! # Production jobs only on production nodes
//! submitter.domains contains "production" => require node.env = "production"
//!
//! # GPU jobs need GPU nodes
//! resource.gpu > 0 => require node.gpu = "true"
//!
//! # Business hours scheduling
//! time in "0 9-17 * * MON-FRI" => prefer group("business-hours-cluster")
//!
//! # Multiple effects
//! job.priority > 100 => require node.env = "production"; exclude node.region = "eu-west"
//! ```
//!
//! # Usage
//!
//! ```rust
//! use marabunta_compute::policy::dsl;
//!
//! // Compile DSL to policies
//! let source = r#"
//!     job.priority > 50 => prefer tag(gpu = "true") weight 0.9
//! "#;
//!
//! // Parse and validate
//! let validation = dsl::validate(source).unwrap();
//! if !validation.valid {
//!     for err in &validation.errors {
//!         eprintln!("Error: {}", err);
//!     }
//! }
//!
//! // Compile to IR
//! let policies = dsl::compile(source).unwrap();
//! ```

pub mod ast;
pub mod compiler;
pub mod error;
pub mod lexer;
pub mod parser;
pub mod validate;

// Re-export main functionality
pub use compiler::{compile, Compiler};
pub use error::{DslError, DslErrorKind, DslResult, SourcePosition, Span};
pub use lexer::{tokenize, Lexer, Token, TokenKind};
pub use parser::{parse, Parser};
pub use validate::{
    validate, validate_with_context, DslValidationResult, DslValidator, DslWarning,
};

/// Format a DSL error with source context
pub fn format_error(error: &DslError, source: &str) -> String {
    error.format_with_source(source)
}

/// Parse, validate, and compile DSL source to policies
pub fn compile_with_validation(
    source: &str,
) -> Result<Vec<crate::policy::ir::Policy>, Vec<DslError>> {
    // First validate
    let validation = match validate(source) {
        Ok(v) => v,
        Err(e) => return Err(vec![e]),
    };

    if !validation.valid {
        return Err(validation.errors);
    }

    // Then compile
    compile(source).map_err(|e| vec![e])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_full_pipeline() {
        let source = r#"
            # High priority ML jobs
            job.priority > 50 AND job.name matches "ml-.*" => prefer tag(gpu = "true") weight 0.9

            # Production only
            submitter.domains contains "production" => require node.env = "production"
        "#;

        let policies = compile_with_validation(source).unwrap();
        assert_eq!(policies.len(), 2);
    }

    #[test]
    fn test_validation_error() {
        let source = r#"job.priority = "not-a-number" => prefer all"#;
        let result = compile_with_validation(source);
        assert!(result.is_err());
    }

    #[test]
    fn test_compile_error() {
        let source = r#"job.priority > 5 => invalid_effect all"#;
        let result = compile_with_validation(source);
        assert!(result.is_err());
    }

    #[test]
    fn test_format_error() {
        let source = r#"job.priority > 5 prefer all"#;
        let result = compile(source);
        assert!(result.is_err());

        let err = result.unwrap_err();
        let formatted = format_error(&err, source);
        assert!(formatted.contains("error:"));
    }
}
