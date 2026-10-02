// Marabunta - Licensed under the MIT License.
//! Wolfram Alpha API oracle.
//!
//! Queries the Wolfram Alpha Short Answers API to evaluate mathematical
//! expressions. Requires a valid App ID configured in `SwitchboardConfig`.

use async_trait::async_trait;

use crate::switchboard::types::{Category, NormalizedExpression, Operation};

use super::{MathOracle, OracleError, OracleResult};

// ============================================================================
// WolframOracle
// ============================================================================

/// Oracle backed by the Wolfram Alpha API.
///
/// # Priority
///
/// Priority is **2** (after SymPy) because it requires an API key and
/// network round-trips.
pub struct WolframOracle {
    app_id: String,
    client: reqwest::Client,
}

impl WolframOracle {
    /// Create a new Wolfram Alpha oracle with the given App ID.
    pub fn new(app_id: impl Into<String>) -> Self {
        Self {
            app_id: app_id.into(),
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl MathOracle for WolframOracle {
    fn name(&self) -> &str {
        "wolfram"
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
        ]
    }

    fn supported_operations(&self) -> Vec<Operation> {
        vec![
            Operation::Solve,
            Operation::Simplify,
            Operation::Factor,
            Operation::Expand,
            Operation::Differentiate,
            Operation::Integrate,
            Operation::Limit,
            Operation::Series,
            Operation::Determinant,
            Operation::MatrixInverse,
            Operation::EigenDecompose,
        ]
    }

    async fn solve(
        &self,
        expr: &NormalizedExpression,
        operation: &Operation,
    ) -> Result<OracleResult, OracleError> {
        // Build the query string with the operation hint.
        let query = format!("{} {}", operation, expr.plain_text);

        // Use the Short Answers API.
        let response = self
            .client
            .get("https://api.wolframalpha.com/v1/result")
            .query(&[("appid", self.app_id.as_str()), ("i", query.as_str())])
            .send()
            .await
            .map_err(|e| OracleError::transient("wolfram", format!("HTTP error: {e}")))?;

        let status = response.status();
        if !status.is_success() {
            return Err(if status.as_u16() == 403 {
                OracleError::permanent("wolfram", "invalid App ID or quota exceeded")
            } else if status.is_server_error() {
                OracleError::transient("wolfram", format!("server error: {status}"))
            } else {
                OracleError::permanent("wolfram", format!("HTTP {status}"))
            });
        }

        let body = response
            .text()
            .await
            .map_err(|e| OracleError::transient("wolfram", format!("body read error: {e}")))?;

        if body.contains("Wolfram|Alpha did not understand")
            || body.contains("No short answer")
        {
            return Err(OracleError::permanent(
                "wolfram",
                "Wolfram Alpha did not understand the query",
            ));
        }

        Ok(OracleResult::new("wolfram")
            .with_plain_text(&body)
            .with_confidence(0.85))
    }

    fn priority(&self) -> u32 {
        2
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oracle_name() {
        let oracle = WolframOracle::new("test-app-id");
        assert_eq!(oracle.name(), "wolfram");
    }

    #[test]
    fn test_oracle_priority() {
        let oracle = WolframOracle::new("test-app-id");
        assert_eq!(oracle.priority(), 2);
    }

    #[test]
    fn test_supported_categories() {
        let oracle = WolframOracle::new("test");
        let cats = oracle.supported_categories();
        assert!(cats.contains(&Category::Algebra));
        assert!(cats.contains(&Category::Calculus));
        assert!(cats.contains(&Category::Statistics));
        assert!(cats.contains(&Category::Geometry));
        assert_eq!(cats.len(), 9);
    }

    #[test]
    fn test_supported_operations() {
        let oracle = WolframOracle::new("test");
        let ops = oracle.supported_operations();
        assert!(ops.contains(&Operation::Solve));
        assert!(ops.contains(&Operation::Integrate));
        assert!(ops.contains(&Operation::Determinant));
        assert_eq!(ops.len(), 11);
    }
}
