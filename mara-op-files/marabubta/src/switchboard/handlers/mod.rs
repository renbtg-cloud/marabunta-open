// Marabunta - Licensed under the MIT License.
//! Compute handler trait and registry for the Switchboard pipeline.
//!
//! Each handler represents a distinct compute strategy (symbolic CAS, step-by-step
//! decomposition, numerical solver, visualization, code generation, etc.). The
//! `HandlerRegistry` collects all registered handlers and supports matching them
//! against a `ClassificationResult` to produce a ranked list of applicable options.

pub mod codegen;
pub mod explain;
pub mod monte_carlo;
pub mod numerical;
pub mod step_by_step;
pub mod sweep;
pub mod symbolic;
pub mod verify;
pub mod visualize;

use std::collections::HashMap;
use std::sync::Arc;

use crate::switchboard::errors::SwitchboardResult;
use crate::switchboard::types::{
    Applicability, Category, ClassificationResult, ComputeResult, HandlerId, MenuOption,
    NormalizedExpression,
};

// ============================================================================
// ComputeHandler trait
// ============================================================================

/// A compute backend that can execute mathematical operations.
///
/// Handlers are registered in the `HandlerRegistry` and selected via the
/// switchboard menu based on the classified input. Each handler declares its
/// supported categories and can self-assess its applicability for a given
/// classification result.
#[async_trait::async_trait]
pub trait ComputeHandler: Send + Sync {
    /// Unique identifier for this handler.
    fn id(&self) -> HandlerId;

    /// Human-readable name shown in the menu.
    fn name(&self) -> &str;

    /// Short description of what this handler does.
    fn description(&self) -> &str;

    /// Mathematical categories this handler can operate on.
    fn supported_categories(&self) -> Vec<Category>;

    /// Self-assess how applicable this handler is for the given classification.
    fn applicability(&self, classification: &ClassificationResult) -> Applicability;

    /// Build a `MenuOption` for presenting this handler to the user.
    fn to_menu_option(&self, classification: &ClassificationResult) -> MenuOption {
        let applicability = self.applicability(classification);
        let mut option = MenuOption::new(
            self.id(),
            self.name().to_string(),
            self.description().to_string(),
            applicability,
        )
        .with_estimated_time(self.estimated_time_ms());
        if self.requires_api_key() {
            option = option.requires_api_key();
        }
        option
    }

    /// Execute the computation against the normalized expression.
    async fn execute(
        &self,
        expr: &NormalizedExpression,
        classification: &ClassificationResult,
        params: &HashMap<String, String>,
    ) -> SwitchboardResult<ComputeResult>;

    /// Estimated wall-clock time in milliseconds for this handler.
    fn estimated_time_ms(&self) -> u64 {
        1000
    }

    /// Whether this handler requires an external API key to function.
    fn requires_api_key(&self) -> bool {
        false
    }
}

// ============================================================================
// Applicability ordering helper
// ============================================================================

/// Returns a numeric rank for `Applicability` where lower values sort first
/// (i.e., `Recommended` = 0, `NotApplicable` = 3).
fn applicability_rank(a: &Applicability) -> u8 {
    match a {
        Applicability::Recommended => 0,
        Applicability::Applicable => 1,
        Applicability::Marginal => 2,
        Applicability::NotApplicable => 3,
    }
}

// ============================================================================
// HandlerRegistry
// ============================================================================

/// Registry that holds all compute handlers and supports matching/lookup.
///
/// Handlers are stored in a `HashMap` keyed by `HandlerId`. The registry is
/// the single source of truth for which handlers are available in a given
/// switchboard instance.
pub struct HandlerRegistry {
    handlers: HashMap<HandlerId, Arc<dyn ComputeHandler>>,
}

impl HandlerRegistry {
    /// Create an empty registry with no handlers.
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    /// Register a handler. If a handler with the same `HandlerId` already
    /// exists it will be replaced silently.
    pub fn register(&mut self, handler: Arc<dyn ComputeHandler>) {
        let id = handler.id();
        self.handlers.insert(id, handler);
    }

    /// Look up a handler by its identifier.
    pub fn get(&self, id: &HandlerId) -> Option<Arc<dyn ComputeHandler>> {
        self.handlers.get(id).cloned()
    }

    /// Return every handler paired with its applicability for the given
    /// classification, sorted so that `Recommended` handlers appear first.
    ///
    /// Handlers whose applicability is `NotApplicable` are still included in
    /// the output so callers can decide whether to filter or dim them in UI.
    pub fn match_handlers(
        &self,
        classification: &ClassificationResult,
    ) -> Vec<(HandlerId, Applicability)> {
        let mut matches: Vec<(HandlerId, Applicability)> = self
            .handlers
            .values()
            .map(|h| (h.id(), h.applicability(classification)))
            .collect();

        matches.sort_by(|a, b| applicability_rank(&a.1).cmp(&applicability_rank(&b.1)));
        matches
    }

    /// Number of handlers currently registered.
    pub fn handler_count(&self) -> usize {
        self.handlers.len()
    }
}

impl Default for HandlerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::switchboard::types::{Category, ClassificationResult, Operation};

    // -----------------------------------------------------------------------
    // Mock handler for testing
    // -----------------------------------------------------------------------

    /// A configurable mock handler used exclusively in tests.
    struct MockHandler {
        handler_id: HandlerId,
        handler_name: String,
        handler_description: String,
        categories: Vec<Category>,
        est_time: u64,
        needs_api_key: bool,
    }

    impl MockHandler {
        fn new(id: &str, categories: Vec<Category>) -> Self {
            Self {
                handler_id: HandlerId::from(id),
                handler_name: format!("Mock {}", id),
                handler_description: format!("Mock handler for {}", id),
                categories,
                est_time: 500,
                needs_api_key: false,
            }
        }

        fn with_api_key(mut self) -> Self {
            self.needs_api_key = true;
            self
        }

        fn with_estimated_time(mut self, ms: u64) -> Self {
            self.est_time = ms;
            self
        }
    }

    #[async_trait::async_trait]
    impl ComputeHandler for MockHandler {
        fn id(&self) -> HandlerId {
            self.handler_id.clone()
        }

        fn name(&self) -> &str {
            &self.handler_name
        }

        fn description(&self) -> &str {
            &self.handler_description
        }

        fn supported_categories(&self) -> Vec<Category> {
            self.categories.clone()
        }

        fn applicability(&self, classification: &ClassificationResult) -> Applicability {
            if self.categories.contains(&classification.category) {
                if classification.confidence >= 0.8 {
                    Applicability::Recommended
                } else if classification.confidence >= 0.5 {
                    Applicability::Applicable
                } else {
                    Applicability::Marginal
                }
            } else {
                Applicability::NotApplicable
            }
        }

        async fn execute(
            &self,
            _expr: &NormalizedExpression,
            _classification: &ClassificationResult,
            _params: &HashMap<String, String>,
        ) -> SwitchboardResult<ComputeResult> {
            Ok(ComputeResult::success(
                self.handler_id.clone(),
                self.handler_name.clone(),
                self.est_time,
            ))
        }

        fn estimated_time_ms(&self) -> u64 {
            self.est_time
        }

        fn requires_api_key(&self) -> bool {
            self.needs_api_key
        }
    }

    // -----------------------------------------------------------------------
    // Helper to build a classification result
    // -----------------------------------------------------------------------

    fn make_classification(category: Category, confidence: f32) -> ClassificationResult {
        ClassificationResult::new(category, vec![Operation::Solve], confidence)
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_register_and_retrieve_handler() {
        let mut registry = HandlerRegistry::new();
        let handler = Arc::new(MockHandler::new("symbolic", vec![Category::Algebra]));
        registry.register(handler);

        let found = registry.get(&HandlerId::from("symbolic"));
        assert!(found.is_some());
        assert_eq!(found.unwrap().id(), HandlerId::from("symbolic"));
    }

    #[test]
    fn test_get_returns_none_for_unknown_id() {
        let registry = HandlerRegistry::new();
        assert!(registry.get(&HandlerId::from("nonexistent")).is_none());
    }

    #[test]
    fn test_handler_count_tracks_correctly() {
        let mut registry = HandlerRegistry::new();
        assert_eq!(registry.handler_count(), 0);

        registry.register(Arc::new(MockHandler::new("a", vec![Category::Algebra])));
        assert_eq!(registry.handler_count(), 1);

        registry.register(Arc::new(MockHandler::new("b", vec![Category::Calculus])));
        assert_eq!(registry.handler_count(), 2);

        // Re-register same ID replaces — count stays at 2
        registry.register(Arc::new(MockHandler::new("a", vec![Category::Geometry])));
        assert_eq!(registry.handler_count(), 2);
    }

    #[test]
    fn test_match_handlers_sorted_by_applicability() {
        let mut registry = HandlerRegistry::new();

        // This handler supports Algebra — will be Recommended for high-confidence Algebra
        registry.register(Arc::new(MockHandler::new("sym", vec![Category::Algebra])));
        // This handler supports Calculus — will be NotApplicable for Algebra
        registry.register(Arc::new(MockHandler::new("num", vec![Category::Calculus])));

        let classification = make_classification(Category::Algebra, 0.9);
        let matches = registry.match_handlers(&classification);

        assert_eq!(matches.len(), 2);
        // First entry should be Recommended (the Algebra handler)
        assert_eq!(matches[0].1, Applicability::Recommended);
        // Second entry should be NotApplicable (the Calculus handler)
        assert_eq!(matches[1].1, Applicability::NotApplicable);
    }

    #[test]
    fn test_mock_handler_applicability_recommended() {
        let handler = MockHandler::new("sym", vec![Category::Algebra]);
        let classification = make_classification(Category::Algebra, 0.9);
        assert_eq!(
            handler.applicability(&classification),
            Applicability::Recommended
        );
    }

    #[test]
    fn test_mock_handler_applicability_applicable() {
        let handler = MockHandler::new("sym", vec![Category::Algebra]);
        let classification = make_classification(Category::Algebra, 0.6);
        assert_eq!(
            handler.applicability(&classification),
            Applicability::Applicable
        );
    }

    #[test]
    fn test_mock_handler_applicability_marginal() {
        let handler = MockHandler::new("sym", vec![Category::Algebra]);
        let classification = make_classification(Category::Algebra, 0.3);
        assert_eq!(
            handler.applicability(&classification),
            Applicability::Marginal
        );
    }

    #[test]
    fn test_mock_handler_applicability_not_applicable() {
        let handler = MockHandler::new("sym", vec![Category::Algebra]);
        let classification = make_classification(Category::Statistics, 0.9);
        assert_eq!(
            handler.applicability(&classification),
            Applicability::NotApplicable
        );
    }

    #[test]
    fn test_to_menu_option_builds_correctly() {
        let handler = MockHandler::new("wolfram", vec![Category::Calculus])
            .with_api_key()
            .with_estimated_time(250);

        let classification = make_classification(Category::Calculus, 0.95);
        let option = handler.to_menu_option(&classification);

        assert_eq!(option.handler_id, HandlerId::from("wolfram"));
        assert_eq!(option.label, "Mock wolfram");
        assert_eq!(option.applicability, Applicability::Recommended);
        assert_eq!(option.estimated_time_ms, Some(250));
        assert!(option.requires_api_key);
    }

    #[test]
    fn test_multiple_handlers_registered_and_matched() {
        let mut registry = HandlerRegistry::new();

        registry.register(Arc::new(
            MockHandler::new("sym", vec![Category::Algebra, Category::Calculus]),
        ));
        registry.register(Arc::new(
            MockHandler::new("num", vec![Category::Calculus, Category::Statistics]),
        ));
        registry.register(Arc::new(
            MockHandler::new("vis", vec![Category::Geometry]),
        ));

        // Classify as Calculus at 0.85 confidence
        let classification = make_classification(Category::Calculus, 0.85);
        let matches = registry.match_handlers(&classification);

        assert_eq!(matches.len(), 3);

        // Both sym and num support Calculus at 0.85 => Recommended
        let recommended: Vec<_> = matches
            .iter()
            .filter(|(_, a)| *a == Applicability::Recommended)
            .collect();
        assert_eq!(recommended.len(), 2);

        // vis does not support Calculus => NotApplicable
        let not_applicable: Vec<_> = matches
            .iter()
            .filter(|(_, a)| *a == Applicability::NotApplicable)
            .collect();
        assert_eq!(not_applicable.len(), 1);
        assert_eq!(not_applicable[0].0, HandlerId::from("vis"));
    }

    #[tokio::test]
    async fn test_mock_handler_execute() {
        let handler = MockHandler::new("sym", vec![Category::Algebra]);
        let expr = NormalizedExpression::new("x^2 + 1".to_string(), "x^2 + 1".to_string());
        let classification = make_classification(Category::Algebra, 0.9);
        let params = HashMap::new();

        let result = handler.execute(&expr, &classification, &params).await;
        assert!(result.is_ok());

        let compute = result.unwrap();
        assert!(compute.success);
        assert_eq!(compute.handler_id, HandlerId::from("sym"));
        assert_eq!(compute.duration_ms, 500);
    }
}
