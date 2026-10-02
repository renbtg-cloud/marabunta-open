// Marabunta - Licensed under the MIT License.
//! Pipeline orchestrator for the Switchboard module.
//!
//! Chains the full switchboard pipeline:
//!   ingest -> classify -> match_handlers -> create session -> return OptionMenu
//!
//! Also provides `execute_handler` for running a selected handler after the user
//! picks from the option menu, and `default_registry` for bootstrapping all
//! built-in handlers.

use std::collections::HashMap;
use std::sync::Arc;

use tracing::{debug, info, warn};

use super::classify::classify;
use super::config::SwitchboardConfig;
use super::errors::{SwitchboardError, SwitchboardResult};
use super::handlers::codegen::GenerateCode;
use super::handlers::explain::Explain;
use super::handlers::monte_carlo::MonteCarlo;
use super::handlers::numerical::NumericalCompute;
use super::handlers::step_by_step::StepByStep;
use super::handlers::sweep::ParameterSweep;
use super::handlers::symbolic::SymbolicSolve;
use super::handlers::verify::Verify;
use super::handlers::visualize::Visualize;
use super::handlers::{ComputeHandler, HandlerRegistry};
use super::ingest::ingest;
use super::oracles::sympy::SympyOracle;
use super::oracles::wolfram::WolframOracle;
use super::oracles::MathOracle;
use super::session::SessionStore;
use super::types::{
    Applicability, ComputeResult, HandlerId, MenuOption, OptionMenu, RawInput, SessionId,
};

// ============================================================================
// Pipeline response types
// ============================================================================

/// Response from the `process_expression` pipeline.
///
/// Contains the session ID, the generated option menu, and the classification
/// confidence so callers can decide how to present the menu.
#[derive(Debug, Clone)]
pub struct PipelineResponse {
    /// Session ID for follow-up requests (execute, get session).
    pub session_id: SessionId,
    /// Option menu with ranked handler choices.
    pub menu: OptionMenu,
    /// Classification confidence (0.0 - 1.0).
    pub confidence: f32,
}

/// Response from `execute_handler`.
#[derive(Debug, Clone)]
pub struct ExecuteResponse {
    /// Session ID.
    pub session_id: SessionId,
    /// Compute result from the selected handler.
    pub result: ComputeResult,
}

// ============================================================================
// default_registry — registers all built-in handlers
// ============================================================================

/// Creates a `HandlerRegistry` pre-populated with all built-in handlers.
///
/// Registers:
/// - **SymbolicSolve** — exact symbolic solution via CAS oracle cascade
/// - **StepByStep** — detailed step-by-step walkthrough
/// - **GenerateCode** — executable code templates
/// - **Explain** — plain-language explanation
/// - **NumericalCompute** — floating-point numerical evaluation
/// - **MonteCarlo** — probabilistic estimation via random sampling
/// - **ParameterSweep** — sweep a variable across a range
/// - **Verify** — cross-check results between oracles
/// - **Visualize** — plot/visualization specification
///
/// The `config` is used to conditionally add API-key-dependent oracles
/// (e.g. Wolfram Alpha).
pub fn default_registry(config: &SwitchboardConfig) -> HandlerRegistry {
    let mut registry = HandlerRegistry::new();

    // Build the shared oracle list.
    let oracles = build_oracle_list(config);
    let oracles = Arc::new(oracles);

    // Register all built-in handlers.
    registry.register(Arc::new(SymbolicSolve::new(oracles.clone())));
    registry.register(Arc::new(StepByStep::new(oracles.clone())));
    registry.register(Arc::new(GenerateCode::default()));
    registry.register(Arc::new(Explain));
    registry.register(Arc::new(NumericalCompute::new(oracles.clone())));
    registry.register(Arc::new(MonteCarlo));
    registry.register(Arc::new(ParameterSweep::new(oracles.clone())));
    registry.register(Arc::new(Verify::new(oracles.clone())));
    registry.register(Arc::new(Visualize));

    info!(
        handler_count = registry.handler_count(),
        "default handler registry initialized"
    );

    registry
}

/// Builds the list of oracle backends based on the configuration.
///
/// Always includes SymPy (local, no API key). Conditionally includes
/// Wolfram Alpha when `wolfram_app_id` is configured.
fn build_oracle_list(config: &SwitchboardConfig) -> Vec<Box<dyn MathOracle>> {
    let mut oracles: Vec<Box<dyn MathOracle>> = Vec::new();

    // SymPy is always available (local subprocess, no API key).
    oracles.push(Box::new(SympyOracle::new(config.sympy_pool_size)));

    // Wolfram Alpha requires an API key.
    if let Some(ref app_id) = config.wolfram_app_id {
        debug!("Wolfram Alpha oracle enabled");
        oracles.push(Box::new(WolframOracle::new(app_id.clone())));
    }

    oracles
}

// ============================================================================
// process_expression — the main pipeline entry point
// ============================================================================

/// Process a raw mathematical expression through the full switchboard pipeline.
///
/// Pipeline stages:
/// 1. **Ingest** — parse and normalize the raw input
/// 2. **Classify** — determine mathematical category and applicable operations
/// 3. **Match handlers** — rank registered handlers by applicability
/// 4. **Create session** — store pipeline state for follow-up requests
/// 5. **Return OptionMenu** — present ranked handler choices to the caller
pub async fn process_expression(
    input: RawInput,
    registry: &HandlerRegistry,
    session_store: &SessionStore,
    config: &SwitchboardConfig,
) -> SwitchboardResult<PipelineResponse> {
    // Stage 1: Ingest
    info!(format = %input.format, "pipeline: ingesting expression");
    let normalized = ingest(input, config).await?;

    // Stage 2: Classify
    debug!(latex = %normalized.latex, "pipeline: classifying expression");
    let classification = classify(&normalized);
    info!(
        category = %classification.category,
        confidence = classification.confidence,
        operations = ?classification.operations,
        "pipeline: classification complete"
    );

    // Stage 3: Match handlers
    let matches = registry.match_handlers(&classification);
    debug!(
        match_count = matches.len(),
        "pipeline: matched handlers"
    );

    // Build menu options from matched handlers.
    let menu_options: Vec<MenuOption> = matches
        .iter()
        .filter(|(_, applicability)| *applicability != Applicability::NotApplicable)
        .filter_map(|(handler_id, _)| {
            registry.get(handler_id).map(|handler| {
                handler.to_menu_option(&classification)
            })
        })
        .take(config.menu_size)
        .collect();

    // Stage 4: Create session
    let session_id = session_store.create_session(None)?;

    // Store expression and classification in the session.
    session_store.update_session(&session_id, |session| {
        session.add_expression(normalized.clone());
    })?;

    session_store.update_session(&session_id, |session| {
        // Transition to Classified state.
        if let Err(e) = session.set_classified(classification.clone()) {
            warn!(error = %e, "pipeline: failed to transition session to Classified");
        }
    })?;

    // Build the option menu.
    let menu = OptionMenu::new(session_id, normalized.plain_text.clone())
        .with_options(menu_options);

    // Transition to MenuPresented state.
    session_store.update_session(&session_id, |session| {
        if let Err(e) = session.set_menu_presented(menu.clone()) {
            warn!(error = %e, "pipeline: failed to transition session to MenuPresented");
        }
    })?;

    info!(
        session_id = %session_id,
        option_count = menu.options.len(),
        "pipeline: expression processed, menu generated"
    );

    Ok(PipelineResponse {
        session_id,
        menu,
        confidence: classification.confidence,
    })
}

// ============================================================================
// execute_handler — run a selected handler
// ============================================================================

/// Execute a selected handler for a previously processed expression.
///
/// The caller provides the session ID (from `process_expression`) and the
/// handler ID chosen from the option menu. The handler runs against the
/// stored normalized expression and classification result.
pub async fn execute_handler(
    session_id: &SessionId,
    handler_id: &HandlerId,
    params: HashMap<String, String>,
    registry: &HandlerRegistry,
    session_store: &SessionStore,
) -> SwitchboardResult<ExecuteResponse> {
    // Retrieve the session.
    let session = session_store.get_session(session_id)?;

    // Get the expression and classification from the session.
    let expression = session
        .expressions
        .last()
        .ok_or_else(|| {
            SwitchboardError::Internal("session has no expressions".to_string())
        })?
        .clone();

    let classification = session
        .classification
        .as_ref()
        .ok_or_else(|| {
            SwitchboardError::Internal("session has no classification".to_string())
        })?
        .clone();

    // Look up the handler.
    let handler = registry
        .get(handler_id)
        .ok_or_else(|| {
            SwitchboardError::HandlerNotFound(handler_id.to_string())
        })?;

    // Transition session to Computing.
    session_store.update_session(session_id, |session| {
        if let Err(e) = session.set_computing() {
            warn!(error = %e, "pipeline: failed to transition session to Computing");
        }
    })?;

    info!(
        session_id = %session_id,
        handler_id = %handler_id,
        "pipeline: executing handler"
    );

    // Execute the handler.
    let result = handler.execute(&expression, &classification, &params).await?;

    // Transition session to Completed or Failed.
    let result_clone = result.clone();
    session_store.update_session(session_id, |session| {
        if result_clone.success {
            if let Err(e) = session.set_completed(result_clone.clone()) {
                warn!(error = %e, "pipeline: failed to transition session to Completed");
            }
        } else {
            session.add_result(result_clone.clone());
            if let Err(e) = session.set_failed() {
                warn!(error = %e, "pipeline: failed to transition session to Failed");
            }
        }
    })?;

    info!(
        session_id = %session_id,
        handler_id = %handler_id,
        success = result.success,
        duration_ms = result.duration_ms,
        "pipeline: handler execution complete"
    );

    Ok(ExecuteResponse {
        session_id: *session_id,
        result,
    })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::switchboard::types::{InputFormat, RawInput};

    fn default_config() -> SwitchboardConfig {
        SwitchboardConfig::default()
    }

    #[test]
    fn test_default_registry_has_all_handlers() {
        let config = default_config();
        let registry = default_registry(&config);

        // Should have 9 built-in handlers.
        assert_eq!(registry.handler_count(), 9);

        // Verify each handler is present.
        assert!(registry.get(&HandlerId::from("symbolic_solve")).is_some());
        assert!(registry.get(&HandlerId::from("step_by_step")).is_some());
        assert!(registry.get(&HandlerId::from("generate_code")).is_some());
        assert!(registry.get(&HandlerId::from("explain")).is_some());
        assert!(registry.get(&HandlerId::from("numerical_compute")).is_some());
        assert!(registry.get(&HandlerId::from("monte_carlo")).is_some());
        assert!(registry.get(&HandlerId::from("parameter_sweep")).is_some());
        assert!(registry.get(&HandlerId::from("verify")).is_some());
        assert!(registry.get(&HandlerId::from("visualize")).is_some());
    }

    #[test]
    fn test_default_registry_with_wolfram() {
        let config = SwitchboardConfig {
            wolfram_app_id: Some("test-wolf-id".to_string()),
            ..SwitchboardConfig::default()
        };
        let registry = default_registry(&config);
        // Same handler count (wolfram affects oracles, not handler count).
        assert_eq!(registry.handler_count(), 9);
    }

    #[test]
    fn test_build_oracle_list_without_wolfram() {
        let config = default_config();
        let oracles = build_oracle_list(&config);
        // Only SymPy.
        assert_eq!(oracles.len(), 1);
        assert_eq!(oracles[0].name(), "sympy");
    }

    #[test]
    fn test_build_oracle_list_with_wolfram() {
        let config = SwitchboardConfig {
            wolfram_app_id: Some("test-wolf-id".to_string()),
            ..SwitchboardConfig::default()
        };
        let oracles = build_oracle_list(&config);
        // SymPy + Wolfram.
        assert_eq!(oracles.len(), 2);
        assert_eq!(oracles[0].name(), "sympy");
        assert_eq!(oracles[1].name(), "wolfram");
    }

    #[tokio::test]
    async fn test_process_expression_algebra() {
        let config = default_config();
        let registry = default_registry(&config);
        let session_store = SessionStore::new();

        let input = RawInput::new(InputFormat::PlainText, "x^2 + 3*x + 2 = 0".to_string());
        let response = process_expression(input, &registry, &session_store, &config)
            .await
            .unwrap();

        // Should have a valid session ID.
        assert!(session_store.get_session(&response.session_id).is_ok());

        // Menu should have at least one option.
        assert!(
            !response.menu.options.is_empty(),
            "menu should have at least one handler option"
        );

        // Confidence should be reasonable for a clear equation.
        assert!(
            response.confidence > 0.5,
            "confidence {} should be > 0.5 for a clear equation",
            response.confidence
        );

        // Session should be in MenuPresented state.
        let session = session_store.get_session(&response.session_id).unwrap();
        assert_eq!(
            session.state,
            crate::switchboard::session::SessionState::MenuPresented
        );
        assert!(session.classification.is_some());
        assert!(session.menu.is_some());
        assert_eq!(session.expressions.len(), 1);
    }

    #[tokio::test]
    async fn test_process_expression_latex() {
        let config = default_config();
        let registry = default_registry(&config);
        let session_store = SessionStore::new();

        let input = RawInput::new(
            InputFormat::Latex,
            r"\int_0^1 x^2 dx".to_string(),
        );
        let response = process_expression(input, &registry, &session_store, &config)
            .await
            .unwrap();

        assert!(!response.menu.options.is_empty());
        // For an integral, we expect calculus handlers to be ranked highly.
        assert!(response.confidence > 0.5);
    }

    #[tokio::test]
    async fn test_process_expression_empty_input_fails() {
        let config = default_config();
        let registry = default_registry(&config);
        let session_store = SessionStore::new();

        let input = RawInput::new(InputFormat::Latex, "".to_string());
        let result = process_expression(input, &registry, &session_store, &config).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_handler_explain() {
        let config = default_config();
        let registry = default_registry(&config);
        let session_store = SessionStore::new();

        // First, process an expression to get a session.
        let input = RawInput::new(InputFormat::PlainText, "x^2 + 1 = 0".to_string());
        let pipeline_resp = process_expression(input, &registry, &session_store, &config)
            .await
            .unwrap();

        // Execute the "explain" handler (always available).
        let exec_resp = execute_handler(
            &pipeline_resp.session_id,
            &HandlerId::from("explain"),
            HashMap::new(),
            &registry,
            &session_store,
        )
        .await
        .unwrap();

        assert!(exec_resp.result.success);
        assert!(exec_resp.result.result_plain.is_some());
        let explanation = exec_resp.result.result_plain.unwrap();
        assert!(
            explanation.contains("algebra") || explanation.contains("Algebra"),
            "explanation should mention algebra: {}",
            explanation
        );
    }

    #[tokio::test]
    async fn test_execute_handler_not_found() {
        let config = default_config();
        let registry = default_registry(&config);
        let session_store = SessionStore::new();

        // Process an expression.
        let input = RawInput::new(InputFormat::PlainText, "x + 1 = 0".to_string());
        let pipeline_resp = process_expression(input, &registry, &session_store, &config)
            .await
            .unwrap();

        // Try to execute a non-existent handler.
        let result = execute_handler(
            &pipeline_resp.session_id,
            &HandlerId::from("nonexistent_handler"),
            HashMap::new(),
            &registry,
            &session_store,
        )
        .await;

        assert!(result.is_err());
        match result {
            Err(SwitchboardError::HandlerNotFound(id)) => {
                assert_eq!(id, "nonexistent_handler");
            }
            other => panic!("expected HandlerNotFound, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_execute_handler_session_not_found() {
        let config = default_config();
        let registry = default_registry(&config);
        let session_store = SessionStore::new();

        let fake_session = SessionId::new();
        let result = execute_handler(
            &fake_session,
            &HandlerId::from("explain"),
            HashMap::new(),
            &registry,
            &session_store,
        )
        .await;

        assert!(result.is_err());
        match result {
            Err(SwitchboardError::SessionNotFound(_)) => {}
            other => panic!("expected SessionNotFound, got: {:?}", other),
        }
    }
}
