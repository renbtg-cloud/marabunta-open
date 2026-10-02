// Marabunta - Licensed under the MIT License.
//! YAML/JSON parser and validator for workflow definitions.
//!
//! Provides two-phase processing:
//! 1. **Parse** — deserialize YAML or JSON into [`WorkflowDefinition`].
//! 2. **Validate** — run structural and semantic checks on the parsed definition.
//!
//! Convenience functions [`parse_and_validate_yaml`] and [`parse_and_validate_json`]
//! combine both phases in one call.

use std::collections::{HashSet, VecDeque};
use std::fmt;

use super::types::{StateType, WorkflowDefinition};

// ============================================================================
// Constants
// ============================================================================

/// Recognized JSON Schema type names for context_schema validation.
const VALID_SCHEMA_TYPES: &[&str] = &[
    "string", "number", "integer", "boolean", "array", "object",
];

// ============================================================================
// Error Severity
// ============================================================================

/// Severity of a validation finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorSeverity {
    /// Fatal — the workflow cannot be loaded.
    Error,
    /// Non-fatal — the workflow can load but the issue should be fixed.
    Warning,
}

impl fmt::Display for ErrorSeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Error => write!(f, "ERROR"),
            Self::Warning => write!(f, "WARNING"),
        }
    }
}

// ============================================================================
// Source Location
// ============================================================================

/// Optional position information extracted from a parse error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceLocation {
    pub line: Option<usize>,
    pub column: Option<usize>,
    pub byte_offset: Option<usize>,
}

impl fmt::Display for SourceLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.line, self.column) {
            (Some(l), Some(c)) => write!(f, "line {}, column {}", l, c),
            (Some(l), None) => write!(f, "line {}", l),
            (None, Some(c)) => write!(f, "column {}", c),
            (None, None) => {
                if let Some(offset) = self.byte_offset {
                    write!(f, "byte offset {}", offset)
                } else {
                    write!(f, "unknown location")
                }
            }
        }
    }
}

// ============================================================================
// Validation Code
// ============================================================================

/// Machine-readable code identifying a specific validation rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ValidationCode {
    NoInitialState,
    MultipleInitialStates,
    InvalidTransitionRef,
    OrphanState,
    DuplicateStateName,
    DuplicateTransitionName,
    TerminalHasOutgoing,
    InvalidContextSchema,
    EmptyWorkflow,
}

impl fmt::Display for ValidationCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoInitialState => write!(f, "NO_INITIAL_STATE"),
            Self::MultipleInitialStates => write!(f, "MULTIPLE_INITIAL_STATES"),
            Self::InvalidTransitionRef => write!(f, "INVALID_TRANSITION_REF"),
            Self::OrphanState => write!(f, "ORPHAN_STATE"),
            Self::DuplicateStateName => write!(f, "DUPLICATE_STATE_NAME"),
            Self::DuplicateTransitionName => write!(f, "DUPLICATE_TRANSITION_NAME"),
            Self::TerminalHasOutgoing => write!(f, "TERMINAL_HAS_OUTGOING"),
            Self::InvalidContextSchema => write!(f, "INVALID_CONTEXT_SCHEMA"),
            Self::EmptyWorkflow => write!(f, "EMPTY_WORKFLOW"),
        }
    }
}

// ============================================================================
// Validation Error
// ============================================================================

/// A single validation finding (error or warning) produced by [`validate_definition`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationError {
    pub code: ValidationCode,
    pub message: String,
    pub severity: ErrorSeverity,
    pub context: Option<String>,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}: {}", self.severity, self.code, self.message)?;
        if let Some(ref ctx) = self.context {
            write!(f, " ({})", ctx)?;
        }
        Ok(())
    }
}

// ============================================================================
// Parse Error
// ============================================================================

/// Errors that can occur during parsing or validation.
#[derive(Debug)]
pub enum ParseError {
    /// YAML deserialization failed.
    YamlError {
        message: String,
        location: Option<SourceLocation>,
    },
    /// JSON deserialization failed.
    JsonError {
        message: String,
        location: Option<SourceLocation>,
    },
    /// The definition parsed successfully but failed validation.
    ValidationFailed {
        errors: Vec<ValidationError>,
    },
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::YamlError { message, location } => {
                write!(f, "YAML parse error: {}", message)?;
                if let Some(loc) = location {
                    write!(f, " at {}", loc)?;
                }
                Ok(())
            }
            Self::JsonError { message, location } => {
                write!(f, "JSON parse error: {}", message)?;
                if let Some(loc) = location {
                    write!(f, " at {}", loc)?;
                }
                Ok(())
            }
            Self::ValidationFailed { errors } => {
                write!(f, "Validation failed with {} issue(s):", errors.len())?;
                for err in errors {
                    write!(f, "\n  - {}", err)?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for ParseError {}

// ============================================================================
// Parse Functions
// ============================================================================

/// Parse a YAML string into a [`WorkflowDefinition`].
///
/// Maps `serde_yaml` errors to [`ParseError::YamlError`] with location info
/// when available.
pub fn parse_workflow_yaml(input: &str) -> Result<WorkflowDefinition, ParseError> {
    serde_yaml::from_str(input).map_err(|e| {
        let location = e.location().map(|loc| SourceLocation {
            line: Some(loc.line()),
            column: Some(loc.column()),
            byte_offset: Some(loc.index()),
        });
        ParseError::YamlError {
            message: e.to_string(),
            location,
        }
    })
}

/// Parse a JSON string into a [`WorkflowDefinition`].
///
/// Maps `serde_json` errors to [`ParseError::JsonError`] with location info
/// when available.
pub fn parse_workflow_json(input: &str) -> Result<WorkflowDefinition, ParseError> {
    serde_json::from_str(input).map_err(|e| {
        let location = SourceLocation {
            line: Some(e.line()),
            column: Some(e.column()),
            byte_offset: None,
        };
        ParseError::JsonError {
            message: e.to_string(),
            location: Some(location),
        }
    })
}

// ============================================================================
// Validation
// ============================================================================

/// Run all validation rules on a parsed [`WorkflowDefinition`].
///
/// Returns `Ok(warnings)` if there are no errors (only warnings or nothing).
/// Returns `Err(all_issues)` if at least one error-severity issue is found
/// (the vector includes both errors and warnings).
pub fn validate_definition(
    def: &WorkflowDefinition,
) -> Result<Vec<ValidationError>, Vec<ValidationError>> {
    let mut issues: Vec<ValidationError> = Vec::new();

    // Rule 1: EmptyWorkflow
    if def.states.is_empty() {
        issues.push(ValidationError {
            code: ValidationCode::EmptyWorkflow,
            message: "Workflow has no states defined".to_string(),
            severity: ErrorSeverity::Error,
            context: None,
        });
    }

    // Rule 2: NoInitialState / MultipleInitialStates
    let initial_states: Vec<&str> = def
        .states
        .iter()
        .filter(|s| s.state_type == StateType::Initial)
        .map(|s| s.name.as_str())
        .collect();

    match initial_states.len() {
        0 => {
            if !def.states.is_empty() {
                issues.push(ValidationError {
                    code: ValidationCode::NoInitialState,
                    message: "No state with type 'initial' found".to_string(),
                    severity: ErrorSeverity::Error,
                    context: None,
                });
            }
        }
        1 => { /* exactly right */ }
        _ => {
            issues.push(ValidationError {
                code: ValidationCode::MultipleInitialStates,
                message: format!(
                    "Found {} initial states: {}",
                    initial_states.len(),
                    initial_states.join(", ")
                ),
                severity: ErrorSeverity::Error,
                context: None,
            });
        }
    }

    // Rule 3: DuplicateStateName
    {
        let mut seen = HashSet::new();
        for state in &def.states {
            if !seen.insert(&state.name) {
                issues.push(ValidationError {
                    code: ValidationCode::DuplicateStateName,
                    message: format!("Duplicate state name: '{}'", state.name),
                    severity: ErrorSeverity::Error,
                    context: Some(state.name.clone()),
                });
            }
        }
    }

    // Rule 4: DuplicateTransitionName
    {
        let mut seen = HashSet::new();
        for transition in &def.transitions {
            if !seen.insert(&transition.name) {
                issues.push(ValidationError {
                    code: ValidationCode::DuplicateTransitionName,
                    message: format!("Duplicate transition name: '{}'", transition.name),
                    severity: ErrorSeverity::Error,
                    context: Some(transition.name.clone()),
                });
            }
        }
    }

    // Rule 5: InvalidTransitionRef
    let state_names: HashSet<&str> = def.states.iter().map(|s| s.name.as_str()).collect();
    for transition in &def.transitions {
        if !state_names.contains(transition.from.as_str()) {
            issues.push(ValidationError {
                code: ValidationCode::InvalidTransitionRef,
                message: format!(
                    "Transition '{}' references unknown source state '{}'",
                    transition.name, transition.from
                ),
                severity: ErrorSeverity::Error,
                context: Some(transition.name.clone()),
            });
        }
        if !state_names.contains(transition.to.as_str()) {
            issues.push(ValidationError {
                code: ValidationCode::InvalidTransitionRef,
                message: format!(
                    "Transition '{}' references unknown target state '{}'",
                    transition.name, transition.to
                ),
                severity: ErrorSeverity::Error,
                context: Some(transition.name.clone()),
            });
        }
    }

    // Rule 6: OrphanState — BFS from initial (only when exactly 1 initial)
    if initial_states.len() == 1 {
        let initial_name = initial_states[0];
        let mut reachable = HashSet::new();
        let mut queue = VecDeque::new();
        reachable.insert(initial_name);
        queue.push_back(initial_name);

        while let Some(current) = queue.pop_front() {
            for transition in &def.transitions {
                if transition.from == current && !reachable.contains(transition.to.as_str()) {
                    reachable.insert(&transition.to);
                    queue.push_back(&transition.to);
                }
            }
        }

        for state in &def.states {
            if !reachable.contains(state.name.as_str()) {
                issues.push(ValidationError {
                    code: ValidationCode::OrphanState,
                    message: format!(
                        "State '{}' is unreachable from initial state '{}'",
                        state.name, initial_name
                    ),
                    severity: ErrorSeverity::Error,
                    context: Some(state.name.clone()),
                });
            }
        }
    }

    // Rule 7: TerminalHasOutgoing (WARNING)
    let terminal_names: HashSet<&str> = def
        .states
        .iter()
        .filter(|s| s.state_type == StateType::Terminal)
        .map(|s| s.name.as_str())
        .collect();

    for transition in &def.transitions {
        if terminal_names.contains(transition.from.as_str()) {
            issues.push(ValidationError {
                code: ValidationCode::TerminalHasOutgoing,
                message: format!(
                    "Terminal state '{}' has outgoing transition '{}'",
                    transition.from, transition.name
                ),
                severity: ErrorSeverity::Warning,
                context: Some(transition.from.clone()),
            });
        }
    }

    // Rule 8: InvalidContextSchema
    if let serde_json::Value::Object(ref map) = def.context_schema {
        for (key, value) in map {
            if let serde_json::Value::String(ref type_name) = value {
                if !VALID_SCHEMA_TYPES.contains(&type_name.as_str()) {
                    issues.push(ValidationError {
                        code: ValidationCode::InvalidContextSchema,
                        message: format!(
                            "Context schema key '{}' has unrecognized type '{}'. Valid types: {}",
                            key,
                            type_name,
                            VALID_SCHEMA_TYPES.join(", ")
                        ),
                        severity: ErrorSeverity::Error,
                        context: Some(key.clone()),
                    });
                }
            }
        }
    } else if !def.context_schema.is_null() {
        issues.push(ValidationError {
            code: ValidationCode::InvalidContextSchema,
            message: "context_schema must be a JSON object or null".to_string(),
            severity: ErrorSeverity::Error,
            context: None,
        });
    }

    // Partition into errors and warnings
    let has_errors = issues
        .iter()
        .any(|e| e.severity == ErrorSeverity::Error);

    if has_errors {
        Err(issues)
    } else {
        Ok(issues)
    }
}

// ============================================================================
// Combined Parse + Validate
// ============================================================================

/// Parse YAML and validate the resulting definition in one step.
///
/// Returns `Ok((definition, warnings))` on success or `Err(ParseError)` on
/// parse failure or validation errors.
pub fn parse_and_validate_yaml(
    input: &str,
) -> Result<(WorkflowDefinition, Vec<ValidationError>), ParseError> {
    let def = parse_workflow_yaml(input)?;
    match validate_definition(&def) {
        Ok(warnings) => Ok((def, warnings)),
        Err(errors) => Err(ParseError::ValidationFailed { errors }),
    }
}

/// Parse JSON and validate the resulting definition in one step.
///
/// Returns `Ok((definition, warnings))` on success or `Err(ParseError)` on
/// parse failure or validation errors.
pub fn parse_and_validate_json(
    input: &str,
) -> Result<(WorkflowDefinition, Vec<ValidationError>), ParseError> {
    let def = parse_workflow_json(input)?;
    match validate_definition(&def) {
        Ok(warnings) => Ok((def, warnings)),
        Err(errors) => Err(ParseError::ValidationFailed { errors }),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swarm::workflow::types::{
        ActionDefinition, AuditConfig, CriticalityLevel, GuardDefinition, HookDefinition,
        StateDefinition, TransitionDefinition, WorkflowDefinition,
    };

    /// Generates a minimal valid workflow with 3 states and 2 transitions.
    fn valid_yaml() -> String {
        r#"
name: test-workflow
version: 1
description: A test workflow
context_schema:
  document_id: string
  author_id: string
states:
  - name: draft
    type: initial
    on_enter: []
    on_exit: []
  - name: review
    type: active
    on_enter: []
    on_exit: []
  - name: done
    type: terminal
    on_enter: []
    on_exit: []
transitions:
  - name: submit
    from: draft
    to: review
    guards: []
    actions: []
    audit:
      criticality: NORMAL
  - name: approve
    from: review
    to: done
    guards: []
    actions: []
    audit:
      criticality: HIGH
triggers: []
hooks:
  on_instance_created: []
  on_instance_completed: []
  on_instance_failed: []
  on_transition_blocked: []
  on_deadlock_detected: []
"#
        .to_string()
    }

    /// Helper to build a valid WorkflowDefinition struct.
    fn valid_definition() -> WorkflowDefinition {
        WorkflowDefinition {
            name: "test-workflow".to_string(),
            version: 1,
            description: "A test workflow".to_string(),
            context_schema: serde_json::json!({
                "document_id": "string",
                "author_id": "string"
            }),
            states: vec![
                StateDefinition {
                    name: "draft".to_string(),
                    state_type: StateType::Initial,
                    on_enter: vec![],
                    on_exit: vec![],
                },
                StateDefinition {
                    name: "review".to_string(),
                    state_type: StateType::Active,
                    on_enter: vec![],
                    on_exit: vec![],
                },
                StateDefinition {
                    name: "done".to_string(),
                    state_type: StateType::Terminal,
                    on_enter: vec![],
                    on_exit: vec![],
                },
            ],
            transitions: vec![
                TransitionDefinition {
                    name: "submit".to_string(),
                    from: "draft".to_string(),
                    to: "review".to_string(),
                    guards: vec![],
                    actions: vec![],
                    audit: AuditConfig {
                        criticality: CriticalityLevel::Normal,
                    },
                },
                TransitionDefinition {
                    name: "approve".to_string(),
                    from: "review".to_string(),
                    to: "done".to_string(),
                    guards: vec![],
                    actions: vec![],
                    audit: AuditConfig {
                        criticality: CriticalityLevel::High,
                    },
                },
            ],
            triggers: vec![],
            hooks: HookDefinition::default(),
        }
    }

    #[test]
    fn test_parse_valid_yaml_roundtrip() {
        let yaml = valid_yaml();
        let def = parse_workflow_yaml(&yaml).expect("should parse valid YAML");
        assert_eq!(def.name, "test-workflow");
        assert_eq!(def.version, 1);
        assert_eq!(def.states.len(), 3);
        assert_eq!(def.transitions.len(), 2);

        // Round-trip through serde_json
        let json = serde_json::to_string(&def).unwrap();
        let back: WorkflowDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(def, back);
    }

    #[test]
    fn test_parse_valid_json() {
        let def = valid_definition();
        let json = serde_json::to_string_pretty(&def).unwrap();
        let parsed = parse_workflow_json(&json).expect("should parse valid JSON");
        assert_eq!(parsed.name, "test-workflow");
        assert_eq!(parsed.states.len(), 3);
        assert_eq!(parsed.transitions.len(), 2);
    }

    #[test]
    fn test_validate_valid_definition() {
        let def = valid_definition();
        let result = validate_definition(&def);
        assert!(result.is_ok(), "valid definition should pass validation");
        let warnings = result.unwrap();
        assert!(warnings.is_empty(), "valid definition should have no warnings");
    }

    #[test]
    fn test_validate_missing_initial_state() {
        let mut def = valid_definition();
        // Remove the initial state and make it active
        def.states[0].state_type = StateType::Active;

        let result = validate_definition(&def);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.code == ValidationCode::NoInitialState));
    }

    #[test]
    fn test_validate_multiple_initial_states() {
        let mut def = valid_definition();
        // Make the second state also initial
        def.states[1].state_type = StateType::Initial;

        let result = validate_definition(&def);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.code == ValidationCode::MultipleInitialStates));
    }

    #[test]
    fn test_validate_orphan_state() {
        let mut def = valid_definition();
        // Add a disconnected state
        def.states.push(StateDefinition {
            name: "orphan".to_string(),
            state_type: StateType::Active,
            on_enter: vec![],
            on_exit: vec![],
        });

        let result = validate_definition(&def);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.code == ValidationCode::OrphanState));
        assert!(errors
            .iter()
            .any(|e| e.message.contains("orphan")));
    }

    #[test]
    fn test_validate_invalid_transition_ref() {
        let mut def = valid_definition();
        // Add a transition referencing a non-existent state
        def.transitions.push(TransitionDefinition {
            name: "bad_transition".to_string(),
            from: "review".to_string(),
            to: "nonexistent".to_string(),
            guards: vec![],
            actions: vec![],
            audit: AuditConfig {
                criticality: CriticalityLevel::Normal,
            },
        });

        let result = validate_definition(&def);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.code == ValidationCode::InvalidTransitionRef));
    }

    #[test]
    fn test_validate_duplicate_state_names() {
        let mut def = valid_definition();
        // Add a state with a duplicate name
        def.states.push(StateDefinition {
            name: "draft".to_string(),
            state_type: StateType::Active,
            on_enter: vec![],
            on_exit: vec![],
        });

        let result = validate_definition(&def);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.code == ValidationCode::DuplicateStateName));
    }

    #[test]
    fn test_validate_duplicate_transition_names() {
        let mut def = valid_definition();
        // Add a transition with a duplicate name
        def.transitions.push(TransitionDefinition {
            name: "submit".to_string(),
            from: "draft".to_string(),
            to: "review".to_string(),
            guards: vec![],
            actions: vec![],
            audit: AuditConfig {
                criticality: CriticalityLevel::Normal,
            },
        });

        let result = validate_definition(&def);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.code == ValidationCode::DuplicateTransitionName));
    }

    #[test]
    fn test_validate_terminal_with_outgoing_is_warning() {
        let mut def = valid_definition();
        // Add a transition from the terminal state
        def.transitions.push(TransitionDefinition {
            name: "reopen".to_string(),
            from: "done".to_string(),
            to: "draft".to_string(),
            guards: vec![],
            actions: vec![],
            audit: AuditConfig {
                criticality: CriticalityLevel::Low,
            },
        });

        let result = validate_definition(&def);
        // Should be Ok because TerminalHasOutgoing is a warning, not an error
        assert!(result.is_ok(), "terminal outgoing should be warning only");
        let warnings = result.unwrap();
        assert!(!warnings.is_empty());
        assert!(warnings
            .iter()
            .any(|e| e.code == ValidationCode::TerminalHasOutgoing
                && e.severity == ErrorSeverity::Warning));
    }

    #[test]
    fn test_validate_empty_workflow() {
        let def = WorkflowDefinition {
            name: "empty".to_string(),
            version: 1,
            description: "Empty workflow".to_string(),
            context_schema: serde_json::Value::Null,
            states: vec![],
            transitions: vec![],
            triggers: vec![],
            hooks: HookDefinition::default(),
        };

        let result = validate_definition(&def);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.code == ValidationCode::EmptyWorkflow));
    }

    #[test]
    fn test_validate_context_schema_invalid_type() {
        let mut def = valid_definition();
        def.context_schema = serde_json::json!({
            "name": "string",
            "magic": "unicorn"
        });

        let result = validate_definition(&def);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.code == ValidationCode::InvalidContextSchema));
        assert!(errors
            .iter()
            .any(|e| e.message.contains("unicorn")));
    }

    #[test]
    fn test_parse_and_validate_yaml_combined() {
        let yaml = valid_yaml();
        let result = parse_and_validate_yaml(&yaml);
        assert!(result.is_ok());
        let (def, warnings) = result.unwrap();
        assert_eq!(def.name, "test-workflow");
        assert!(warnings.is_empty());
    }

    #[test]
    fn test_parse_invalid_yaml_syntax() {
        let bad_yaml = "name: [unmatched bracket";
        let result = parse_workflow_yaml(bad_yaml);
        assert!(result.is_err());
        match result.unwrap_err() {
            ParseError::YamlError { message, .. } => {
                assert!(!message.is_empty());
            }
            other => panic!("Expected YamlError, got: {}", other),
        }
    }

    #[test]
    fn test_parse_invalid_json_syntax() {
        let bad_json = r#"{"name": "test", invalid}"#;
        let result = parse_workflow_json(bad_json);
        assert!(result.is_err());
        match result.unwrap_err() {
            ParseError::JsonError { message, location } => {
                assert!(!message.is_empty());
                assert!(location.is_some());
            }
            other => panic!("Expected JsonError, got: {}", other),
        }
    }
}
