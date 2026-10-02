// Marabunta - Licensed under the MIT License.
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use uuid::Uuid;

/// Unique session identifier for a Switchboard interaction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionId(Uuid);

impl SessionId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Uuid> for SessionId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

/// Input format type for mathematical expressions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputFormat {
    NapkinPhoto,
    Latex,
    PlainText,
    MathML,
    AsciiMath,
}

impl fmt::Display for InputFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NapkinPhoto => write!(f, "napkin_photo"),
            Self::Latex => write!(f, "latex"),
            Self::PlainText => write!(f, "plain_text"),
            Self::MathML => write!(f, "mathml"),
            Self::AsciiMath => write!(f, "ascii_math"),
        }
    }
}

/// Raw mathematical input from user
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawInput {
    pub format: InputFormat,
    pub content: String,
    pub image_data: Option<Vec<u8>>,
    pub metadata: HashMap<String, String>,
}

impl RawInput {
    pub fn new(format: InputFormat, content: String) -> Self {
        Self {
            format,
            content,
            image_data: None,
            metadata: HashMap::new(),
        }
    }

    pub fn with_image(format: InputFormat, content: String, image_data: Vec<u8>) -> Self {
        Self {
            format,
            content,
            image_data: Some(image_data),
            metadata: HashMap::new(),
        }
    }

    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }
}

/// Normalized mathematical expression with structural analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedExpression {
    pub latex: String,
    pub plain_text: String,
    pub symbols: Vec<String>,
    pub is_equation: bool,
    pub is_inequality: bool,
    pub has_integral: bool,
    pub has_derivative: bool,
    pub has_summation: bool,
    pub has_limit: bool,
    pub has_matrix: bool,
    pub variable_count: usize,
    pub normalized_at: DateTime<Utc>,
}

impl NormalizedExpression {
    pub fn new(latex: String, plain_text: String) -> Self {
        Self {
            latex,
            plain_text,
            symbols: Vec::new(),
            is_equation: false,
            is_inequality: false,
            has_integral: false,
            has_derivative: false,
            has_summation: false,
            has_limit: false,
            has_matrix: false,
            variable_count: 0,
            normalized_at: Utc::now(),
        }
    }
}

/// Mathematical category classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Category {
    Algebra,
    Calculus,
    LinearAlgebra,
    DifferentialEquations,
    Statistics,
    NumberTheory,
    Combinatorics,
    Geometry,
    Optimization,
    Unknown,
}

impl fmt::Display for Category {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Algebra => write!(f, "algebra"),
            Self::Calculus => write!(f, "calculus"),
            Self::LinearAlgebra => write!(f, "linear_algebra"),
            Self::DifferentialEquations => write!(f, "differential_equations"),
            Self::Statistics => write!(f, "statistics"),
            Self::NumberTheory => write!(f, "number_theory"),
            Self::Combinatorics => write!(f, "combinatorics"),
            Self::Geometry => write!(f, "geometry"),
            Self::Optimization => write!(f, "optimization"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

/// Mathematical operation to perform
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Solve,
    Simplify,
    Factor,
    Expand,
    Differentiate,
    Integrate,
    Limit,
    Series,
    EigenDecompose,
    MatrixInverse,
    Determinant,
    Regression,
    Probability,
    Custom(String),
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Solve => write!(f, "solve"),
            Self::Simplify => write!(f, "simplify"),
            Self::Factor => write!(f, "factor"),
            Self::Expand => write!(f, "expand"),
            Self::Differentiate => write!(f, "differentiate"),
            Self::Integrate => write!(f, "integrate"),
            Self::Limit => write!(f, "limit"),
            Self::Series => write!(f, "series"),
            Self::EigenDecompose => write!(f, "eigen_decompose"),
            Self::MatrixInverse => write!(f, "matrix_inverse"),
            Self::Determinant => write!(f, "determinant"),
            Self::Regression => write!(f, "regression"),
            Self::Probability => write!(f, "probability"),
            Self::Custom(s) => write!(f, "custom:{}", s),
        }
    }
}

/// Ambiguity detected during classification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ambiguity {
    pub description: String,
    pub alternatives: Vec<String>,
    pub position: Option<usize>,
}

impl Ambiguity {
    pub fn new(description: String) -> Self {
        Self {
            description,
            alternatives: Vec::new(),
            position: None,
        }
    }

    pub fn with_alternatives(mut self, alternatives: Vec<String>) -> Self {
        self.alternatives = alternatives;
        self
    }

    pub fn with_position(mut self, position: usize) -> Self {
        self.position = Some(position);
        self
    }
}

/// Classification result with confidence and detected ambiguities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassificationResult {
    pub category: Category,
    pub operations: Vec<Operation>,
    pub confidence: f32,
    pub ambiguities: Vec<Ambiguity>,
    pub classified_at: DateTime<Utc>,
}

impl ClassificationResult {
    pub fn new(category: Category, operations: Vec<Operation>, confidence: f32) -> Self {
        Self {
            category,
            operations,
            confidence: confidence.clamp(0.0, 1.0),
            ambiguities: Vec::new(),
            classified_at: Utc::now(),
        }
    }

    pub fn with_ambiguities(mut self, ambiguities: Vec<Ambiguity>) -> Self {
        self.ambiguities = ambiguities;
        self
    }
}

/// Applicability rating for a handler option
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Applicability {
    Recommended,
    Applicable,
    Marginal,
    NotApplicable,
}

impl fmt::Display for Applicability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Recommended => write!(f, "recommended"),
            Self::Applicable => write!(f, "applicable"),
            Self::Marginal => write!(f, "marginal"),
            Self::NotApplicable => write!(f, "not_applicable"),
        }
    }
}

/// Handler identifier for compute backends
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HandlerId(String);

impl HandlerId {
    pub fn new(id: String) -> Self {
        Self(id)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for HandlerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for HandlerId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<String> for HandlerId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Menu option representing a compute handler
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MenuOption {
    pub handler_id: HandlerId,
    pub label: String,
    pub description: String,
    pub applicability: Applicability,
    pub estimated_time_ms: Option<u64>,
    pub requires_api_key: bool,
}

impl MenuOption {
    pub fn new(
        handler_id: HandlerId,
        label: String,
        description: String,
        applicability: Applicability,
    ) -> Self {
        Self {
            handler_id,
            label,
            description,
            applicability,
            estimated_time_ms: None,
            requires_api_key: false,
        }
    }

    pub fn with_estimated_time(mut self, ms: u64) -> Self {
        self.estimated_time_ms = Some(ms);
        self
    }

    pub fn requires_api_key(mut self) -> Self {
        self.requires_api_key = true;
        self
    }
}

/// Option menu presented to user after classification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptionMenu {
    pub session_id: SessionId,
    pub options: Vec<MenuOption>,
    pub expression_summary: String,
    pub generated_at: DateTime<Utc>,
}

impl OptionMenu {
    pub fn new(session_id: SessionId, expression_summary: String) -> Self {
        Self {
            session_id,
            options: Vec::new(),
            expression_summary,
            generated_at: Utc::now(),
        }
    }

    pub fn add_option(mut self, option: MenuOption) -> Self {
        self.options.push(option);
        self
    }

    pub fn with_options(mut self, options: Vec<MenuOption>) -> Self {
        self.options = options;
        self
    }
}

/// Individual step in a computation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputeStep {
    pub step_number: u32,
    pub description: String,
    pub expression: String,
}

impl ComputeStep {
    pub fn new(step_number: u32, description: String, expression: String) -> Self {
        Self {
            step_number,
            description,
            expression,
        }
    }
}

/// Result from a compute backend execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputeResult {
    pub handler_id: HandlerId,
    pub success: bool,
    pub result_latex: Option<String>,
    pub result_plain: Option<String>,
    pub steps: Vec<ComputeStep>,
    pub duration_ms: u64,
    pub backend: String,
    pub completed_at: DateTime<Utc>,
}

impl ComputeResult {
    pub fn success(handler_id: HandlerId, backend: String, duration_ms: u64) -> Self {
        Self {
            handler_id,
            success: true,
            result_latex: None,
            result_plain: None,
            steps: Vec::new(),
            duration_ms,
            backend,
            completed_at: Utc::now(),
        }
    }

    pub fn failure(handler_id: HandlerId, backend: String, duration_ms: u64) -> Self {
        Self {
            handler_id,
            success: false,
            result_latex: None,
            result_plain: None,
            steps: Vec::new(),
            duration_ms,
            backend,
            completed_at: Utc::now(),
        }
    }

    pub fn with_latex(mut self, latex: String) -> Self {
        self.result_latex = Some(latex);
        self
    }

    pub fn with_plain(mut self, plain: String) -> Self {
        self.result_plain = Some(plain);
        self
    }

    pub fn with_steps(mut self, steps: Vec<ComputeStep>) -> Self {
        self.steps = steps;
        self
    }
}

/// Output format preference
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum OutputFormat {
    Latex,
    PlainText,
    #[default]
    Both,
}

impl fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Latex => write!(f, "latex"),
            Self::PlainText => write!(f, "plain_text"),
            Self::Both => write!(f, "both"),
        }
    }
}


/// User preferences for computation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserPreference {
    pub preferred_handlers: Vec<HandlerId>,
    pub preferred_format: OutputFormat,
    pub show_steps: bool,
    pub max_wait_ms: u64,
}

impl Default for UserPreference {
    fn default() -> Self {
        Self {
            preferred_handlers: Vec::new(),
            preferred_format: OutputFormat::Both,
            show_steps: true,
            max_wait_ms: 30000,
        }
    }
}

impl UserPreference {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_handler(mut self, handler_id: HandlerId) -> Self {
        self.preferred_handlers.push(handler_id);
        self
    }

    pub fn with_format(mut self, format: OutputFormat) -> Self {
        self.preferred_format = format;
        self
    }
}

/// Back-reference to previous session expressions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackReference {
    pub session_id: SessionId,
    pub expression_index: usize,
    pub label: Option<String>,
}

impl BackReference {
    pub fn new(session_id: SessionId, expression_index: usize) -> Self {
        Self {
            session_id,
            expression_index,
            label: None,
        }
    }

    pub fn with_label(mut self, label: String) -> Self {
        self.label = Some(label);
        self
    }
}

/// Request to compute a mathematical expression via swarm
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwitchboardRequest {
    pub session_id: SessionId,
    pub expression: String,
    pub handler_id: HandlerId,
    pub params: HashMap<String, String>,
    pub from: crate::swarm::types::NodeId,
}

impl SwitchboardRequest {
    pub fn new(
        session_id: SessionId,
        expression: String,
        handler_id: HandlerId,
        from: crate::swarm::types::NodeId,
    ) -> Self {
        Self {
            session_id,
            expression,
            handler_id,
            params: HashMap::new(),
            from,
        }
    }

    pub fn with_param(mut self, key: String, value: String) -> Self {
        self.params.insert(key, value);
        self
    }
}

/// Response from compute backend via swarm
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwitchboardResponse {
    pub session_id: SessionId,
    pub result: ComputeResult,
    pub from: crate::swarm::types::NodeId,
}

impl SwitchboardResponse {
    pub fn new(
        session_id: SessionId,
        result: ComputeResult,
        from: crate::swarm::types::NodeId,
    ) -> Self {
        Self {
            session_id,
            result,
            from,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_id_creation() {
        let id1 = SessionId::new();
        let id2 = SessionId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_session_id_default() {
        let id = SessionId::default();
        assert!(id.to_string().len() > 0);
    }

    #[test]
    fn test_session_id_display() {
        let uuid = Uuid::new_v4();
        let id = SessionId::from(uuid);
        assert_eq!(id.to_string(), uuid.to_string());
    }

    #[test]
    fn test_input_format_display() {
        assert_eq!(InputFormat::NapkinPhoto.to_string(), "napkin_photo");
        assert_eq!(InputFormat::Latex.to_string(), "latex");
        assert_eq!(InputFormat::PlainText.to_string(), "plain_text");
        assert_eq!(InputFormat::MathML.to_string(), "mathml");
        assert_eq!(InputFormat::AsciiMath.to_string(), "ascii_math");
    }

    #[test]
    fn test_raw_input_creation() {
        let input = RawInput::new(InputFormat::Latex, "x^2 + 1".to_string());
        assert_eq!(input.format, InputFormat::Latex);
        assert_eq!(input.content, "x^2 + 1");
        assert!(input.image_data.is_none());
    }

    #[test]
    fn test_raw_input_with_image() {
        let image_data = vec![1, 2, 3, 4];
        let input = RawInput::with_image(
            InputFormat::NapkinPhoto,
            "scanned".to_string(),
            image_data.clone(),
        );
        assert_eq!(input.image_data, Some(image_data));
    }

    #[test]
    fn test_raw_input_with_metadata() {
        let input = RawInput::new(InputFormat::PlainText, "test".to_string())
            .with_metadata("source".to_string(), "user".to_string());
        assert_eq!(input.metadata.get("source"), Some(&"user".to_string()));
    }

    #[test]
    fn test_normalized_expression_creation() {
        let expr = NormalizedExpression::new("x^2".to_string(), "x^2".to_string());
        assert_eq!(expr.latex, "x^2");
        assert_eq!(expr.variable_count, 0);
        assert!(!expr.has_integral);
    }

    #[test]
    fn test_category_display() {
        assert_eq!(Category::Algebra.to_string(), "algebra");
        assert_eq!(Category::Calculus.to_string(), "calculus");
        assert_eq!(Category::LinearAlgebra.to_string(), "linear_algebra");
        assert_eq!(
            Category::DifferentialEquations.to_string(),
            "differential_equations"
        );
    }

    #[test]
    fn test_operation_display() {
        assert_eq!(Operation::Solve.to_string(), "solve");
        assert_eq!(Operation::Integrate.to_string(), "integrate");
        assert_eq!(
            Operation::Custom("special".to_string()).to_string(),
            "custom:special"
        );
    }

    #[test]
    fn test_ambiguity_builder() {
        let amb = Ambiguity::new("unclear variable".to_string())
            .with_alternatives(vec!["x".to_string(), "y".to_string()])
            .with_position(5);
        assert_eq!(amb.alternatives.len(), 2);
        assert_eq!(amb.position, Some(5));
    }

    #[test]
    fn test_classification_result() {
        let result = ClassificationResult::new(
            Category::Algebra,
            vec![Operation::Solve, Operation::Simplify],
            0.85,
        );
        assert_eq!(result.category, Category::Algebra);
        assert_eq!(result.operations.len(), 2);
        assert_eq!(result.confidence, 0.85);
    }

    #[test]
    fn test_classification_confidence_clamp() {
        let result1 = ClassificationResult::new(Category::Unknown, vec![], 1.5);
        assert_eq!(result1.confidence, 1.0);

        let result2 = ClassificationResult::new(Category::Unknown, vec![], -0.5);
        assert_eq!(result2.confidence, 0.0);
    }

    #[test]
    fn test_applicability_display() {
        assert_eq!(Applicability::Recommended.to_string(), "recommended");
        assert_eq!(Applicability::Applicable.to_string(), "applicable");
        assert_eq!(Applicability::Marginal.to_string(), "marginal");
        assert_eq!(Applicability::NotApplicable.to_string(), "not_applicable");
    }

    #[test]
    fn test_handler_id() {
        let id: HandlerId = "sympy".into();
        assert_eq!(id.as_str(), "sympy");
        assert_eq!(id.to_string(), "sympy");
    }

    #[test]
    fn test_menu_option_builder() {
        let option = MenuOption::new(
            "wolfram".into(),
            "Wolfram Alpha".to_string(),
            "Computational engine".to_string(),
            Applicability::Recommended,
        )
        .with_estimated_time(500)
        .requires_api_key();

        assert_eq!(option.estimated_time_ms, Some(500));
        assert!(option.requires_api_key);
    }

    #[test]
    fn test_option_menu_builder() {
        let session_id = SessionId::new();
        let menu = OptionMenu::new(session_id, "x^2 + 1 = 0".to_string()).add_option(
            MenuOption::new(
                "test".into(),
                "Test".to_string(),
                "desc".to_string(),
                Applicability::Applicable,
            ),
        );

        assert_eq!(menu.session_id, session_id);
        assert_eq!(menu.options.len(), 1);
    }

    #[test]
    fn test_compute_step() {
        let step = ComputeStep::new(1, "Factor".to_string(), "(x+1)(x-1)".to_string());
        assert_eq!(step.step_number, 1);
        assert_eq!(step.description, "Factor");
    }

    #[test]
    fn test_compute_result_success() {
        let result = ComputeResult::success("sympy".into(), "SymPy".to_string(), 123)
            .with_latex("x=1".to_string())
            .with_plain("x = 1".to_string());

        assert!(result.success);
        assert_eq!(result.result_latex, Some("x=1".to_string()));
        assert_eq!(result.duration_ms, 123);
    }

    #[test]
    fn test_compute_result_failure() {
        let result = ComputeResult::failure("test".into(), "Test".to_string(), 50);
        assert!(!result.success);
        assert!(result.result_latex.is_none());
    }

    #[test]
    fn test_output_format_default() {
        let format = OutputFormat::default();
        assert_eq!(format, OutputFormat::Both);
    }

    #[test]
    fn test_user_preference_builder() {
        let pref = UserPreference::new()
            .with_handler("sympy".into())
            .with_handler("wolfram".into())
            .with_format(OutputFormat::Latex);

        assert_eq!(pref.preferred_handlers.len(), 2);
        assert_eq!(pref.preferred_format, OutputFormat::Latex);
        assert!(pref.show_steps);
    }

    #[test]
    fn test_back_reference() {
        let session_id = SessionId::new();
        let back_ref = BackReference::new(session_id, 3).with_label("eq1".to_string());

        assert_eq!(back_ref.session_id, session_id);
        assert_eq!(back_ref.expression_index, 3);
        assert_eq!(back_ref.label, Some("eq1".to_string()));
    }

    #[test]
    fn test_switchboard_request() {
        use crate::swarm::types::NodeId;
        let session_id = SessionId::new();
        let node_id = NodeId::new();
        let req = SwitchboardRequest::new(
            session_id,
            "x^2 + 1".to_string(),
            "sympy".into(),
            node_id,
        )
        .with_param("precision".to_string(), "high".to_string());

        assert_eq!(req.params.get("precision"), Some(&"high".to_string()));
    }

    #[test]
    fn test_serde_input_format() {
        let format = InputFormat::Latex;
        let json = serde_json::to_string(&format).unwrap();
        let deserialized: InputFormat = serde_json::from_str(&json).unwrap();
        assert_eq!(format, deserialized);
    }

    #[test]
    fn test_serde_category() {
        let category = Category::DifferentialEquations;
        let json = serde_json::to_string(&category).unwrap();
        let deserialized: Category = serde_json::from_str(&json).unwrap();
        assert_eq!(category, deserialized);
    }

    #[test]
    fn test_serde_operation() {
        let op = Operation::Custom("test".to_string());
        let json = serde_json::to_string(&op).unwrap();
        let deserialized: Operation = serde_json::from_str(&json).unwrap();
        assert_eq!(op, deserialized);
    }
}
