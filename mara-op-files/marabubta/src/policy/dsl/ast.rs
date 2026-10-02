// Marabunta - Licensed under the MIT License.
//! Abstract Syntax Tree for Policy DSL
//!
//! Defines the AST nodes that represent parsed policy expressions
//! before compilation to Policy IR.

use super::error::Span;

/// A complete policy definition
#[derive(Debug, Clone)]
pub struct PolicyAst {
    /// Optional policy name
    pub name: Option<Spanned<String>>,
    /// Optional policy ID
    pub id: Option<Spanned<String>>,
    /// Optional description
    pub description: Option<Spanned<String>>,
    /// The rules in this policy
    pub rules: Vec<Spanned<RuleAst>>,
    /// Full span of the policy
    pub span: Span,
}

/// A single policy rule (condition => effect)
#[derive(Debug, Clone)]
pub struct RuleAst {
    /// Condition that triggers this rule
    pub condition: Spanned<ConditionAst>,
    /// Effect(s) when condition is met
    pub effects: Vec<Spanned<EffectAst>>,
}

/// A condition expression
#[derive(Debug, Clone)]
pub enum ConditionAst {
    /// Always true
    Always,
    /// Never true (disabled)
    Never,
    /// Comparison expression (e.g., job.priority > 5)
    Comparison {
        left: Box<Spanned<ExprAst>>,
        operator: Spanned<ComparisonOp>,
        right: Box<Spanned<ExprAst>>,
    },
    /// Logical AND of conditions
    And(Vec<Spanned<ConditionAst>>),
    /// Logical OR of conditions
    Or(Vec<Spanned<ConditionAst>>),
    /// Logical NOT of condition
    Not(Box<Spanned<ConditionAst>>),
    /// Property has a value (e.g., job.tags has "gpu")
    Has {
        object: Spanned<ObjectAccess>,
        key: Spanned<String>,
    },
    /// Property contains value (e.g., submitter.domains contains "ml")
    Contains {
        object: Spanned<ObjectAccess>,
        value: Spanned<String>,
    },
    /// Value matches regex (e.g., job.name matches "ml-.*")
    Matches {
        object: Spanned<ObjectAccess>,
        pattern: Spanned<String>,
    },
    /// Value is in range (e.g., job.priority between 0 and 100)
    Between {
        object: Spanned<ObjectAccess>,
        low: Box<Spanned<ExprAst>>,
        high: Box<Spanned<ExprAst>>,
    },
    /// Value is in a set (e.g., job.type in ["monte_carlo", "parameter_sweep"])
    In {
        object: Spanned<ObjectAccess>,
        values: Vec<Spanned<LiteralAst>>,
    },
    /// Time window condition (e.g., time in "0 9-17 * * MON-FRI")
    TimeWindow { cron: Spanned<String> },
    /// Parenthesized condition
    Grouped(Box<Spanned<ConditionAst>>),
}

/// An effect specification
#[derive(Debug, Clone)]
pub enum EffectAst {
    /// Prefer nodes matching selector
    Prefer {
        selector: Spanned<SelectorAst>,
        weight: Option<Spanned<f64>>,
    },
    /// Require nodes matching selector
    Require { selector: Spanned<SelectorAst> },
    /// Exclude nodes matching selector
    Exclude { selector: Spanned<SelectorAst> },
    /// Affinity with other jobs/tasks
    Affinity {
        target: Spanned<AffinityTargetAst>,
        scope: Option<Spanned<String>>,
        weight: Option<Spanned<f64>>,
    },
    /// Anti-affinity with other jobs/tasks
    AntiAffinity {
        target: Spanned<AffinityTargetAst>,
        scope: Option<Spanned<String>>,
        weight: Option<Spanned<f64>>,
    },
    /// Set a resource limit
    SetResourceLimit {
        resource: Spanned<String>,
        limit: Spanned<f64>,
    },
    /// Adjust job priority
    SetPriority {
        value: Spanned<i64>,
        mode: Option<Spanned<PriorityModeAst>>,
    },
    /// Charge quota
    ChargeQuota {
        quota_id: Spanned<String>,
        multiplier: Option<Spanned<f64>>,
    },
    /// Allow preemption
    AllowPreemption { min_priority: Option<Spanned<u32>> },
    /// Disallow preemption
    DisallowPreemption,
}

/// Node selector specification
#[derive(Debug, Clone)]
pub enum SelectorAst {
    /// All nodes
    All,
    /// Specific node IDs
    NodeIds(Vec<Spanned<String>>),
    /// Node group
    Group(Spanned<String>),
    /// Tag-based selector
    Tag(Spanned<TagSelectorAst>),
    /// Property comparison (e.g., node.gpu = true)
    Property {
        object: Spanned<ObjectAccess>,
        operator: Spanned<ComparisonOp>,
        value: Spanned<LiteralAst>,
    },
}

/// Tag selector for node matching
#[derive(Debug, Clone)]
pub enum TagSelectorAst {
    /// Tag has key
    HasKey(Spanned<String>),
    /// Tag equals key-value
    Equals {
        key: Spanned<String>,
        value: Spanned<String>,
    },
    /// Tag key matches pattern
    KeyMatches {
        key: Spanned<String>,
        pattern: Spanned<String>,
    },
    /// Tag value matches pattern
    ValueMatches {
        key: Spanned<String>,
        pattern: Spanned<String>,
    },
    /// Logical AND
    And(Vec<Spanned<TagSelectorAst>>),
    /// Logical OR
    Or(Vec<Spanned<TagSelectorAst>>),
    /// Logical NOT
    Not(Box<Spanned<TagSelectorAst>>),
}

/// Affinity target specification
#[derive(Debug, Clone)]
pub enum AffinityTargetAst {
    /// Same job
    SameJob,
    /// Jobs matching pattern
    JobPattern(Spanned<String>),
    /// Jobs with tag
    Tag(Spanned<TagSelectorAst>),
}

/// Priority mode for SetPriority
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriorityModeAst {
    Set,
    Add,
    Multiply,
    Max,
    Min,
}

/// An expression
#[derive(Debug, Clone)]
pub enum ExprAst {
    /// Literal value
    Literal(LiteralAst),
    /// Object property access (e.g., job.priority)
    Access(ObjectAccess),
    /// Binary operation
    Binary {
        left: Box<Spanned<ExprAst>>,
        operator: Spanned<BinaryOp>,
        right: Box<Spanned<ExprAst>>,
    },
    /// Unary operation
    Unary {
        operator: Spanned<UnaryOp>,
        operand: Box<Spanned<ExprAst>>,
    },
    /// Parenthesized expression
    Grouped(Box<Spanned<ExprAst>>),
}

/// Object property access
#[derive(Debug, Clone)]
pub struct ObjectAccess {
    /// Object type (job, node, submitter, resource)
    pub object_type: ObjectType,
    /// Property path (e.g., ["tags", "gpu"])
    pub properties: Vec<Spanned<String>>,
}

/// Object types in the DSL
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectType {
    Job,
    Node,
    Submitter,
    Resource,
    Time,
    Tag,
    Group,
}

impl ObjectType {
    /// Get the name of this object type
    pub fn name(&self) -> &'static str {
        match self {
            ObjectType::Job => "job",
            ObjectType::Node => "node",
            ObjectType::Submitter => "submitter",
            ObjectType::Resource => "resource",
            ObjectType::Time => "time",
            ObjectType::Tag => "tag",
            ObjectType::Group => "group",
        }
    }
}

/// Literal values
#[derive(Debug, Clone)]
pub enum LiteralAst {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Array(Vec<Spanned<LiteralAst>>),
}

impl LiteralAst {
    /// Get the type name of this literal
    pub fn type_name(&self) -> &'static str {
        match self {
            LiteralAst::String(_) => "string",
            LiteralAst::Integer(_) => "integer",
            LiteralAst::Float(_) => "float",
            LiteralAst::Boolean(_) => "boolean",
            LiteralAst::Array(_) => "array",
        }
    }
}

/// Comparison operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOp {
    Equals,
    NotEquals,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Matches,
}

impl ComparisonOp {
    /// Get the symbol for this operator
    pub fn symbol(&self) -> &'static str {
        match self {
            ComparisonOp::Equals => "=",
            ComparisonOp::NotEquals => "!=",
            ComparisonOp::GreaterThan => ">",
            ComparisonOp::GreaterThanOrEqual => ">=",
            ComparisonOp::LessThan => "<",
            ComparisonOp::LessThanOrEqual => "<=",
            ComparisonOp::Matches => "~",
        }
    }
}

/// Binary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
}

/// Unary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Negate,
    Not,
}

/// A value with its span in source
#[derive(Debug, Clone)]
pub struct Spanned<T> {
    /// The value
    pub value: T,
    /// Span in source
    pub span: Span,
}

impl<T> Spanned<T> {
    /// Create a new spanned value
    pub fn new(value: T, span: Span) -> Self {
        Self { value, span }
    }

    /// Map the inner value
    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> Spanned<U> {
        Spanned {
            value: f(self.value),
            span: self.span,
        }
    }

    /// Get a reference to the inner value
    pub fn as_ref(&self) -> Spanned<&T> {
        Spanned {
            value: &self.value,
            span: self.span,
        }
    }
}

impl<T: Clone> Spanned<T> {
    /// Get the inner value
    pub fn into_inner(self) -> T {
        self.value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::dsl::error::SourcePosition;

    fn dummy_span() -> Span {
        Span::new(SourcePosition::new(1, 1, 0), SourcePosition::new(1, 10, 10))
    }

    #[test]
    fn test_spanned_map() {
        let spanned: Spanned<i32> = Spanned::new(42, dummy_span());
        let mapped = spanned.map(|x| x * 2);
        assert_eq!(mapped.value, 84);
    }

    #[test]
    fn test_literal_type_names() {
        assert_eq!(LiteralAst::String("test".into()).type_name(), "string");
        assert_eq!(LiteralAst::Integer(42).type_name(), "integer");
        assert_eq!(LiteralAst::Float(3.14).type_name(), "float");
        assert_eq!(LiteralAst::Boolean(true).type_name(), "boolean");
        assert_eq!(LiteralAst::Array(vec![]).type_name(), "array");
    }

    #[test]
    fn test_comparison_op_symbols() {
        assert_eq!(ComparisonOp::Equals.symbol(), "=");
        assert_eq!(ComparisonOp::NotEquals.symbol(), "!=");
        assert_eq!(ComparisonOp::GreaterThan.symbol(), ">");
    }

    #[test]
    fn test_object_type_names() {
        assert_eq!(ObjectType::Job.name(), "job");
        assert_eq!(ObjectType::Node.name(), "node");
        assert_eq!(ObjectType::Submitter.name(), "submitter");
    }
}
