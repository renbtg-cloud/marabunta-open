// Marabunta - Licensed under the MIT License.
//! Policy DSL Parser
//!
//! Recursive descent parser that converts tokens into an AST.
//!
//! # Grammar
//!
//! ```text
//! policy       ::= rule+
//! rule         ::= condition "=>" effect (";" effect)*
//! condition    ::= or_expr
//! or_expr      ::= and_expr ("OR" and_expr)*
//! and_expr     ::= not_expr ("AND" not_expr)*
//! not_expr     ::= "NOT" not_expr | primary_cond
//! primary_cond ::= comparison | has_expr | contains_expr | matches_expr |
//!                  between_expr | in_expr | time_expr | "(" condition ")"
//! comparison   ::= expr comp_op expr
//! expr         ::= object_access | literal
//! object_access::= object_type "." property ("." property)*
//! effect       ::= prefer_effect | require_effect | exclude_effect |
//!                  affinity_effect | set_priority | charge_quota | preemption
//! prefer_effect::= "prefer" selector ("weight" number)?
//! require_effect::= "require" selector
//! exclude_effect::= "exclude" selector
//! selector     ::= "all" | node_selector
//! node_selector::= "node" "." property comp_op value | "group" "(" string ")" |
//!                  "tag" "(" tag_expr ")"
//! ```

use super::ast::*;
use super::error::{DslError, DslErrorKind, DslResult, Span};
use super::lexer::{Token, TokenKind};

/// Parser for the policy DSL
pub struct Parser {
    /// Tokens to parse
    tokens: Vec<Token>,
    /// Current position in tokens
    current: usize,
}

impl Parser {
    /// Create a new parser
    pub fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, current: 0 }
    }

    /// Parse a complete policy
    pub fn parse_policy(mut self) -> DslResult<PolicyAst> {
        let start = self.current_span();
        let mut rules = Vec::new();

        // Skip initial comments
        self.skip_comments();

        while !self.is_at_end() {
            self.skip_comments();
            if self.is_at_end() {
                break;
            }
            rules.push(self.parse_rule()?);
            self.skip_comments();
        }

        if rules.is_empty() {
            return Err(DslError::new(DslErrorKind::InvalidSyntax(
                "policy must contain at least one rule".to_string(),
            ))
            .with_span(start));
        }

        let end = self.previous_span();

        Ok(PolicyAst {
            name: None,
            id: None,
            description: None,
            rules,
            span: start.merge(end),
        })
    }

    /// Parse a single rule (condition => effects)
    fn parse_rule(&mut self) -> DslResult<Spanned<RuleAst>> {
        let start = self.current_span();

        // Parse condition
        let condition = self.parse_condition()?;

        // Expect =>
        self.expect(TokenKind::Arrow, "'=>'")?;

        // Parse effects (semicolon-separated)
        let mut effects = Vec::new();
        effects.push(self.parse_effect()?);

        while self.check(&TokenKind::Semicolon) {
            self.advance();
            if self.is_at_end() || self.check_condition_start() {
                break;
            }
            effects.push(self.parse_effect()?);
        }

        let end = self.previous_span();

        Ok(Spanned::new(
            RuleAst { condition, effects },
            start.merge(end),
        ))
    }

    /// Parse a condition expression
    fn parse_condition(&mut self) -> DslResult<Spanned<ConditionAst>> {
        self.parse_or_condition()
    }

    /// Parse OR expression
    fn parse_or_condition(&mut self) -> DslResult<Spanned<ConditionAst>> {
        let start = self.current_span();
        let mut left = self.parse_and_condition()?;

        while self.check(&TokenKind::Or) {
            self.advance();
            let right = self.parse_and_condition()?;
            let end = right.span;

            let combined_span = start.merge(end);
            let conditions = match left.value {
                ConditionAst::Or(mut conds) => {
                    conds.push(right);
                    conds
                }
                _ => vec![left, right],
            };

            left = Spanned::new(ConditionAst::Or(conditions), combined_span);
        }

        Ok(left)
    }

    /// Parse AND expression
    fn parse_and_condition(&mut self) -> DslResult<Spanned<ConditionAst>> {
        let start = self.current_span();
        let mut left = self.parse_not_condition()?;

        while self.check(&TokenKind::And) {
            self.advance();
            let right = self.parse_not_condition()?;
            let end = right.span;

            let combined_span = start.merge(end);
            let conditions = match left.value {
                ConditionAst::And(mut conds) => {
                    conds.push(right);
                    conds
                }
                _ => vec![left, right],
            };

            left = Spanned::new(ConditionAst::And(conditions), combined_span);
        }

        Ok(left)
    }

    /// Parse NOT expression
    fn parse_not_condition(&mut self) -> DslResult<Spanned<ConditionAst>> {
        if self.check(&TokenKind::Not) {
            let start = self.current_span();
            self.advance();
            let inner = self.parse_not_condition()?;
            let end = inner.span;

            Ok(Spanned::new(
                ConditionAst::Not(Box::new(inner)),
                start.merge(end),
            ))
        } else {
            self.parse_primary_condition()
        }
    }

    /// Parse primary condition (comparison, has, contains, matches, etc.)
    fn parse_primary_condition(&mut self) -> DslResult<Spanned<ConditionAst>> {
        let start = self.current_span();

        // Parenthesized condition
        if self.check(&TokenKind::LeftParen) {
            self.advance();
            let inner = self.parse_condition()?;
            self.expect(TokenKind::RightParen, "')'")?;
            let end = self.previous_span();

            return Ok(Spanned::new(
                ConditionAst::Grouped(Box::new(inner)),
                start.merge(end),
            ));
        }

        // Time window condition
        if self.check(&TokenKind::Time) {
            self.advance();
            self.expect(TokenKind::In, "'in'")?;
            let cron_token = self.expect_string()?;
            let end = self.previous_span();

            return Ok(Spanned::new(
                ConditionAst::TimeWindow { cron: cron_token },
                start.merge(end),
            ));
        }

        // Object access based conditions
        let object_access = self.parse_object_access()?;

        // Check what follows
        if self.check(&TokenKind::Has) {
            self.advance();
            let key = self.expect_string()?;
            let end = self.previous_span();

            return Ok(Spanned::new(
                ConditionAst::Has {
                    object: object_access,
                    key,
                },
                start.merge(end),
            ));
        }

        if self.check(&TokenKind::Contains) {
            self.advance();
            let value = self.expect_string()?;
            let end = self.previous_span();

            return Ok(Spanned::new(
                ConditionAst::Contains {
                    object: object_access,
                    value,
                },
                start.merge(end),
            ));
        }

        if self.check(&TokenKind::Matches) {
            self.advance();
            let pattern = self.expect_string()?;
            let end = self.previous_span();

            return Ok(Spanned::new(
                ConditionAst::Matches {
                    object: object_access,
                    pattern,
                },
                start.merge(end),
            ));
        }

        if self.check(&TokenKind::Between) {
            self.advance();
            let low = self.parse_expression()?;
            self.expect(TokenKind::And, "'and'")?;
            let high = self.parse_expression()?;
            let end = self.previous_span();

            return Ok(Spanned::new(
                ConditionAst::Between {
                    object: object_access,
                    low: Box::new(low),
                    high: Box::new(high),
                },
                start.merge(end),
            ));
        }

        if self.check(&TokenKind::In) {
            self.advance();
            let values = self.parse_literal_array()?;
            let end = self.previous_span();

            return Ok(Spanned::new(
                ConditionAst::In {
                    object: object_access,
                    values,
                },
                start.merge(end),
            ));
        }

        // Comparison
        let operator = self.parse_comparison_op()?;
        let right = self.parse_expression()?;
        let end = self.previous_span();

        Ok(Spanned::new(
            ConditionAst::Comparison {
                left: Box::new(Spanned::new(
                    ExprAst::Access(object_access.value),
                    object_access.span,
                )),
                operator,
                right: Box::new(right),
            },
            start.merge(end),
        ))
    }

    /// Parse an effect
    fn parse_effect(&mut self) -> DslResult<Spanned<EffectAst>> {
        let start = self.current_span();

        match self.peek().kind.clone() {
            TokenKind::Prefer => {
                self.advance();
                let selector = self.parse_selector()?;
                let weight = if self.check(&TokenKind::Weight) {
                    self.advance();
                    Some(self.expect_float()?)
                } else {
                    None
                };
                let end = self.previous_span();

                Ok(Spanned::new(
                    EffectAst::Prefer { selector, weight },
                    start.merge(end),
                ))
            }
            TokenKind::Require => {
                self.advance();
                let selector = self.parse_selector()?;
                let end = self.previous_span();

                Ok(Spanned::new(
                    EffectAst::Require { selector },
                    start.merge(end),
                ))
            }
            TokenKind::Exclude => {
                self.advance();
                let selector = self.parse_selector()?;
                let end = self.previous_span();

                Ok(Spanned::new(
                    EffectAst::Exclude { selector },
                    start.merge(end),
                ))
            }
            TokenKind::Affinity => {
                self.advance();
                let target = self.parse_affinity_target()?;
                let scope = if self.check(&TokenKind::At) || self.check(&TokenKind::Scope) {
                    self.advance();
                    Some(self.expect_string_or_identifier()?)
                } else {
                    None
                };
                let weight = if self.check(&TokenKind::Weight) {
                    self.advance();
                    Some(self.expect_float()?)
                } else {
                    None
                };
                let end = self.previous_span();

                Ok(Spanned::new(
                    EffectAst::Affinity {
                        target,
                        scope,
                        weight,
                    },
                    start.merge(end),
                ))
            }
            TokenKind::AntiAffinity => {
                self.advance();
                let target = self.parse_affinity_target()?;
                let scope = if self.check(&TokenKind::At) || self.check(&TokenKind::Scope) {
                    self.advance();
                    Some(self.expect_string_or_identifier()?)
                } else {
                    None
                };
                let weight = if self.check(&TokenKind::Weight) {
                    self.advance();
                    Some(self.expect_float()?)
                } else {
                    None
                };
                let end = self.previous_span();

                Ok(Spanned::new(
                    EffectAst::AntiAffinity {
                        target,
                        scope,
                        weight,
                    },
                    start.merge(end),
                ))
            }
            TokenKind::Identifier(ref name) if name == "set_priority" => {
                self.advance();
                let value = self.expect_integer()?;
                let mode = self.parse_priority_mode()?;
                let end = self.previous_span();

                Ok(Spanned::new(
                    EffectAst::SetPriority { value, mode },
                    start.merge(end),
                ))
            }
            TokenKind::Identifier(ref name) if name == "charge_quota" => {
                self.advance();
                let quota_id = self.expect_string()?;
                let multiplier = if self.check(&TokenKind::Star) || self.check(&TokenKind::At) {
                    self.advance();
                    Some(self.expect_float()?)
                } else {
                    None
                };
                let end = self.previous_span();

                Ok(Spanned::new(
                    EffectAst::ChargeQuota {
                        quota_id,
                        multiplier,
                    },
                    start.merge(end),
                ))
            }
            TokenKind::Identifier(ref name) if name == "allow_preemption" => {
                self.advance();
                let min_priority = if self.check(&TokenKind::GreaterThanOrEqual)
                    || self.check(&TokenKind::GreaterThan)
                {
                    self.advance();
                    Some(self.expect_u32()?)
                } else {
                    None
                };
                let end = self.previous_span();

                Ok(Spanned::new(
                    EffectAst::AllowPreemption { min_priority },
                    start.merge(end),
                ))
            }
            TokenKind::Identifier(ref name) if name == "disallow_preemption" => {
                self.advance();
                let end = self.previous_span();
                Ok(Spanned::new(
                    EffectAst::DisallowPreemption,
                    start.merge(end),
                ))
            }
            _ => {
                let token = self.peek();
                Err(DslError::new(DslErrorKind::UnexpectedToken {
                    expected: "effect (prefer, require, exclude, affinity, etc.)".to_string(),
                    found: token.kind.name().to_string(),
                })
                .with_span(token.span))
            }
        }
    }

    /// Parse a node selector
    fn parse_selector(&mut self) -> DslResult<Spanned<SelectorAst>> {
        let start = self.current_span();

        // "all" selector
        if self.check(&TokenKind::All) {
            self.advance();
            let end = self.previous_span();
            return Ok(Spanned::new(SelectorAst::All, start.merge(end)));
        }

        // "group" selector
        if self.check(&TokenKind::Group) {
            self.advance();
            self.expect(TokenKind::LeftParen, "'('")?;
            let name = self.expect_string()?;
            self.expect(TokenKind::RightParen, "')'")?;
            let end = self.previous_span();
            return Ok(Spanned::new(SelectorAst::Group(name), start.merge(end)));
        }

        // "tag" selector
        if self.check(&TokenKind::Tag) {
            self.advance();
            self.expect(TokenKind::LeftParen, "'('")?;
            let tag_selector = self.parse_tag_selector()?;
            self.expect(TokenKind::RightParen, "')'")?;
            let end = self.previous_span();
            return Ok(Spanned::new(
                SelectorAst::Tag(tag_selector),
                start.merge(end),
            ));
        }

        // "node" property selector
        if self.check(&TokenKind::Node) {
            let object_access = self.parse_object_access()?;
            let operator = self.parse_comparison_op()?;
            let value = self.parse_literal()?;
            let end = self.previous_span();

            return Ok(Spanned::new(
                SelectorAst::Property {
                    object: object_access,
                    operator,
                    value,
                },
                start.merge(end),
            ));
        }

        // Array of node IDs
        if self.check(&TokenKind::LeftBracket) {
            let ids = self.parse_string_array()?;
            let end = self.previous_span();
            return Ok(Spanned::new(SelectorAst::NodeIds(ids), start.merge(end)));
        }

        let token = self.peek();
        Err(DslError::new(DslErrorKind::UnexpectedToken {
            expected: "selector (all, group, tag, node, or node IDs)".to_string(),
            found: token.kind.name().to_string(),
        })
        .with_span(token.span))
    }

    /// Parse a tag selector expression
    fn parse_tag_selector(&mut self) -> DslResult<Spanned<TagSelectorAst>> {
        self.parse_tag_or()
    }

    fn parse_tag_or(&mut self) -> DslResult<Spanned<TagSelectorAst>> {
        let start = self.current_span();
        let mut left = self.parse_tag_and()?;

        while self.check(&TokenKind::Or) {
            self.advance();
            let right = self.parse_tag_and()?;
            let end = right.span;

            let combined_span = start.merge(end);
            let selectors = match left.value {
                TagSelectorAst::Or(mut sels) => {
                    sels.push(right);
                    sels
                }
                _ => vec![left, right],
            };

            left = Spanned::new(TagSelectorAst::Or(selectors), combined_span);
        }

        Ok(left)
    }

    fn parse_tag_and(&mut self) -> DslResult<Spanned<TagSelectorAst>> {
        let start = self.current_span();
        let mut left = self.parse_tag_not()?;

        while self.check(&TokenKind::And) {
            self.advance();
            let right = self.parse_tag_not()?;
            let end = right.span;

            let combined_span = start.merge(end);
            let selectors = match left.value {
                TagSelectorAst::And(mut sels) => {
                    sels.push(right);
                    sels
                }
                _ => vec![left, right],
            };

            left = Spanned::new(TagSelectorAst::And(selectors), combined_span);
        }

        Ok(left)
    }

    fn parse_tag_not(&mut self) -> DslResult<Spanned<TagSelectorAst>> {
        if self.check(&TokenKind::Not) {
            let start = self.current_span();
            self.advance();
            let inner = self.parse_tag_not()?;
            let end = inner.span;

            Ok(Spanned::new(
                TagSelectorAst::Not(Box::new(inner)),
                start.merge(end),
            ))
        } else {
            self.parse_tag_primary()
        }
    }

    fn parse_tag_primary(&mut self) -> DslResult<Spanned<TagSelectorAst>> {
        let start = self.current_span();

        // Parenthesized expression
        if self.check(&TokenKind::LeftParen) {
            self.advance();
            let inner = self.parse_tag_selector()?;
            self.expect(TokenKind::RightParen, "')'")?;
            return Ok(inner);
        }

        // Tag expression: key = value, key ~ pattern, or just key
        let key = self.expect_string_or_identifier()?;

        if self.check(&TokenKind::Equals) {
            self.advance();
            let value = self.expect_string()?;
            let end = self.previous_span();

            Ok(Spanned::new(
                TagSelectorAst::Equals { key, value },
                start.merge(end),
            ))
        } else if self.check(&TokenKind::Tilde) {
            self.advance();
            let pattern = self.expect_string()?;
            let end = self.previous_span();

            Ok(Spanned::new(
                TagSelectorAst::ValueMatches { key, pattern },
                start.merge(end),
            ))
        } else {
            let end = key.span;
            Ok(Spanned::new(TagSelectorAst::HasKey(key), start.merge(end)))
        }
    }

    /// Parse affinity target
    fn parse_affinity_target(&mut self) -> DslResult<Spanned<AffinityTargetAst>> {
        let start = self.current_span();

        if self.check(&TokenKind::Job) {
            self.advance();
            let end = self.previous_span();
            return Ok(Spanned::new(AffinityTargetAst::SameJob, start.merge(end)));
        }

        if self.check(&TokenKind::With) {
            self.advance();
        }

        if self.check(&TokenKind::Tag) {
            self.advance();
            self.expect(TokenKind::LeftParen, "'('")?;
            let tag = self.parse_tag_selector()?;
            self.expect(TokenKind::RightParen, "')'")?;
            let end = self.previous_span();

            return Ok(Spanned::new(AffinityTargetAst::Tag(tag), start.merge(end)));
        }

        // Pattern string
        let pattern = self.expect_string()?;
        let end = self.previous_span();

        Ok(Spanned::new(
            AffinityTargetAst::JobPattern(pattern),
            start.merge(end),
        ))
    }

    /// Parse priority mode
    fn parse_priority_mode(&mut self) -> DslResult<Option<Spanned<PriorityModeAst>>> {
        let start = self.current_span();

        if let TokenKind::Identifier(ref name) = self.peek().kind.clone() {
            let mode = match name.as_str() {
                "set" => Some(PriorityModeAst::Set),
                "add" => Some(PriorityModeAst::Add),
                "multiply" | "mul" => Some(PriorityModeAst::Multiply),
                "max" => Some(PriorityModeAst::Max),
                "min" => Some(PriorityModeAst::Min),
                _ => None,
            };

            if mode.is_some() {
                self.advance();
                let end = self.previous_span();
                return Ok(Some(Spanned::new(mode.unwrap(), start.merge(end))));
            }
        }

        Ok(None)
    }

    /// Parse object access (e.g., job.priority, node.tags.gpu)
    fn parse_object_access(&mut self) -> DslResult<Spanned<ObjectAccess>> {
        let start = self.current_span();

        let object_type = match self.peek().kind.clone() {
            TokenKind::Job => ObjectType::Job,
            TokenKind::Node => ObjectType::Node,
            TokenKind::Submitter => ObjectType::Submitter,
            TokenKind::Resource => ObjectType::Resource,
            TokenKind::Time => ObjectType::Time,
            TokenKind::Tag => ObjectType::Tag,
            TokenKind::Group => ObjectType::Group,
            _ => {
                let token = self.peek();
                return Err(DslError::new(DslErrorKind::UnexpectedToken {
                    expected: "object type (job, node, submitter, resource, time)".to_string(),
                    found: token.kind.name().to_string(),
                })
                .with_span(token.span));
            }
        };
        self.advance();

        let mut properties = Vec::new();
        while self.check(&TokenKind::Dot) {
            self.advance();
            let prop = self.expect_identifier()?;
            properties.push(prop);
        }

        let end = self.previous_span();

        Ok(Spanned::new(
            ObjectAccess {
                object_type,
                properties,
            },
            start.merge(end),
        ))
    }

    /// Parse an expression
    fn parse_expression(&mut self) -> DslResult<Spanned<ExprAst>> {
        // Literal
        if let Some(lit) = self.try_parse_literal()? {
            return Ok(Spanned::new(ExprAst::Literal(lit.value), lit.span));
        }

        // Object access
        let access = self.parse_object_access()?;
        let span = access.span;

        Ok(Spanned::new(ExprAst::Access(access.value), span))
    }

    /// Parse a comparison operator
    fn parse_comparison_op(&mut self) -> DslResult<Spanned<ComparisonOp>> {
        let token = self.peek().clone();
        let op = match token.kind {
            TokenKind::Equals => ComparisonOp::Equals,
            TokenKind::NotEquals => ComparisonOp::NotEquals,
            TokenKind::GreaterThan => ComparisonOp::GreaterThan,
            TokenKind::GreaterThanOrEqual => ComparisonOp::GreaterThanOrEqual,
            TokenKind::LessThan => ComparisonOp::LessThan,
            TokenKind::LessThanOrEqual => ComparisonOp::LessThanOrEqual,
            TokenKind::Tilde => ComparisonOp::Matches,
            _ => {
                return Err(DslError::new(DslErrorKind::UnexpectedToken {
                    expected: "comparison operator (=, !=, >, >=, <, <=, ~)".to_string(),
                    found: token.kind.name().to_string(),
                })
                .with_span(token.span));
            }
        };

        self.advance();
        Ok(Spanned::new(op, token.span))
    }

    /// Try to parse a literal
    fn try_parse_literal(&mut self) -> DslResult<Option<Spanned<LiteralAst>>> {
        let token = self.peek().clone();

        let lit = match token.kind {
            TokenKind::String(ref s) => {
                self.advance();
                Some(Spanned::new(LiteralAst::String(s.clone()), token.span))
            }
            TokenKind::Integer(i) => {
                self.advance();
                Some(Spanned::new(LiteralAst::Integer(i), token.span))
            }
            TokenKind::Float(f) => {
                self.advance();
                Some(Spanned::new(LiteralAst::Float(f), token.span))
            }
            TokenKind::True => {
                self.advance();
                Some(Spanned::new(LiteralAst::Boolean(true), token.span))
            }
            TokenKind::False => {
                self.advance();
                Some(Spanned::new(LiteralAst::Boolean(false), token.span))
            }
            TokenKind::LeftBracket => {
                let arr = self.parse_literal_array()?;
                let span = if arr.is_empty() {
                    token.span
                } else {
                    token.span.merge(self.previous_span())
                };
                Some(Spanned::new(LiteralAst::Array(arr), span))
            }
            _ => None,
        };

        Ok(lit)
    }

    /// Parse a literal
    fn parse_literal(&mut self) -> DslResult<Spanned<LiteralAst>> {
        self.try_parse_literal()?.ok_or_else(|| {
            let token = self.peek();
            DslError::new(DslErrorKind::UnexpectedToken {
                expected: "literal value".to_string(),
                found: token.kind.name().to_string(),
            })
            .with_span(token.span)
        })
    }

    /// Parse a literal array
    fn parse_literal_array(&mut self) -> DslResult<Vec<Spanned<LiteralAst>>> {
        self.expect(TokenKind::LeftBracket, "'['")?;

        let mut values = Vec::new();
        if !self.check(&TokenKind::RightBracket) {
            values.push(self.parse_literal()?);
            while self.check(&TokenKind::Comma) {
                self.advance();
                if self.check(&TokenKind::RightBracket) {
                    break;
                }
                values.push(self.parse_literal()?);
            }
        }

        self.expect(TokenKind::RightBracket, "']'")?;
        Ok(values)
    }

    /// Parse a string array
    fn parse_string_array(&mut self) -> DslResult<Vec<Spanned<String>>> {
        self.expect(TokenKind::LeftBracket, "'['")?;

        let mut values = Vec::new();
        if !self.check(&TokenKind::RightBracket) {
            values.push(self.expect_string()?);
            while self.check(&TokenKind::Comma) {
                self.advance();
                if self.check(&TokenKind::RightBracket) {
                    break;
                }
                values.push(self.expect_string()?);
            }
        }

        self.expect(TokenKind::RightBracket, "']'")?;
        Ok(values)
    }

    // =========================================================================
    // Helper methods
    // =========================================================================

    fn skip_comments(&mut self) {
        while let TokenKind::Comment(_) = self.peek().kind {
            self.advance();
        }
    }

    fn is_at_end(&self) -> bool {
        self.peek().kind == TokenKind::Eof
    }

    fn peek(&self) -> &Token {
        &self.tokens[self.current]
    }

    fn previous(&self) -> &Token {
        &self.tokens[self.current.saturating_sub(1)]
    }

    fn current_span(&self) -> Span {
        self.peek().span
    }

    fn previous_span(&self) -> Span {
        self.previous().span
    }

    fn advance(&mut self) -> &Token {
        if !self.is_at_end() {
            self.current += 1;
        }
        self.previous()
    }

    fn check(&self, kind: &TokenKind) -> bool {
        std::mem::discriminant(&self.peek().kind) == std::mem::discriminant(kind)
    }

    fn check_condition_start(&self) -> bool {
        matches!(
            self.peek().kind,
            TokenKind::Job
                | TokenKind::Node
                | TokenKind::Submitter
                | TokenKind::Resource
                | TokenKind::Time
                | TokenKind::LeftParen
                | TokenKind::Not
        )
    }

    fn expect(&mut self, kind: TokenKind, expected: &str) -> DslResult<&Token> {
        if self.check(&kind) {
            Ok(self.advance())
        } else {
            let token = self.peek();
            Err(DslError::new(DslErrorKind::UnexpectedToken {
                expected: expected.to_string(),
                found: token.kind.name().to_string(),
            })
            .with_span(token.span))
        }
    }

    fn expect_string(&mut self) -> DslResult<Spanned<String>> {
        let token = self.peek().clone();
        match token.kind {
            TokenKind::String(s) => {
                self.advance();
                Ok(Spanned::new(s, token.span))
            }
            _ => Err(DslError::new(DslErrorKind::UnexpectedToken {
                expected: "string".to_string(),
                found: token.kind.name().to_string(),
            })
            .with_span(token.span)),
        }
    }

    fn expect_identifier(&mut self) -> DslResult<Spanned<String>> {
        let token = self.peek().clone();
        match token.kind {
            TokenKind::Identifier(s) => {
                self.advance();
                Ok(Spanned::new(s, token.span))
            }
            _ => Err(DslError::new(DslErrorKind::UnexpectedToken {
                expected: "identifier".to_string(),
                found: token.kind.name().to_string(),
            })
            .with_span(token.span)),
        }
    }

    fn expect_string_or_identifier(&mut self) -> DslResult<Spanned<String>> {
        let token = self.peek().clone();
        match token.kind {
            TokenKind::String(s) | TokenKind::Identifier(s) => {
                self.advance();
                Ok(Spanned::new(s, token.span))
            }
            // Also accept keywords that could be valid scope values
            TokenKind::Node => {
                self.advance();
                Ok(Spanned::new("node".to_string(), token.span))
            }
            TokenKind::Job => {
                self.advance();
                Ok(Spanned::new("job".to_string(), token.span))
            }
            _ => Err(DslError::new(DslErrorKind::UnexpectedToken {
                expected: "string or identifier".to_string(),
                found: token.kind.name().to_string(),
            })
            .with_span(token.span)),
        }
    }

    fn expect_integer(&mut self) -> DslResult<Spanned<i64>> {
        let token = self.peek().clone();
        match token.kind {
            TokenKind::Integer(i) => {
                self.advance();
                Ok(Spanned::new(i, token.span))
            }
            _ => Err(DslError::new(DslErrorKind::UnexpectedToken {
                expected: "integer".to_string(),
                found: token.kind.name().to_string(),
            })
            .with_span(token.span)),
        }
    }

    fn expect_float(&mut self) -> DslResult<Spanned<f64>> {
        let token = self.peek().clone();
        match token.kind {
            TokenKind::Float(f) => {
                self.advance();
                Ok(Spanned::new(f, token.span))
            }
            TokenKind::Integer(i) => {
                self.advance();
                Ok(Spanned::new(i as f64, token.span))
            }
            _ => Err(DslError::new(DslErrorKind::UnexpectedToken {
                expected: "number".to_string(),
                found: token.kind.name().to_string(),
            })
            .with_span(token.span)),
        }
    }

    fn expect_u32(&mut self) -> DslResult<Spanned<u32>> {
        let int = self.expect_integer()?;
        if int.value < 0 {
            return Err(DslError::new(DslErrorKind::InvalidSyntax(
                "expected positive integer".to_string(),
            ))
            .with_span(int.span));
        }
        Ok(Spanned::new(int.value as u32, int.span))
    }
}

/// Parse policy DSL source code into an AST
pub fn parse(source: &str) -> DslResult<PolicyAst> {
    let tokens = super::lexer::tokenize(source)?;
    Parser::new(tokens).parse_policy()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_rule() {
        let ast = parse(r#"job.priority > 5 => prefer all"#).unwrap();
        assert_eq!(ast.rules.len(), 1);
    }

    #[test]
    fn test_and_condition() {
        let ast = parse(r#"job.priority > 5 AND node.region = "us-east" => prefer all"#).unwrap();
        assert_eq!(ast.rules.len(), 1);
        match &ast.rules[0].value.condition.value {
            ConditionAst::And(conds) => assert_eq!(conds.len(), 2),
            _ => panic!("Expected AND condition"),
        }
    }

    #[test]
    fn test_or_condition() {
        let ast = parse(r#"job.priority > 5 OR job.priority < 2 => prefer all"#).unwrap();
        match &ast.rules[0].value.condition.value {
            ConditionAst::Or(conds) => assert_eq!(conds.len(), 2),
            _ => panic!("Expected OR condition"),
        }
    }

    #[test]
    fn test_not_condition() {
        let ast = parse(r#"NOT job.priority > 5 => prefer all"#).unwrap();
        match &ast.rules[0].value.condition.value {
            ConditionAst::Not(_) => {}
            _ => panic!("Expected NOT condition"),
        }
    }

    #[test]
    fn test_prefer_with_weight() {
        let ast = parse(r#"job.priority > 5 => prefer node.gpu = true weight 0.8"#).unwrap();
        match &ast.rules[0].value.effects[0].value {
            EffectAst::Prefer { weight, .. } => {
                assert!(weight.is_some());
                assert_eq!(weight.as_ref().unwrap().value, 0.8);
            }
            _ => panic!("Expected Prefer effect"),
        }
    }

    #[test]
    fn test_require_effect() {
        let ast = parse(r#"job.priority > 5 => require node.region = "us-east""#).unwrap();
        match &ast.rules[0].value.effects[0].value {
            EffectAst::Require { .. } => {}
            _ => panic!("Expected Require effect"),
        }
    }

    #[test]
    fn test_exclude_effect() {
        let ast = parse(r#"job.priority > 5 => exclude node.region = "eu-west""#).unwrap();
        match &ast.rules[0].value.effects[0].value {
            EffectAst::Exclude { .. } => {}
            _ => panic!("Expected Exclude effect"),
        }
    }

    #[test]
    fn test_group_selector() {
        let ast = parse(r#"job.priority > 5 => prefer group("ml-cluster")"#).unwrap();
        match &ast.rules[0].value.effects[0].value {
            EffectAst::Prefer { selector, .. } => match &selector.value {
                SelectorAst::Group(name) => assert_eq!(name.value, "ml-cluster"),
                _ => panic!("Expected Group selector"),
            },
            _ => panic!("Expected Prefer effect"),
        }
    }

    #[test]
    fn test_tag_selector() {
        let ast = parse(r#"job.priority > 5 => prefer tag(gpu = "true")"#).unwrap();
        match &ast.rules[0].value.effects[0].value {
            EffectAst::Prefer { selector, .. } => match &selector.value {
                SelectorAst::Tag(_) => {}
                _ => panic!("Expected Tag selector"),
            },
            _ => panic!("Expected Prefer effect"),
        }
    }

    #[test]
    fn test_has_condition() {
        let ast = parse(r#"job.tags has "gpu" => prefer all"#).unwrap();
        match &ast.rules[0].value.condition.value {
            ConditionAst::Has { key, .. } => assert_eq!(key.value, "gpu"),
            _ => panic!("Expected Has condition"),
        }
    }

    #[test]
    fn test_contains_condition() {
        let ast = parse(r#"submitter.domains contains "ml" => prefer all"#).unwrap();
        match &ast.rules[0].value.condition.value {
            ConditionAst::Contains { value, .. } => assert_eq!(value.value, "ml"),
            _ => panic!("Expected Contains condition"),
        }
    }

    #[test]
    fn test_matches_condition() {
        let ast = parse(r#"job.name matches "ml-.*" => prefer all"#).unwrap();
        match &ast.rules[0].value.condition.value {
            ConditionAst::Matches { pattern, .. } => assert_eq!(pattern.value, "ml-.*"),
            _ => panic!("Expected Matches condition"),
        }
    }

    #[test]
    fn test_between_condition() {
        let ast = parse(r#"job.priority between 0 and 100 => prefer all"#).unwrap();
        match &ast.rules[0].value.condition.value {
            ConditionAst::Between { .. } => {}
            _ => panic!("Expected Between condition"),
        }
    }

    #[test]
    fn test_in_condition() {
        let ast = parse(r#"job.type in ["monte_carlo", "parameter_sweep"] => prefer all"#).unwrap();
        match &ast.rules[0].value.condition.value {
            ConditionAst::In { values, .. } => assert_eq!(values.len(), 2),
            _ => panic!("Expected In condition"),
        }
    }

    #[test]
    fn test_time_condition() {
        let ast = parse(r#"time in "0 9-17 * * MON-FRI" => prefer all"#).unwrap();
        match &ast.rules[0].value.condition.value {
            ConditionAst::TimeWindow { cron } => {
                assert_eq!(cron.value, "0 9-17 * * MON-FRI")
            }
            _ => panic!("Expected TimeWindow condition"),
        }
    }

    #[test]
    fn test_multiple_effects() {
        let ast =
            parse(r#"job.priority > 5 => prefer all; exclude node.region = "eu-west""#).unwrap();
        assert_eq!(ast.rules[0].value.effects.len(), 2);
    }

    #[test]
    fn test_complex_policy() {
        let source = r#"
            # High priority ML jobs go to GPU nodes
            job.priority > 50 AND job.name matches "ml-.*" => prefer tag(gpu = "true") weight 0.9

            # Production jobs only on production nodes
            submitter.domains contains "production" => require node.env = "production"
        "#;

        let ast = parse(source).unwrap();
        assert_eq!(ast.rules.len(), 2);
    }

    #[test]
    fn test_affinity_effect() {
        let ast = parse(r#"job.priority > 5 => affinity job at node weight 0.8"#).unwrap();
        match &ast.rules[0].value.effects[0].value {
            EffectAst::Affinity { scope, weight, .. } => {
                assert!(scope.is_some());
                assert!(weight.is_some());
            }
            _ => panic!("Expected Affinity effect"),
        }
    }

    #[test]
    fn test_error_missing_arrow() {
        let result = parse(r#"job.priority > 5 prefer all"#);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err.kind, DslErrorKind::UnexpectedToken { .. }));
    }

    #[test]
    fn test_error_invalid_effect() {
        let result = parse(r#"job.priority > 5 => invalid_effect all"#);
        assert!(result.is_err());
    }
}
