// Marabunta - Licensed under the MIT License.
//! Principal and Hierarchy Management
//!
//! This module defines principals (users, groups, services, system) and their
//! authority domains with pattern matching.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// Unique identifier for a principal.
pub type PrincipalId = String;

/// A principal represents an entity that can have authority and make decisions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Principal {
    /// Unique identifier for this principal.
    pub id: PrincipalId,
    /// Human-readable name.
    pub name: String,
    /// Type of principal (user, group, service, system).
    pub principal_type: PrincipalType,
    /// Priority level - higher values mean more authority (CEO=1000, postdoc=100).
    pub priority: u32,
    /// Domains where this principal has direct authority.
    pub authority_domains: Vec<DomainPattern>,
    /// Principals that have delegated authority to this principal.
    pub delegates_from: Vec<PrincipalId>,
    /// Additional metadata.
    pub metadata: PrincipalMetadata,
}

impl Principal {
    /// Creates a new user principal.
    pub fn new_user(id: impl Into<String>, name: impl Into<String>, priority: u32) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            principal_type: PrincipalType::User,
            priority,
            authority_domains: Vec::new(),
            delegates_from: Vec::new(),
            metadata: PrincipalMetadata::new(),
        }
    }

    /// Creates a new group principal.
    pub fn new_group(id: impl Into<String>, name: impl Into<String>, priority: u32) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            principal_type: PrincipalType::Group,
            priority,
            authority_domains: Vec::new(),
            delegates_from: Vec::new(),
            metadata: PrincipalMetadata::new(),
        }
    }

    /// Creates a new service principal.
    pub fn new_service(id: impl Into<String>, name: impl Into<String>, priority: u32) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            principal_type: PrincipalType::Service,
            priority,
            authority_domains: Vec::new(),
            delegates_from: Vec::new(),
            metadata: PrincipalMetadata::new(),
        }
    }

    /// Creates the system principal with maximum authority.
    pub fn system() -> Self {
        Self {
            id: "system".to_string(),
            name: "System".to_string(),
            principal_type: PrincipalType::System,
            priority: u32::MAX,
            authority_domains: vec![DomainPattern::Global],
            delegates_from: Vec::new(),
            metadata: PrincipalMetadata::new(),
        }
    }

    /// Adds a domain to this principal's authority.
    pub fn with_domain(mut self, domain: DomainPattern) -> Self {
        self.authority_domains.push(domain);
        self
    }

    /// Adds multiple domains to this principal's authority.
    pub fn with_domains(mut self, domains: Vec<DomainPattern>) -> Self {
        self.authority_domains.extend(domains);
        self
    }

    /// Sets the email for this principal.
    pub fn with_email(mut self, email: impl Into<String>) -> Self {
        self.metadata.email = Some(email.into());
        self
    }

    /// Adds a metadata attribute.
    pub fn with_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.attributes.insert(key.into(), value.into());
        self
    }

    /// Sets who created this principal.
    pub fn with_created_by(mut self, creator: impl Into<PrincipalId>) -> Self {
        self.metadata.created_by = Some(creator.into());
        self
    }

    /// Checks if this principal has direct authority over a domain.
    pub fn has_direct_authority(&self, domain: &str) -> bool {
        self.authority_domains.iter().any(|p| p.matches(domain))
    }
}

impl fmt::Display for Principal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{} (priority={}, type={:?})",
            self.id, self.name, self.priority, self.principal_type
        )
    }
}

/// Type of principal entity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PrincipalType {
    /// A human user.
    User,
    /// A group of users or services (e.g., "physics-dept").
    Group,
    /// An automated service or system component.
    Service,
    /// The system itself - has highest authority.
    System,
}

impl fmt::Display for PrincipalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PrincipalType::User => write!(f, "User"),
            PrincipalType::Group => write!(f, "Group"),
            PrincipalType::Service => write!(f, "Service"),
            PrincipalType::System => write!(f, "System"),
        }
    }
}

/// Additional metadata for a principal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrincipalMetadata {
    /// When this principal was created.
    pub created_at: DateTime<Utc>,
    /// Who created this principal.
    pub created_by: Option<PrincipalId>,
    /// Email address (for users).
    pub email: Option<String>,
    /// Extensible key-value attributes.
    pub attributes: HashMap<String, String>,
}

impl PrincipalMetadata {
    /// Creates new metadata with current timestamp.
    pub fn new() -> Self {
        Self {
            created_at: Utc::now(),
            created_by: None,
            email: None,
            attributes: HashMap::new(),
        }
    }
}

impl Default for PrincipalMetadata {
    fn default() -> Self {
        Self::new()
    }
}

/// Domain patterns for specifying authority scope.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DomainPattern {
    /// Global authority - matches everything ("*").
    Global,
    /// Exact match (e.g., "dept:physics").
    Exact(String),
    /// Prefix match (e.g., "project:collider-*" matches "project:collider-a", "project:collider-b").
    Prefix(String),
    /// Regex pattern for complex matching.
    Regex(String),
    /// Union of multiple patterns - matches if any pattern matches.
    Union(Vec<DomainPattern>),
}

impl DomainPattern {
    /// Creates a global pattern that matches everything.
    pub fn global() -> Self {
        DomainPattern::Global
    }

    /// Creates an exact match pattern.
    pub fn exact(domain: impl Into<String>) -> Self {
        DomainPattern::Exact(domain.into())
    }

    /// Creates a prefix pattern (ending with '*').
    pub fn prefix(prefix: impl Into<String>) -> Self {
        let p = prefix.into();
        // Remove trailing '*' if present for storage
        let clean = p.trim_end_matches('*').to_string();
        DomainPattern::Prefix(clean)
    }

    /// Creates a regex pattern.
    pub fn regex(pattern: impl Into<String>) -> Self {
        DomainPattern::Regex(pattern.into())
    }

    /// Creates a union of patterns.
    pub fn union(patterns: Vec<DomainPattern>) -> Self {
        DomainPattern::Union(patterns)
    }

    /// Checks if this pattern matches a domain string.
    pub fn matches(&self, domain: &str) -> bool {
        match self {
            DomainPattern::Global => true,
            DomainPattern::Exact(exact) => domain == exact,
            DomainPattern::Prefix(prefix) => domain.starts_with(prefix),
            DomainPattern::Regex(pattern) => {
                // Compile and match regex
                match regex_lite::Regex::new(pattern) {
                    Ok(re) => re.is_match(domain),
                    Err(_) => false, // Invalid regex doesn't match
                }
            }
            DomainPattern::Union(patterns) => patterns.iter().any(|p| p.matches(domain)),
        }
    }

    /// Checks if this pattern is a subset of another pattern.
    /// Returns true if every domain matching `self` would also match `other`.
    pub fn is_subset_of(&self, other: &DomainPattern) -> bool {
        match (self, other) {
            // Global is superset of everything
            (_, DomainPattern::Global) => true,
            // Nothing is a subset of something smaller than global unless it's the same
            (DomainPattern::Global, _) => false,

            // Exact patterns
            (DomainPattern::Exact(a), DomainPattern::Exact(b)) => a == b,
            (DomainPattern::Exact(a), DomainPattern::Prefix(prefix)) => a.starts_with(prefix),

            // Prefix patterns
            (DomainPattern::Prefix(a), DomainPattern::Prefix(b)) => a.starts_with(b),
            (DomainPattern::Prefix(_), DomainPattern::Exact(_)) => false,

            // Regex comparisons are conservative - assume not subset
            (DomainPattern::Regex(_), _) => false,
            (_, DomainPattern::Regex(_)) => false,

            // Union patterns
            (DomainPattern::Union(patterns), other) => {
                patterns.iter().all(|p| p.is_subset_of(other))
            }
            (pattern, DomainPattern::Union(others)) => {
                others.iter().any(|o| pattern.is_subset_of(o))
            }
        }
    }

    /// Returns a human-readable description of this pattern.
    pub fn description(&self) -> String {
        match self {
            DomainPattern::Global => "*".to_string(),
            DomainPattern::Exact(s) => s.clone(),
            DomainPattern::Prefix(p) => format!("{}*", p),
            DomainPattern::Regex(r) => format!("/{}/", r),
            DomainPattern::Union(patterns) => {
                let parts: Vec<_> = patterns.iter().map(|p| p.description()).collect();
                format!("({})", parts.join(" | "))
            }
        }
    }
}

impl fmt::Display for DomainPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.description())
    }
}

// Include regex-lite for lightweight regex support without pulling in full regex crate
// Note: This uses a simple pattern matching approach for common cases

mod regex_lite {
    /// A lightweight regex implementation for simple patterns.
    pub struct Regex {
        pattern: String,
    }

    impl Regex {
        pub fn new(pattern: &str) -> Result<Self, &'static str> {
            // Basic validation - ensure it's not empty
            if pattern.is_empty() {
                return Err("empty pattern");
            }
            Ok(Self {
                pattern: pattern.to_string(),
            })
        }

        pub fn is_match(&self, text: &str) -> bool {
            // Simple pattern matching implementation
            // Supports: . (any char), * (zero or more of previous), ^ (start), $ (end)
            self.match_pattern(&self.pattern, text)
        }

        fn match_pattern(&self, pattern: &str, text: &str) -> bool {
            let pattern_chars: Vec<char> = pattern.chars().collect();
            let text_chars: Vec<char> = text.chars().collect();

            // Handle anchors
            let (pat, anchored_start) = if pattern.starts_with('^') {
                (&pattern_chars[1..], true)
            } else {
                (&pattern_chars[..], false)
            };

            let (pat, anchored_end) = if !pat.is_empty() && pat[pat.len() - 1] == '$' {
                (&pat[..pat.len() - 1], true)
            } else {
                (pat, false)
            };

            if anchored_start {
                self.match_here(pat, &text_chars, anchored_end)
            } else {
                // Try matching at each position
                for i in 0..=text_chars.len() {
                    if self.match_here(pat, &text_chars[i..], anchored_end) {
                        return true;
                    }
                }
                false
            }
        }

        fn match_here(&self, pattern: &[char], text: &[char], anchored_end: bool) -> bool {
            if pattern.is_empty() {
                return !anchored_end || text.is_empty();
            }

            // Handle * quantifier
            if pattern.len() >= 2 && pattern[1] == '*' {
                return self.match_star(pattern[0], &pattern[2..], text, anchored_end);
            }

            // Handle . (any character)
            if !text.is_empty() && (pattern[0] == '.' || pattern[0] == text[0]) {
                return self.match_here(&pattern[1..], &text[1..], anchored_end);
            }

            false
        }

        fn match_star(&self, c: char, pattern: &[char], text: &[char], anchored_end: bool) -> bool {
            // Try matching zero or more of character c
            let mut i = 0;
            loop {
                if self.match_here(pattern, &text[i..], anchored_end) {
                    return true;
                }
                if i >= text.len() || (c != '.' && text[i] != c) {
                    break;
                }
                i += 1;
            }
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_principal_creation() {
        let user = Principal::new_user("alice", "Alice Smith", 100)
            .with_email("alice@example.com")
            .with_domain(DomainPattern::exact("project:alpha"));

        assert_eq!(user.id, "alice");
        assert_eq!(user.name, "Alice Smith");
        assert_eq!(user.priority, 100);
        assert_eq!(user.principal_type, PrincipalType::User);
        assert!(user.has_direct_authority("project:alpha"));
        assert!(!user.has_direct_authority("project:beta"));
    }

    #[test]
    fn test_system_principal() {
        let system = Principal::system();
        assert_eq!(system.id, "system");
        assert_eq!(system.priority, u32::MAX);
        assert_eq!(system.principal_type, PrincipalType::System);
        assert!(system.has_direct_authority("anything"));
        assert!(system.has_direct_authority("project:whatever"));
    }

    #[test]
    fn test_domain_pattern_global() {
        let pattern = DomainPattern::global();
        assert!(pattern.matches("anything"));
        assert!(pattern.matches("dept:physics"));
        assert!(pattern.matches(""));
    }

    #[test]
    fn test_domain_pattern_exact() {
        let pattern = DomainPattern::exact("dept:physics");
        assert!(pattern.matches("dept:physics"));
        assert!(!pattern.matches("dept:physics:lab1"));
        assert!(!pattern.matches("dept:chemistry"));
    }

    #[test]
    fn test_domain_pattern_prefix() {
        let pattern = DomainPattern::prefix("project:collider-");
        assert!(pattern.matches("project:collider-a"));
        assert!(pattern.matches("project:collider-test-123"));
        assert!(!pattern.matches("project:accelerator-a"));

        // Test with trailing asterisk
        let pattern2 = DomainPattern::prefix("dept:*");
        assert!(pattern2.matches("dept:physics"));
        assert!(pattern2.matches("dept:chemistry"));
    }

    #[test]
    fn test_domain_pattern_regex() {
        let pattern = DomainPattern::regex("^project:.*-test$");
        assert!(pattern.matches("project:alpha-test"));
        assert!(pattern.matches("project:beta-test"));
        assert!(!pattern.matches("project:alpha-prod"));
        assert!(!pattern.matches("project:test")); // doesn't have the hyphen
    }

    #[test]
    fn test_domain_pattern_union() {
        let pattern = DomainPattern::union(vec![
            DomainPattern::exact("dept:physics"),
            DomainPattern::prefix("project:collider-"),
        ]);
        assert!(pattern.matches("dept:physics"));
        assert!(pattern.matches("project:collider-a"));
        assert!(!pattern.matches("dept:chemistry"));
    }

    #[test]
    fn test_pattern_is_subset_of() {
        // Everything is subset of global
        assert!(DomainPattern::exact("foo").is_subset_of(&DomainPattern::global()));
        assert!(DomainPattern::prefix("foo").is_subset_of(&DomainPattern::global()));

        // Global is not subset of non-global
        assert!(!DomainPattern::global().is_subset_of(&DomainPattern::exact("foo")));

        // Exact patterns
        assert!(DomainPattern::exact("foo").is_subset_of(&DomainPattern::exact("foo")));
        assert!(!DomainPattern::exact("foo").is_subset_of(&DomainPattern::exact("bar")));

        // Exact is subset of prefix if it starts with prefix
        assert!(DomainPattern::exact("project:collider-a")
            .is_subset_of(&DomainPattern::prefix("project:collider-")));
        assert!(!DomainPattern::exact("project:other")
            .is_subset_of(&DomainPattern::prefix("project:collider-")));

        // Prefix subset of prefix
        assert!(DomainPattern::prefix("project:collider-test-")
            .is_subset_of(&DomainPattern::prefix("project:collider-")));
        assert!(!DomainPattern::prefix("project:")
            .is_subset_of(&DomainPattern::prefix("project:collider-")));
    }

    #[test]
    fn test_principal_display() {
        let user = Principal::new_user("alice", "Alice Smith", 100);
        let display = format!("{}", user);
        assert!(display.contains("alice"));
        assert!(display.contains("Alice Smith"));
        assert!(display.contains("100"));
    }

    #[test]
    fn test_domain_pattern_display() {
        assert_eq!(DomainPattern::global().to_string(), "*");
        assert_eq!(DomainPattern::exact("foo").to_string(), "foo");
        assert_eq!(DomainPattern::prefix("foo").to_string(), "foo*");
        assert_eq!(DomainPattern::regex("^foo$").to_string(), "/^foo$/");
    }
}
