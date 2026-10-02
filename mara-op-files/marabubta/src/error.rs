// Marabunta - Licensed under the MIT License.
//! Marabunta Compute Error System
//!
//! This module provides a comprehensive error handling system for Marabunta Compute,
//! with structured errors that include:
//!
//! - Unique error codes (E001, E002, etc.) for documentation and troubleshooting
//! - Contextual information about what went wrong
//! - Suggestions for how to resolve the error
//! - Proper error chaining via `source()`
//!
//! # Error Code Index
//!
//! ## Configuration Errors (E001-E099)
//!
//! | Code | Error | Description |
//! |------|-------|-------------|
//! | E001 | `ConfigNotFound` | Configuration file not found at any expected location |
//! | E002 | `ConfigParseError` | Failed to parse configuration file (TOML syntax error) |
//! | E003 | `ConfigValidationError` | Configuration value failed validation |
//! | E004 | `ConfigFileReadError` | Could not read configuration file from disk |
//! | E005 | `ConfigFileWriteError` | Could not write configuration file to disk |
//! | E006 | `ConfigEnvVarError` | Invalid environment variable value |
//! | E007 | `ConfigMissingField` | Required configuration field is missing |
//!
//! ## Network Errors (E100-E199)
//!
//! | Code | Error | Description |
//! |------|-------|-------------|
//! | E100 | `ConnectionFailed` | Failed to establish connection to remote host |
//! | E101 | `ConnectionTimeout` | Connection attempt timed out |
//! | E102 | `RequestTimeout` | Request timed out waiting for response |
//! | E103 | `ConnectionRefused` | Remote host refused connection |
//! | E104 | `DnsResolutionFailed` | Could not resolve hostname |
//! | E105 | `TlsError` | TLS/SSL handshake or certificate error |
//! | E106 | `ProtocolError` | Protocol-level communication error |
//! | E107 | `ConnectionClosed` | Connection was closed unexpectedly |
//!
//! ## Scheduling Errors (E200-E299)
//!
//! | Code | Error | Description |
//! |------|-------|-------------|
//! | E200 | `NoNodesAvailable` | No compute nodes available to run the task |
//! | E201 | `InsufficientResources` | Not enough resources to satisfy job requirements |
//! | E202 | `NodeSelectionFailed` | Could not find nodes matching placement constraints |
//! | E203 | `TaskQueueFull` | Task queue is at capacity |
//! | E204 | `PreemptionFailed` | Failed to preempt lower priority tasks |
//! | E205 | `SchedulingTimeout` | Scheduling decision timed out |
//! | E206 | `DependencyNotMet` | Task dependencies have not completed |
//! | E207 | `CircularDependency` | Circular dependency detected in task graph |
//!
//! ## Quota Errors (E300-E399)
//!
//! | Code | Error | Description |
//! |------|-------|-------------|
//! | E300 | `QuotaExceeded` | Resource quota has been exceeded |
//! | E301 | `InsufficientBalance` | Insufficient token balance for operation |
//! | E302 | `AllocationExpired` | Resource allocation has expired |
//! | E303 | `ReservationNotFound` | Resource reservation not found |
//! | E304 | `AccountNotFound` | Quota account does not exist |
//! | E305 | `TransferNotAllowed` | Resource transfer not permitted |
//!
//! ## Policy Errors (E400-E499)
//!
//! | Code | Error | Description |
//! |------|-------|-------------|
//! | E400 | `PolicyViolation` | Action violates an active policy |
//! | E401 | `PolicyNotFound` | Referenced policy does not exist |
//! | E402 | `PolicyConflict` | Conflicting policies detected |
//! | E403 | `PolicyInvalid` | Policy definition is invalid |
//! | E404 | `InsufficientAuthority` | Principal lacks authority for action |
//! | E405 | `OverrideNotAllowed` | Policy override not permitted |
//!
//! ## Validation Errors (E500-E599)
//!
//! | Code | Error | Description |
//! |------|-------|-------------|
//! | E500 | `InvalidInput` | Input validation failed |
//! | E501 | `InvalidJobSpec` | Job specification is invalid |
//! | E502 | `InvalidTaskPayload` | Task payload is malformed |
//! | E503 | `InvalidResourceRequest` | Resource request is invalid |
//! | E504 | `InvalidIdentifier` | ID format is invalid |
//! | E505 | `InvalidTimeRange` | Time range specification is invalid |
//!
//! ## Internal Errors (E900-E999)
//!
//! | Code | Error | Description |
//! |------|-------|-------------|
//! | E900 | `InternalError` | Unexpected internal error |
//! | E901 | `StorageError` | Database or storage operation failed |
//! | E902 | `SerializationError` | Data serialization/deserialization failed |
//! | E903 | `StateCorruption` | Detected corrupted internal state |
//!
//! # Example Usage
//!
//! ```rust
//! use marabunta_compute::error::{MarabuntaError, ErrorContext};
//!
//! fn load_config(path: &str) -> Result<Config, MarabuntaError> {
//!     std::fs::read_to_string(path)
//!         .map_err(|e| MarabuntaError::config_file_read(path, e))?;
//!     // ...
//! }
//!
//! // Errors display with full context:
//! // [E004] Configuration file read error
//! //   File: /etc/marabunta/config.toml
//! //   Cause: Permission denied (os error 13)
//! //   Suggestion: Check file permissions and ensure the file is readable
//! ```

use std::collections::HashMap;
use std::fmt;

use thiserror::Error;

// ─────────────────────────────────────────────────────────────────────────────
// ERROR CODES
// ─────────────────────────────────────────────────────────────────────────────

/// Error code for documentation and troubleshooting
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ErrorCode(&'static str);

impl ErrorCode {
    // Configuration Errors (E001-E099)
    pub const CONFIG_NOT_FOUND: Self = Self("E001");
    pub const CONFIG_PARSE_ERROR: Self = Self("E002");
    pub const CONFIG_VALIDATION_ERROR: Self = Self("E003");
    pub const CONFIG_FILE_READ_ERROR: Self = Self("E004");
    pub const CONFIG_FILE_WRITE_ERROR: Self = Self("E005");
    pub const CONFIG_ENV_VAR_ERROR: Self = Self("E006");
    pub const CONFIG_MISSING_FIELD: Self = Self("E007");

    // Network Errors (E100-E199)
    pub const CONNECTION_FAILED: Self = Self("E100");
    pub const CONNECTION_TIMEOUT: Self = Self("E101");
    pub const REQUEST_TIMEOUT: Self = Self("E102");
    pub const CONNECTION_REFUSED: Self = Self("E103");
    pub const DNS_RESOLUTION_FAILED: Self = Self("E104");
    pub const TLS_ERROR: Self = Self("E105");
    pub const PROTOCOL_ERROR: Self = Self("E106");
    pub const CONNECTION_CLOSED: Self = Self("E107");

    // Scheduling Errors (E200-E299)
    pub const NO_NODES_AVAILABLE: Self = Self("E200");
    pub const INSUFFICIENT_RESOURCES: Self = Self("E201");
    pub const NODE_SELECTION_FAILED: Self = Self("E202");
    pub const TASK_QUEUE_FULL: Self = Self("E203");
    pub const PREEMPTION_FAILED: Self = Self("E204");
    pub const SCHEDULING_TIMEOUT: Self = Self("E205");
    pub const DEPENDENCY_NOT_MET: Self = Self("E206");
    pub const CIRCULAR_DEPENDENCY: Self = Self("E207");

    // Quota Errors (E300-E399)
    pub const QUOTA_EXCEEDED: Self = Self("E300");
    pub const INSUFFICIENT_BALANCE: Self = Self("E301");
    pub const ALLOCATION_EXPIRED: Self = Self("E302");
    pub const RESERVATION_NOT_FOUND: Self = Self("E303");
    pub const ACCOUNT_NOT_FOUND: Self = Self("E304");
    pub const TRANSFER_NOT_ALLOWED: Self = Self("E305");

    // Policy Errors (E400-E499)
    pub const POLICY_VIOLATION: Self = Self("E400");
    pub const POLICY_NOT_FOUND: Self = Self("E401");
    pub const POLICY_CONFLICT: Self = Self("E402");
    pub const POLICY_INVALID: Self = Self("E403");
    pub const INSUFFICIENT_AUTHORITY: Self = Self("E404");
    pub const OVERRIDE_NOT_ALLOWED: Self = Self("E405");

    // Validation Errors (E500-E599)
    pub const INVALID_INPUT: Self = Self("E500");
    pub const INVALID_JOB_SPEC: Self = Self("E501");
    pub const INVALID_TASK_PAYLOAD: Self = Self("E502");
    pub const INVALID_RESOURCE_REQUEST: Self = Self("E503");
    pub const INVALID_IDENTIFIER: Self = Self("E504");
    pub const INVALID_TIME_RANGE: Self = Self("E505");

    // Internal Errors (E900-E999)
    pub const INTERNAL_ERROR: Self = Self("E900");
    pub const STORAGE_ERROR: Self = Self("E901");
    pub const SERIALIZATION_ERROR: Self = Self("E902");
    pub const STATE_CORRUPTION: Self = Self("E903");

    /// Get the error code string
    pub fn as_str(&self) -> &'static str {
        self.0
    }

    /// Get documentation URL for this error code
    pub fn docs_url(&self) -> String {
        format!("https://marabunta-compute.io/docs/errors/{}", self.0)
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ERROR CONTEXT
// ─────────────────────────────────────────────────────────────────────────────

/// Contextual information for an error
#[derive(Debug, Clone, Default)]
pub struct ErrorContext {
    /// Key-value pairs providing context
    pub fields: HashMap<String, String>,
    /// Suggested actions to resolve the error
    pub suggestions: Vec<String>,
    /// Retry information if applicable
    pub retry_after: Option<std::time::Duration>,
    /// Whether this error is transient and may succeed on retry
    pub is_transient: bool,
}

impl ErrorContext {
    /// Create a new empty context
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a context field
    pub fn with_field(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.fields.insert(key.into(), value.into());
        self
    }

    /// Add a suggestion for resolving the error
    pub fn with_suggestion(mut self, suggestion: impl Into<String>) -> Self {
        self.suggestions.push(suggestion.into());
        self
    }

    /// Mark this error as transient (may succeed on retry)
    pub fn transient(mut self) -> Self {
        self.is_transient = true;
        self
    }

    /// Set retry-after duration
    pub fn with_retry_after(mut self, duration: std::time::Duration) -> Self {
        self.retry_after = Some(duration);
        self.is_transient = true;
        self
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN ERROR TYPE
// ─────────────────────────────────────────────────────────────────────────────

/// Main error type for Marabunta Compute
///
/// All errors include:
/// - A unique error code for documentation lookup
/// - A human-readable message
/// - Contextual information (file paths, field names, etc.)
/// - Suggestions for resolution
/// - Optional cause for error chaining
#[derive(Error, Debug)]
pub struct MarabuntaError {
    /// Unique error code
    pub code: ErrorCode,
    /// Human-readable error message
    pub message: String,
    /// Additional context
    pub context: ErrorContext,
    /// Underlying cause (if any)
    #[source]
    pub cause: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl fmt::Display for MarabuntaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)?;

        // Add context fields
        for (key, value) in &self.context.fields {
            write!(f, "\n  {}: {}", key, value)?;
        }

        // Add cause if present
        if let Some(ref cause) = self.cause {
            write!(f, "\n  Cause: {}", cause)?;
        }

        // Add suggestions
        for suggestion in &self.context.suggestions {
            write!(f, "\n  Suggestion: {}", suggestion)?;
        }

        Ok(())
    }
}

impl MarabuntaError {
    /// Create a new error with code and message
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            context: ErrorContext::new(),
            cause: None,
        }
    }

    /// Add context to the error
    pub fn with_context(mut self, context: ErrorContext) -> Self {
        self.context = context;
        self
    }

    /// Add a context field
    pub fn with_field(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.context.fields.insert(key.into(), value.into());
        self
    }

    /// Add a suggestion
    pub fn with_suggestion(mut self, suggestion: impl Into<String>) -> Self {
        self.context.suggestions.push(suggestion.into());
        self
    }

    /// Set the underlying cause
    pub fn with_cause<E: std::error::Error + Send + Sync + 'static>(mut self, cause: E) -> Self {
        self.cause = Some(Box::new(cause));
        self
    }

    /// Mark as transient (may succeed on retry)
    pub fn transient(mut self) -> Self {
        self.context.is_transient = true;
        self
    }

    /// Check if this error is transient
    pub fn is_transient(&self) -> bool {
        self.context.is_transient
    }

    /// Get the error code
    pub fn code(&self) -> ErrorCode {
        self.code
    }

    /// Get suggestions for resolving this error
    pub fn suggestions(&self) -> &[String] {
        &self.context.suggestions
    }

    // ─────────────────────────────────────────────────────────────────────────
    // CONFIGURATION ERRORS
    // ─────────────────────────────────────────────────────────────────────────

    /// Configuration file not found
    pub fn config_not_found(searched_paths: &[String]) -> Self {
        Self::new(ErrorCode::CONFIG_NOT_FOUND, "No configuration file found")
            .with_field("Searched locations", searched_paths.join(", "))
            .with_suggestion("Create a configuration file at one of the searched locations")
            .with_suggestion("Run 'marabunta config init' to generate a default configuration")
            .with_suggestion("Set MARABUNTA_CONFIG_PATH environment variable to specify config location")
    }

    /// Configuration parse error
    pub fn config_parse_error(
        path: impl Into<String>,
        line: Option<usize>,
        message: impl Into<String>,
    ) -> Self {
        let mut err = Self::new(
            ErrorCode::CONFIG_PARSE_ERROR,
            "Failed to parse configuration file",
        )
        .with_field("File", path.into())
        .with_field("Error", message.into());

        if let Some(line_num) = line {
            err = err.with_field("Line", line_num.to_string());
        }

        err.with_suggestion("Check the configuration file for syntax errors")
            .with_suggestion("Validate TOML syntax at https://www.toml-lint.com/")
    }

    /// Configuration validation error
    pub fn config_validation_error(
        field: impl Into<String>,
        value: impl Into<String>,
        expected: impl Into<String>,
    ) -> Self {
        Self::new(
            ErrorCode::CONFIG_VALIDATION_ERROR,
            "Configuration validation failed",
        )
        .with_field("Field", field.into())
        .with_field("Value", value.into())
        .with_field("Expected", expected.into())
        .with_suggestion("Review the configuration documentation for valid values")
    }

    /// Configuration file read error
    pub fn config_file_read<E: std::error::Error + Send + Sync + 'static>(
        path: impl Into<String>,
        cause: E,
    ) -> Self {
        Self::new(
            ErrorCode::CONFIG_FILE_READ_ERROR,
            "Failed to read configuration file",
        )
        .with_field("File", path.into())
        .with_cause(cause)
        .with_suggestion("Check that the file exists and is readable")
        .with_suggestion("Verify file permissions allow read access")
    }

    /// Configuration file write error
    pub fn config_file_write<E: std::error::Error + Send + Sync + 'static>(
        path: impl Into<String>,
        cause: E,
    ) -> Self {
        Self::new(
            ErrorCode::CONFIG_FILE_WRITE_ERROR,
            "Failed to write configuration file",
        )
        .with_field("File", path.into())
        .with_cause(cause)
        .with_suggestion("Check that the directory exists")
        .with_suggestion("Verify file permissions allow write access")
    }

    /// Environment variable error
    pub fn config_env_var_error(
        var_name: impl Into<String>,
        value: impl Into<String>,
        expected: impl Into<String>,
    ) -> Self {
        Self::new(
            ErrorCode::CONFIG_ENV_VAR_ERROR,
            "Invalid environment variable value",
        )
        .with_field("Variable", var_name.into())
        .with_field("Value", value.into())
        .with_field("Expected", expected.into())
        .with_suggestion("Set the environment variable to a valid value")
    }

    /// Missing required field
    pub fn config_missing_field(field: impl Into<String>) -> Self {
        Self::new(
            ErrorCode::CONFIG_MISSING_FIELD,
            "Required configuration field is missing",
        )
        .with_field("Field", field.into())
        .with_suggestion("Add the missing field to your configuration file")
    }

    // ─────────────────────────────────────────────────────────────────────────
    // NETWORK ERRORS
    // ─────────────────────────────────────────────────────────────────────────

    /// Connection failed
    pub fn connection_failed(
        host: impl Into<String>,
        port: u16,
        reason: impl Into<String>,
    ) -> Self {
        Self::new(
            ErrorCode::CONNECTION_FAILED,
            "Failed to connect to remote host",
        )
        .with_field("Host", host.into())
        .with_field("Port", port.to_string())
        .with_field("Reason", reason.into())
        .transient()
        .with_suggestion("Check that the remote host is reachable")
        .with_suggestion("Verify network connectivity and firewall rules")
        .with_suggestion("Ensure the service is running on the specified port")
    }

    /// Connection timeout
    pub fn connection_timeout(host: impl Into<String>, timeout_secs: u64) -> Self {
        Self::new(
            ErrorCode::CONNECTION_TIMEOUT,
            "Connection attempt timed out",
        )
        .with_field("Host", host.into())
        .with_field("Timeout", format!("{}s", timeout_secs))
        .transient()
        .with_context(ErrorContext::new().with_retry_after(std::time::Duration::from_secs(5)))
        .with_suggestion("The remote host may be slow or overloaded - retry after a delay")
        .with_suggestion("Check network latency to the host")
        .with_suggestion("Consider increasing the connection timeout")
    }

    /// Request timeout
    pub fn request_timeout(operation: impl Into<String>, timeout_secs: u64) -> Self {
        Self::new(ErrorCode::REQUEST_TIMEOUT, "Request timed out")
            .with_field("Operation", operation.into())
            .with_field("Timeout", format!("{}s", timeout_secs))
            .transient()
            .with_suggestion(
                "The operation may be taking longer than expected - retry with a longer timeout",
            )
            .with_suggestion("Check if the service is overloaded")
    }

    /// Connection refused
    pub fn connection_refused(host: impl Into<String>, port: u16) -> Self {
        Self::new(
            ErrorCode::CONNECTION_REFUSED,
            "Connection refused by remote host",
        )
        .with_field("Host", host.into())
        .with_field("Port", port.to_string())
        .with_suggestion("Verify the service is running on the target host and port")
        .with_suggestion("Check firewall rules are not blocking the connection")
        .with_suggestion("Ensure you're connecting to the correct address")
    }

    /// DNS resolution failed
    pub fn dns_resolution_failed(hostname: impl Into<String>) -> Self {
        Self::new(
            ErrorCode::DNS_RESOLUTION_FAILED,
            "Failed to resolve hostname",
        )
        .with_field("Hostname", hostname.into())
        .transient()
        .with_suggestion("Check that the hostname is spelled correctly")
        .with_suggestion("Verify DNS configuration and network connectivity")
        .with_suggestion("Try using an IP address instead")
    }

    /// TLS error
    pub fn tls_error(host: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::new(ErrorCode::TLS_ERROR, "TLS/SSL error")
            .with_field("Host", host.into())
            .with_field("Reason", reason.into())
            .with_suggestion("Verify the server's TLS certificate is valid")
            .with_suggestion("Check that TLS settings match between client and server")
            .with_suggestion("Ensure the CA certificate is properly configured")
    }

    // ─────────────────────────────────────────────────────────────────────────
    // SCHEDULING ERRORS
    // ─────────────────────────────────────────────────────────────────────────

    /// No nodes available
    pub fn no_nodes_available(job_id: impl Into<String>) -> Self {
        Self::new(ErrorCode::NO_NODES_AVAILABLE, "No compute nodes available")
            .with_field("Job", job_id.into())
            .transient()
            .with_suggestion("Wait for nodes to become available")
            .with_suggestion("Check cluster health with 'marabunta nodes'")
            .with_suggestion("Relax placement constraints if possible")
    }

    /// Insufficient resources
    pub fn insufficient_resources(
        requested: impl Into<String>,
        available: impl Into<String>,
        resource_type: impl Into<String>,
    ) -> Self {
        Self::new(
            ErrorCode::INSUFFICIENT_RESOURCES,
            "Insufficient resources available",
        )
        .with_field("Resource", resource_type.into())
        .with_field("Requested", requested.into())
        .with_field("Available", available.into())
        .transient()
        .with_suggestion("Reduce resource requirements for the job")
        .with_suggestion("Wait for resources to become available")
        .with_suggestion("Consider splitting into smaller jobs")
    }

    /// Node selection failed
    pub fn node_selection_failed(
        job_id: impl Into<String>,
        reason: impl Into<String>,
        constraints: Vec<String>,
    ) -> Self {
        let mut err = Self::new(
            ErrorCode::NODE_SELECTION_FAILED,
            "Could not find nodes matching constraints",
        )
        .with_field("Job", job_id.into())
        .with_field("Reason", reason.into());

        if !constraints.is_empty() {
            err = err.with_field("Constraints", constraints.join(", "));
        }

        err.with_suggestion("Review and relax placement constraints")
            .with_suggestion("Check that nodes matching your requirements exist")
    }

    /// Task queue full
    pub fn task_queue_full(queue_size: usize, max_size: usize) -> Self {
        Self::new(ErrorCode::TASK_QUEUE_FULL, "Task queue is at capacity")
            .with_field("Current size", queue_size.to_string())
            .with_field("Maximum size", max_size.to_string())
            .transient()
            .with_context(ErrorContext::new().with_retry_after(std::time::Duration::from_secs(30)))
            .with_suggestion("Wait for existing tasks to complete")
            .with_suggestion("Reduce job submission rate")
    }

    /// Circular dependency detected
    pub fn circular_dependency(task_id: impl Into<String>, cycle_path: Vec<String>) -> Self {
        Self::new(
            ErrorCode::CIRCULAR_DEPENDENCY,
            "Circular dependency detected in task graph",
        )
        .with_field("Task", task_id.into())
        .with_field("Cycle", cycle_path.join(" -> "))
        .with_suggestion("Review task dependencies and remove the cycle")
        .with_suggestion("Use 'marabunta job validate' to check dependency graph")
    }

    // ─────────────────────────────────────────────────────────────────────────
    // QUOTA ERRORS
    // ─────────────────────────────────────────────────────────────────────────

    /// Quota exceeded
    pub fn quota_exceeded(
        resource: impl Into<String>,
        requested: f64,
        available: f64,
        quota_id: impl Into<String>,
    ) -> Self {
        Self::new(ErrorCode::QUOTA_EXCEEDED, "Resource quota exceeded")
            .with_field("Resource", resource.into())
            .with_field("Requested", format!("{:.2}", requested))
            .with_field("Available", format!("{:.2}", available))
            .with_field("Quota", quota_id.into())
            .with_suggestion("Wait for quota to replenish")
            .with_suggestion("Request a quota increase from your administrator")
            .with_suggestion("Reduce resource usage in current operations")
    }

    /// Insufficient balance
    pub fn insufficient_balance(required: f64, available: f64, account: impl Into<String>) -> Self {
        Self::new(
            ErrorCode::INSUFFICIENT_BALANCE,
            "Insufficient token balance",
        )
        .with_field("Required", format!("{:.4}", required))
        .with_field("Available", format!("{:.4}", available))
        .with_field("Account", account.into())
        .with_suggestion("Earn tokens by contributing compute resources")
        .with_suggestion("Check 'marabunta tokens balance' for current balance")
        .with_suggestion("Use 'marabunta tokens claim' to claim pending rewards")
    }

    /// Account not found
    pub fn account_not_found(account_id: impl Into<String>) -> Self {
        Self::new(ErrorCode::ACCOUNT_NOT_FOUND, "Quota account not found")
            .with_field("Account", account_id.into())
            .with_suggestion("Verify the account ID is correct")
            .with_suggestion("Contact your administrator to create the account")
    }

    // ─────────────────────────────────────────────────────────────────────────
    // POLICY ERRORS
    // ─────────────────────────────────────────────────────────────────────────

    /// Policy violation
    pub fn policy_violation(
        policy_id: impl Into<String>,
        policy_name: impl Into<String>,
        action: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self::new(ErrorCode::POLICY_VIOLATION, "Action blocked by policy")
            .with_field("Policy ID", policy_id.into())
            .with_field("Policy name", policy_name.into())
            .with_field("Action", action.into())
            .with_field("Reason", reason.into())
            .with_suggestion("Review the policy requirements")
            .with_suggestion("Contact the policy owner for exceptions")
            .with_suggestion("Modify your request to comply with the policy")
    }

    /// Policy not found
    pub fn policy_not_found(policy_id: impl Into<String>) -> Self {
        Self::new(ErrorCode::POLICY_NOT_FOUND, "Policy not found")
            .with_field("Policy ID", policy_id.into())
            .with_suggestion("Verify the policy ID is correct")
            .with_suggestion("List available policies with 'marabunta policy list'")
    }

    /// Policy conflict
    pub fn policy_conflict(
        policy_a: impl Into<String>,
        policy_b: impl Into<String>,
        conflict_type: impl Into<String>,
    ) -> Self {
        Self::new(ErrorCode::POLICY_CONFLICT, "Conflicting policies detected")
            .with_field("Policy A", policy_a.into())
            .with_field("Policy B", policy_b.into())
            .with_field("Conflict type", conflict_type.into())
            .with_suggestion("Review policy priorities")
            .with_suggestion("Adjust one of the conflicting policies")
    }

    /// Insufficient authority
    pub fn insufficient_authority(
        principal: impl Into<String>,
        action: impl Into<String>,
        domain: impl Into<String>,
    ) -> Self {
        Self::new(
            ErrorCode::INSUFFICIENT_AUTHORITY,
            "Insufficient authority for action",
        )
        .with_field("Principal", principal.into())
        .with_field("Action", action.into())
        .with_field("Domain", domain.into())
        .with_suggestion("Request delegation from a principal with authority")
        .with_suggestion("Contact your administrator for access")
    }

    // ─────────────────────────────────────────────────────────────────────────
    // VALIDATION ERRORS
    // ─────────────────────────────────────────────────────────────────────────

    /// Invalid input
    pub fn invalid_input(
        field: impl Into<String>,
        value: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self::new(ErrorCode::INVALID_INPUT, "Invalid input value")
            .with_field("Field", field.into())
            .with_field("Value", value.into())
            .with_field("Reason", reason.into())
            .with_suggestion("Review the input requirements")
    }

    /// Invalid job specification
    pub fn invalid_job_spec(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::new(ErrorCode::INVALID_JOB_SPEC, "Invalid job specification")
            .with_field("Field", field.into())
            .with_field("Reason", reason.into())
            .with_suggestion("Review job specification format")
            .with_suggestion("See 'marabunta submit --help' for examples")
    }

    /// Invalid identifier
    pub fn invalid_identifier(id_type: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(ErrorCode::INVALID_IDENTIFIER, "Invalid identifier format")
            .with_field("Type", id_type.into())
            .with_field("Value", value.into())
            .with_suggestion("IDs must be valid UUIDs or alphanumeric strings")
    }

    // ─────────────────────────────────────────────────────────────────────────
    // INTERNAL ERRORS
    // ─────────────────────────────────────────────────────────────────────────

    /// Internal error
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::INTERNAL_ERROR, "Internal error")
            .with_field("Details", message.into())
            .with_suggestion("This is likely a bug - please report it")
            .with_suggestion("Include the full error message and context in your report")
    }

    /// Storage error
    pub fn storage<E: std::error::Error + Send + Sync + 'static>(
        operation: impl Into<String>,
        cause: E,
    ) -> Self {
        Self::new(ErrorCode::STORAGE_ERROR, "Storage operation failed")
            .with_field("Operation", operation.into())
            .with_cause(cause)
            .transient()
            .with_suggestion("Retry the operation")
            .with_suggestion("Check database connectivity and disk space")
    }

    /// Serialization error
    pub fn serialization<E: std::error::Error + Send + Sync + 'static>(
        data_type: impl Into<String>,
        cause: E,
    ) -> Self {
        Self::new(
            ErrorCode::SERIALIZATION_ERROR,
            "Data serialization/deserialization failed",
        )
        .with_field("Data type", data_type.into())
        .with_cause(cause)
        .with_suggestion("This may indicate data corruption or version mismatch")
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ERROR DISPLAY FOR CLI
// ─────────────────────────────────────────────────────────────────────────────

/// Formatted error display for CLI output with colors
pub struct CliErrorDisplay<'a> {
    error: &'a MarabuntaError,
    use_color: bool,
}

impl<'a> CliErrorDisplay<'a> {
    /// Create a new CLI error display
    pub fn new(error: &'a MarabuntaError, use_color: bool) -> Self {
        Self { error, use_color }
    }

    /// Format the error for terminal output
    pub fn format(&self) -> String {
        let mut output = String::new();

        // Error header
        if self.use_color {
            output.push_str("\x1b[1;31m"); // Bold red
        }
        output.push_str(&format!("error[{}]", self.error.code));
        if self.use_color {
            output.push_str("\x1b[0m"); // Reset
        }
        output.push_str(": ");

        // Error message
        if self.use_color {
            output.push_str("\x1b[1m"); // Bold
        }
        output.push_str(&self.error.message);
        if self.use_color {
            output.push_str("\x1b[0m"); // Reset
        }
        output.push('\n');

        // Context fields
        for (key, value) in &self.error.context.fields {
            if self.use_color {
                output.push_str("\x1b[36m"); // Cyan
            }
            output.push_str(&format!("  {}: ", key));
            if self.use_color {
                output.push_str("\x1b[0m"); // Reset
            }
            output.push_str(value);
            output.push('\n');
        }

        // Cause
        if let Some(ref cause) = self.error.cause {
            if self.use_color {
                output.push_str("\x1b[33m"); // Yellow
            }
            output.push_str("  Caused by: ");
            if self.use_color {
                output.push_str("\x1b[0m"); // Reset
            }
            output.push_str(&cause.to_string());
            output.push('\n');
        }

        // Suggestions
        if !self.error.context.suggestions.is_empty() {
            output.push('\n');
            if self.use_color {
                output.push_str("\x1b[1;32m"); // Bold green
            }
            output.push_str("help");
            if self.use_color {
                output.push_str("\x1b[0m"); // Reset
            }
            output.push_str(": ");

            for (i, suggestion) in self.error.context.suggestions.iter().enumerate() {
                if i > 0 {
                    output.push_str("\n      ");
                }
                output.push_str(suggestion);
            }
            output.push('\n');
        }

        // Retry information
        if let Some(retry_after) = self.error.context.retry_after {
            output.push('\n');
            if self.use_color {
                output.push_str("\x1b[33m"); // Yellow
            }
            output.push_str(&format!(
                "note: this error is transient, retry after {}s\n",
                retry_after.as_secs()
            ));
            if self.use_color {
                output.push_str("\x1b[0m"); // Reset
            }
        }

        // Documentation link
        if self.use_color {
            output.push_str("\x1b[2m"); // Dim
        }
        output.push_str(&format!(
            "For more info, see {}\n",
            self.error.code.docs_url()
        ));
        if self.use_color {
            output.push_str("\x1b[0m"); // Reset
        }

        output
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// RESULT TYPE
// ─────────────────────────────────────────────────────────────────────────────

/// Result type alias for Marabunta operations
pub type MarabuntaResult<T> = Result<T, MarabuntaError>;

// ─────────────────────────────────────────────────────────────────────────────
// CONVERSION TRAITS
// ─────────────────────────────────────────────────────────────────────────────

impl From<std::io::Error> for MarabuntaError {
    fn from(err: std::io::Error) -> Self {
        MarabuntaError::new(ErrorCode::INTERNAL_ERROR, "I/O error").with_cause(err)
    }
}

impl From<serde_json::Error> for MarabuntaError {
    fn from(err: serde_json::Error) -> Self {
        MarabuntaError::serialization("JSON", err)
    }
}

impl From<toml::de::Error> for MarabuntaError {
    fn from(err: toml::de::Error) -> Self {
        let message = err.message().to_string();
        let line = err.span().map(|s| s.start);
        MarabuntaError::config_parse_error("<config>", line, message)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TESTS
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_code_display() {
        assert_eq!(ErrorCode::CONFIG_NOT_FOUND.to_string(), "E001");
        assert_eq!(ErrorCode::CONNECTION_FAILED.to_string(), "E100");
        assert_eq!(ErrorCode::QUOTA_EXCEEDED.to_string(), "E300");
    }

    #[test]
    fn test_error_docs_url() {
        assert_eq!(
            ErrorCode::CONFIG_NOT_FOUND.docs_url(),
            "https://marabunta-compute.io/docs/errors/E001"
        );
    }

    #[test]
    fn test_config_not_found_error() {
        let err = MarabuntaError::config_not_found(&[
            "/etc/marabunta/config.toml".to_string(),
            "~/.config/marabunta/config.toml".to_string(),
        ]);

        assert_eq!(err.code, ErrorCode::CONFIG_NOT_FOUND);
        assert!(err.to_string().contains("E001"));
        assert!(err.to_string().contains("No configuration file found"));
        assert!(!err.context.suggestions.is_empty());
    }

    #[test]
    fn test_quota_exceeded_error() {
        let err = MarabuntaError::quota_exceeded("cpu_hours", 100.0, 50.0, "default-quota");

        assert_eq!(err.code, ErrorCode::QUOTA_EXCEEDED);
        assert!(err.context.fields.contains_key("Resource"));
        assert!(err.context.fields.contains_key("Requested"));
        assert!(err.context.fields.contains_key("Available"));
    }

    #[test]
    fn test_transient_error() {
        let err = MarabuntaError::connection_timeout("example.com", 30);

        assert!(err.is_transient());
        assert!(err.context.retry_after.is_some());
    }

    #[test]
    fn test_error_with_cause() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err = MarabuntaError::config_file_read("/path/to/config.toml", io_err);

        assert!(err.cause.is_some());
        assert!(err.to_string().contains("file not found"));
    }

    #[test]
    fn test_cli_error_display() {
        let err = MarabuntaError::insufficient_resources("128GB", "64GB", "memory");
        let display = CliErrorDisplay::new(&err, false);
        let output = display.format();

        assert!(output.contains("error[E201]"));
        assert!(output.contains("Insufficient resources"));
        assert!(output.contains("help"));
        assert!(output.contains("docs/errors/E201"));
    }

    #[test]
    fn test_error_context_builder() {
        let ctx = ErrorContext::new()
            .with_field("key1", "value1")
            .with_field("key2", "value2")
            .with_suggestion("Try this")
            .with_suggestion("Or this")
            .transient()
            .with_retry_after(std::time::Duration::from_secs(60));

        assert_eq!(ctx.fields.len(), 2);
        assert_eq!(ctx.suggestions.len(), 2);
        assert!(ctx.is_transient);
        assert_eq!(ctx.retry_after, Some(std::time::Duration::from_secs(60)));
    }

    #[test]
    fn test_policy_violation_error() {
        let err = MarabuntaError::policy_violation(
            "policy-123",
            "Production Only",
            "submit_job",
            "Job submitted to non-production nodes from production user",
        );

        assert_eq!(err.code, ErrorCode::POLICY_VIOLATION);
        assert!(err.context.fields.contains_key("Policy ID"));
        assert!(err.context.fields.contains_key("Policy name"));
        assert!(err.context.fields.contains_key("Reason"));
    }

    #[test]
    fn test_circular_dependency_error() {
        let err = MarabuntaError::circular_dependency(
            "task-5",
            vec![
                "task-5".to_string(),
                "task-2".to_string(),
                "task-3".to_string(),
                "task-5".to_string(),
            ],
        );

        assert_eq!(err.code, ErrorCode::CIRCULAR_DEPENDENCY);
        assert!(err.context.fields["Cycle"].contains("task-5 -> task-2"));
    }
}
