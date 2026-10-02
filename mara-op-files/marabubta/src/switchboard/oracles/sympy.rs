// Marabunta - Licensed under the MIT License.
//! SymPy persistent subprocess pool oracle.
//!
//! Manages a pool of long-running Python 3 subprocesses, each hosting a SymPy
//! REPL loop.  JSON commands are sent over stdin and JSON results are read from
//! stdout, avoiding the startup cost of a fresh Python interpreter per query.
//!
//! The pool uses lazy initialization: processes are spawned on first use and
//! recycled after [`SYMPY_IDLE_TIMEOUT`] of inactivity.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tracing::{debug, warn};

use crate::switchboard::config::{ORACLE_TIMEOUT, SYMPY_IDLE_TIMEOUT, SYMPY_POOL_SIZE};
use crate::switchboard::types::{Category, NormalizedExpression, Operation};

use super::{MathOracle, OracleError, OracleResult};

// ============================================================================
// Python REPL script (embedded)
// ============================================================================

/// Persistent Python REPL script that reads JSON commands from stdin and writes
/// JSON results to stdout.  The script imports SymPy once at startup and keeps
/// running until stdin is closed.
const PYTHON_REPL_SCRIPT: &str = r#"
import sys, json, sympy
from sympy.parsing.sympy_parser import parse_expr, standard_transformations, implicit_multiplication_application

transformations = standard_transformations + (implicit_multiplication_application,)

def execute(cmd):
    command = cmd.get("command", "")
    expression = cmd.get("expression", "")
    variable = cmd.get("variable", "x")

    var = sympy.Symbol(variable)
    expr = parse_expr(expression, transformations=transformations)

    if command == "solve":
        result = sympy.solve(expr, var)
    elif command == "simplify":
        result = sympy.simplify(expr)
    elif command == "factor":
        result = sympy.factor(expr)
    elif command == "expand":
        result = sympy.expand(expr)
    elif command == "differentiate":
        result = sympy.diff(expr, var)
    elif command == "integrate":
        result = sympy.integrate(expr, var)
    elif command == "limit":
        point = cmd.get("point", "oo")
        if point == "oo":
            pt = sympy.oo
        elif point == "-oo":
            pt = -sympy.oo
        else:
            pt = parse_expr(point, transformations=transformations)
        result = sympy.limit(expr, var, pt)
    elif command == "series":
        order = cmd.get("order", 6)
        result = sympy.series(expr, var, n=order)
    elif command == "determinant":
        result = sympy.Matrix(expr).det()
    elif command == "matrix_inverse":
        result = sympy.Matrix(expr).inv()
    elif command == "eigen_decompose":
        m = sympy.Matrix(expr)
        eigenvals = m.eigenvals()
        result = {str(k): v for k, v in eigenvals.items()}
        return {"result": str(result), "latex": sympy.latex(m), "plain": str(result)}
    else:
        return {"error": "unknown command: " + command}

    return {
        "result": str(result),
        "latex": sympy.latex(result),
        "plain": str(result)
    }

while True:
    line = sys.stdin.readline()
    if not line:
        break
    line = line.strip()
    if not line:
        continue
    try:
        cmd = json.loads(line)
        out = execute(cmd)
        print(json.dumps(out), flush=True)
    except Exception as e:
        print(json.dumps({"error": str(e)}), flush=True)
"#;

// ============================================================================
// SympyProcess — single managed subprocess
// ============================================================================

/// A single persistent SymPy subprocess.
struct SympyProcess {
    /// The child process handle.
    child: Child,
    /// Writer for the child's stdin.
    stdin: ChildStdin,
    /// Buffered reader for the child's stdout.
    stdout: BufReader<ChildStdout>,
    /// Timestamp of the last successful interaction.
    last_used: Instant,
    /// Whether this process is currently executing a command.
    busy: bool,
}

impl SympyProcess {
    /// Spawn a new Python REPL subprocess.
    async fn spawn() -> Result<Self, OracleError> {
        let mut child = Command::new("python3")
            .arg("-c")
            .arg(PYTHON_REPL_SCRIPT.trim())
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| {
                OracleError::permanent("sympy", format!("failed to spawn python3: {e}"))
            })?;

        let stdin = child.stdin.take().ok_or_else(|| {
            OracleError::permanent("sympy", "failed to capture stdin")
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            OracleError::permanent("sympy", "failed to capture stdout")
        })?;

        Ok(Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            last_used: Instant::now(),
            busy: false,
        })
    }

    /// Send a JSON command and read the JSON response line.
    async fn execute(&mut self, json_cmd: &str) -> Result<SympyResponse, OracleError> {
        self.busy = true;

        // Write command followed by newline.
        let payload = format!("{}\n", json_cmd);
        self.stdin
            .write_all(payload.as_bytes())
            .await
            .map_err(|e| {
                OracleError::permanent("sympy", format!("stdin write failed: {e}"))
            })?;
        self.stdin.flush().await.map_err(|e| {
            OracleError::permanent("sympy", format!("stdin flush failed: {e}"))
        })?;

        // Read one line of JSON output.
        let mut line = String::new();
        self.stdout.read_line(&mut line).await.map_err(|e| {
            OracleError::permanent("sympy", format!("stdout read failed: {e}"))
        })?;

        if line.is_empty() {
            return Err(OracleError::permanent(
                "sympy",
                "process closed stdout unexpectedly",
            ));
        }

        self.last_used = Instant::now();
        self.busy = false;

        let resp: SympyResponse = serde_json::from_str(line.trim()).map_err(|e| {
            OracleError::permanent("sympy", format!("invalid JSON from sympy: {e}"))
        })?;

        Ok(resp)
    }

    /// Returns true if the process has exceeded the idle timeout.
    fn is_expired(&self) -> bool {
        self.last_used.elapsed() > SYMPY_IDLE_TIMEOUT
    }

    /// Returns true if the underlying child process has exited.
    fn is_dead(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(Some(_)) | Err(_))
    }
}

// ============================================================================
// SympyResponse — JSON envelope from the Python REPL
// ============================================================================

/// JSON response from the Python REPL subprocess.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SympyResponse {
    #[serde(default)]
    result: Option<String>,
    #[serde(default)]
    latex: Option<String>,
    #[serde(default)]
    plain: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

// ============================================================================
// SympyPool — fixed-size process pool with round-robin selection
// ============================================================================

/// Fixed-size pool of SymPy subprocess slots.
///
/// Each slot is a `Mutex<Option<SympyProcess>>`.  Processes are lazily spawned
/// on first use and recycled when idle or dead.  Selection is round-robin via
/// an atomic index.
pub struct SympyPool {
    processes: Vec<Mutex<Option<SympyProcess>>>,
    next_index: AtomicUsize,
    pool_size: usize,
}

impl SympyPool {
    /// Create a new pool with `pool_size` empty slots.
    fn new(pool_size: usize) -> Self {
        let mut processes = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            processes.push(Mutex::new(None));
        }
        Self {
            processes,
            next_index: AtomicUsize::new(0),
            pool_size,
        }
    }

    /// Try to acquire a usable process slot.  Returns the slot index.
    ///
    /// Iterates starting from the round-robin index, skipping busy slots.
    /// Returns `None` if every slot is busy.
    fn try_acquire(&self) -> Option<usize> {
        let start = self.next_index.fetch_add(1, Ordering::Relaxed) % self.pool_size;
        for offset in 0..self.pool_size {
            let idx = (start + offset) % self.pool_size;
            let guard = self.processes[idx].lock();
            match guard.as_ref() {
                Some(proc) if proc.busy => continue,
                _ => {
                    drop(guard);
                    // Update next_index to hint at the slot after this one.
                    self.next_index
                        .store((idx + 1) % self.pool_size, Ordering::Relaxed);
                    return Some(idx);
                }
            }
        }
        None
    }
}

// ============================================================================
// SympyOracle — the public oracle implementation
// ============================================================================

/// Oracle backed by a pool of persistent SymPy subprocesses.
///
/// # Priority
///
/// Priority is **1** (highest) because SymPy is free and local — no API keys,
/// no network round-trips.
pub struct SympyOracle {
    pool: Arc<SympyPool>,
}

impl SympyOracle {
    /// Create a new oracle with `pool_size` subprocess slots (lazy init).
    pub fn new(pool_size: usize) -> Self {
        Self {
            pool: Arc::new(SympyPool::new(pool_size)),
        }
    }

    /// Create a new oracle using the default pool size from config.
    pub fn default_pool() -> Self {
        Self::new(SYMPY_POOL_SIZE)
    }
}

#[async_trait]
impl MathOracle for SympyOracle {
    fn name(&self) -> &str {
        "sympy"
    }

    fn supported_categories(&self) -> Vec<Category> {
        vec![
            Category::Algebra,
            Category::Calculus,
            Category::DifferentialEquations,
            Category::LinearAlgebra,
            Category::NumberTheory,
            Category::Statistics,
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
        op: &Operation,
    ) -> Result<OracleResult, OracleError> {
        let start = Instant::now();

        // Validate operation is supported.
        if !self.supported_operations().contains(op) {
            return Err(OracleError::permanent(
                "sympy",
                format!("sympy does not support operation: {op}"),
            ));
        }

        // Build the JSON command.
        let json_cmd = build_sympy_command(expr, op);

        // Acquire a pool slot.
        let idx = self
            .pool
            .try_acquire()
            .ok_or_else(|| OracleError::transient("sympy", "pool exhausted"))?;

        // Get or spawn the process.
        // parking_lot MutexGuard is !Send, so we must never hold it across
        // an .await point.  Split into: check → drop guard → spawn → reacquire.
        let needs_respawn = {
            let mut guard = self.pool.processes[idx].lock();
            match guard.as_mut() {
                None => true,
                Some(proc) => proc.is_expired() || proc.is_dead(),
            }
        };

        if needs_respawn {
            debug!(slot = idx, "spawning new sympy subprocess");
            // Clear the old process (kill_on_drop handles cleanup).
            {
                let mut guard = self.pool.processes[idx].lock();
                *guard = None;
            }
            // Spawn without holding the lock.
            let proc = SympyProcess::spawn().await?;
            let mut guard = self.pool.processes[idx].lock();
            *guard = Some(proc);
        }

        // Take the process out for the async execute call.
        let response = {
            let mut guard = self.pool.processes[idx].lock();
            guard.take()
        };

        let mut proc = response.ok_or_else(|| {
            OracleError::permanent("sympy", "slot unexpectedly empty after spawn")
        })?;

        // Execute with timeout.
        let timeout = ORACLE_TIMEOUT;
        let result = tokio::time::timeout(timeout, proc.execute(&json_cmd)).await;

        // Put the process back regardless of outcome.
        {
            let mut guard = self.pool.processes[idx].lock();
            *guard = Some(proc);
        }

        let _elapsed_ms = start.elapsed().as_millis() as u64;

        match result {
            Ok(Ok(resp)) => {
                if let Some(err) = resp.error {
                    Err(OracleError::permanent("sympy", err))
                } else {
                    let mut oracle_result = OracleResult::new("sympy");
                    if let Some(latex) = resp.latex {
                        oracle_result = oracle_result.with_latex(latex);
                    }
                    if let Some(plain) = resp.plain {
                        oracle_result = oracle_result.with_plain_text(plain);
                    }
                    Ok(oracle_result.with_confidence(0.9))
                }
            }
            Ok(Err(e)) => {
                // Process-level error; mark slot for respawn next time.
                let mut guard = self.pool.processes[idx].lock();
                *guard = None;
                Err(e)
            }
            Err(_elapsed) => {
                warn!(slot = idx, "sympy subprocess timed out");
                // Kill the timed-out process so the slot respawns.
                let mut guard = self.pool.processes[idx].lock();
                *guard = None;
                Err(OracleError::transient(
                    "sympy",
                    format!("timeout after {}ms", timeout.as_millis()),
                ))
            }
        }
    }

    fn priority(&self) -> u32 {
        1
    }
}

// ============================================================================
// Command builder
// ============================================================================

/// Build a JSON command string for the SymPy REPL subprocess.
///
/// Maps each [`Operation`] to its corresponding SymPy function call and
/// serializes the request as a single-line JSON object.
pub fn build_sympy_command(expr: &NormalizedExpression, op: &Operation) -> String {
    let expression = &expr.plain_text;
    let variable = expr
        .symbols
        .first()
        .map(|s| s.as_str())
        .unwrap_or("x");

    let mut map = serde_json::Map::new();
    map.insert(
        "expression".to_string(),
        serde_json::Value::String(expression.clone()),
    );
    map.insert(
        "variable".to_string(),
        serde_json::Value::String(variable.to_string()),
    );

    let command = match op {
        Operation::Solve => "solve",
        Operation::Simplify => "simplify",
        Operation::Factor => "factor",
        Operation::Expand => "expand",
        Operation::Differentiate => "differentiate",
        Operation::Integrate => "integrate",
        Operation::Limit => "limit",
        Operation::Series => "series",
        Operation::Determinant => "determinant",
        Operation::MatrixInverse => "matrix_inverse",
        Operation::EigenDecompose => "eigen_decompose",
        _ => "simplify", // fallback for custom/unsupported
    };
    map.insert(
        "command".to_string(),
        serde_json::Value::String(command.to_string()),
    );

    serde_json::Value::Object(map).to_string()
}

/// Parse a raw JSON response string from the SymPy subprocess into an
/// [`OracleResult`].
pub fn parse_sympy_response(json_str: &str, _elapsed_ms: u64) -> Result<OracleResult, OracleError> {
    let resp: SympyResponse = serde_json::from_str(json_str)
        .map_err(|e| OracleError::permanent("sympy", format!("invalid JSON: {e}")))?;

    if let Some(err) = resp.error {
        return Err(OracleError::permanent("sympy", err));
    }

    let mut result = OracleResult::new("sympy");
    if let Some(latex) = resp.latex {
        result = result.with_latex(latex);
    }
    if let Some(plain) = resp.plain {
        result = result.with_plain_text(plain);
    }
    Ok(result.with_confidence(0.9))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    /// Helper: build a minimal NormalizedExpression for testing.
    fn test_expr(plain_text: &str, symbols: Vec<&str>) -> NormalizedExpression {
        NormalizedExpression {
            latex: plain_text.to_string(),
            plain_text: plain_text.to_string(),
            symbols: symbols.into_iter().map(String::from).collect(),
            is_equation: false,
            is_inequality: false,
            has_integral: false,
            has_derivative: false,
            has_summation: false,
            has_limit: false,
            has_matrix: false,
            variable_count: 1,
            normalized_at: Utc::now(),
        }
    }

    // ---- Construction ----

    #[test]
    fn test_oracle_construction() {
        let oracle = SympyOracle::new(4);
        assert_eq!(oracle.pool.pool_size, 4);
        // All slots should be empty (lazy init).
        for slot in &oracle.pool.processes {
            assert!(slot.lock().is_none());
        }
    }

    #[test]
    fn test_oracle_default_pool() {
        let oracle = SympyOracle::default_pool();
        assert_eq!(oracle.pool.pool_size, SYMPY_POOL_SIZE);
    }

    // ---- Name ----

    #[test]
    fn test_oracle_name() {
        let oracle = SympyOracle::new(1);
        assert_eq!(oracle.name(), "sympy");
    }

    // ---- Priority ----

    #[test]
    fn test_oracle_priority() {
        let oracle = SympyOracle::new(1);
        assert_eq!(oracle.priority(), 1);
    }

    // ---- Supported categories ----

    #[test]
    fn test_supported_categories() {
        let oracle = SympyOracle::new(1);
        let cats = oracle.supported_categories();
        assert!(cats.contains(&Category::Algebra));
        assert!(cats.contains(&Category::Calculus));
        assert!(cats.contains(&Category::DifferentialEquations));
        assert!(cats.contains(&Category::LinearAlgebra));
        assert!(cats.contains(&Category::NumberTheory));
        assert!(cats.contains(&Category::Statistics));
        assert_eq!(cats.len(), 6);
    }

    // ---- Supported operations ----

    #[test]
    fn test_supported_operations() {
        let oracle = SympyOracle::new(1);
        let ops = oracle.supported_operations();
        assert!(ops.contains(&Operation::Solve));
        assert!(ops.contains(&Operation::Simplify));
        assert!(ops.contains(&Operation::Factor));
        assert!(ops.contains(&Operation::Expand));
        assert!(ops.contains(&Operation::Differentiate));
        assert!(ops.contains(&Operation::Integrate));
        assert!(ops.contains(&Operation::Limit));
        assert!(ops.contains(&Operation::Series));
        assert!(ops.contains(&Operation::Determinant));
        assert!(ops.contains(&Operation::MatrixInverse));
        assert!(ops.contains(&Operation::EigenDecompose));
        assert_eq!(ops.len(), 11);
    }

    // ---- build_sympy_command ----

    #[test]
    fn test_build_command_solve() {
        let expr = test_expr("x**2 - 4", vec!["x"]);
        let cmd = build_sympy_command(&expr, &Operation::Solve);
        let parsed: serde_json::Value = serde_json::from_str(&cmd).unwrap();
        assert_eq!(parsed["command"], "solve");
        assert_eq!(parsed["expression"], "x**2 - 4");
        assert_eq!(parsed["variable"], "x");
    }

    #[test]
    fn test_build_command_differentiate() {
        let expr = test_expr("sin(x)", vec!["x"]);
        let cmd = build_sympy_command(&expr, &Operation::Differentiate);
        let parsed: serde_json::Value = serde_json::from_str(&cmd).unwrap();
        assert_eq!(parsed["command"], "differentiate");
        assert_eq!(parsed["expression"], "sin(x)");
    }

    #[test]
    fn test_build_command_integrate() {
        let expr = test_expr("x**2", vec!["x"]);
        let cmd = build_sympy_command(&expr, &Operation::Integrate);
        let parsed: serde_json::Value = serde_json::from_str(&cmd).unwrap();
        assert_eq!(parsed["command"], "integrate");
    }

    #[test]
    fn test_build_command_factor() {
        let expr = test_expr("x**2 + 2*x + 1", vec!["x"]);
        let cmd = build_sympy_command(&expr, &Operation::Factor);
        let parsed: serde_json::Value = serde_json::from_str(&cmd).unwrap();
        assert_eq!(parsed["command"], "factor");
    }

    #[test]
    fn test_build_command_expand() {
        let expr = test_expr("(x + 1)**2", vec!["x"]);
        let cmd = build_sympy_command(&expr, &Operation::Expand);
        let parsed: serde_json::Value = serde_json::from_str(&cmd).unwrap();
        assert_eq!(parsed["command"], "expand");
    }

    #[test]
    fn test_build_command_simplify() {
        let expr = test_expr("sin(x)**2 + cos(x)**2", vec!["x"]);
        let cmd = build_sympy_command(&expr, &Operation::Simplify);
        let parsed: serde_json::Value = serde_json::from_str(&cmd).unwrap();
        assert_eq!(parsed["command"], "simplify");
    }

    #[test]
    fn test_build_command_default_variable() {
        // No symbols provided -- should default to "x".
        let expr = test_expr("a + b", vec![]);
        let cmd = build_sympy_command(&expr, &Operation::Solve);
        let parsed: serde_json::Value = serde_json::from_str(&cmd).unwrap();
        assert_eq!(parsed["variable"], "x");
    }

    #[test]
    fn test_build_command_multivar() {
        // First symbol should be chosen as the variable.
        let expr = test_expr("x + y", vec!["y", "x"]);
        let cmd = build_sympy_command(&expr, &Operation::Solve);
        let parsed: serde_json::Value = serde_json::from_str(&cmd).unwrap();
        assert_eq!(parsed["variable"], "y");
    }

    #[test]
    fn test_build_command_limit() {
        let expr = test_expr("1/x", vec!["x"]);
        let cmd = build_sympy_command(&expr, &Operation::Limit);
        let parsed: serde_json::Value = serde_json::from_str(&cmd).unwrap();
        assert_eq!(parsed["command"], "limit");
    }

    #[test]
    fn test_build_command_series() {
        let expr = test_expr("exp(x)", vec!["x"]);
        let cmd = build_sympy_command(&expr, &Operation::Series);
        let parsed: serde_json::Value = serde_json::from_str(&cmd).unwrap();
        assert_eq!(parsed["command"], "series");
    }

    #[test]
    fn test_build_command_determinant() {
        let expr = test_expr("[[1,2],[3,4]]", vec![]);
        let cmd = build_sympy_command(&expr, &Operation::Determinant);
        let parsed: serde_json::Value = serde_json::from_str(&cmd).unwrap();
        assert_eq!(parsed["command"], "determinant");
    }

    #[test]
    fn test_build_command_matrix_inverse() {
        let expr = test_expr("[[1,0],[0,1]]", vec![]);
        let cmd = build_sympy_command(&expr, &Operation::MatrixInverse);
        let parsed: serde_json::Value = serde_json::from_str(&cmd).unwrap();
        assert_eq!(parsed["command"], "matrix_inverse");
    }

    #[test]
    fn test_build_command_eigen_decompose() {
        let expr = test_expr("[[2,1],[1,2]]", vec![]);
        let cmd = build_sympy_command(&expr, &Operation::EigenDecompose);
        let parsed: serde_json::Value = serde_json::from_str(&cmd).unwrap();
        assert_eq!(parsed["command"], "eigen_decompose");
    }

    // ---- Response parsing ----

    #[test]
    fn test_parse_success_response() {
        let json = r#"{"result": "[2, -2]", "latex": "\\left[ 2, -2\\right]", "plain": "[2, -2]"}"#;
        let result = parse_sympy_response(json, 42).unwrap();
        assert_eq!(result.backend, "sympy");
        assert_eq!(result.latex.as_deref(), Some("\\left[ 2, -2\\right]"));
        assert_eq!(result.plain_text.as_deref(), Some("[2, -2]"));
    }

    #[test]
    fn test_parse_error_response() {
        let json = r#"{"error": "name 'z' is not defined"}"#;
        let result = parse_sympy_response(json, 10);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.backend, "sympy");
        assert!(err.message.contains("not defined"));
    }

    #[test]
    fn test_parse_invalid_json() {
        let result = parse_sympy_response("not json at all", 5);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.backend, "sympy");
        assert!(err.message.contains("invalid JSON"));
    }

    #[test]
    fn test_parse_partial_response() {
        // Only latex present, no plain.
        let json = r#"{"result": "1", "latex": "1"}"#;
        let result = parse_sympy_response(json, 3).unwrap();
        assert_eq!(result.latex.as_deref(), Some("1"));
        assert!(result.plain_text.is_none());
    }

    // ---- Pool ----

    #[test]
    fn test_pool_size_configuration() {
        let pool = SympyPool::new(5);
        assert_eq!(pool.pool_size, 5);
        assert_eq!(pool.processes.len(), 5);
    }

    #[test]
    fn test_pool_try_acquire_round_robin() {
        let pool = SympyPool::new(3);
        // All slots empty (not busy) so each acquire should succeed.
        let idx0 = pool.try_acquire().unwrap();
        let idx1 = pool.try_acquire().unwrap();
        let idx2 = pool.try_acquire().unwrap();
        // Should cycle through all three slots.
        let indices = vec![idx0, idx1, idx2];
        // All indices should be in 0..3
        for &i in &indices {
            assert!(i < 3);
        }
    }

    #[test]
    fn test_oracle_result_fields() {
        let result = OracleResult::new("sympy")
            .with_latex("x = 1")
            .with_plain_text("x = 1")
            .with_steps(vec!["step 1".to_string()])
            .with_confidence(1.0);
        assert_eq!(result.backend, "sympy");
        assert_eq!(result.steps.len(), 1);
        assert_eq!(result.latex.as_deref(), Some("x = 1"));
        assert_eq!(result.plain_text.as_deref(), Some("x = 1"));
    }

    // ---- Integration-level tests (require Python 3 + sympy) ----

    // NOTE: The following tests require a working Python 3 installation with
    // the `sympy` package.  They are marked #[ignore] so they do not run in
    // CI unless explicitly enabled with `cargo test -- --ignored`.

    #[tokio::test]
    #[ignore]
    async fn integration_sympy_solve() {
        let oracle = SympyOracle::new(1);
        let expr = test_expr("x**2 - 4", vec!["x"]);
        let result = oracle.solve(&expr, &Operation::Solve).await.unwrap();
        assert!(result.plain_text.is_some());
        let plain = result.plain_text.unwrap();
        // SymPy solves x^2-4=0 as [-2, 2]
        assert!(plain.contains("2") && plain.contains("-2"));
    }

    #[tokio::test]
    #[ignore]
    async fn integration_sympy_differentiate() {
        let oracle = SympyOracle::new(1);
        let expr = test_expr("x**3", vec!["x"]);
        let result = oracle
            .solve(&expr, &Operation::Differentiate)
            .await
            .unwrap();
        let plain = result.plain_text.unwrap();
        // d/dx(x^3) = 3*x^2
        assert!(plain.contains("3"));
    }

    #[tokio::test]
    #[ignore]
    async fn integration_sympy_integrate() {
        let oracle = SympyOracle::new(1);
        let expr = test_expr("2*x", vec!["x"]);
        let result = oracle
            .solve(&expr, &Operation::Integrate)
            .await
            .unwrap();
        let plain = result.plain_text.unwrap();
        // integral of 2x dx = x^2
        assert!(plain.contains("x**2"));
    }

    #[tokio::test]
    #[ignore]
    async fn integration_sympy_unsupported_op() {
        let oracle = SympyOracle::new(1);
        let expr = test_expr("x", vec!["x"]);
        let result = oracle
            .solve(&expr, &Operation::Custom("unknown".into()))
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.backend, "sympy");
        assert!(err.message.contains("does not support"));
    }
}
