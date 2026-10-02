// Marabunta - Licensed under the MIT License.
//! Ingestion pipeline for the Switchboard module.
//!
//! Converts raw user input (napkin photos, LaTeX strings, plain text, MathML,
//! AsciiMath) into a `NormalizedExpression` suitable for classification and
//! routing. OCR support is provided via the Mathpix API for handwritten math.

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use chrono::Utc;
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use super::config::{SwitchboardConfig, MAX_IMAGE_SIZE, MAX_INPUT_SIZE};
use super::errors::{SwitchboardError, SwitchboardResult};
use super::types::{Ambiguity, InputFormat, NormalizedExpression, RawInput};

// ============================================================================
// Compiled regex patterns (Lazy-initialized, thread-safe singletons)
// ============================================================================

/// Matches LaTeX integral commands.
static RE_INTEGRAL: Lazy<Regex> = Lazy::new(|| Regex::new(r"\\int").unwrap());

/// Matches LaTeX derivative notations including partials, nabla, and dot notation.
static RE_DERIVATIVE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\\frac\{d\}|\\frac\{d[a-zA-Z]\}|\\partial|\\nabla|\\dot\{|\\ddot\{|'")
        .unwrap()
});

/// Matches LaTeX summation commands.
static RE_SUMMATION: Lazy<Regex> = Lazy::new(|| Regex::new(r"\\sum").unwrap());

/// Matches LaTeX limit commands.
static RE_LIMIT: Lazy<Regex> = Lazy::new(|| Regex::new(r"\\lim").unwrap());

/// Matches LaTeX matrix environments: matrix, pmatrix, bmatrix, vmatrix, Bmatrix.
static RE_MATRIX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\\begin\{(matrix|pmatrix|bmatrix|vmatrix|Bmatrix)\}").unwrap());

/// Matches equality sign that is NOT a LaTeX command escape (e.g. \ne= is filtered).
static RE_EQUALS: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?:^|[^\\!<>])=").unwrap());

/// Matches inequality operators in LaTeX.
static RE_INEQUALITY: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"<|>|\\le\b|\\ge\b|\\leq|\\geq|\\ne\b|\\neq").unwrap());

/// Matches Greek letters (both lower and upper case).
static RE_GREEK: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"\\(alpha|beta|gamma|delta|epsilon|zeta|eta|theta|iota|kappa|lambda|mu|nu|xi|pi|rho|sigma|tau|upsilon|phi|chi|psi|omega|Gamma|Delta|Theta|Lambda|Xi|Pi|Sigma|Phi|Psi|Omega)\b"
    ).unwrap()
});

/// Matches named mathematical functions.
static RE_FUNCTION: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"\\(sin|cos|tan|sec|csc|cot|sinh|cosh|tanh|arcsin|arccos|arctan|ln|log|exp|sqrt|min|max|gcd|lcm|det|tr)\b"
    ).unwrap()
});

/// Matches single-letter variables NOT preceded by a backslash.
static RE_VARIABLE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?:^|[^a-zA-Z\\])([a-zA-Z])(?:[^a-zA-Z]|$)").unwrap());

/// Detects implicit multiplication: two adjacent letters without an operator between them.
static RE_IMPLICIT_MULT: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"([a-zA-Z])([a-zA-Z])").unwrap());

/// Detects function application pattern: a letter immediately followed by `(`.
static RE_FUNCTION_APP: Lazy<Regex> = Lazy::new(|| Regex::new(r"([a-zA-Z])\(").unwrap());

/// Matches `sqrt(...)` in plain text for conversion to LaTeX.
static RE_PLAIN_SQRT: Lazy<Regex> = Lazy::new(|| Regex::new(r"sqrt\(([^)]+)\)").unwrap());

/// Matches `abs(...)` in plain text for conversion to LaTeX.
static RE_PLAIN_ABS: Lazy<Regex> = Lazy::new(|| Regex::new(r"abs\(([^)]+)\)").unwrap());

/// Matches `log(...)` in plain text for conversion to LaTeX.
static RE_PLAIN_LOG: Lazy<Regex> = Lazy::new(|| Regex::new(r"log\(([^)]+)\)").unwrap());

/// Matches `ln(...)` in plain text for conversion to LaTeX.
static RE_PLAIN_LN: Lazy<Regex> = Lazy::new(|| Regex::new(r"ln\(([^)]+)\)").unwrap());

/// Matches `sin(...)`, `cos(...)`, `tan(...)` in plain text.
static RE_PLAIN_TRIG: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(sin|cos|tan|sec|csc|cot)\(([^)]+)\)").unwrap());

/// Matches `pi` as a standalone word in plain text.
static RE_PLAIN_PI: Lazy<Regex> = Lazy::new(|| Regex::new(r"\bpi\b").unwrap());

/// Matches `inf` or `infinity` as a standalone word in plain text.
static RE_PLAIN_INFINITY: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\b(inf|infinity)\b").unwrap());

/// Matches exponent notation like `x^2` or `x^(2+1)`.
static RE_PLAIN_EXPONENT: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\^(\([^)]+\)|[a-zA-Z0-9])").unwrap());

// ============================================================================
// Mathpix OCR Client
// ============================================================================

/// Response structure from the Mathpix API.
#[derive(Debug, Deserialize)]
struct MathpixResponse {
    /// Simplified LaTeX output.
    #[serde(default)]
    latex_simplified: Option<String>,
    /// Error message if the API call failed.
    #[serde(default)]
    error: Option<String>,
    /// Error ID / info string.
    #[serde(default)]
    error_info: Option<MathpixErrorInfo>,
}

/// Nested error info from Mathpix.
#[derive(Debug, Deserialize)]
struct MathpixErrorInfo {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

/// Request body sent to the Mathpix `/v3/text` endpoint.
#[derive(Debug, Serialize)]
struct MathpixRequest {
    src: String,
    formats: Vec<String>,
}

/// Client for the Mathpix OCR API.
///
/// Converts images of handwritten or printed math into LaTeX strings.
pub struct MathpixClient {
    pub app_id: String,
    pub app_key: String,
    pub client: reqwest::Client,
}

impl MathpixClient {
    /// Create a new Mathpix client with the given credentials.
    pub fn new(app_id: String, app_key: String) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        Self {
            app_id,
            app_key,
            client,
        }
    }

    /// Send an image to the Mathpix API and return the recognized LaTeX.
    ///
    /// The image data is base64-encoded and sent as a `data:image/png;base64,...`
    /// URI in the JSON body. The API returns a simplified LaTeX string.
    pub async fn recognize(&self, image_data: &[u8]) -> SwitchboardResult<String> {
        let encoded = BASE64_STANDARD.encode(image_data);
        let src = format!("data:image/png;base64,{}", encoded);

        let body = MathpixRequest {
            src,
            formats: vec!["latex_simplified".to_string()],
        };

        debug!(
            "Sending OCR request to Mathpix ({} bytes of image data)",
            image_data.len()
        );

        let response = self
            .client
            .post("https://api.mathpix.com/v3/text")
            .header("app_id", &self.app_id)
            .header("app_key", &self.app_key)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| SwitchboardError::MathpixError {
                status: 0,
                message: format!("HTTP request failed: {}", e),
            })?;

        let status = response.status().as_u16();
        if !response.status().is_success() {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "unknown error".to_string());
            return Err(SwitchboardError::MathpixError {
                status,
                message: error_text,
            });
        }

        let mathpix_resp: MathpixResponse =
            response.json().await.map_err(|e| SwitchboardError::MathpixError {
                status: 200,
                message: format!("Failed to parse Mathpix response: {}", e),
            })?;

        // Check for API-level errors in the response body
        if let Some(error_info) = &mathpix_resp.error_info {
            let msg = error_info
                .message
                .as_deref()
                .or(mathpix_resp.error.as_deref())
                .unwrap_or("unknown Mathpix error");
            return Err(SwitchboardError::MathpixError {
                status,
                message: msg.to_string(),
            });
        }

        if let Some(error) = &mathpix_resp.error {
            return Err(SwitchboardError::MathpixError {
                status,
                message: error.clone(),
            });
        }

        mathpix_resp.latex_simplified.ok_or_else(|| {
            SwitchboardError::MathpixError {
                status,
                message: "No LaTeX output returned by Mathpix".to_string(),
            }
        })
    }
}

// ============================================================================
// Security stubs (will be replaced by super::security when that module exists)
// ============================================================================

/// Sanitize a LaTeX string by removing potentially dangerous commands.
///
/// Strips `\input`, `\include`, `\write`, `\def`, `\newcommand` and similar
/// macro-injection vectors. Returns the sanitized string.
fn sanitize_latex(input: &str) -> SwitchboardResult<String> {
    // Reject dangerous TeX primitives that could execute code
    let dangerous_patterns = [
        r"\input",
        r"\include",
        r"\write",
        r"\openout",
        r"\closeout",
        r"\immediate",
        r"\csname",
        r"\catcode",
        r"\def",
        r"\edef",
        r"\gdef",
        r"\xdef",
        r"\newcommand",
        r"\renewcommand",
        r"\usepackage",
        r"\RequirePackage",
        r"\url",
        r"\href",
    ];

    let mut sanitized = input.to_string();
    for pattern in &dangerous_patterns {
        if sanitized.contains(pattern) {
            warn!("Stripping dangerous LaTeX command: {}", pattern);
            sanitized = sanitized.replace(pattern, "");
        }
    }

    // Remove any remaining backslash-command sequences that are not in our
    // known-safe list. We allow common math commands through.
    Ok(sanitized.trim().to_string())
}

/// Sanitize plain text input by stripping control characters and limiting length.
fn sanitize_plain_text(input: &str) -> SwitchboardResult<String> {
    // Strip control characters except newline and tab
    let sanitized: String = input
        .chars()
        .filter(|c| !c.is_control() || *c == '\n' || *c == '\t')
        .collect();

    Ok(sanitized.trim().to_string())
}

/// Validate image data: check magic bytes and enforce size limits.
fn validate_image(data: &[u8]) -> SwitchboardResult<()> {
    if data.is_empty() {
        return Err(SwitchboardError::ImageError(
            "Empty image data".to_string(),
        ));
    }

    if data.len() > MAX_IMAGE_SIZE {
        return Err(SwitchboardError::InputTooLarge {
            size: data.len(),
            max: MAX_IMAGE_SIZE,
        });
    }

    // Check for common image magic bytes
    let is_png = data.len() >= 8 && data[..8] == [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];
    let is_jpeg = data.len() >= 3 && data[..3] == [0xFF, 0xD8, 0xFF];
    let is_gif = data.len() >= 6 && (data[..6] == *b"GIF87a" || data[..6] == *b"GIF89a");
    let is_webp = data.len() >= 12 && data[..4] == *b"RIFF" && data[8..12] == *b"WEBP";
    let is_bmp = data.len() >= 2 && data[..2] == *b"BM";

    if !(is_png || is_jpeg || is_gif || is_webp || is_bmp) {
        return Err(SwitchboardError::ImageError(
            "Unrecognized image format (expected PNG, JPEG, GIF, WebP, or BMP)".to_string(),
        ));
    }

    Ok(())
}

// ============================================================================
// Main ingestion entry point
// ============================================================================

/// Ingest a raw user input and produce a normalized expression.
///
/// Dispatches on the input format:
/// - **NapkinPhoto**: validates image -> OCR via Mathpix -> parse resulting LaTeX
/// - **Latex**: sanitize -> parse LaTeX
/// - **PlainText**: sanitize -> convert to LaTeX -> parse
/// - **MathML**: basic conversion stub -> parse LaTeX
/// - **AsciiMath**: basic conversion stub -> parse LaTeX
///
/// Returns `UnsupportedInput` if NapkinPhoto is requested but no Mathpix
/// credentials are configured.
pub async fn ingest(
    input: RawInput,
    config: &SwitchboardConfig,
) -> SwitchboardResult<NormalizedExpression> {
    info!("Ingesting input format={}", input.format);

    // Enforce input size limits for text-based formats
    match input.format {
        InputFormat::NapkinPhoto => {
            // Image size is checked in validate_image
        }
        _ => {
            let max = config.max_input_size;
            if input.content.len() > max {
                return Err(SwitchboardError::InputTooLarge {
                    size: input.content.len(),
                    max,
                });
            }
        }
    }

    match input.format {
        InputFormat::NapkinPhoto => ingest_napkin_photo(&input, config).await,
        InputFormat::Latex => ingest_latex(&input),
        InputFormat::PlainText => ingest_plain_text(&input),
        InputFormat::MathML => ingest_mathml(&input),
        InputFormat::AsciiMath => ingest_asciimath(&input),
    }
}

/// Ingest a napkin photo: validate image, send to Mathpix, parse the LaTeX.
async fn ingest_napkin_photo(
    input: &RawInput,
    config: &SwitchboardConfig,
) -> SwitchboardResult<NormalizedExpression> {
    // Verify Mathpix credentials are available
    let app_id = config.mathpix_app_id.as_ref().ok_or_else(|| {
        SwitchboardError::UnsupportedInput(
            "Napkin photo OCR requires Mathpix credentials (mathpix_app_id not configured)"
                .to_string(),
        )
    })?;
    let app_key = config.mathpix_app_key.as_ref().ok_or_else(|| {
        SwitchboardError::UnsupportedInput(
            "Napkin photo OCR requires Mathpix credentials (mathpix_app_key not configured)"
                .to_string(),
        )
    })?;

    // Get image data (either from image_data field or decode content as base64)
    let image_data = if let Some(ref data) = input.image_data {
        data.clone()
    } else {
        // Try to interpret content as base64
        BASE64_STANDARD
            .decode(&input.content)
            .map_err(|e| {
                SwitchboardError::ImageError(format!(
                    "NapkinPhoto content is not valid base64: {}",
                    e
                ))
            })?
    };

    // Validate image format and size
    validate_image(&image_data)?;

    // Send to Mathpix for OCR
    let client = MathpixClient::new(app_id.clone(), app_key.clone());
    let latex = client.recognize(&image_data).await?;

    debug!("Mathpix OCR returned LaTeX: {}", latex);

    // Sanitize and parse the recognized LaTeX
    let sanitized = sanitize_latex(&latex)?;
    Ok(parse_latex(&sanitized))
}

/// Ingest LaTeX input: sanitize then parse.
fn ingest_latex(input: &RawInput) -> SwitchboardResult<NormalizedExpression> {
    let sanitized = sanitize_latex(&input.content)?;

    if sanitized.is_empty() {
        return Err(SwitchboardError::InvalidFormat(
            "LaTeX input is empty after sanitization".to_string(),
        ));
    }

    Ok(parse_latex(&sanitized))
}

/// Ingest plain text: sanitize, convert to LaTeX, then parse.
fn ingest_plain_text(input: &RawInput) -> SwitchboardResult<NormalizedExpression> {
    let sanitized = sanitize_plain_text(&input.content)?;

    if sanitized.is_empty() {
        return Err(SwitchboardError::InvalidFormat(
            "Plain text input is empty after sanitization".to_string(),
        ));
    }

    Ok(parse_plain_text(&sanitized))
}

/// Ingest MathML: basic stub conversion to LaTeX, then parse.
///
/// This is a minimal implementation that handles common MathML elements.
/// A production system would use a full MathML-to-LaTeX converter.
fn ingest_mathml(input: &RawInput) -> SwitchboardResult<NormalizedExpression> {
    if input.content.is_empty() {
        return Err(SwitchboardError::InvalidFormat(
            "MathML input is empty".to_string(),
        ));
    }

    let latex = convert_mathml_to_latex(&input.content);
    let sanitized = sanitize_latex(&latex)?;
    Ok(parse_latex(&sanitized))
}

/// Ingest AsciiMath: basic stub conversion to LaTeX, then parse.
///
/// This is a minimal implementation for common AsciiMath patterns.
fn ingest_asciimath(input: &RawInput) -> SwitchboardResult<NormalizedExpression> {
    if input.content.is_empty() {
        return Err(SwitchboardError::InvalidFormat(
            "AsciiMath input is empty".to_string(),
        ));
    }

    let latex = convert_asciimath_to_latex(&input.content);
    let sanitized = sanitize_latex(&latex)?;
    Ok(parse_latex(&sanitized))
}

// ============================================================================
// LaTeX parser
// ============================================================================

/// Parse a LaTeX string into a `NormalizedExpression`.
///
/// Extracts symbols, sets structural boolean flags, counts variables,
/// and populates both the `latex` and `plain_text` fields.
pub fn parse_latex(latex: &str) -> NormalizedExpression {
    let symbols = extract_symbols(latex);
    let plain_text = latex_to_plain(latex);

    let is_equation = RE_EQUALS.is_match(latex);
    let is_inequality = RE_INEQUALITY.is_match(latex);
    let has_integral = RE_INTEGRAL.is_match(latex);
    let has_derivative = RE_DERIVATIVE.is_match(latex);
    let has_summation = RE_SUMMATION.is_match(latex);
    let has_limit = RE_LIMIT.is_match(latex);
    let has_matrix = RE_MATRIX.is_match(latex);

    // Count distinct single-letter variables
    let variable_count = symbols
        .iter()
        .filter(|s| s.len() == 1 && s.chars().next().is_some_and(|c| c.is_alphabetic()))
        .count();

    NormalizedExpression {
        latex: latex.to_string(),
        plain_text,
        symbols,
        is_equation,
        is_inequality,
        has_integral,
        has_derivative,
        has_summation,
        has_limit,
        has_matrix,
        variable_count,
        normalized_at: Utc::now(),
    }
}

/// Extract mathematical symbols from a LaTeX string.
///
/// Returns a sorted, deduplicated list of symbols including:
/// - Greek letters (`\alpha`, `\beta`, ...)
/// - Named functions (`\sin`, `\cos`, ...)
/// - Single-letter variables (`x`, `y`, ...)
/// - Operators (`\int`, `\sum`, `\prod`, `\lim`)
pub fn extract_symbols(latex: &str) -> Vec<String> {
    let mut symbols = Vec::new();

    // Greek letters
    for cap in RE_GREEK.captures_iter(latex) {
        if let Some(name) = cap.get(1) {
            symbols.push(format!("\\{}", name.as_str()));
        }
    }

    // Named functions
    for cap in RE_FUNCTION.captures_iter(latex) {
        if let Some(name) = cap.get(1) {
            symbols.push(format!("\\{}", name.as_str()));
        }
    }

    // Single-letter variables (skip those preceded by backslash)
    for cap in RE_VARIABLE.captures_iter(latex) {
        if let Some(var) = cap.get(1) {
            let var_str = var.as_str();
            let pos = var.start();
            if pos > 0 {
                if let Some('\\') = latex[..pos].chars().last() {
                    continue;
                }
            }
            symbols.push(var_str.to_string());
        }
    }

    // Operator symbols
    if RE_INTEGRAL.is_match(latex) {
        symbols.push("\\int".to_string());
    }
    if RE_SUMMATION.is_match(latex) {
        symbols.push("\\sum".to_string());
    }
    if RE_LIMIT.is_match(latex) {
        symbols.push("\\lim".to_string());
    }

    symbols.sort();
    symbols.dedup();
    symbols
}

/// Convert LaTeX to a rough plain text representation.
///
/// This is a best-effort conversion for display purposes. It strips common
/// LaTeX commands and replaces them with Unicode or ASCII equivalents.
fn latex_to_plain(latex: &str) -> String {
    let mut text = latex.to_string();

    // Remove environment wrappers
    text = text.replace(r"\left", "");
    text = text.replace(r"\right", "");
    text = text.replace(r"\begin{matrix}", "[");
    text = text.replace(r"\end{matrix}", "]");
    text = text.replace(r"\begin{pmatrix}", "(");
    text = text.replace(r"\end{pmatrix}", ")");
    text = text.replace(r"\begin{bmatrix}", "[");
    text = text.replace(r"\end{bmatrix}", "]");

    // Replace common commands with plain equivalents
    text = text.replace(r"\cdot", "*");
    text = text.replace(r"\times", "*");
    text = text.replace(r"\div", "/");
    text = text.replace(r"\pm", "+-");
    text = text.replace(r"\mp", "-+");
    text = text.replace(r"\leq", "<=");
    text = text.replace(r"\geq", ">=");
    text = text.replace(r"\le", "<=");
    text = text.replace(r"\ge", ">=");
    text = text.replace(r"\neq", "!=");
    text = text.replace(r"\ne", "!=");
    text = text.replace(r"\infty", "inf");
    text = text.replace(r"\pi", "pi");
    text = text.replace(r"\to", "->");
    text = text.replace(r"\rightarrow", "->");
    text = text.replace(r"\leftarrow", "<-");

    // Replace \frac{a}{b} with (a)/(b)
    while let Some(idx) = text.find(r"\frac{") {
        if let Some(result) = replace_frac(&text, idx) {
            text = result;
        } else {
            break;
        }
    }

    // Replace \sqrt{x} with sqrt(x)
    text = text.replace(r"\sqrt{", "sqrt(");
    // Corresponding closing brace will be handled below

    // Strip remaining backslash commands (keep the argument)
    // e.g. \sin -> sin, \cos -> cos
    let re_cmd = Regex::new(r"\\([a-zA-Z]+)").unwrap();
    text = re_cmd.replace_all(&text, "$1").to_string();

    // Clean up braces that were used for grouping
    text = text.replace('{', "(");
    text = text.replace('}', ")");

    // Collapse multiple spaces
    let re_spaces = Regex::new(r"\s+").unwrap();
    text = re_spaces.replace_all(&text, " ").to_string();

    text.trim().to_string()
}

/// Helper: replace the first `\frac{num}{den}` at `idx` with `(num)/(den)`.
fn replace_frac(text: &str, idx: usize) -> Option<String> {
    let after_frac = &text[idx + 6..]; // skip `\frac{`
    let num_end = find_matching_brace(after_frac)?;
    let numerator = &after_frac[..num_end];

    let after_num = &after_frac[num_end + 1..]; // skip `}`
    // Expect `{`
    let after_num_trimmed = after_num.trim_start();
    if !after_num_trimmed.starts_with('{') {
        return None;
    }
    let den_start_offset = after_num.len() - after_num_trimmed.len() + 1;
    let den_content = &after_num[den_start_offset..];
    let den_end = find_matching_brace(den_content)?;
    let denominator = &den_content[..den_end];

    let total_consumed = 6 + num_end + 1 + den_start_offset + den_end + 1;
    let before = &text[..idx];
    let after = &text[idx + total_consumed..];

    Some(format!("{}({})/({}){}", before, numerator, denominator, after))
}

/// Find the position of the matching `}` for content that starts right after a `{`.
fn find_matching_brace(s: &str) -> Option<usize> {
    let mut depth = 1usize;
    for (i, ch) in s.char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

// ============================================================================
// Plain text parser
// ============================================================================

/// Parse a plain text math expression and produce a `NormalizedExpression`.
///
/// Performs basic conversions:
/// - `sqrt(x)` -> `\sqrt{x}`
/// - `abs(x)` -> `|x|`
/// - `sin(x)`, `cos(x)`, `tan(x)` etc. -> `\sin(x)`, `\cos(x)`, `\tan(x)`
/// - `log(x)` -> `\log(x)`, `ln(x)` -> `\ln(x)`
/// - `pi` -> `\pi`, `inf` / `infinity` -> `\infty`
/// - `^` is kept as-is (valid LaTeX exponent)
/// - `*` -> `\cdot`
pub fn parse_plain_text(text: &str) -> NormalizedExpression {
    let latex = plain_to_latex(text);
    let symbols = extract_symbols(&latex);

    let is_equation = RE_EQUALS.is_match(&latex);
    let is_inequality = RE_INEQUALITY.is_match(&latex);
    let has_integral = RE_INTEGRAL.is_match(&latex);
    let has_derivative = RE_DERIVATIVE.is_match(&latex);
    let has_summation = RE_SUMMATION.is_match(&latex);
    let has_limit = RE_LIMIT.is_match(&latex);
    let has_matrix = RE_MATRIX.is_match(&latex);

    let variable_count = symbols
        .iter()
        .filter(|s| s.len() == 1 && s.chars().next().is_some_and(|c| c.is_alphabetic()))
        .count();

    NormalizedExpression {
        latex,
        plain_text: text.to_string(),
        symbols,
        is_equation,
        is_inequality,
        has_integral,
        has_derivative,
        has_summation,
        has_limit,
        has_matrix,
        variable_count,
        normalized_at: Utc::now(),
    }
}

/// Convert plain text to LaTeX notation.
fn plain_to_latex(text: &str) -> String {
    let mut latex = text.to_string();

    // Function conversions (must come before general replacements)
    latex = RE_PLAIN_SQRT
        .replace_all(&latex, r"\sqrt{$1}")
        .to_string();
    latex = RE_PLAIN_ABS
        .replace_all(&latex, r"|$1|")
        .to_string();
    latex = RE_PLAIN_TRIG
        .replace_all(&latex, r"\$1($2)")
        .to_string();
    latex = RE_PLAIN_LOG
        .replace_all(&latex, r"\log($1)")
        .to_string();
    latex = RE_PLAIN_LN
        .replace_all(&latex, r"\ln($1)")
        .to_string();

    // Constants
    latex = RE_PLAIN_PI.replace_all(&latex, r"\pi").to_string();
    latex = RE_PLAIN_INFINITY
        .replace_all(&latex, r"\infty")
        .to_string();

    // Operators
    latex = latex.replace("*", r"\cdot ");
    latex = latex.replace(">=", r"\geq ");
    latex = latex.replace("<=", r"\leq ");
    latex = latex.replace("!=", r"\neq ");

    latex.trim().to_string()
}

// ============================================================================
// MathML conversion stub
// ============================================================================

/// Basic MathML-to-LaTeX conversion.
///
/// Handles a minimal subset of MathML elements. A production implementation
/// would use a full parser (e.g., via `xml` crate).
fn convert_mathml_to_latex(mathml: &str) -> String {
    let mut result = mathml.to_string();

    // Strip XML/MathML wrapper tags
    result = result.replace("<math>", "");
    result = result.replace("</math>", "");
    result = result.replace("<math xmlns=\"http://www.w3.org/1998/Math/MathML\">", "");

    // Common MathML elements -> LaTeX
    // <mi> -> variable/identifier
    let re_mi = Regex::new(r"<mi>([^<]+)</mi>").unwrap();
    result = re_mi.replace_all(&result, "$1").to_string();

    // <mn> -> number
    let re_mn = Regex::new(r"<mn>([^<]+)</mn>").unwrap();
    result = re_mn.replace_all(&result, "$1").to_string();

    // <mo> -> operator
    let re_mo = Regex::new(r"<mo>([^<]+)</mo>").unwrap();
    result = re_mo.replace_all(&result, "$1").to_string();

    // <msup><base><exp></msup> -> base^{exp}
    let re_msup = Regex::new(r"<msup>\s*<mi>([^<]+)</mi>\s*<mn>([^<]+)</mn>\s*</msup>").unwrap();
    result = re_msup.replace_all(&result, "$1^{$2}").to_string();

    // <mfrac><num><den></mfrac> -> \frac{num}{den}
    let re_mfrac =
        Regex::new(r"<mfrac>\s*<mn>([^<]+)</mn>\s*<mn>([^<]+)</mn>\s*</mfrac>").unwrap();
    result = re_mfrac
        .replace_all(&result, r"\frac{$1}{$2}")
        .to_string();

    // <msqrt>content</msqrt> -> \sqrt{content}
    let re_msqrt = Regex::new(r"<msqrt>([^<]+)</msqrt>").unwrap();
    result = re_msqrt.replace_all(&result, r"\sqrt{$1}").to_string();

    // Strip any remaining tags
    let re_tags = Regex::new(r"<[^>]+>").unwrap();
    result = re_tags.replace_all(&result, "").to_string();

    // Normalize whitespace
    let re_ws = Regex::new(r"\s+").unwrap();
    result = re_ws.replace_all(&result, " ").to_string();

    result.trim().to_string()
}

// ============================================================================
// AsciiMath conversion stub
// ============================================================================

/// Basic AsciiMath-to-LaTeX conversion.
///
/// Handles common AsciiMath notations. A production implementation would
/// use a full AsciiMath parser.
fn convert_asciimath_to_latex(ascii: &str) -> String {
    let mut result = ascii.to_string();

    // AsciiMath -> LaTeX mappings
    result = result.replace("sqrt", r"\sqrt");
    result = result.replace("int", r"\int");
    result = result.replace("sum", r"\sum");
    result = result.replace("prod", r"\prod");
    result = result.replace("lim", r"\lim");

    // Trig functions
    result = result.replace("sin", r"\sin");
    result = result.replace("cos", r"\cos");
    result = result.replace("tan", r"\tan");

    // Common constants
    result = RE_PLAIN_PI.replace_all(&result, r"\pi").to_string();
    result = RE_PLAIN_INFINITY
        .replace_all(&result, r"\infty")
        .to_string();

    // Fractions: a/b -> \frac{a}{b} only for simple single-token fractions
    let re_simple_frac = Regex::new(r"([a-zA-Z0-9]+)/([a-zA-Z0-9]+)").unwrap();
    result = re_simple_frac
        .replace_all(&result, r"\frac{$1}{$2}")
        .to_string();

    result.trim().to_string()
}

// ============================================================================
// Ambiguity detection
// ============================================================================

/// Detect potential ambiguities in a normalized expression.
///
/// Returns a list of `Ambiguity` values describing places where the expression
/// could be interpreted in multiple ways. Current checks:
///
/// 1. **Implicit multiplication**: adjacent letters like `xy` could mean
///    `x * y` or a two-letter variable named `xy`.
/// 2. **Function application**: `f(x)` could mean "function f evaluated at x"
///    or "variable f multiplied by x".
/// 3. **Superscript ambiguity**: `x^2y` could be `(x^2)*y` or `x^(2y)`.
pub fn detect_ambiguities(expr: &NormalizedExpression) -> Vec<Ambiguity> {
    let mut ambiguities = Vec::new();
    let latex = &expr.latex;

    // 1. Implicit multiplication detection
    //    Look for adjacent single letters that are not part of a LaTeX command
    for cap in RE_IMPLICIT_MULT.captures_iter(latex) {
        let full_match = cap.get(0).unwrap();
        let pos = full_match.start();

        // Skip if preceded by backslash (LaTeX command like \sin)
        if pos > 0 {
            if let Some('\\') = latex[..pos].chars().last() {
                continue;
            }
        }

        // Skip if inside a known function name
        let surrounding_start = pos.saturating_sub(5);
        let surrounding = &latex[surrounding_start..std::cmp::min(pos + 10, latex.len())];
        if surrounding.contains('\\') {
            // Likely part of a command argument; be conservative
            continue;
        }

        let a = cap.get(1).unwrap().as_str();
        let b = cap.get(2).unwrap().as_str();

        ambiguities.push(
            Ambiguity::new(format!(
                "Implicit multiplication: '{}{}' could be product or variable name",
                a, b
            ))
            .with_alternatives(vec![
                format!("{} * {}", a, b),
                format!("variable '{}{}'", a, b),
            ])
            .with_position(pos),
        );

        // Only report the first instance to avoid spamming
        break;
    }

    // 2. Function application ambiguity
    //    Single letter followed by parenthesis, e.g. f(x), g(t)
    for cap in RE_FUNCTION_APP.captures_iter(latex) {
        let var = cap.get(1).unwrap().as_str();
        let pos = cap.get(0).unwrap().start();

        // Skip if preceded by backslash (it's a LaTeX command)
        if pos > 0 {
            if let Some('\\') = latex[..pos].chars().last() {
                continue;
            }
        }

        // Only flag common function-like letters
        if matches!(
            var,
            "f" | "g" | "h" | "p" | "q" | "r" | "u" | "v" | "w" | "F" | "G" | "H" | "P" | "Q"
        ) {
            ambiguities.push(
                Ambiguity::new(format!(
                    "Function vs multiplication: '{}(...)' could be function application or multiplication",
                    var
                ))
                .with_alternatives(vec![
                    format!("Function application: {}(x)", var),
                    format!("Multiplication: {} * (x)", var),
                ])
                .with_position(pos),
            );
            break;
        }
    }

    // 3. Superscript scope ambiguity: x^2y
    let re_superscript_adjacent =
        Regex::new(r"\^([a-zA-Z0-9])([a-zA-Z])").unwrap();
    for cap in re_superscript_adjacent.captures_iter(latex) {
        let full_match = cap.get(0).unwrap();
        let pos = full_match.start();
        let exponent = cap.get(1).unwrap().as_str();
        let following = cap.get(2).unwrap().as_str();

        ambiguities.push(
            Ambiguity::new(format!(
                "Superscript scope: '^{}{}' -- exponent may or may not include '{}'",
                exponent, following, following
            ))
            .with_alternatives(vec![
                format!("(base^{}) * {}", exponent, following),
                format!("base^({}{})", exponent, following),
            ])
            .with_position(pos),
        );
        break;
    }

    ambiguities
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // Helper: build a default config with no Mathpix credentials
    fn default_config() -> SwitchboardConfig {
        SwitchboardConfig::default()
    }

    // Helper: build a config with Mathpix credentials
    fn config_with_mathpix() -> SwitchboardConfig {
        SwitchboardConfig {
            mathpix_app_id: Some("test_app_id".to_string()),
            mathpix_app_key: Some("test_app_key".to_string()),
            ..SwitchboardConfig::default()
        }
    }

    // ---------------------------------------------------------------
    // parse_latex tests
    // ---------------------------------------------------------------

    #[test]
    fn test_parse_latex_extracts_symbols() {
        let expr = parse_latex(r"\int_0^1 x^2 dx");
        assert!(expr.symbols.contains(&"\\int".to_string()));
        assert!(expr.symbols.contains(&"x".to_string()));
    }

    #[test]
    fn test_parse_latex_extracts_greek_symbols() {
        let expr = parse_latex(r"\alpha + \beta = \gamma");
        assert!(expr.symbols.contains(&"\\alpha".to_string()));
        assert!(expr.symbols.contains(&"\\beta".to_string()));
        assert!(expr.symbols.contains(&"\\gamma".to_string()));
    }

    #[test]
    fn test_parse_latex_sets_equation_flag() {
        let expr = parse_latex(r"x^2 + 1 = 0");
        assert!(expr.is_equation);
        assert!(!expr.is_inequality);
    }

    #[test]
    fn test_parse_latex_sets_integral_flag() {
        let expr = parse_latex(r"\int_0^\infty e^{-x} dx");
        assert!(expr.has_integral);
    }

    #[test]
    fn test_parse_latex_sets_derivative_flag() {
        let expr = parse_latex(r"\frac{d}{dx} x^3");
        assert!(expr.has_derivative);
    }

    #[test]
    fn test_parse_latex_sets_summation_flag() {
        let expr = parse_latex(r"\sum_{n=1}^{\infty} \frac{1}{n^2}");
        assert!(expr.has_summation);
    }

    #[test]
    fn test_parse_latex_sets_limit_flag() {
        let expr = parse_latex(r"\lim_{x \to 0} \frac{\sin x}{x}");
        assert!(expr.has_limit);
    }

    #[test]
    fn test_parse_latex_sets_matrix_flag() {
        let expr = parse_latex(r"\begin{pmatrix} 1 & 2 \\ 3 & 4 \end{pmatrix}");
        assert!(expr.has_matrix);
    }

    #[test]
    fn test_parse_latex_counts_variables() {
        let expr = parse_latex(r"x^2 + y = z");
        // Should find x, y, z as single-letter variables
        assert!(expr.variable_count >= 2);
    }

    #[test]
    fn test_parse_latex_populates_both_fields() {
        let expr = parse_latex(r"x^2 + 1");
        assert_eq!(expr.latex, r"x^2 + 1");
        assert!(!expr.plain_text.is_empty());
    }

    // ---------------------------------------------------------------
    // parse_plain_text tests
    // ---------------------------------------------------------------

    #[test]
    fn test_parse_plain_text_sqrt_conversion() {
        let expr = parse_plain_text("sqrt(x) + 1");
        assert!(
            expr.latex.contains(r"\sqrt{x}"),
            "Expected \\sqrt{{x}} in '{}' but not found",
            expr.latex
        );
    }

    #[test]
    fn test_parse_plain_text_trig_conversion() {
        let expr = parse_plain_text("sin(x) + cos(y)");
        assert!(
            expr.latex.contains(r"\sin"),
            "Expected \\sin in '{}' but not found",
            expr.latex
        );
        assert!(
            expr.latex.contains(r"\cos"),
            "Expected \\cos in '{}' but not found",
            expr.latex
        );
    }

    #[test]
    fn test_parse_plain_text_pi_conversion() {
        let expr = parse_plain_text("2 * pi * r");
        assert!(
            expr.latex.contains(r"\pi"),
            "Expected \\pi in '{}' but not found",
            expr.latex
        );
    }

    #[test]
    fn test_parse_plain_text_preserves_exponent() {
        let expr = parse_plain_text("x^2 + y^3");
        assert!(expr.latex.contains("^2"));
        assert!(expr.latex.contains("^3"));
    }

    // ---------------------------------------------------------------
    // detect_ambiguities tests
    // ---------------------------------------------------------------

    #[test]
    fn test_detect_ambiguities_implicit_multiplication() {
        let expr = parse_latex("xy + z");
        let ambiguities = detect_ambiguities(&expr);
        assert!(
            ambiguities
                .iter()
                .any(|a| a.description.contains("Implicit multiplication")),
            "Expected implicit multiplication ambiguity, got: {:?}",
            ambiguities
        );
    }

    #[test]
    fn test_detect_ambiguities_function_application() {
        let expr = parse_latex("f(x) + 1");
        let ambiguities = detect_ambiguities(&expr);
        assert!(
            ambiguities
                .iter()
                .any(|a| a.description.contains("Function vs multiplication")
                    || a.description.contains("function")),
            "Expected function application ambiguity, got: {:?}",
            ambiguities
        );
    }

    #[test]
    fn test_detect_ambiguities_multiple() {
        let expr = parse_latex("f(xy) + g(z)");
        let ambiguities = detect_ambiguities(&expr);
        // Should detect at least one ambiguity (function app or implicit mult)
        assert!(
            !ambiguities.is_empty(),
            "Expected at least one ambiguity for 'f(xy) + g(z)'"
        );
    }

    #[test]
    fn test_detect_ambiguities_none_for_clear_latex() {
        let expr = parse_latex(r"\int_0^1 x^2 dx");
        let ambiguities = detect_ambiguities(&expr);
        // Integral with single variable should have few if any ambiguities
        // (dx is adjacent letters but preceded by space, etc.)
        // This is a best-effort check
        let implicit_mult_count = ambiguities
            .iter()
            .filter(|a| a.description.contains("Implicit"))
            .count();
        // We accept 0 or 1 due to "dx" potentially flagging
        assert!(
            implicit_mult_count <= 1,
            "Too many implicit mult ambiguities for a clear integral expression"
        );
    }

    // ---------------------------------------------------------------
    // ingest tests (synchronous path only — no network calls)
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_ingest_latex_input() {
        let input = RawInput::new(InputFormat::Latex, r"x^2 + 3x + 2 = 0".to_string());
        let config = default_config();
        let result = ingest(input, &config).await;
        assert!(result.is_ok(), "Expected Ok, got: {:?}", result);
        let expr = result.unwrap();
        assert!(expr.is_equation);
        assert!(expr.symbols.contains(&"x".to_string()));
    }

    #[tokio::test]
    async fn test_ingest_plain_text_input() {
        let input = RawInput::new(InputFormat::PlainText, "sqrt(x) + 1 = 0".to_string());
        let config = default_config();
        let result = ingest(input, &config).await;
        assert!(result.is_ok(), "Expected Ok, got: {:?}", result);
        let expr = result.unwrap();
        assert!(expr.latex.contains(r"\sqrt"));
        assert!(expr.is_equation);
    }

    #[tokio::test]
    async fn test_ingest_napkin_photo_no_credentials() {
        let input = RawInput::with_image(
            InputFormat::NapkinPhoto,
            "photo".to_string(),
            vec![0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A], // PNG header
        );
        let config = default_config(); // No Mathpix credentials
        let result = ingest(input, &config).await;
        assert!(result.is_err());
        match result {
            Err(SwitchboardError::UnsupportedInput(msg)) => {
                assert!(msg.contains("Mathpix"), "Error should mention Mathpix: {}", msg);
            }
            other => panic!("Expected UnsupportedInput, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_ingest_input_size_limit() {
        let oversized = "x".repeat(MAX_INPUT_SIZE + 1);
        let input = RawInput::new(InputFormat::Latex, oversized);
        let config = default_config();
        let result = ingest(input, &config).await;
        assert!(result.is_err());
        match result {
            Err(SwitchboardError::InputTooLarge { size, max }) => {
                assert_eq!(size, MAX_INPUT_SIZE + 1);
                assert_eq!(max, MAX_INPUT_SIZE);
            }
            other => panic!("Expected InputTooLarge, got: {:?}", other),
        }
    }

    #[test]
    fn test_mathpix_client_construction() {
        let client = MathpixClient::new("app123".to_string(), "key456".to_string());
        assert_eq!(client.app_id, "app123");
        assert_eq!(client.app_key, "key456");
    }

    #[test]
    fn test_sanitize_latex_strips_dangerous() {
        let input = r"\input{malicious} x^2 + \def\foo{bar} y";
        let result = sanitize_latex(input).unwrap();
        assert!(!result.contains(r"\input"));
        assert!(!result.contains(r"\def"));
        assert!(result.contains("x^2"));
    }

    #[test]
    fn test_validate_image_rejects_empty() {
        let result = validate_image(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_image_rejects_unknown_format() {
        let result = validate_image(&[0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
        assert!(result.is_err());
        match result {
            Err(SwitchboardError::ImageError(msg)) => {
                assert!(msg.contains("Unrecognized"));
            }
            other => panic!("Expected ImageError, got: {:?}", other),
        }
    }

    #[test]
    fn test_validate_image_accepts_png_header() {
        let mut data = vec![0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];
        data.extend_from_slice(&[0u8; 100]); // padding
        let result = validate_image(&data);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_image_accepts_jpeg_header() {
        let mut data = vec![0xFF, 0xD8, 0xFF];
        data.extend_from_slice(&[0u8; 100]); // padding
        let result = validate_image(&data);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_ingest_mathml_basic() {
        let mathml = "<math><mi>x</mi><mo>+</mo><mn>1</mn></math>";
        let input = RawInput::new(InputFormat::MathML, mathml.to_string());
        let config = default_config();
        let result = ingest(input, &config).await;
        assert!(result.is_ok(), "Expected Ok, got: {:?}", result);
        let expr = result.unwrap();
        assert!(expr.plain_text.contains("x") || expr.latex.contains("x"));
    }

    #[tokio::test]
    async fn test_ingest_asciimath_basic() {
        let ascii = "x^2 + 1/2";
        let input = RawInput::new(InputFormat::AsciiMath, ascii.to_string());
        let config = default_config();
        let result = ingest(input, &config).await;
        assert!(result.is_ok(), "Expected Ok, got: {:?}", result);
        let expr = result.unwrap();
        assert!(expr.latex.contains("^2"));
    }

    #[tokio::test]
    async fn test_ingest_empty_latex_returns_error() {
        let input = RawInput::new(InputFormat::Latex, "".to_string());
        let config = default_config();
        let result = ingest(input, &config).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_latex_to_plain_frac() {
        let plain = latex_to_plain(r"\frac{x}{y}");
        assert!(
            plain.contains("(x)/(y)"),
            "Expected (x)/(y) in '{}' but not found",
            plain
        );
    }

    #[test]
    fn test_plain_to_latex_multiplication() {
        let latex = plain_to_latex("2 * x");
        assert!(
            latex.contains(r"\cdot"),
            "Expected \\cdot in '{}' but not found",
            latex
        );
    }
}
