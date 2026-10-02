// Marabunta - Licensed under the MIT License.
//! Phase 2: The JCL-to-Polars AST Transpiler
//!
//! Replaces the old SQL transpiler. Parses native Job Control Language (JCL) 
//! and maps it to a WASM-executable Python/Polars DAG execution string.

use std::fmt::Write;

#[derive(Debug)]
pub enum JclError {
    SyntaxError(String),
}

impl std::fmt::Display for JclError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JclError::SyntaxError(msg) => write!(f, "JCL Syntax Error: {}", msg),
        }
    }
}
impl std::error::Error for JclError {}

#[derive(Debug)]
pub struct JclAst {
    pub map_func: String,
    pub stream_source: String,
    pub stream_format: String,
    pub reduce_by: Option<String>,
    pub shuffle_mesh: Option<String>,
    pub max_bid_usd: Option<f64>,
}

pub struct JclParser;

impl JclParser {
    pub fn parse(jcl: &str) -> Result<JclAst, JclError> {
        let input = jcl.to_uppercase();
        
        // Very basic string tokenization for the WMD architectural proof
        let map_func = extract_between(jcl, "MAP ", " PULL").unwrap_or("unknown_func".into());
        let stream_source = extract_between(jcl, "source => '", "',").unwrap_or("unknown_source".into());
        let reduce_by = extract_between(&input, "REDUCE BY ", " GIVEN").or_else(|| extract_between(&input, "REDUCE BY ", ""));
        let max_bid = extract_between(&input, "max_bid_usd=", ")").and_then(|s| s.parse::<f64>().ok());

        Ok(JclAst {
            map_func,
            stream_source,
            stream_format: "parquet".into(),
            reduce_by,
            shuffle_mesh: None,
            max_bid_usd: max_bid,
        })
    }
}

fn extract_between(text: &str, start_delim: &str, end_delim: &str) -> Option<String> {
    let start = text.find(start_delim)? + start_delim.len();
    if end_delim.is_empty() {
        Some(text[start..].trim().to_string())
    } else {
        let end = text[start..].find(end_delim)?;
        Some(text[start..start + end].trim().to_string())
    }
}

pub fn transpile_jcl_to_polars(jcl: &str, job_name: &str) -> Result<String, JclError> {
    let ast = JclParser::parse(jcl)?;
    let mut py = String::new();

    // The Python Polars Transpilation
    writeln!(&mut py, "import polars as pl").unwrap();
    writeln!(&mut py, "from marabunta import workflow, python_task, S3Source
").unwrap();

    writeln!(&mut py, "@python_task(memory_min=1024)").unwrap();
    writeln!(&mut py, "def map_chunk(chunk_uri):").unwrap();
    writeln!(&mut py, "    # Transpiled JCL: PULL FROM STREAM").unwrap();
    writeln!(&mut py, "    df = pl.read_{}(chunk_uri)", ast.stream_format).unwrap();
    writeln!(&mut py, "    return {}(df).to_dicts()
", ast.map_func).unwrap();

    if let Some(ref group_col) = ast.reduce_by {
        writeln!(&mut py, "@python_task(memory_min=8192)").unwrap();
        writeln!(&mut py, "def reduce_aggregations(results):").unwrap();
        writeln!(&mut py, "    # Transpiled JCL: REDUCE BY {}", group_col).unwrap();
        writeln!(&mut py, "    combined = pl.concat([pl.DataFrame(r) for r in results])").unwrap();
        writeln!(&mut py, "    return combined.group_by('{}').agg(pl.all().sum()).to_dicts()
", group_col).unwrap();
    }

    writeln!(&mut py, "@workflow(name='{}', bid_usd={:?})", job_name, ast.max_bid_usd.unwrap_or(0.01)).unwrap();
    writeln!(&mut py, "def execute_jcl():").unwrap();
    writeln!(&mut py, "    dataset = S3Source('{}').get_chunks()", ast.stream_source).unwrap();
    writeln!(&mut py, "    mapped = [map_chunk(c) for c in dataset]").unwrap();
    
    if ast.reduce_by.is_some() {
        writeln!(&mut py, "    return reduce_aggregations(mapped)").unwrap();
    } else {
        writeln!(&mut py, "    return mapped").unwrap();
    }

    Ok(py)
}
