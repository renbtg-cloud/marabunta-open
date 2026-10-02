// Marabunta - Licensed under the MIT License.
//! Distributed Word Count Example (Map-Reduce Style)
//!
//! This example demonstrates a classic distributed computing pattern: word count
//! using a map-reduce approach. The job processes large text files across multiple
//! workers, counting word frequencies, and aggregates the results.
//!
//! # Architecture
//!
//! ```text
//! Input Files
//!     |
//!     v
//! +----------+     +----------+     +----------+
//! | Mapper 1 |     | Mapper 2 |     | Mapper 3 |   <- Map Phase: count words locally
//! +----------+     +----------+     +----------+
//!     |                |                |
//!     +--------+-------+--------+-------+
//!              |                |
//!              v                v
//!         +---------+      +---------+
//!         | Reducer |      | Reducer |             <- Reduce Phase: merge counts
//!         +---------+      +---------+
//!              |                |
//!              +-------+--------+
//!                      |
//!                      v
//!              +---------------+
//!              | Final Results |
//!              +---------------+
//! ```
//!
//! # Running this example
//!
//! ```bash
//! # Submit to the cluster with input files
//! marabunta submit examples/word_count.rs --runtime native \
//!     --input "s3://bucket/texts/*.txt" \
//!     --workers 16
//!
//! # Or run locally for testing
//! cargo run --example word_count
//! ```

use marabunta_compute::common::{Job, JobId, Task, TaskId, TaskPayload};
use marabunta_compute::sdk::{
    marabunta_checkpoint_save, marabunta_get_worker_count, marabunta_get_worker_rank, marabunta_log_info,
    marabunta_progress_set, marabunta_progress_set_stage, marabunta_result_set_json, marabunta_task_cleanup,
    marabunta_task_init_ex, marabunta_yield, MARABUNTA_YIELD_ABORT, MARABUNTA_YIELD_CONTINUE,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ffi::CString;

// ============================================================================
// Configuration and Data Types
// ============================================================================

/// Configuration for the word count job
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WordCountConfig {
    /// Input file paths or patterns
    pub input_paths: Vec<String>,
    /// Minimum word length to count
    pub min_word_length: usize,
    /// Whether to convert to lowercase
    pub case_insensitive: bool,
    /// Stop words to exclude
    pub stop_words: Vec<String>,
    /// Number of top words to return
    pub top_n: usize,
}

impl Default for WordCountConfig {
    fn default() -> Self {
        Self {
            input_paths: vec![],
            min_word_length: 2,
            case_insensitive: true,
            stop_words: vec![
                "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for", "of", "with",
                "by", "from", "as", "is", "was", "are", "were", "been", "be", "have", "has", "had",
                "do", "does", "did", "will", "would", "could", "should", "may", "might", "must",
                "it", "this", "that", "these", "those", "i", "you", "he", "she", "we", "they",
            ]
            .into_iter()
            .map(String::from)
            .collect(),
            top_n: 100,
        }
    }
}

/// Result from a mapper task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapperResult {
    pub worker_rank: u32,
    pub input_file: String,
    pub total_words: u64,
    pub unique_words: u64,
    pub word_counts: HashMap<String, u64>,
    pub elapsed_ms: u64,
}

/// Result from a reducer task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReducerResult {
    pub reducer_id: u32,
    pub merged_from: Vec<u32>,
    pub word_counts: HashMap<String, u64>,
}

/// Final aggregated result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WordCountResult {
    pub total_words: u64,
    pub unique_words: u64,
    pub top_words: Vec<(String, u64)>,
    pub files_processed: u64,
    pub elapsed_ms: u64,
}

// ============================================================================
// Text Processing Utilities
// ============================================================================

/// Extract words from text, applying filtering rules
fn extract_words(text: &str, config: &WordCountConfig) -> Vec<String> {
    let stop_words: std::collections::HashSet<_> = config.stop_words.iter().collect();

    text.split(|c: char| !c.is_alphabetic())
        .filter(|word| word.len() >= config.min_word_length)
        .map(|word| {
            if config.case_insensitive {
                word.to_lowercase()
            } else {
                word.to_string()
            }
        })
        .filter(|word| !stop_words.contains(word))
        .collect()
}

/// Count word frequencies in a list of words
fn count_words(words: Vec<String>) -> HashMap<String, u64> {
    let mut counts = HashMap::new();
    for word in words {
        *counts.entry(word).or_insert(0) += 1;
    }
    counts
}

/// Merge multiple word count maps
fn merge_counts(maps: Vec<HashMap<String, u64>>) -> HashMap<String, u64> {
    let mut merged = HashMap::new();
    for map in maps {
        for (word, count) in map {
            *merged.entry(word).or_insert(0) += count;
        }
    }
    merged
}

/// Get top N words by count
fn get_top_words(counts: &HashMap<String, u64>, n: usize) -> Vec<(String, u64)> {
    let mut sorted: Vec<_> = counts.iter().map(|(k, v)| (k.clone(), *v)).collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1));
    sorted.truncate(n);
    sorted
}

// ============================================================================
// Mapper Task
// ============================================================================

/// Run the mapper phase for a single input file
///
/// The mapper reads a text file, counts words locally, and outputs
/// the word frequencies for this chunk.
pub fn run_mapper(
    input_text: &str,
    input_file: &str,
    config: &WordCountConfig,
) -> Result<MapperResult, String> {
    let start = std::time::Instant::now();

    // Initialize Marabunta context
    let checkpoint_dir = CString::new("/tmp/marabunta_checkpoints").unwrap();
    let ctx = unsafe { marabunta_task_init_ex(1, 0, 1, checkpoint_dir.as_ptr()) };

    if ctx.is_null() {
        return Err("Failed to initialize Marabunta context".to_string());
    }

    let worker_rank = unsafe { marabunta_get_worker_rank(ctx) };

    // Set stage
    let stage = CString::new("Mapping words").unwrap();
    unsafe { marabunta_progress_set_stage(ctx, stage.as_ptr()) };

    let log_msg =
        CString::new(format!("Mapper {} processing: {}", worker_rank, input_file)).unwrap();
    unsafe { marabunta_log_info(ctx, log_msg.as_ptr()) };

    // Process text in chunks for better memory efficiency
    let chunk_size = 1_000_000; // 1MB chunks
    let mut all_counts: HashMap<String, u64> = HashMap::new();
    let total_len = input_text.len();
    let mut processed = 0;

    for (i, chunk) in input_text.as_bytes().chunks(chunk_size).enumerate() {
        // Check for abort/pause
        let yield_result = unsafe { marabunta_yield(ctx) };
        if yield_result == MARABUNTA_YIELD_ABORT {
            let msg = CString::new("Mapper aborted").unwrap();
            unsafe { marabunta_log_info(ctx, msg.as_ptr()) };
            unsafe { marabunta_task_cleanup(ctx) };
            return Err("Aborted".to_string());
        }

        // Convert chunk to string (handle UTF-8 boundaries)
        let chunk_str = String::from_utf8_lossy(chunk);

        // Extract and count words
        let words = extract_words(&chunk_str, config);
        let chunk_counts = count_words(words);

        // Merge into total counts
        for (word, count) in chunk_counts {
            *all_counts.entry(word).or_insert(0) += count;
        }

        processed += chunk.len();
        let progress = ((processed as f64 / total_len as f64) * 100.0) as u8;
        unsafe { marabunta_progress_set(ctx, progress) };

        // Periodic checkpoint
        if i > 0 && i % 10 == 0 {
            let checkpoint_data = serde_json::to_vec(&all_counts).unwrap();
            unsafe { marabunta_checkpoint_save(ctx, checkpoint_data.as_ptr(), checkpoint_data.len()) };
        }
    }

    let total_words: u64 = all_counts.values().sum();
    let unique_words = all_counts.len() as u64;

    let result = MapperResult {
        worker_rank,
        input_file: input_file.to_string(),
        total_words,
        unique_words,
        word_counts: all_counts,
        elapsed_ms: start.elapsed().as_millis() as u64,
    };

    // Set result
    let result_json = serde_json::to_string(&result).unwrap();
    let result_cstr = CString::new(result_json).unwrap();
    unsafe { marabunta_result_set_json(ctx, result_cstr.as_ptr()) };

    let log_msg = CString::new(format!(
        "Mapper {} complete: {} total words, {} unique",
        worker_rank, total_words, unique_words
    ))
    .unwrap();
    unsafe { marabunta_log_info(ctx, log_msg.as_ptr()) };

    unsafe { marabunta_progress_set(ctx, 100) };
    unsafe { marabunta_task_cleanup(ctx) };

    Ok(result)
}

// ============================================================================
// Reducer Task
// ============================================================================

/// Run the reducer phase to merge mapper results
///
/// The reducer takes outputs from multiple mappers and merges them
/// into a single combined word count.
pub fn run_reducer(
    mapper_results: Vec<MapperResult>,
    reducer_id: u32,
) -> Result<ReducerResult, String> {
    // Initialize Marabunta context
    let checkpoint_dir = CString::new("/tmp/marabunta_checkpoints").unwrap();
    let ctx = unsafe { marabunta_task_init_ex(2, reducer_id, 1, checkpoint_dir.as_ptr()) };

    if ctx.is_null() {
        return Err("Failed to initialize Marabunta context".to_string());
    }

    let stage = CString::new("Reducing word counts").unwrap();
    unsafe { marabunta_progress_set_stage(ctx, stage.as_ptr()) };

    let log_msg = CString::new(format!(
        "Reducer {} merging {} mapper results",
        reducer_id,
        mapper_results.len()
    ))
    .unwrap();
    unsafe { marabunta_log_info(ctx, log_msg.as_ptr()) };

    let merged_from: Vec<u32> = mapper_results.iter().map(|r| r.worker_rank).collect();
    let maps: Vec<HashMap<String, u64>> =
        mapper_results.into_iter().map(|r| r.word_counts).collect();

    let merged = merge_counts(maps);

    let result = ReducerResult {
        reducer_id,
        merged_from,
        word_counts: merged,
    };

    let result_json = serde_json::to_string(&result).unwrap();
    let result_cstr = CString::new(result_json).unwrap();
    unsafe { marabunta_result_set_json(ctx, result_cstr.as_ptr()) };

    unsafe { marabunta_progress_set(ctx, 100) };
    unsafe { marabunta_task_cleanup(ctx) };

    Ok(result)
}

// ============================================================================
// Final Aggregation
// ============================================================================

/// Aggregate all reducer results into final output
pub fn aggregate_final(
    reducer_results: Vec<ReducerResult>,
    config: &WordCountConfig,
    total_files: u64,
    total_elapsed_ms: u64,
) -> WordCountResult {
    let maps: Vec<HashMap<String, u64>> =
        reducer_results.into_iter().map(|r| r.word_counts).collect();

    let merged = merge_counts(maps);
    let total_words: u64 = merged.values().sum();
    let unique_words = merged.len() as u64;
    let top_words = get_top_words(&merged, config.top_n);

    WordCountResult {
        total_words,
        unique_words,
        top_words,
        files_processed: total_files,
        elapsed_ms: total_elapsed_ms,
    }
}

// ============================================================================
// Job Creation Helpers
// ============================================================================

/// Create a word count job for cluster submission
pub fn create_word_count_job(input_files: Vec<String>, config: WordCountConfig) -> Job {
    let mut job = Job::new("word-count");
    job.priority = 100;
    job.metadata = serde_json::json!({
        "input_files": input_files,
        "config": config,
        "description": "Distributed word count (map-reduce)"
    });

    // Create mapper tasks (one per input file)
    let mapper_task_ids: Vec<TaskId> = input_files
        .iter()
        .enumerate()
        .map(|(i, file)| {
            let mut task = Task::new(
                job.id,
                TaskPayload::Function {
                    name: "word_count_mapper".to_string(),
                    input: serde_json::to_vec(&(file.clone(), config.clone())).unwrap(),
                },
            );
            task.name = Some(format!("mapper-{}", i));
            let id = task.id;
            job.tasks.push(id);
            id
        })
        .collect();

    // Create reducer task (depends on all mappers)
    let mut reducer_task = Task::with_dependencies(
        job.id,
        TaskPayload::Function {
            name: "word_count_reducer".to_string(),
            input: vec![],
        },
        mapper_task_ids,
    );
    reducer_task.name = Some("reducer".to_string());
    job.tasks.push(reducer_task.id);

    job
}

// ============================================================================
// Main Entry Point (Demo)
// ============================================================================

fn main() {
    println!("=== Marabunta Compute: Distributed Word Count ===\n");

    let config = WordCountConfig::default();

    // Sample texts for demonstration
    let sample_texts = vec![
        (
            "sample1.txt",
            r#"
            The quick brown fox jumps over the lazy dog.
            This pangram contains every letter of the alphabet.
            Quick brown foxes are known for their agility.
            The lazy dog remained unimpressed by the fox's antics.
            "#,
        ),
        (
            "sample2.txt",
            r#"
            Programming is the art of telling computers what to do.
            Distributed computing spreads work across many machines.
            Parallel processing can significantly speed up computation.
            Word count is a classic example of distributed computing.
            Map reduce patterns enable scalable data processing.
            "#,
        ),
        (
            "sample3.txt",
            r#"
            Rust is a systems programming language focused on safety.
            Memory safety without garbage collection is a key feature.
            Concurrency without data races makes Rust unique.
            The borrow checker ensures memory safety at compile time.
            "#,
        ),
    ];

    println!("Configuration:");
    println!("  Min word length: {}", config.min_word_length);
    println!("  Case insensitive: {}", config.case_insensitive);
    println!("  Stop words: {} words", config.stop_words.len());
    println!("  Top N results: {}", config.top_n);
    println!();

    let start = std::time::Instant::now();

    // Phase 1: Map
    println!("=== Map Phase ===");
    let mut mapper_results = Vec::new();
    for (filename, text) in &sample_texts {
        println!("Processing: {}", filename);
        match run_mapper(text, filename, &config) {
            Ok(result) => {
                println!(
                    "  {} total words, {} unique words",
                    result.total_words, result.unique_words
                );
                mapper_results.push(result);
            }
            Err(e) => {
                eprintln!("  Error: {}", e);
            }
        }
    }
    println!();

    // Phase 2: Reduce
    println!("=== Reduce Phase ===");
    let reducer_result = match run_reducer(mapper_results, 0) {
        Ok(result) => {
            println!("Merged {} mapper results", result.merged_from.len());
            result
        }
        Err(e) => {
            eprintln!("Reducer error: {}", e);
            return;
        }
    };
    println!();

    // Phase 3: Final aggregation
    let total_elapsed = start.elapsed().as_millis() as u64;
    let final_result = aggregate_final(
        vec![reducer_result],
        &config,
        sample_texts.len() as u64,
        total_elapsed,
    );

    // Display results
    println!("=== Results ===");
    println!("Files processed:  {}", final_result.files_processed);
    println!("Total words:      {}", final_result.total_words);
    println!("Unique words:     {}", final_result.unique_words);
    println!("Elapsed time:     {} ms", final_result.elapsed_ms);
    println!();

    println!("Top 20 words:");
    for (i, (word, count)) in final_result.top_words.iter().take(20).enumerate() {
        println!("  {:2}. {:15} {:5}", i + 1, word, count);
    }
    println!();

    // Show JSON result
    println!("JSON Result (top 10):");
    let mut display_result = final_result.clone();
    display_result.top_words.truncate(10);
    println!("{}", serde_json::to_string_pretty(&display_result).unwrap());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_words() {
        let config = WordCountConfig::default();
        let text = "Hello, World! This is a TEST.";
        let words = extract_words(text, &config);

        assert!(words.contains(&"hello".to_string()));
        assert!(words.contains(&"world".to_string()));
        assert!(words.contains(&"test".to_string()));
        // "a" and "is" are stop words or too short
        assert!(!words.contains(&"a".to_string()));
    }

    #[test]
    fn test_count_words() {
        let words = vec![
            "hello".to_string(),
            "world".to_string(),
            "hello".to_string(),
            "rust".to_string(),
            "hello".to_string(),
        ];
        let counts = count_words(words);

        assert_eq!(counts.get("hello"), Some(&3));
        assert_eq!(counts.get("world"), Some(&1));
        assert_eq!(counts.get("rust"), Some(&1));
    }

    #[test]
    fn test_merge_counts() {
        let map1: HashMap<String, u64> = [("hello".to_string(), 5), ("world".to_string(), 3)]
            .into_iter()
            .collect();
        let map2: HashMap<String, u64> = [("hello".to_string(), 2), ("rust".to_string(), 7)]
            .into_iter()
            .collect();

        let merged = merge_counts(vec![map1, map2]);

        assert_eq!(merged.get("hello"), Some(&7));
        assert_eq!(merged.get("world"), Some(&3));
        assert_eq!(merged.get("rust"), Some(&7));
    }

    #[test]
    fn test_get_top_words() {
        let counts: HashMap<String, u64> = [
            ("a".to_string(), 100),
            ("b".to_string(), 50),
            ("c".to_string(), 200),
            ("d".to_string(), 25),
        ]
        .into_iter()
        .collect();

        let top = get_top_words(&counts, 2);

        assert_eq!(top.len(), 2);
        assert_eq!(top[0], ("c".to_string(), 200));
        assert_eq!(top[1], ("a".to_string(), 100));
    }
}
