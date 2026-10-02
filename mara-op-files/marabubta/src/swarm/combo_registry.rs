#![allow(dead_code)]
// Marabunta - Licensed under the MIT License.

//! Pre-packaged compute module registry for the Marabunta Swarm.
//!
//! Defines all 22 CR combo modules described in the documentation. Each
//! module has metadata, parameter schemas, verification defaults, and
//! example requests that the API layer can serve to clients.
//!
//! # Module categories
//!
//! | Category    | Modules |
//! |-------------|---------|
//! | computation | MonteCarlo, Brute, Evolve, Crunch, Bootstrap, Replicate |
//! | data        | Classify, Validate, Match, Scan, Grade |
//! | media       | Render, Transcode, OCR |
//! | science     | Dock, Sweep |
//! | ai          | Inference, Embed, Tune, Eval |
//! | finance     | Risk, Price |
//! | enterprise  | Route |

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tracing::info;

use super::types::{ChunkStrategy, NodeClass, VerificationStrategy};

// ============================================================================
// Public types returned to the API layer
// ============================================================================

/// Summary info returned by `list_modules()`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleInfo {
    pub id: String,
    pub name: String,
    pub category: String,
    pub description: String,
    pub version: String,
}

/// Detailed info returned by `get_module()`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleDetail {
    pub id: String,
    pub name: String,
    pub category: String,
    pub description: String,
    pub version: String,
    pub supported_input_types: Vec<String>,
    pub output_types: Vec<String>,
    pub default_chunk_strategy: String,
    pub verification_supported: bool,
}

/// Usage example returned by `get_module_example()`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleExample {
    pub request: serde_json::Value,
    pub curl: String,
    pub notes: String,
}

// ============================================================================
// Internal module definition
// ============================================================================

/// Full module definition stored in the registry.
#[derive(Debug, Clone)]
struct ModuleDef {
    id: String,
    name: String,
    category: String,
    description: String,
    version: String,
    supported_input_types: Vec<String>,
    output_types: Vec<String>,
    default_verification: VerificationStrategy,
    default_chunk_strategy: ChunkStrategy,
    min_node_class: NodeClass,
    supports_streaming: bool,
    supports_statistical_verification: bool,
    cloud_comparison_base_usd: Option<f64>,
    example: ModuleExample,
}

impl ModuleDef {
    fn to_info(&self) -> ModuleInfo {
        ModuleInfo {
            id: self.id.clone(),
            name: self.name.clone(),
            category: self.category.clone(),
            description: self.description.clone(),
            version: self.version.clone(),
        }
    }

    fn to_detail(&self) -> ModuleDetail {
        let chunk_str = match &self.default_chunk_strategy {
            ChunkStrategy::PerLine => "per_line".to_string(),
            ChunkStrategy::PerFile => "per_file".to_string(),
            ChunkStrategy::PerNBytes { bytes } => format!("per_{}bytes", bytes),
            ChunkStrategy::Fixed { count } => format!("fixed_{}", count),
            ChunkStrategy::ParameterSweep { .. } => "parameter_sweep".to_string(),
            ChunkStrategy::Single => "single".to_string(),
        };
        ModuleDetail {
            id: self.id.clone(),
            name: self.name.clone(),
            category: self.category.clone(),
            description: self.description.clone(),
            version: self.version.clone(),
            supported_input_types: self.supported_input_types.clone(),
            output_types: self.output_types.clone(),
            default_chunk_strategy: chunk_str,
            verification_supported: !matches!(self.default_verification, VerificationStrategy::None),
        }
    }
}

// ============================================================================
// ComboRegistry
// ============================================================================

/// Registry of all pre-packaged combo modules.
pub struct ComboRegistry {
    modules: HashMap<String, ModuleDef>,
    /// Category → list of module IDs for fast category listing.
    categories: HashMap<String, Vec<String>>,
    /// Insertion order for stable listing.
    order: Vec<String>,
}

impl ComboRegistry {
    /// Create a new registry pre-populated with all 22 modules.
    pub fn new() -> Self {
        let mut registry = Self {
            modules: HashMap::new(),
            categories: HashMap::new(),
            order: Vec::new(),
        };
        registry.populate_builtin_modules();
        info!(modules = registry.modules.len(), "combo registry initialised");
        registry
    }

    // ----------------------------------------------------------------
    // API methods (called by api.rs handlers)
    // ----------------------------------------------------------------

    /// List all modules (summary form).
    pub fn list_modules(&self) -> Vec<ModuleInfo> {
        self.order
            .iter()
            .filter_map(|id| self.modules.get(id))
            .map(|m| m.to_info())
            .collect()
    }

    /// Get detailed info for a module by ID.
    pub fn get_module(&self, id: &str) -> Option<ModuleDetail> {
        self.modules.get(id).map(|m| m.to_detail())
    }

    /// Get a usage example for a module.
    pub fn get_module_example(&self, id: &str) -> Option<ModuleExample> {
        self.modules.get(id).map(|m| m.example.clone())
    }

    /// List all categories (sorted).
    pub fn categories(&self) -> Vec<String> {
        let mut cats: Vec<String> = self.categories.keys().cloned().collect();
        cats.sort();
        cats
    }

    /// List modules in a specific category.
    pub fn modules_in_category(&self, category: &str) -> Vec<ModuleInfo> {
        self.categories
            .get(category)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.modules.get(id))
                    .map(|m| m.to_info())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get the number of registered modules.
    pub fn module_count(&self) -> usize {
        self.modules.len()
    }

    /// Check if a module ID exists.
    pub fn has_module(&self, id: &str) -> bool {
        self.modules.contains_key(id)
    }

    /// Get the default verification strategy for a module.
    pub fn default_verification(&self, id: &str) -> Option<VerificationStrategy> {
        self.modules.get(id).map(|m| m.default_verification.clone())
    }

    /// Get the minimum node class for a module.
    pub fn min_node_class(&self, id: &str) -> Option<NodeClass> {
        self.modules.get(id).map(|m| m.min_node_class)
    }

    // ----------------------------------------------------------------
    // Registration helper
    // ----------------------------------------------------------------

    fn register(&mut self, def: ModuleDef) {
        let id = def.id.clone();
        let category = def.category.clone();
        self.categories
            .entry(category)
            .or_default()
            .push(id.clone());
        self.order.push(id.clone());
        self.modules.insert(id, def);
    }

    // ----------------------------------------------------------------
    // Builtin module definitions
    // ----------------------------------------------------------------

    fn populate_builtin_modules(&mut self) {
        // ========== Computation ==========

        self.register(ModuleDef {
            id: "cr.montecarlo".into(),
            name: "CR.MonteCarlo".into(),
            category: "computation".into(),
            description: "Monte Carlo simulation — embarrassingly parallel random sampling for risk analysis, physics, finance.".into(),
            version: "1.0.0".into(),
            supported_input_types: vec!["json".into(), "csv".into(), "python".into()],
            output_types: vec!["json".into(), "csv".into()],
            default_verification: VerificationStrategy::Statistical { outlier_tolerance: 3.0 },
            default_chunk_strategy: ChunkStrategy::Fixed { count: 1000 },
            min_node_class: NodeClass::Edge,
            supports_streaming: true,
            supports_statistical_verification: true,
            cloud_comparison_base_usd: Some(340.0),
            example: ModuleExample {
                request: serde_json::json!({
                    "module": "cr.montecarlo",
                    "simulations": 10_000_000,
                    "script": "simulate.py",
                    "params": { "volatility": 0.2, "drift": 0.05, "time_horizon": 1.0 }
                }),
                curl: "curl -X POST http://api/v1/combo/jobs -F 'params={\"module\":\"cr.montecarlo\",\"simulations\":10000000}' -F 'script=@simulate.py'".into(),
                notes: "Each chunk runs N/chunks simulations independently. Results are aggregated via statistical merge.".into(),
            },
        });

        self.register(ModuleDef {
            id: "cr.brute".into(),
            name: "CR.Brute".into(),
            category: "computation".into(),
            description: "Brute-force search — exhaustive key space exploration, hash cracking, combinatorial search.".into(),
            version: "1.0.0".into(),
            supported_input_types: vec!["json".into(), "binary".into()],
            output_types: vec!["json".into()],
            default_verification: VerificationStrategy::Redundant { replicas: 2 },
            default_chunk_strategy: ChunkStrategy::ParameterSweep { params: vec![] },
            min_node_class: NodeClass::Edge,
            supports_streaming: true,
            supports_statistical_verification: false,
            cloud_comparison_base_usd: Some(500.0),
            example: ModuleExample {
                request: serde_json::json!({
                    "module": "cr.brute",
                    "target_hash": "5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8",
                    "charset": "abcdefghijklmnopqrstuvwxyz0123456789",
                    "max_length": 8
                }),
                curl: "curl -X POST http://api/v1/combo/jobs -H 'Content-Type: application/json' -d '{\"module\":\"cr.brute\",\"target_hash\":\"...\",\"max_length\":8}'".into(),
                notes: "Search space is partitioned by prefix ranges. First match terminates all remaining chunks.".into(),
            },
        });

        self.register(ModuleDef {
            id: "cr.evolve".into(),
            name: "CR.Evolve".into(),
            category: "computation".into(),
            description: "Evolutionary / genetic algorithms — population-parallel optimisation with crossover and mutation.".into(),
            version: "1.0.0".into(),
            supported_input_types: vec!["json".into(), "python".into(), "wasm".into()],
            output_types: vec!["json".into()],
            default_verification: VerificationStrategy::Statistical { outlier_tolerance: 2.5 },
            default_chunk_strategy: ChunkStrategy::Fixed { count: 100 },
            min_node_class: NodeClass::Light,
            supports_streaming: true,
            supports_statistical_verification: true,
            cloud_comparison_base_usd: Some(280.0),
            example: ModuleExample {
                request: serde_json::json!({
                    "module": "cr.evolve",
                    "population_size": 10000,
                    "generations": 500,
                    "fitness_fn": "fitness.py",
                    "crossover_rate": 0.7,
                    "mutation_rate": 0.01
                }),
                curl: "curl -X POST http://api/v1/combo/jobs -F 'params={\"module\":\"cr.evolve\",\"population_size\":10000}' -F 'fitness=@fitness.py'".into(),
                notes: "Population is partitioned across nodes. Periodic migration exchanges top individuals between islands.".into(),
            },
        });

        self.register(ModuleDef {
            id: "cr.crunch".into(),
            name: "CR.Crunch".into(),
            category: "computation".into(),
            description: "Number crunching — matrix operations, FFT, linear algebra on large datasets.".into(),
            version: "1.0.0".into(),
            supported_input_types: vec!["csv".into(), "binary".into(), "numpy".into()],
            output_types: vec!["csv".into(), "binary".into(), "numpy".into()],
            default_verification: VerificationStrategy::Redundant { replicas: 2 },
            default_chunk_strategy: ChunkStrategy::PerNBytes { bytes: 64 * 1024 * 1024 },
            min_node_class: NodeClass::Light,
            supports_streaming: false,
            supports_statistical_verification: false,
            cloud_comparison_base_usd: Some(420.0),
            example: ModuleExample {
                request: serde_json::json!({
                    "module": "cr.crunch",
                    "operation": "matrix_multiply",
                    "input_a": "matrix_a.npy",
                    "input_b": "matrix_b.npy"
                }),
                curl: "curl -X POST http://api/v1/combo/jobs -F 'params={\"module\":\"cr.crunch\",\"operation\":\"matrix_multiply\"}' -F 'a=@matrix_a.npy' -F 'b=@matrix_b.npy'".into(),
                notes: "Matrices are block-partitioned. Each node computes a sub-block of the result.".into(),
            },
        });

        self.register(ModuleDef {
            id: "cr.bootstrap".into(),
            name: "CR.Bootstrap".into(),
            category: "computation".into(),
            description: "Statistical bootstrapping — resample-and-compute confidence intervals at scale.".into(),
            version: "1.0.0".into(),
            supported_input_types: vec!["csv".into(), "json".into()],
            output_types: vec!["json".into(), "csv".into()],
            default_verification: VerificationStrategy::Statistical { outlier_tolerance: 3.0 },
            default_chunk_strategy: ChunkStrategy::Fixed { count: 500 },
            min_node_class: NodeClass::Edge,
            supports_streaming: true,
            supports_statistical_verification: true,
            cloud_comparison_base_usd: Some(180.0),
            example: ModuleExample {
                request: serde_json::json!({
                    "module": "cr.bootstrap",
                    "resamples": 100000,
                    "statistic": "median",
                    "confidence": 0.95,
                    "data_file": "observations.csv"
                }),
                curl: "curl -X POST http://api/v1/combo/jobs -F 'params={\"module\":\"cr.bootstrap\",\"resamples\":100000}' -F 'data=@observations.csv'".into(),
                notes: "Each chunk generates N/chunks resamples. Aggregator merges bootstrap distributions.".into(),
            },
        });

        self.register(ModuleDef {
            id: "cr.replicate".into(),
            name: "CR.Replicate".into(),
            category: "computation".into(),
            description: "Experiment replication — run the same experiment N times with different seeds for reproducibility.".into(),
            version: "1.0.0".into(),
            supported_input_types: vec!["python".into(), "shell".into(), "wasm".into()],
            output_types: vec!["json".into(), "csv".into()],
            default_verification: VerificationStrategy::Statistical { outlier_tolerance: 3.0 },
            default_chunk_strategy: ChunkStrategy::Fixed { count: 100 },
            min_node_class: NodeClass::Edge,
            supports_streaming: true,
            supports_statistical_verification: true,
            cloud_comparison_base_usd: Some(150.0),
            example: ModuleExample {
                request: serde_json::json!({
                    "module": "cr.replicate",
                    "replications": 10000,
                    "script": "experiment.py",
                    "seed_strategy": "sequential"
                }),
                curl: "curl -X POST http://api/v1/combo/jobs -F 'params={\"module\":\"cr.replicate\",\"replications\":10000}' -F 'script=@experiment.py'".into(),
                notes: "Each chunk runs replications/chunks experiments with deterministic seeds.".into(),
            },
        });

        // ========== Data ==========

        self.register(ModuleDef {
            id: "cr.classify".into(),
            name: "CR.Classify".into(),
            category: "data".into(),
            description: "Data classification — apply labels, categories, or tags to large datasets in parallel.".into(),
            version: "1.0.0".into(),
            supported_input_types: vec!["csv".into(), "json".into(), "jsonl".into()],
            output_types: vec!["csv".into(), "json".into(), "jsonl".into()],
            default_verification: VerificationStrategy::SpotCheck { check_rate: 0.05 },
            default_chunk_strategy: ChunkStrategy::PerLine,
            min_node_class: NodeClass::Edge,
            supports_streaming: true,
            supports_statistical_verification: false,
            cloud_comparison_base_usd: Some(95.0),
            example: ModuleExample {
                request: serde_json::json!({
                    "module": "cr.classify",
                    "model": "rules.json",
                    "input": "data.csv",
                    "label_column": "category"
                }),
                curl: "curl -X POST http://api/v1/combo/jobs -F 'params={\"module\":\"cr.classify\"}' -F 'model=@rules.json' -F 'data=@data.csv'".into(),
                notes: "Input is split by line. Each node applies the model to its chunk. Results are concatenated.".into(),
            },
        });

        self.register(ModuleDef {
            id: "cr.validate".into(),
            name: "CR.Validate".into(),
            category: "data".into(),
            description: "Data validation — schema checks, constraint validation, anomaly detection across massive datasets.".into(),
            version: "1.0.0".into(),
            supported_input_types: vec!["csv".into(), "json".into(), "parquet".into()],
            output_types: vec!["json".into()],
            default_verification: VerificationStrategy::Redundant { replicas: 2 },
            default_chunk_strategy: ChunkStrategy::PerLine,
            min_node_class: NodeClass::Edge,
            supports_streaming: true,
            supports_statistical_verification: false,
            cloud_comparison_base_usd: Some(75.0),
            example: ModuleExample {
                request: serde_json::json!({
                    "module": "cr.validate",
                    "schema": "schema.json",
                    "input": "records.csv",
                    "rules": ["not_null:email", "format:date:iso8601", "range:age:0:150"]
                }),
                curl: "curl -X POST http://api/v1/combo/jobs -F 'params={\"module\":\"cr.validate\"}' -F 'schema=@schema.json' -F 'data=@records.csv'".into(),
                notes: "Each chunk validates a slice of rows. Aggregator merges violation reports.".into(),
            },
        });

        self.register(ModuleDef {
            id: "cr.match".into(),
            name: "CR.Match".into(),
            category: "data".into(),
            description: "Record matching — fuzzy deduplication, entity resolution, link detection across datasets.".into(),
            version: "1.0.0".into(),
            supported_input_types: vec!["csv".into(), "json".into()],
            output_types: vec!["json".into(), "csv".into()],
            default_verification: VerificationStrategy::SpotCheck { check_rate: 0.05 },
            default_chunk_strategy: ChunkStrategy::Fixed { count: 200 },
            min_node_class: NodeClass::Light,
            supports_streaming: false,
            supports_statistical_verification: false,
            cloud_comparison_base_usd: Some(210.0),
            example: ModuleExample {
                request: serde_json::json!({
                    "module": "cr.match",
                    "dataset_a": "customers.csv",
                    "dataset_b": "prospects.csv",
                    "match_fields": ["name", "email", "phone"],
                    "threshold": 0.85
                }),
                curl: "curl -X POST http://api/v1/combo/jobs -F 'params={\"module\":\"cr.match\",\"threshold\":0.85}' -F 'a=@customers.csv' -F 'b=@prospects.csv'".into(),
                notes: "Dataset A is broadcast; dataset B is chunked. Each node compares its B-chunk against all of A.".into(),
            },
        });

        self.register(ModuleDef {
            id: "cr.scan".into(),
            name: "CR.Scan".into(),
            category: "data".into(),
            description: "Pattern scanning — regex, YARA, or custom rules across large file collections.".into(),
            version: "1.0.0".into(),
            supported_input_types: vec!["binary".into(), "text".into(), "archive".into()],
            output_types: vec!["json".into()],
            default_verification: VerificationStrategy::Redundant { replicas: 2 },
            default_chunk_strategy: ChunkStrategy::PerFile,
            min_node_class: NodeClass::Edge,
            supports_streaming: true,
            supports_statistical_verification: false,
            cloud_comparison_base_usd: Some(120.0),
            example: ModuleExample {
                request: serde_json::json!({
                    "module": "cr.scan",
                    "rules_file": "patterns.yara",
                    "input_archive": "samples.tar.gz"
                }),
                curl: "curl -X POST http://api/v1/combo/jobs -F 'params={\"module\":\"cr.scan\"}' -F 'rules=@patterns.yara' -F 'archive=@samples.tar.gz'".into(),
                notes: "Archive is extracted and files distributed one-per-chunk. Each node applies all rules to its file.".into(),
            },
        });

        self.register(ModuleDef {
            id: "cr.grade".into(),
            name: "CR.Grade".into(),
            category: "data".into(),
            description: "Automated grading — score submissions, essays, code, or responses against rubrics at scale.".into(),
            version: "1.0.0".into(),
            supported_input_types: vec!["json".into(), "csv".into(), "text".into()],
            output_types: vec!["json".into(), "csv".into()],
            default_verification: VerificationStrategy::Redundant { replicas: 2 },
            default_chunk_strategy: ChunkStrategy::PerLine,
            min_node_class: NodeClass::Light,
            supports_streaming: true,
            supports_statistical_verification: false,
            cloud_comparison_base_usd: Some(85.0),
            example: ModuleExample {
                request: serde_json::json!({
                    "module": "cr.grade",
                    "rubric": "rubric.json",
                    "submissions": "submissions.jsonl",
                    "grading_script": "grade.py"
                }),
                curl: "curl -X POST http://api/v1/combo/jobs -F 'params={\"module\":\"cr.grade\"}' -F 'rubric=@rubric.json' -F 'submissions=@submissions.jsonl'".into(),
                notes: "Each submission is an independent grading task. Rubric is broadcast to all nodes.".into(),
            },
        });

        // ========== Media ==========

        self.register(ModuleDef {
            id: "cr.render".into(),
            name: "CR.Render".into(),
            category: "media".into(),
            description: "3D rendering — distribute frame rendering for Blender, POV-Ray, or custom ray tracers.".into(),
            version: "1.0.0".into(),
            supported_input_types: vec!["blend".into(), "pov".into(), "scene".into()],
            output_types: vec!["png".into(), "exr".into(), "mp4".into()],
            default_verification: VerificationStrategy::Redundant { replicas: 2 },
            default_chunk_strategy: ChunkStrategy::Fixed { count: 250 },
            min_node_class: NodeClass::Standard,
            supports_streaming: true,
            supports_statistical_verification: false,
            cloud_comparison_base_usd: Some(680.0),
            example: ModuleExample {
                request: serde_json::json!({
                    "module": "cr.render",
                    "scene": "scene.blend",
                    "frames": "1-1000",
                    "resolution": "1920x1080",
                    "samples": 128
                }),
                curl: "curl -X POST http://api/v1/combo/jobs -F 'params={\"module\":\"cr.render\",\"frames\":\"1-1000\"}' -F 'scene=@scene.blend'".into(),
                notes: "Each chunk renders a range of frames. Enterprise nodes get larger ranges. Final assembly stitches into video.".into(),
            },
        });

        self.register(ModuleDef {
            id: "cr.transcode".into(),
            name: "CR.Transcode".into(),
            category: "media".into(),
            description: "Media transcoding — parallel video/audio format conversion, resolution scaling, codec changes.".into(),
            version: "1.0.0".into(),
            supported_input_types: vec!["mp4".into(), "mkv".into(), "avi".into(), "wav".into(), "flac".into()],
            output_types: vec!["mp4".into(), "webm".into(), "mp3".into(), "aac".into()],
            default_verification: VerificationStrategy::SpotCheck { check_rate: 0.10 },
            default_chunk_strategy: ChunkStrategy::Fixed { count: 100 },
            min_node_class: NodeClass::Light,
            supports_streaming: true,
            supports_statistical_verification: false,
            cloud_comparison_base_usd: Some(250.0),
            example: ModuleExample {
                request: serde_json::json!({
                    "module": "cr.transcode",
                    "input": "video.mp4",
                    "output_format": "webm",
                    "resolution": "1280x720",
                    "bitrate": "2M"
                }),
                curl: "curl -X POST http://api/v1/combo/jobs -F 'params={\"module\":\"cr.transcode\",\"output_format\":\"webm\"}' -F 'video=@video.mp4'".into(),
                notes: "Video is split into GOP-aligned segments. Each node transcodes one segment. Segments are concatenated.".into(),
            },
        });

        self.register(ModuleDef {
            id: "cr.ocr".into(),
            name: "CR.OCR".into(),
            category: "media".into(),
            description: "Optical character recognition — extract text from images, PDFs, scanned documents at scale.".into(),
            version: "1.0.0".into(),
            supported_input_types: vec!["pdf".into(), "png".into(), "jpg".into(), "tiff".into()],
            output_types: vec!["json".into(), "text".into(), "hocr".into()],
            default_verification: VerificationStrategy::Redundant { replicas: 2 },
            default_chunk_strategy: ChunkStrategy::PerFile,
            min_node_class: NodeClass::Light,
            supports_streaming: true,
            supports_statistical_verification: false,
            cloud_comparison_base_usd: Some(160.0),
            example: ModuleExample {
                request: serde_json::json!({
                    "module": "cr.ocr",
                    "input_archive": "documents.zip",
                    "language": "eng",
                    "output_format": "json"
                }),
                curl: "curl -X POST http://api/v1/combo/jobs -F 'params={\"module\":\"cr.ocr\",\"language\":\"eng\"}' -F 'docs=@documents.zip'".into(),
                notes: "Each page/image is an independent OCR task. Results include bounding boxes and confidence scores.".into(),
            },
        });

        // ========== Science ==========

        self.register(ModuleDef {
            id: "cr.dock".into(),
            name: "CR.Dock".into(),
            category: "science".into(),
            description: "Molecular docking — screen compound libraries against protein targets in parallel.".into(),
            version: "1.0.0".into(),
            supported_input_types: vec!["pdb".into(), "sdf".into(), "mol2".into(), "smiles".into()],
            output_types: vec!["json".into(), "sdf".into()],
            default_verification: VerificationStrategy::Redundant { replicas: 2 },
            default_chunk_strategy: ChunkStrategy::PerLine,
            min_node_class: NodeClass::Light,
            supports_streaming: true,
            supports_statistical_verification: false,
            cloud_comparison_base_usd: Some(520.0),
            example: ModuleExample {
                request: serde_json::json!({
                    "module": "cr.dock",
                    "target": "protein.pdb",
                    "library": "compounds.sdf",
                    "scoring_function": "vina",
                    "exhaustiveness": 8
                }),
                curl: "curl -X POST http://api/v1/combo/jobs -F 'params={\"module\":\"cr.dock\",\"scoring_function\":\"vina\"}' -F 'target=@protein.pdb' -F 'library=@compounds.sdf'".into(),
                notes: "Compound library is split across nodes. Each node docks its compounds against the broadcast target. Top-K results are merged.".into(),
            },
        });

        self.register(ModuleDef {
            id: "cr.sweep".into(),
            name: "CR.Sweep".into(),
            category: "science".into(),
            description: "Parameter sweep — explore multi-dimensional parameter spaces for simulations and models.".into(),
            version: "1.0.0".into(),
            supported_input_types: vec!["json".into(), "yaml".into(), "python".into()],
            output_types: vec!["json".into(), "csv".into()],
            default_verification: VerificationStrategy::Statistical { outlier_tolerance: 3.0 },
            default_chunk_strategy: ChunkStrategy::ParameterSweep { params: vec![] },
            min_node_class: NodeClass::Edge,
            supports_streaming: true,
            supports_statistical_verification: true,
            cloud_comparison_base_usd: Some(390.0),
            example: ModuleExample {
                request: serde_json::json!({
                    "module": "cr.sweep",
                    "script": "simulate.py",
                    "parameters": {
                        "temperature": { "min": 200, "max": 400, "step": 10 },
                        "pressure": { "min": 1, "max": 100, "step": 5 }
                    }
                }),
                curl: "curl -X POST http://api/v1/combo/jobs -F 'params={\"module\":\"cr.sweep\"}' -F 'script=@simulate.py'".into(),
                notes: "Cartesian product of parameters is computed. Each combination is an independent chunk.".into(),
            },
        });

        // ========== AI ==========

        self.register(ModuleDef {
            id: "cr.inference".into(),
            name: "CR.Inference".into(),
            category: "ai".into(),
            description: "Batch inference — run ML models on large datasets in parallel.".into(),
            version: "1.0.0".into(),
            supported_input_types: vec!["json".into(), "csv".into(), "images".into()],
            output_types: vec!["json".into(), "csv".into()],
            default_verification: VerificationStrategy::SpotCheck { check_rate: 0.05 },
            default_chunk_strategy: ChunkStrategy::PerLine,
            min_node_class: NodeClass::Light,
            supports_streaming: true,
            supports_statistical_verification: false,
            cloud_comparison_base_usd: Some(450.0),
            example: ModuleExample {
                request: serde_json::json!({
                    "module": "cr.inference",
                    "model": "classifier.onnx",
                    "input": "images.tar.gz",
                    "batch_size": 32
                }),
                curl: "curl -X POST http://api/v1/combo/jobs -F 'params={\"module\":\"cr.inference\",\"batch_size\":32}' -F 'model=@classifier.onnx' -F 'data=@images.tar.gz'".into(),
                notes: "Model is broadcast. Data is chunked. Each node runs inference on its chunk. Results are concatenated.".into(),
            },
        });

        self.register(ModuleDef {
            id: "cr.embed".into(),
            name: "CR.Embed".into(),
            category: "ai".into(),
            description: "Embedding generation — compute vector embeddings for text, images, or audio at scale.".into(),
            version: "1.0.0".into(),
            supported_input_types: vec!["text".into(), "json".into(), "csv".into()],
            output_types: vec!["json".into(), "numpy".into()],
            default_verification: VerificationStrategy::SpotCheck { check_rate: 0.05 },
            default_chunk_strategy: ChunkStrategy::PerLine,
            min_node_class: NodeClass::Light,
            supports_streaming: true,
            supports_statistical_verification: false,
            cloud_comparison_base_usd: Some(310.0),
            example: ModuleExample {
                request: serde_json::json!({
                    "module": "cr.embed",
                    "model": "sentence-transformers/all-MiniLM-L6-v2",
                    "input": "documents.jsonl",
                    "dimensions": 384
                }),
                curl: "curl -X POST http://api/v1/combo/jobs -F 'params={\"module\":\"cr.embed\",\"model\":\"all-MiniLM-L6-v2\"}' -F 'data=@documents.jsonl'".into(),
                notes: "Each line is an independent embedding task. Model weights are cached on nodes after first download.".into(),
            },
        });

        self.register(ModuleDef {
            id: "cr.tune".into(),
            name: "CR.Tune".into(),
            category: "ai".into(),
            description: "Hyperparameter tuning — grid search, random search, or Bayesian optimisation across the swarm.".into(),
            version: "1.0.0".into(),
            supported_input_types: vec!["python".into(), "json".into()],
            output_types: vec!["json".into()],
            default_verification: VerificationStrategy::Statistical { outlier_tolerance: 2.5 },
            default_chunk_strategy: ChunkStrategy::ParameterSweep { params: vec![] },
            min_node_class: NodeClass::Standard,
            supports_streaming: true,
            supports_statistical_verification: true,
            cloud_comparison_base_usd: Some(800.0),
            example: ModuleExample {
                request: serde_json::json!({
                    "module": "cr.tune",
                    "script": "train.py",
                    "search_space": {
                        "learning_rate": { "type": "log_uniform", "low": 1e-5, "high": 1e-1 },
                        "batch_size": { "type": "choice", "values": [16, 32, 64, 128] },
                        "dropout": { "type": "uniform", "low": 0.0, "high": 0.5 }
                    },
                    "trials": 200,
                    "metric": "val_accuracy",
                    "direction": "maximize"
                }),
                curl: "curl -X POST http://api/v1/combo/jobs -F 'params={\"module\":\"cr.tune\",\"trials\":200}' -F 'train=@train.py'".into(),
                notes: "Each trial is an independent training run with sampled hyperparameters. Best result wins.".into(),
            },
        });

        self.register(ModuleDef {
            id: "cr.eval".into(),
            name: "CR.Eval".into(),
            category: "ai".into(),
            description: "Model evaluation — benchmark models against test suites, compute metrics in parallel.".into(),
            version: "1.0.0".into(),
            supported_input_types: vec!["json".into(), "csv".into(), "python".into()],
            output_types: vec!["json".into()],
            default_verification: VerificationStrategy::Redundant { replicas: 2 },
            default_chunk_strategy: ChunkStrategy::PerFile,
            min_node_class: NodeClass::Light,
            supports_streaming: true,
            supports_statistical_verification: false,
            cloud_comparison_base_usd: Some(260.0),
            example: ModuleExample {
                request: serde_json::json!({
                    "module": "cr.eval",
                    "model": "model.onnx",
                    "test_suite": "tests.jsonl",
                    "metrics": ["accuracy", "f1", "auc"]
                }),
                curl: "curl -X POST http://api/v1/combo/jobs -F 'params={\"module\":\"cr.eval\"}' -F 'model=@model.onnx' -F 'tests=@tests.jsonl'".into(),
                notes: "Test suite is chunked. Each node evaluates the model on its chunk. Metrics are aggregated.".into(),
            },
        });

        // ========== Finance ==========

        self.register(ModuleDef {
            id: "cr.risk".into(),
            name: "CR.Risk".into(),
            category: "finance".into(),
            description: "Risk analysis — Value-at-Risk, stress testing, scenario analysis across portfolios.".into(),
            version: "1.0.0".into(),
            supported_input_types: vec!["json".into(), "csv".into()],
            output_types: vec!["json".into(), "csv".into()],
            default_verification: VerificationStrategy::Statistical { outlier_tolerance: 3.0 },
            default_chunk_strategy: ChunkStrategy::Fixed { count: 1000 },
            min_node_class: NodeClass::Edge,
            supports_streaming: true,
            supports_statistical_verification: true,
            cloud_comparison_base_usd: Some(380.0),
            example: ModuleExample {
                request: serde_json::json!({
                    "module": "cr.risk",
                    "portfolio": "portfolio.json",
                    "scenarios": 1_000_000,
                    "var_confidence": 0.99,
                    "time_horizon_days": 10
                }),
                curl: "curl -X POST http://api/v1/combo/jobs -F 'params={\"module\":\"cr.risk\",\"scenarios\":1000000}' -F 'portfolio=@portfolio.json'".into(),
                notes: "Each chunk simulates N/chunks market scenarios. VaR is computed from the merged distribution.".into(),
            },
        });

        self.register(ModuleDef {
            id: "cr.price".into(),
            name: "CR.Price".into(),
            category: "finance".into(),
            description: "Option pricing — Black-Scholes, binomial trees, or Monte Carlo pricing for derivatives.".into(),
            version: "1.0.0".into(),
            supported_input_types: vec!["json".into(), "csv".into()],
            output_types: vec!["json".into(), "csv".into()],
            default_verification: VerificationStrategy::Statistical { outlier_tolerance: 3.0 },
            default_chunk_strategy: ChunkStrategy::PerLine,
            min_node_class: NodeClass::Edge,
            supports_streaming: true,
            supports_statistical_verification: true,
            cloud_comparison_base_usd: Some(290.0),
            example: ModuleExample {
                request: serde_json::json!({
                    "module": "cr.price",
                    "instruments": "options.csv",
                    "pricing_model": "monte_carlo",
                    "paths": 100000,
                    "steps": 252
                }),
                curl: "curl -X POST http://api/v1/combo/jobs -F 'params={\"module\":\"cr.price\",\"pricing_model\":\"monte_carlo\"}' -F 'instruments=@options.csv'".into(),
                notes: "Each instrument is priced independently. Monte Carlo paths are further parallelised per instrument.".into(),
            },
        });

        // ========== Enterprise ==========

        self.register(ModuleDef {
            id: "cr.route".into(),
            name: "CR.Route".into(),
            category: "enterprise".into(),
            description: "Route optimisation — vehicle routing, logistics planning, delivery scheduling at scale.".into(),
            version: "1.0.0".into(),
            supported_input_types: vec!["json".into(), "csv".into()],
            output_types: vec!["json".into()],
            default_verification: VerificationStrategy::Redundant { replicas: 2 },
            default_chunk_strategy: ChunkStrategy::Fixed { count: 50 },
            min_node_class: NodeClass::Standard,
            supports_streaming: false,
            supports_statistical_verification: false,
            cloud_comparison_base_usd: Some(470.0),
            example: ModuleExample {
                request: serde_json::json!({
                    "module": "cr.route",
                    "depots": "depots.json",
                    "deliveries": "deliveries.csv",
                    "constraints": { "max_distance_km": 500, "max_weight_kg": 10000, "time_windows": true }
                }),
                curl: "curl -X POST http://api/v1/combo/jobs -F 'params={\"module\":\"cr.route\"}' -F 'depots=@depots.json' -F 'deliveries=@deliveries.csv'".into(),
                notes: "Problem is decomposed geographically. Each chunk solves a regional sub-problem. Solutions are merged and refined.".into(),
            },
        });
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_populated() {
        let reg = ComboRegistry::new();
        assert_eq!(reg.module_count(), 23);
    }

    #[test]
    fn test_list_modules() {
        let reg = ComboRegistry::new();
        let modules = reg.list_modules();
        assert_eq!(modules.len(), 23);
        assert_eq!(modules[0].id, "cr.montecarlo");
    }

    #[test]
    fn test_get_module() {
        let reg = ComboRegistry::new();
        let detail = reg.get_module("cr.montecarlo").unwrap();
        assert_eq!(detail.name, "CR.MonteCarlo");
        assert_eq!(detail.category, "computation");
        assert!(detail.verification_supported);
    }

    #[test]
    fn test_get_module_not_found() {
        let reg = ComboRegistry::new();
        assert!(reg.get_module("cr.nonexistent").is_none());
    }

    #[test]
    fn test_get_example() {
        let reg = ComboRegistry::new();
        let example = reg.get_module_example("cr.dock").unwrap();
        assert!(!example.curl.is_empty());
        assert!(!example.notes.is_empty());
    }

    #[test]
    fn test_categories() {
        let reg = ComboRegistry::new();
        let cats = reg.categories();
        assert!(cats.contains(&"computation".to_string()));
        assert!(cats.contains(&"data".to_string()));
        assert!(cats.contains(&"media".to_string()));
        assert!(cats.contains(&"science".to_string()));
        assert!(cats.contains(&"ai".to_string()));
        assert!(cats.contains(&"finance".to_string()));
        assert!(cats.contains(&"enterprise".to_string()));
        assert_eq!(cats.len(), 7);
    }

    #[test]
    fn test_modules_in_category() {
        let reg = ComboRegistry::new();
        let comp = reg.modules_in_category("computation");
        assert_eq!(comp.len(), 6); // MonteCarlo, Brute, Evolve, Crunch, Bootstrap, Replicate
    }

    #[test]
    fn test_has_module() {
        let reg = ComboRegistry::new();
        assert!(reg.has_module("cr.render"));
        assert!(!reg.has_module("cr.imaginary"));
    }

    #[test]
    fn test_default_verification() {
        let reg = ComboRegistry::new();
        let v = reg.default_verification("cr.montecarlo").unwrap();
        assert!(matches!(v, VerificationStrategy::Statistical { .. }));
    }

    #[test]
    fn test_min_node_class() {
        let reg = ComboRegistry::new();
        assert_eq!(reg.min_node_class("cr.render"), Some(NodeClass::Standard));
        assert_eq!(reg.min_node_class("cr.montecarlo"), Some(NodeClass::Edge));
    }

    #[test]
    fn test_all_modules_have_examples() {
        let reg = ComboRegistry::new();
        for m in reg.list_modules() {
            assert!(
                reg.get_module_example(&m.id).is_some(),
                "module {} missing example",
                m.id
            );
        }
    }

    #[test]
    fn test_all_modules_have_details() {
        let reg = ComboRegistry::new();
        for m in reg.list_modules() {
            let detail = reg.get_module(&m.id).expect(&format!("module {} missing detail", m.id));
            assert!(!detail.supported_input_types.is_empty());
            assert!(!detail.output_types.is_empty());
        }
    }
}
