// Marabunta - Licensed under the MIT License.
//! Backup types and data structures
//!
//! This module defines the core types for the Marabunta Compute backup system,
//! including backup metadata, types, and target configurations.

use base64::prelude::*;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::PathBuf;

/// Unique identifier for a backup
pub type BackupId = String;

/// Backup metadata and configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Backup {
    /// Unique backup identifier (format: YYYYMMDD_HHMMSS_<random>)
    pub id: BackupId,
    /// When the backup was created
    pub timestamp: DateTime<Utc>,
    /// Type of backup (full, incremental, snapshot)
    pub backup_type: BackupType,
    /// Size of the backup in bytes
    pub size_bytes: u64,
    /// Where the backup is stored
    pub location: BackupLocation,
    /// Current status of the backup
    pub status: BackupStatus,
    /// SHA-256 checksum of the backup file
    pub checksum: Option<String>,
    /// Description or notes about the backup
    pub description: Option<String>,
    /// Namespaces included in this backup
    pub namespaces: Vec<String>,
    /// Parent backup ID (for incremental backups)
    pub parent_backup_id: Option<BackupId>,
    /// Cluster state version at backup time
    pub cluster_version: Option<u64>,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

impl Backup {
    /// Create a new backup with the given type
    pub fn new(backup_type: BackupType, location: BackupLocation) -> Self {
        let timestamp = Utc::now();
        let id = format!(
            "{}_{:08x}",
            timestamp.format("%Y%m%d_%H%M%S"),
            rand::random::<u32>()
        );

        Self {
            id,
            timestamp,
            backup_type,
            size_bytes: 0,
            location,
            status: BackupStatus::InProgress,
            checksum: None,
            description: None,
            namespaces: Vec::new(),
            parent_backup_id: None,
            cluster_version: None,
            metadata: HashMap::new(),
        }
    }

    /// Set the backup description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Set the parent backup (for incremental backups)
    pub fn with_parent(mut self, parent_id: BackupId) -> Self {
        self.parent_backup_id = Some(parent_id);
        self
    }

    /// Mark the backup as completed with size and checksum
    pub fn complete(&mut self, size_bytes: u64, checksum: String) {
        self.size_bytes = size_bytes;
        self.checksum = Some(checksum);
        self.status = BackupStatus::Completed;
    }

    /// Mark the backup as failed
    pub fn fail(&mut self, error: impl Into<String>) {
        self.status = BackupStatus::Failed;
        self.metadata.insert("error".to_string(), error.into());
    }

    /// Check if this backup is complete and valid
    pub fn is_valid(&self) -> bool {
        matches!(self.status, BackupStatus::Completed) && self.checksum.is_some()
    }

    /// Get the backup file path for local backups
    pub fn file_path(&self) -> Option<PathBuf> {
        match &self.location {
            BackupLocation::Local { path } => Some(PathBuf::from(path)),
            _ => None,
        }
    }
}

/// Type of backup operation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackupType {
    /// Complete backup of all cluster state
    Full,
    /// Backup only changes since last backup
    Incremental,
    /// Point-in-time snapshot (similar to full but with write pause)
    Snapshot,
}

impl BackupType {
    /// Check if this backup type requires a parent backup
    pub fn requires_parent(&self) -> bool {
        matches!(self, BackupType::Incremental)
    }
}

impl std::fmt::Display for BackupType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackupType::Full => write!(f, "full"),
            BackupType::Incremental => write!(f, "incremental"),
            BackupType::Snapshot => write!(f, "snapshot"),
        }
    }
}

impl std::str::FromStr for BackupType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "full" => Ok(BackupType::Full),
            "incremental" | "incr" => Ok(BackupType::Incremental),
            "snapshot" | "snap" => Ok(BackupType::Snapshot),
            _ => Err(format!("Unknown backup type: {}", s)),
        }
    }
}

/// Target location for backup storage
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BackupTarget {
    /// Local filesystem storage
    Local {
        /// Directory path for backup storage
        path: PathBuf,
    },
    /// Amazon S3 bucket
    S3 {
        /// S3 bucket name
        bucket: String,
        /// Optional prefix within the bucket
        prefix: Option<String>,
        /// AWS region
        region: String,
    },
    /// Google Cloud Storage bucket
    GCS {
        /// GCS bucket name
        bucket: String,
        /// Optional prefix within the bucket
        prefix: Option<String>,
    },
}

impl BackupTarget {
    /// Create a local backup target
    pub fn local(path: impl Into<PathBuf>) -> Self {
        BackupTarget::Local { path: path.into() }
    }

    /// Create an S3 backup target
    pub fn s3(bucket: impl Into<String>, region: impl Into<String>) -> Self {
        BackupTarget::S3 {
            bucket: bucket.into(),
            prefix: None,
            region: region.into(),
        }
    }

    /// Create a GCS backup target
    pub fn gcs(bucket: impl Into<String>) -> Self {
        BackupTarget::GCS {
            bucket: bucket.into(),
            prefix: None,
        }
    }

    /// Get the backup file location for a given backup ID
    pub fn location_for(&self, backup_id: &str) -> BackupLocation {
        match self {
            BackupTarget::Local { path } => BackupLocation::Local {
                path: path
                    .join(format!("{}.marabunta.bak.gz", backup_id))
                    .to_string_lossy()
                    .to_string(),
            },
            BackupTarget::S3 {
                bucket,
                prefix,
                region,
            } => {
                let key = match prefix {
                    Some(p) => format!("{}/{}.marabunta.bak.gz", p, backup_id),
                    None => format!("{}.marabunta.bak.gz", backup_id),
                };
                BackupLocation::S3 {
                    bucket: bucket.clone(),
                    key,
                    region: region.clone(),
                }
            }
            BackupTarget::GCS { bucket, prefix } => {
                let object = match prefix {
                    Some(p) => format!("{}/{}.marabunta.bak.gz", p, backup_id),
                    None => format!("{}.marabunta.bak.gz", backup_id),
                };
                BackupLocation::GCS {
                    bucket: bucket.clone(),
                    object,
                }
            }
        }
    }
}

impl Default for BackupTarget {
    fn default() -> Self {
        BackupTarget::Local {
            path: PathBuf::from("./backups"),
        }
    }
}

/// Actual location where a backup is stored
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BackupLocation {
    /// Local filesystem
    Local {
        /// Full path to the backup file
        path: String,
    },
    /// Amazon S3
    S3 {
        /// S3 bucket name
        bucket: String,
        /// Object key
        key: String,
        /// AWS region
        region: String,
    },
    /// Google Cloud Storage
    GCS {
        /// GCS bucket name
        bucket: String,
        /// Object name
        object: String,
    },
}

impl std::fmt::Display for BackupLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackupLocation::Local { path } => write!(f, "local://{}", path),
            BackupLocation::S3 { bucket, key, .. } => write!(f, "s3://{}/{}", bucket, key),
            BackupLocation::GCS { bucket, object } => write!(f, "gs://{}/{}", bucket, object),
        }
    }
}

/// Status of a backup operation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackupStatus {
    /// Backup is currently being created
    InProgress,
    /// Backup completed successfully
    Completed,
    /// Backup failed
    Failed,
    /// Backup is being deleted
    Deleting,
    /// Backup was deleted
    Deleted,
}

impl std::fmt::Display for BackupStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackupStatus::InProgress => write!(f, "in_progress"),
            BackupStatus::Completed => write!(f, "completed"),
            BackupStatus::Failed => write!(f, "failed"),
            BackupStatus::Deleting => write!(f, "deleting"),
            BackupStatus::Deleted => write!(f, "deleted"),
        }
    }
}

/// Data structure representing the backup file contents
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupData {
    /// Version of the backup format
    pub format_version: u32,
    /// When the backup was created
    pub created_at: DateTime<Utc>,
    /// Backup metadata
    pub backup: Backup,
    /// Exported data by namespace
    pub data: HashMap<String, NamespaceData>,
}

impl BackupData {
    /// Current backup format version
    pub const FORMAT_VERSION: u32 = 1;

    /// Create a new backup data structure
    pub fn new(backup: Backup) -> Self {
        Self {
            format_version: Self::FORMAT_VERSION,
            created_at: Utc::now(),
            backup,
            data: HashMap::new(),
        }
    }

    /// Add data for a namespace
    pub fn add_namespace(&mut self, namespace: String, data: NamespaceData) {
        self.data.insert(namespace, data);
    }

    /// Calculate the SHA-256 checksum of the serialized data
    pub fn calculate_checksum(&self) -> Result<String, serde_json::Error> {
        let json = serde_json::to_vec(self)?;
        let mut hasher = Sha256::new();
        hasher.update(&json);
        Ok(format!("{:x}", hasher.finalize()))
    }
}

/// Data for a single namespace within a backup
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceData {
    /// Namespace name
    pub namespace: String,
    /// Number of items in this namespace
    pub item_count: usize,
    /// Key-value pairs (keys are stored, values are base64-encoded)
    pub items: Vec<BackupItem>,
}

impl NamespaceData {
    /// Create a new namespace data container
    pub fn new(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            item_count: 0,
            items: Vec::new(),
        }
    }

    /// Add an item to the namespace
    pub fn add_item(&mut self, key: String, value: Vec<u8>) {
        self.items.push(BackupItem {
            key,
            value: BASE64_STANDARD.encode(&value),
        });
        self.item_count = self.items.len();
    }
}

/// A single key-value item in a backup
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupItem {
    /// Key
    pub key: String,
    /// Base64-encoded value
    pub value: String,
}

impl BackupItem {
    /// Decode the value from base64
    pub fn decode_value(&self) -> Result<Vec<u8>, base64::DecodeError> {
        BASE64_STANDARD.decode(&self.value)
    }
}

/// Options for restore operations
#[derive(Debug, Clone, Default)]
pub struct RestoreOptions {
    /// Clear existing data before restoring
    pub clear_existing: bool,
    /// Only restore specific namespaces (empty = all)
    pub namespaces: Vec<String>,
    /// Skip checksum validation (not recommended)
    pub skip_checksum: bool,
    /// Dry run - validate but don't actually restore
    pub dry_run: bool,
}

impl RestoreOptions {
    /// Create default restore options
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable clearing existing data
    pub fn with_clear_existing(mut self) -> Self {
        self.clear_existing = true;
        self
    }

    /// Limit restore to specific namespaces
    pub fn with_namespaces(mut self, namespaces: Vec<String>) -> Self {
        self.namespaces = namespaces;
        self
    }

    /// Enable dry run mode
    pub fn with_dry_run(mut self) -> Self {
        self.dry_run = true;
        self
    }
}

/// Result of a restore operation
#[derive(Debug, Clone)]
pub struct RestoreResult {
    /// Backup ID that was restored
    pub backup_id: BackupId,
    /// Number of namespaces restored
    pub namespaces_restored: usize,
    /// Number of items restored per namespace
    pub items_restored: HashMap<String, usize>,
    /// Total items restored
    pub total_items: usize,
    /// Duration of restore operation
    pub duration_ms: u64,
    /// Warnings during restore (non-fatal issues)
    pub warnings: Vec<String>,
}

impl RestoreResult {
    /// Create a new restore result
    pub fn new(backup_id: BackupId) -> Self {
        Self {
            backup_id,
            namespaces_restored: 0,
            items_restored: HashMap::new(),
            total_items: 0,
            duration_ms: 0,
            warnings: Vec::new(),
        }
    }

    /// Record items restored for a namespace
    pub fn record_namespace(&mut self, namespace: String, item_count: usize) {
        self.items_restored.insert(namespace, item_count);
        self.total_items += item_count;
        self.namespaces_restored += 1;
    }

    /// Add a warning message
    pub fn add_warning(&mut self, warning: impl Into<String>) {
        self.warnings.push(warning.into());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backup_creation() {
        let backup = Backup::new(
            BackupType::Full,
            BackupLocation::Local {
                path: "/tmp/backup.gz".to_string(),
            },
        );

        assert!(!backup.id.is_empty());
        assert_eq!(backup.backup_type, BackupType::Full);
        assert_eq!(backup.status, BackupStatus::InProgress);
        assert!(backup.checksum.is_none());
    }

    #[test]
    fn test_backup_complete() {
        let mut backup = Backup::new(
            BackupType::Full,
            BackupLocation::Local {
                path: "/tmp/backup.gz".to_string(),
            },
        );

        backup.complete(1024, "abc123".to_string());

        assert_eq!(backup.status, BackupStatus::Completed);
        assert_eq!(backup.size_bytes, 1024);
        assert_eq!(backup.checksum, Some("abc123".to_string()));
        assert!(backup.is_valid());
    }

    #[test]
    fn test_backup_fail() {
        let mut backup = Backup::new(
            BackupType::Full,
            BackupLocation::Local {
                path: "/tmp/backup.gz".to_string(),
            },
        );

        backup.fail("Test error");

        assert_eq!(backup.status, BackupStatus::Failed);
        assert_eq!(
            backup.metadata.get("error"),
            Some(&"Test error".to_string())
        );
        assert!(!backup.is_valid());
    }

    #[test]
    fn test_backup_type_parsing() {
        assert_eq!("full".parse::<BackupType>().unwrap(), BackupType::Full);
        assert_eq!(
            "incremental".parse::<BackupType>().unwrap(),
            BackupType::Incremental
        );
        assert_eq!(
            "incr".parse::<BackupType>().unwrap(),
            BackupType::Incremental
        );
        assert_eq!(
            "snapshot".parse::<BackupType>().unwrap(),
            BackupType::Snapshot
        );
        assert!("invalid".parse::<BackupType>().is_err());
    }

    #[test]
    fn test_backup_target_location() {
        let local = BackupTarget::local("/backups");
        let location = local.location_for("20240101_120000_12345678");

        if let BackupLocation::Local { path } = location {
            assert!(path.contains("20240101_120000_12345678"));
            assert!(path.ends_with(".marabunta.bak.gz"));
        } else {
            panic!("Expected local location");
        }
    }

    #[test]
    fn test_s3_location() {
        let s3 = BackupTarget::s3("my-bucket", "us-west-2");
        let location = s3.location_for("backup123");

        if let BackupLocation::S3 {
            bucket,
            key,
            region,
        } = location
        {
            assert_eq!(bucket, "my-bucket");
            assert_eq!(key, "backup123.marabunta.bak.gz");
            assert_eq!(region, "us-west-2");
        } else {
            panic!("Expected S3 location");
        }
    }

    #[test]
    fn test_backup_location_display() {
        let local = BackupLocation::Local {
            path: "/var/backups/test.gz".to_string(),
        };
        assert_eq!(local.to_string(), "local:///var/backups/test.gz");

        let s3 = BackupLocation::S3 {
            bucket: "bucket".to_string(),
            key: "key.gz".to_string(),
            region: "us-east-1".to_string(),
        };
        assert_eq!(s3.to_string(), "s3://bucket/key.gz");

        let gcs = BackupLocation::GCS {
            bucket: "bucket".to_string(),
            object: "object.gz".to_string(),
        };
        assert_eq!(gcs.to_string(), "gs://bucket/object.gz");
    }

    #[test]
    fn test_namespace_data() {
        let mut ns_data = NamespaceData::new("test");
        ns_data.add_item("key1".to_string(), vec![1, 2, 3]);
        ns_data.add_item("key2".to_string(), vec![4, 5, 6]);

        assert_eq!(ns_data.item_count, 2);
        assert_eq!(ns_data.items.len(), 2);
    }

    #[test]
    fn test_backup_item_decode() {
        let item = BackupItem {
            key: "test".to_string(),
            value: BASE64_STANDARD.encode([1, 2, 3, 4]),
        };

        let decoded = item.decode_value().unwrap();
        assert_eq!(decoded, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_restore_options() {
        let opts = RestoreOptions::new()
            .with_clear_existing()
            .with_namespaces(vec!["jobs".to_string()])
            .with_dry_run();

        assert!(opts.clear_existing);
        assert!(opts.dry_run);
        assert_eq!(opts.namespaces, vec!["jobs".to_string()]);
    }

    #[test]
    fn test_restore_result() {
        let mut result = RestoreResult::new("backup123".to_string());
        result.record_namespace("jobs".to_string(), 100);
        result.record_namespace("nodes".to_string(), 50);
        result.add_warning("Skipped corrupt item");

        assert_eq!(result.namespaces_restored, 2);
        assert_eq!(result.total_items, 150);
        assert_eq!(result.warnings.len(), 1);
    }
}
