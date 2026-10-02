// Marabunta - Licensed under the MIT License.
//! Backup restoration functionality
//!
//! This module provides backup restoration logic for Marabunta Compute,
//! including integrity validation, partial restore, and clear options.

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use flate2::read::GzDecoder;
use sha2::{Digest, Sha256};
use tracing::{debug, error, info, warn};

use crate::storage::persistence::PersistenceBackend;

use super::errors::BackupError;
use super::types::{Backup, BackupData, BackupId, BackupLocation, RestoreOptions, RestoreResult};

/// Backup restorer for recovering cluster state
pub struct BackupRestorer<B: PersistenceBackend> {
    /// Persistence backend to restore to
    backend: Arc<B>,
}

impl<B: PersistenceBackend> BackupRestorer<B> {
    /// Create a new backup restorer
    pub fn new(backend: Arc<B>) -> Self {
        Self { backend }
    }

    /// Restore from a backup file with default options
    pub async fn restore(&self, backup: &Backup) -> Result<RestoreResult, BackupError> {
        self.restore_with_options(backup, RestoreOptions::default())
            .await
    }

    /// Restore from a backup file with custom options
    pub async fn restore_with_options(
        &self,
        backup: &Backup,
        options: RestoreOptions,
    ) -> Result<RestoreResult, BackupError> {
        let start = Instant::now();
        let mut result = RestoreResult::new(backup.id.clone());

        info!(
            backup_id = %backup.id,
            clear_existing = options.clear_existing,
            dry_run = options.dry_run,
            "Starting backup restoration"
        );

        // Load and validate backup data
        let (backup_data, actual_checksum) = self.load_backup(&backup.location).await?;

        // Validate checksum unless skipped
        if !options.skip_checksum {
            self.validate_checksum(&actual_checksum, backup.checksum.as_deref())?;
        } else {
            result.add_warning("Checksum validation was skipped".to_string());
        }

        // Validate format version
        if backup_data.format_version > BackupData::FORMAT_VERSION {
            return Err(BackupError::IncompatibleVersion {
                expected: BackupData::FORMAT_VERSION,
                found: backup_data.format_version,
            });
        }

        if options.dry_run {
            info!("Dry run mode - validating backup without restoring");
            return self.dry_run_restore(&backup_data, &options, start).await;
        }

        // Determine namespaces to restore
        let namespaces_to_restore: Vec<&String> = if options.namespaces.is_empty() {
            backup_data.data.keys().collect()
        } else {
            backup_data
                .data
                .keys()
                .filter(|ns| options.namespaces.contains(ns))
                .collect()
        };

        // Clear existing data if requested
        if options.clear_existing {
            for namespace in &namespaces_to_restore {
                match self.backend.clear_namespace(namespace).await {
                    Ok(count) => {
                        debug!(namespace = %namespace, items_cleared = count, "Cleared namespace");
                    }
                    Err(e) => {
                        warn!(namespace = %namespace, error = %e, "Failed to clear namespace");
                        result
                            .add_warning(format!("Failed to clear namespace {}: {}", namespace, e));
                    }
                }
            }
        }

        // Restore each namespace
        for namespace in namespaces_to_restore {
            if let Some(ns_data) = backup_data.data.get(namespace) {
                match self.restore_namespace(namespace, ns_data).await {
                    Ok(item_count) => {
                        debug!(namespace = %namespace, items = item_count, "Restored namespace");
                        result.record_namespace(namespace.clone(), item_count);
                    }
                    Err(e) => {
                        error!(namespace = %namespace, error = %e, "Failed to restore namespace");
                        result.add_warning(format!(
                            "Failed to restore namespace {}: {}",
                            namespace, e
                        ));
                    }
                }
            }
        }

        result.duration_ms = start.elapsed().as_millis() as u64;

        info!(
            backup_id = %backup.id,
            namespaces = result.namespaces_restored,
            items = result.total_items,
            duration_ms = result.duration_ms,
            "Backup restoration completed"
        );

        Ok(result)
    }

    /// Load backup data from a location, returning both the data and its checksum
    async fn load_backup(&self, location: &BackupLocation) -> Result<(BackupData, String), BackupError> {
        match location {
            BackupLocation::Local { path } => self.load_local_backup(path).await,
            BackupLocation::S3 { .. } => Err(BackupError::NotImplemented(
                "S3 restore not yet implemented".to_string(),
            )),
            BackupLocation::GCS { .. } => Err(BackupError::NotImplemented(
                "GCS restore not yet implemented".to_string(),
            )),
        }
    }

    /// Load backup from local filesystem, returning both data and checksum
    async fn load_local_backup(&self, path: &str) -> Result<(BackupData, String), BackupError> {
        let path = Path::new(path);

        if !path.exists() {
            return Err(BackupError::NotFound(format!(
                "Backup file not found: {}",
                path.display()
            )));
        }

        let file = File::open(path)
            .map_err(|e| BackupError::Io(format!("Failed to open backup file: {}", e)))?;

        let reader = BufReader::new(file);
        let mut decoder = GzDecoder::new(reader);
        let mut json = Vec::new();

        decoder
            .read_to_end(&mut json)
            .map_err(|e| BackupError::Io(format!("Failed to decompress backup: {}", e)))?;

        // Calculate checksum from the raw decompressed JSON bytes
        let mut hasher = Sha256::new();
        hasher.update(&json);
        let checksum = format!("{:x}", hasher.finalize());

        let backup_data: BackupData = serde_json::from_slice(&json)
            .map_err(|e| BackupError::Serialization(format!("Failed to parse backup: {}", e)))?;

        Ok((backup_data, checksum))
    }

    /// Validate backup checksum
    fn validate_checksum(
        &self,
        actual_checksum: &str,
        expected: Option<&str>,
    ) -> Result<(), BackupError> {
        let expected = expected.ok_or_else(|| {
            BackupError::InvalidChecksum("Backup has no checksum to validate".to_string())
        })?;

        if actual_checksum != expected {
            return Err(BackupError::InvalidChecksum(format!(
                "Checksum mismatch: expected {}, got {}",
                expected, actual_checksum
            )));
        }

        debug!("Backup checksum validated successfully");
        Ok(())
    }

    /// Restore a single namespace
    async fn restore_namespace(
        &self,
        namespace: &str,
        data: &super::types::NamespaceData,
    ) -> Result<usize, BackupError> {
        let mut restored = 0;

        for item in &data.items {
            let value = item.decode_value().map_err(|e| {
                BackupError::Serialization(format!(
                    "Failed to decode item {}/{}: {}",
                    namespace, item.key, e
                ))
            })?;

            self.backend
                .put(namespace, &item.key, &value)
                .await
                .map_err(|e| BackupError::Storage(e.to_string()))?;

            restored += 1;
        }

        Ok(restored)
    }

    /// Perform a dry-run restore (validation only)
    async fn dry_run_restore(
        &self,
        data: &BackupData,
        options: &RestoreOptions,
        start: Instant,
    ) -> Result<RestoreResult, BackupError> {
        let mut result = RestoreResult::new(data.backup.id.clone());

        let namespaces_to_check: Vec<&String> = if options.namespaces.is_empty() {
            data.data.keys().collect()
        } else {
            data.data
                .keys()
                .filter(|ns| options.namespaces.contains(ns))
                .collect()
        };

        for namespace in namespaces_to_check {
            if let Some(ns_data) = data.data.get(namespace) {
                // Validate each item can be decoded
                let mut valid_items = 0;
                for item in &ns_data.items {
                    match item.decode_value() {
                        Ok(_) => valid_items += 1,
                        Err(e) => {
                            result.add_warning(format!(
                                "Invalid item {}/{}: {}",
                                namespace, item.key, e
                            ));
                        }
                    }
                }
                result.record_namespace(namespace.clone(), valid_items);
            }
        }

        result.duration_ms = start.elapsed().as_millis() as u64;
        result.add_warning("Dry run - no data was actually restored".to_string());

        Ok(result)
    }

    /// Restore from backup ID by looking up in a backup registry
    pub async fn restore_by_id(
        &self,
        backup_id: &BackupId,
        backup_registry: &dyn BackupRegistry,
        options: RestoreOptions,
    ) -> Result<RestoreResult, BackupError> {
        let backup = backup_registry
            .get_backup(backup_id)
            .await?
            .ok_or_else(|| BackupError::NotFound(format!("Backup not found: {}", backup_id)))?;

        self.restore_with_options(&backup, options).await
    }
}

/// Trait for backup registry lookup (used by restore_by_id)
#[async_trait::async_trait]
pub trait BackupRegistry: Send + Sync {
    /// Get a backup by ID
    async fn get_backup(&self, id: &BackupId) -> Result<Option<Backup>, BackupError>;

    /// List all backups
    async fn list_backups(&self) -> Result<Vec<Backup>, BackupError>;
}

/// Restore a backup directly from a file path
pub async fn restore_from_file<B: PersistenceBackend>(
    backend: Arc<B>,
    path: impl AsRef<Path>,
    options: RestoreOptions,
) -> Result<RestoreResult, BackupError> {
    let restorer = BackupRestorer::new(backend);

    // Create a minimal backup struct just for the location
    let backup = Backup::new(
        super::types::BackupType::Full,
        BackupLocation::Local {
            path: path.as_ref().to_string_lossy().to_string(),
        },
    );

    restorer.restore_with_options(&backup, options).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup::types::{BackupType, NamespaceData};
    use parking_lot::RwLock as SyncRwLock;
    use std::collections::HashMap;
    use tempfile::tempdir;

    /// Simple in-memory backend for testing
    struct MemoryBackend {
        data: SyncRwLock<HashMap<String, HashMap<String, Vec<u8>>>>,
    }

    impl MemoryBackend {
        fn new() -> Self {
            Self {
                data: SyncRwLock::new(HashMap::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl PersistenceBackend for MemoryBackend {
        async fn put(
            &self,
            namespace: &str,
            key: &str,
            value: &[u8],
        ) -> Result<(), crate::storage::persistence::PersistenceError> {
            let mut data = self.data.write();
            data.entry(namespace.to_string())
                .or_insert_with(HashMap::new)
                .insert(key.to_string(), value.to_vec());
            Ok(())
        }

        async fn get(
            &self,
            namespace: &str,
            key: &str,
        ) -> Result<Option<Vec<u8>>, crate::storage::persistence::PersistenceError> {
            let data = self.data.read();
            Ok(data.get(namespace).and_then(|ns| ns.get(key).cloned()))
        }

        async fn delete(
            &self,
            namespace: &str,
            key: &str,
        ) -> Result<bool, crate::storage::persistence::PersistenceError> {
            let mut data = self.data.write();
            if let Some(ns) = data.get_mut(namespace) {
                Ok(ns.remove(key).is_some())
            } else {
                Ok(false)
            }
        }

        async fn list_keys(
            &self,
            namespace: &str,
            prefix: Option<&str>,
        ) -> Result<Vec<String>, crate::storage::persistence::PersistenceError> {
            let data = self.data.read();
            Ok(data
                .get(namespace)
                .map(|ns| {
                    ns.keys()
                        .filter(|k| prefix.map_or(true, |p| k.starts_with(p)))
                        .cloned()
                        .collect()
                })
                .unwrap_or_default())
        }

        async fn exists(
            &self,
            namespace: &str,
            key: &str,
        ) -> Result<bool, crate::storage::persistence::PersistenceError> {
            let data = self.data.read();
            Ok(data.get(namespace).map_or(false, |ns| ns.contains_key(key)))
        }

        async fn batch_put(
            &self,
            namespace: &str,
            items: &[(&str, &[u8])],
        ) -> Result<(), crate::storage::persistence::PersistenceError> {
            let mut data = self.data.write();
            let ns = data
                .entry(namespace.to_string())
                .or_insert_with(HashMap::new);
            for (key, value) in items {
                ns.insert(key.to_string(), value.to_vec());
            }
            Ok(())
        }

        async fn clear_namespace(
            &self,
            namespace: &str,
        ) -> Result<u64, crate::storage::persistence::PersistenceError> {
            let mut data = self.data.write();
            if let Some(ns) = data.remove(namespace) {
                Ok(ns.len() as u64)
            } else {
                Ok(0)
            }
        }
    }

    fn create_test_backup_data() -> BackupData {
        let backup = Backup::new(
            BackupType::Full,
            BackupLocation::Local {
                path: "/tmp/test.bak.gz".to_string(),
            },
        );
        let mut data = BackupData::new(backup);

        // Add test namespace data
        let mut jobs = NamespaceData::new("jobs");
        jobs.add_item("job1".to_string(), b"job1_data".to_vec());
        jobs.add_item("job2".to_string(), b"job2_data".to_vec());
        data.add_namespace("jobs".to_string(), jobs);

        let mut nodes = NamespaceData::new("nodes");
        nodes.add_item("node1".to_string(), b"node1_data".to_vec());
        data.add_namespace("nodes".to_string(), nodes);

        data
    }

    #[tokio::test]
    async fn test_restore_namespace() {
        let backend = Arc::new(MemoryBackend::new());
        let restorer = BackupRestorer::new(backend.clone());

        let mut ns_data = NamespaceData::new("test");
        ns_data.add_item("key1".to_string(), b"value1".to_vec());
        ns_data.add_item("key2".to_string(), b"value2".to_vec());

        let count = restorer.restore_namespace("test", &ns_data).await.unwrap();
        assert_eq!(count, 2);

        // Verify data was restored
        let value1 = backend.get("test", "key1").await.unwrap();
        assert_eq!(value1, Some(b"value1".to_vec()));

        let value2 = backend.get("test", "key2").await.unwrap();
        assert_eq!(value2, Some(b"value2".to_vec()));
    }

    #[tokio::test]
    async fn test_dry_run_restore() {
        let backend = Arc::new(MemoryBackend::new());
        let restorer = BackupRestorer::new(backend.clone());

        let backup_data = create_test_backup_data();
        let options = RestoreOptions::new().with_dry_run();
        let start = std::time::Instant::now();

        let result = restorer
            .dry_run_restore(&backup_data, &options, start)
            .await
            .unwrap();

        assert_eq!(result.namespaces_restored, 2);
        assert_eq!(result.total_items, 3);

        // Verify nothing was actually restored
        let value = backend.get("jobs", "job1").await.unwrap();
        assert!(value.is_none());
    }

    #[tokio::test]
    async fn test_restore_with_clear_existing() {
        let backend = Arc::new(MemoryBackend::new());

        // Pre-populate with some data
        backend.put("jobs", "old_job", b"old_data").await.unwrap();

        let restorer = BackupRestorer::new(backend.clone());

        let backup_data = create_test_backup_data();

        // First restore without clear
        let mut backup = Backup::new(
            BackupType::Full,
            BackupLocation::Local {
                path: "/tmp/test.bak.gz".to_string(),
            },
        );
        backup.checksum = backup_data.calculate_checksum().ok();

        // For this test, we'll manually call restore_namespace
        let ns_data = backup_data.data.get("jobs").unwrap();
        restorer.restore_namespace("jobs", ns_data).await.unwrap();

        // Old data should still exist
        let old_value = backend.get("jobs", "old_job").await.unwrap();
        assert!(old_value.is_some());

        // Clear and restore
        backend.clear_namespace("jobs").await.unwrap();
        restorer.restore_namespace("jobs", ns_data).await.unwrap();

        // Old data should be gone
        let old_value = backend.get("jobs", "old_job").await.unwrap();
        assert!(old_value.is_none());

        // New data should exist
        let new_value = backend.get("jobs", "job1").await.unwrap();
        assert!(new_value.is_some());
    }

    #[tokio::test]
    async fn test_validate_checksum() {
        let backend = Arc::new(MemoryBackend::new());
        let restorer = BackupRestorer::new(backend);

        let actual_checksum = "abc123def456";

        // Valid checksum - actual matches expected
        let result = restorer.validate_checksum(actual_checksum, Some(actual_checksum));
        assert!(result.is_ok());

        // Invalid checksum - actual doesn't match expected
        let result = restorer.validate_checksum(actual_checksum, Some("invalid_checksum"));
        assert!(result.is_err());
        assert!(matches!(result, Err(BackupError::InvalidChecksum(_))));

        // Missing checksum - no expected checksum provided
        let result = restorer.validate_checksum(actual_checksum, None);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_restore_specific_namespaces() {
        let backend = Arc::new(MemoryBackend::new());
        let restorer = BackupRestorer::new(backend.clone());

        let backup_data = create_test_backup_data();
        let options = RestoreOptions::new()
            .with_namespaces(vec!["jobs".to_string()])
            .with_dry_run();

        let start = std::time::Instant::now();
        let result = restorer
            .dry_run_restore(&backup_data, &options, start)
            .await
            .unwrap();

        // Should only count jobs namespace
        assert_eq!(result.namespaces_restored, 1);
        assert_eq!(result.total_items, 2); // job1 and job2
    }

    #[tokio::test]
    async fn test_restore_options_builder() {
        let options = RestoreOptions::new()
            .with_clear_existing()
            .with_namespaces(vec!["jobs".to_string(), "nodes".to_string()])
            .with_dry_run();

        assert!(options.clear_existing);
        assert!(options.dry_run);
        assert_eq!(options.namespaces.len(), 2);
    }
}
