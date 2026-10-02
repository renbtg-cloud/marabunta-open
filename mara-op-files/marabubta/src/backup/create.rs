// Marabunta - Licensed under the MIT License.
//! Backup creation functionality
//!
//! This module provides the backup creation logic for Marabunta Compute,
//! including consistent snapshots and compression.

use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use flate2::write::GzEncoder;
use flate2::Compression;
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::storage::persistence::PersistenceBackend;

use super::errors::BackupError;
use super::types::{
    Backup, BackupData, BackupId, BackupLocation, BackupTarget, BackupType, NamespaceData,
};

/// Default namespaces to include in a backup
pub const DEFAULT_NAMESPACES: &[&str] = &[
    "raft",
    "jobs",
    "nodes",
    "tasks",
    "meta",
    "governance:principals",
    "governance:delegations",
    "governance:registry",
    "quotas",
    "quota_accounts",
    "quota_reservations",
    "policies",
];

/// Backup creator with support for consistent snapshots
pub struct BackupCreator<B: PersistenceBackend> {
    /// Persistence backend to backup from
    backend: Arc<B>,
    /// Target location for backups
    target: BackupTarget,
    /// Write lock for consistent snapshots
    write_lock: Arc<RwLock<()>>,
}

impl<B: PersistenceBackend> BackupCreator<B> {
    /// Create a new backup creator
    pub fn new(backend: Arc<B>, target: BackupTarget) -> Self {
        Self {
            backend,
            target,
            write_lock: Arc::new(RwLock::new(())),
        }
    }

    /// Create a new backup creator with a shared write lock
    pub fn with_write_lock(
        backend: Arc<B>,
        target: BackupTarget,
        write_lock: Arc<RwLock<()>>,
    ) -> Self {
        Self {
            backend,
            target,
            write_lock,
        }
    }

    /// Create a full backup of all namespaces
    pub async fn create_full_backup(&self) -> Result<Backup, BackupError> {
        self.create_backup(BackupType::Full, None).await
    }

    /// Create a snapshot backup (with write pause for consistency)
    pub async fn create_snapshot(&self) -> Result<Backup, BackupError> {
        self.create_backup(BackupType::Snapshot, None).await
    }

    /// Create an incremental backup based on a parent backup
    pub async fn create_incremental_backup(
        &self,
        parent_id: BackupId,
    ) -> Result<Backup, BackupError> {
        self.create_backup(BackupType::Incremental, Some(parent_id))
            .await
    }

    /// Create a backup with the specified type and optional parent
    pub async fn create_backup(
        &self,
        backup_type: BackupType,
        parent_id: Option<BackupId>,
    ) -> Result<Backup, BackupError> {
        let start = Instant::now();

        // Create backup metadata
        let location = self.target.location_for(&format!(
            "{}_{:08x}",
            chrono::Utc::now().format("%Y%m%d_%H%M%S"),
            rand::random::<u32>()
        ));
        let mut backup = Backup::new(backup_type, location.clone());
        backup.namespaces = DEFAULT_NAMESPACES.iter().map(|s| s.to_string()).collect();

        if let Some(parent) = parent_id {
            if !backup_type.requires_parent() {
                warn!(
                    "Parent backup specified for non-incremental backup type {:?}",
                    backup_type
                );
            }
            backup = backup.with_parent(parent);
        }

        info!(
            backup_id = %backup.id,
            backup_type = %backup_type,
            "Starting backup creation"
        );

        // For snapshot backups, acquire write lock for consistency
        let _lock_guard = if matches!(backup_type, BackupType::Snapshot) {
            debug!("Acquiring write lock for snapshot backup");
            Some(self.write_lock.write().await)
        } else {
            None
        };

        // Create backup data structure
        let mut backup_data = BackupData::new(backup.clone());

        // Export all namespaces
        for namespace in &backup.namespaces {
            match self.export_namespace(namespace).await {
                Ok(ns_data) => {
                    debug!(
                        namespace = %namespace,
                        items = ns_data.item_count,
                        "Exported namespace"
                    );
                    backup_data.add_namespace(namespace.clone(), ns_data);
                }
                Err(e) => {
                    // Log warning but continue with other namespaces
                    warn!(
                        namespace = %namespace,
                        error = %e,
                        "Failed to export namespace, skipping"
                    );
                }
            }
        }

        // Write backup to target location
        let (size_bytes, checksum) = self.write_backup(&location, &backup_data).await?;

        // Update backup metadata
        backup.complete(size_bytes, checksum);
        backup.metadata.insert(
            "duration_ms".to_string(),
            start.elapsed().as_millis().to_string(),
        );

        info!(
            backup_id = %backup.id,
            size_bytes = size_bytes,
            duration_ms = start.elapsed().as_millis(),
            "Backup completed successfully"
        );

        Ok(backup)
    }

    /// Export a single namespace to backup data
    async fn export_namespace(&self, namespace: &str) -> Result<NamespaceData, BackupError> {
        let mut ns_data = NamespaceData::new(namespace);

        // List all keys in the namespace
        let keys = self
            .backend
            .list_keys(namespace, None)
            .await
            .map_err(|e| BackupError::Storage(e.to_string()))?;

        // Export each key-value pair
        for key in keys {
            match self.backend.get(namespace, &key).await {
                Ok(Some(value)) => {
                    ns_data.add_item(key, value);
                }
                Ok(None) => {
                    // Key was deleted between list and get, skip
                    debug!(namespace = %namespace, key = %key, "Key not found during export");
                }
                Err(e) => {
                    warn!(
                        namespace = %namespace,
                        key = %key,
                        error = %e,
                        "Failed to read key, skipping"
                    );
                }
            }
        }

        Ok(ns_data)
    }

    /// Write backup data to the target location with compression
    async fn write_backup(
        &self,
        location: &BackupLocation,
        data: &BackupData,
    ) -> Result<(u64, String), BackupError> {
        match location {
            BackupLocation::Local { path } => self.write_local_backup(path, data).await,
            BackupLocation::S3 { .. } => Err(BackupError::NotImplemented(
                "S3 backup not yet implemented".to_string(),
            )),
            BackupLocation::GCS { .. } => Err(BackupError::NotImplemented(
                "GCS backup not yet implemented".to_string(),
            )),
        }
    }

    /// Write backup to local filesystem with gzip compression
    async fn write_local_backup(
        &self,
        path: &str,
        data: &BackupData,
    ) -> Result<(u64, String), BackupError> {
        let path = Path::new(path);

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                BackupError::Io(format!("Failed to create backup directory: {}", e))
            })?;
        }

        // Serialize to JSON
        let json = serde_json::to_vec_pretty(data)
            .map_err(|e| BackupError::Serialization(e.to_string()))?;

        // Calculate checksum of uncompressed data
        let mut hasher = Sha256::new();
        hasher.update(&json);
        let checksum = format!("{:x}", hasher.finalize());

        // Write compressed data
        let file = File::create(path)
            .map_err(|e| BackupError::Io(format!("Failed to create backup file: {}", e)))?;
        let writer = BufWriter::new(file);
        let mut encoder = GzEncoder::new(writer, Compression::default());

        encoder
            .write_all(&json)
            .map_err(|e| BackupError::Io(format!("Failed to write backup data: {}", e)))?;

        encoder
            .finish()
            .map_err(|e| BackupError::Io(format!("Failed to finish compression: {}", e)))?;

        // Get file size
        let metadata = fs::metadata(path)
            .map_err(|e| BackupError::Io(format!("Failed to get backup file size: {}", e)))?;

        Ok((metadata.len(), checksum))
    }

    /// Create a backup with specific namespaces only
    pub async fn create_backup_for_namespaces(
        &self,
        backup_type: BackupType,
        namespaces: Vec<String>,
    ) -> Result<Backup, BackupError> {
        let start = Instant::now();

        let location = self.target.location_for(&format!(
            "{}_{:08x}",
            chrono::Utc::now().format("%Y%m%d_%H%M%S"),
            rand::random::<u32>()
        ));
        let mut backup = Backup::new(backup_type, location.clone());
        backup.namespaces = namespaces;

        info!(
            backup_id = %backup.id,
            backup_type = %backup_type,
            namespaces = ?backup.namespaces,
            "Starting partial backup creation"
        );

        let _lock_guard = if matches!(backup_type, BackupType::Snapshot) {
            Some(self.write_lock.write().await)
        } else {
            None
        };

        let mut backup_data = BackupData::new(backup.clone());

        for namespace in &backup.namespaces {
            match self.export_namespace(namespace).await {
                Ok(ns_data) => {
                    backup_data.add_namespace(namespace.clone(), ns_data);
                }
                Err(e) => {
                    warn!(
                        namespace = %namespace,
                        error = %e,
                        "Failed to export namespace"
                    );
                }
            }
        }

        let (size_bytes, checksum) = self.write_backup(&location, &backup_data).await?;

        backup.complete(size_bytes, checksum);
        backup.metadata.insert(
            "duration_ms".to_string(),
            start.elapsed().as_millis().to_string(),
        );

        Ok(backup)
    }
}

/// Helper function to create a quick backup from a persistence backend
pub async fn create_backup<B: PersistenceBackend>(
    backend: Arc<B>,
    target: BackupTarget,
    backup_type: BackupType,
) -> Result<Backup, BackupError> {
    let creator = BackupCreator::new(backend, target);
    creator.create_backup(backup_type, None).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::PersistenceError;
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

        fn with_test_data() -> Self {
            let backend = Self::new();

            // Add test data synchronously
            {
                let mut data = backend.data.write();

                // Jobs namespace
                let mut jobs = HashMap::new();
                jobs.insert("job1".to_string(), b"job1_data".to_vec());
                jobs.insert("job2".to_string(), b"job2_data".to_vec());
                data.insert("jobs".to_string(), jobs);

                // Nodes namespace
                let mut nodes = HashMap::new();
                nodes.insert("node1".to_string(), b"node1_data".to_vec());
                data.insert("nodes".to_string(), nodes);

                // Raft namespace
                let mut raft = HashMap::new();
                raft.insert("state".to_string(), b"raft_state".to_vec());
                data.insert("raft".to_string(), raft);
            }

            backend
        }
    }

    #[async_trait::async_trait]
    impl PersistenceBackend for MemoryBackend {
        async fn put(
            &self,
            namespace: &str,
            key: &str,
            value: &[u8],
        ) -> Result<(), PersistenceError> {
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
        ) -> Result<Option<Vec<u8>>, PersistenceError> {
            let data = self.data.read();
            Ok(data.get(namespace).and_then(|ns| ns.get(key).cloned()))
        }

        async fn delete(&self, namespace: &str, key: &str) -> Result<bool, PersistenceError> {
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
        ) -> Result<Vec<String>, PersistenceError> {
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

        async fn exists(&self, namespace: &str, key: &str) -> Result<bool, PersistenceError> {
            let data = self.data.read();
            Ok(data.get(namespace).map_or(false, |ns| ns.contains_key(key)))
        }

        async fn batch_put(
            &self,
            namespace: &str,
            items: &[(&str, &[u8])],
        ) -> Result<(), PersistenceError> {
            let mut data = self.data.write();
            let ns = data
                .entry(namespace.to_string())
                .or_insert_with(HashMap::new);
            for (key, value) in items {
                ns.insert(key.to_string(), value.to_vec());
            }
            Ok(())
        }

        async fn clear_namespace(&self, namespace: &str) -> Result<u64, PersistenceError> {
            let mut data = self.data.write();
            if let Some(ns) = data.remove(namespace) {
                Ok(ns.len() as u64)
            } else {
                Ok(0)
            }
        }
    }

    #[tokio::test]
    async fn test_create_full_backup() {
        let temp = tempdir().unwrap();
        let backend = Arc::new(MemoryBackend::with_test_data());
        let target = BackupTarget::local(temp.path());

        let creator = BackupCreator::new(backend, target);
        let backup = creator.create_full_backup().await.unwrap();

        assert!(backup.is_valid());
        assert_eq!(backup.backup_type, BackupType::Full);
        assert!(backup.size_bytes > 0);
        assert!(backup.checksum.is_some());

        // Verify file exists
        let file_path = backup.file_path().unwrap();
        assert!(file_path.exists());
    }

    #[tokio::test]
    async fn test_create_snapshot_backup() {
        let temp = tempdir().unwrap();
        let backend = Arc::new(MemoryBackend::with_test_data());
        let target = BackupTarget::local(temp.path());

        let creator = BackupCreator::new(backend, target);
        let backup = creator.create_snapshot().await.unwrap();

        assert!(backup.is_valid());
        assert_eq!(backup.backup_type, BackupType::Snapshot);
    }

    #[tokio::test]
    async fn test_create_backup_for_specific_namespaces() {
        let temp = tempdir().unwrap();
        let backend = Arc::new(MemoryBackend::with_test_data());
        let target = BackupTarget::local(temp.path());

        let creator = BackupCreator::new(backend, target);
        let backup = creator
            .create_backup_for_namespaces(
                BackupType::Full,
                vec!["jobs".to_string(), "nodes".to_string()],
            )
            .await
            .unwrap();

        assert!(backup.is_valid());
        assert_eq!(backup.namespaces.len(), 2);
        assert!(backup.namespaces.contains(&"jobs".to_string()));
        assert!(backup.namespaces.contains(&"nodes".to_string()));
    }

    #[tokio::test]
    async fn test_backup_with_empty_namespace() {
        let temp = tempdir().unwrap();
        let backend = Arc::new(MemoryBackend::new()); // Empty
        let target = BackupTarget::local(temp.path());

        let creator = BackupCreator::new(backend, target);
        let backup = creator.create_full_backup().await.unwrap();

        assert!(backup.is_valid());
        // Should still create a valid backup even with empty data
    }

    #[tokio::test]
    async fn test_backup_metadata() {
        let temp = tempdir().unwrap();
        let backend = Arc::new(MemoryBackend::with_test_data());
        let target = BackupTarget::local(temp.path());

        let creator = BackupCreator::new(backend, target);
        let backup = creator.create_full_backup().await.unwrap();

        // Should have duration metadata
        assert!(backup.metadata.contains_key("duration_ms"));
    }

    #[tokio::test]
    async fn test_export_namespace() {
        let backend = Arc::new(MemoryBackend::with_test_data());
        let target = BackupTarget::local("/tmp");

        let creator = BackupCreator::new(backend, target);
        let ns_data = creator.export_namespace("jobs").await.unwrap();

        assert_eq!(ns_data.namespace, "jobs");
        assert_eq!(ns_data.item_count, 2);
        assert_eq!(ns_data.items.len(), 2);
    }
}
