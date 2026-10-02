// Marabunta - Licensed under the MIT License.
//! Backup and Recovery Module for Marabunta Compute
//!
//! This module provides comprehensive backup and recovery functionality for
//! the Marabunta Compute cluster state, including:
//!
//! - **Full Backups**: Complete snapshot of all cluster data
//! - **Incremental Backups**: Only changes since last backup
//! - **Snapshot Backups**: Point-in-time consistent snapshots
//! - **Scheduled Backups**: Automated backup with cron-like scheduling
//! - **Retention Policies**: Automatic cleanup of old backups
//!
//! # Architecture
//!
//! The backup system is organized into several components:
//!
//! - [`types`] - Core types (Backup, BackupType, BackupTarget, etc.)
//! - [`create`] - Backup creation with compression
//! - [`restore`] - Backup restoration with integrity validation
//! - [`scheduler`] - Scheduled backups and retention management
//! - [`errors`] - Error types for backup operations
//!
//! # Example Usage
//!
//! ## Creating a Backup
//!
//! ```rust,no_run
//! use marabunta_compute::backup::{BackupCreator, BackupTarget, BackupType};
//! use marabunta_compute::storage::SqliteBackend;
//! use std::sync::Arc;
//!
//! async fn create_backup() -> Result<(), Box<dyn std::error::Error>> {
//!     let backend = Arc::new(SqliteBackend::new("./data/marabunta.db")?);
//!     let target = BackupTarget::local("./backups");
//!
//!     let creator = BackupCreator::new(backend, target);
//!     let backup = creator.create_full_backup().await?;
//!
//!     println!("Created backup: {}", backup.id);
//!     println!("Size: {} bytes", backup.size_bytes);
//!     Ok(())
//! }
//! ```
//!
//! ## Restoring from Backup
//!
//! ```rust,no_run
//! use marabunta_compute::backup::{BackupRestorer, RestoreOptions, restore_from_file};
//! use marabunta_compute::storage::SqliteBackend;
//! use std::sync::Arc;
//!
//! async fn restore_backup() -> Result<(), Box<dyn std::error::Error>> {
//!     let backend = Arc::new(SqliteBackend::new("./data/marabunta.db")?);
//!
//!     let options = RestoreOptions::new()
//!         .with_clear_existing()  // Clear existing data before restore
//!         .with_namespaces(vec!["jobs".to_string()]); // Only restore jobs
//!
//!     let result = restore_from_file(
//!         backend,
//!         "./backups/20240115_120000_12345678.marabunta.bak.gz",
//!         options,
//!     ).await?;
//!
//!     println!("Restored {} items from {} namespaces",
//!              result.total_items, result.namespaces_restored);
//!     Ok(())
//! }
//! ```
//!
//! ## Scheduled Backups
//!
//! ```rust,no_run
//! use marabunta_compute::backup::{
//!     BackupCreator, BackupScheduler, BackupSchedule,
//!     ScheduleExpression, BackupTarget, BackupType, RetentionPolicy,
//! };
//! use marabunta_compute::storage::SqliteBackend;
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! async fn setup_scheduled_backups() -> Result<(), Box<dyn std::error::Error>> {
//!     let backend = Arc::new(SqliteBackend::new("./data/marabunta.db")?);
//!     let target = BackupTarget::local("./backups");
//!     let creator = BackupCreator::new(backend, target);
//!
//!     let scheduler = Arc::new(BackupScheduler::new(creator));
//!
//!     // Add daily full backup at 3 AM
//!     scheduler.add_schedule(BackupSchedule::new(
//!         ScheduleExpression::daily_at(3),
//!         BackupType::Full,
//!     ).with_description("Daily full backup"));
//!
//!     // Add hourly snapshots
//!     scheduler.add_schedule(BackupSchedule::new(
//!         ScheduleExpression::every_hours(1),
//!         BackupType::Snapshot,
//!     ).with_description("Hourly snapshot"));
//!
//!     // Set retention policy
//!     scheduler.set_retention_policy(RetentionPolicy::with_max_count(10));
//!
//!     // Start scheduler (checks every minute)
//!     let shutdown = scheduler.start(Duration::from_secs(60)).await;
//!
//!     // ... later, to stop the scheduler:
//!     // shutdown.send(()).await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! # Backup Format
//!
//! Backups are stored as gzip-compressed JSON files with the following structure:
//!
//! ```json
//! {
//!   "format_version": 1,
//!   "created_at": "2024-01-15T12:00:00Z",
//!   "backup": { /* backup metadata */ },
//!   "data": {
//!     "namespace1": {
//!       "namespace": "namespace1",
//!       "item_count": 10,
//!       "items": [
//!         { "key": "key1", "value": "base64encoded..." },
//!         // ...
//!       ]
//!     }
//!   }
//! }
//! ```
//!
//! # Data Included in Backups
//!
//! By default, backups include:
//!
//! - Raft state (term, voted_for, log entries)
//! - Jobs (job definitions and status)
//! - Tasks (task definitions and status)
//! - Nodes (worker and master information)
//! - Governance (principals and delegations)
//! - Quotas (quota definitions and accounts)
//! - Policies (placement policies)
//! - Cluster metadata and configuration

pub mod create;
pub mod errors;
pub mod restore;
pub mod scheduler;
pub mod types;

// Re-exports for convenient access
pub use create::{BackupCreator, DEFAULT_NAMESPACES};
pub use errors::BackupError;
pub use restore::{restore_from_file, BackupRegistry, BackupRestorer};
pub use scheduler::{BackupSchedule, BackupScheduler, RetentionPolicy, ScheduleExpression};
pub use types::{
    Backup, BackupData, BackupId, BackupItem, BackupLocation, BackupStatus, BackupTarget,
    BackupType, NamespaceData, RestoreOptions, RestoreResult,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::persistence::PersistenceBackend;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::tempdir;

    /// Test in-memory backend
    struct TestBackend {
        data: RwLock<HashMap<String, HashMap<String, Vec<u8>>>>,
    }

    impl TestBackend {
        fn new() -> Self {
            Self {
                data: RwLock::new(HashMap::new()),
            }
        }

        fn with_test_data() -> Self {
            let backend = Self::new();
            {
                let mut data = backend.data.write();
                let mut jobs = HashMap::new();
                jobs.insert("job1".to_string(), b"test_job_1".to_vec());
                jobs.insert("job2".to_string(), b"test_job_2".to_vec());
                data.insert("jobs".to_string(), jobs);

                let mut nodes = HashMap::new();
                nodes.insert("node1".to_string(), b"test_node_1".to_vec());
                data.insert("nodes".to_string(), nodes);
            }
            backend
        }
    }

    #[async_trait::async_trait]
    impl PersistenceBackend for TestBackend {
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

    #[tokio::test]
    async fn test_full_backup_restore_cycle() {
        let temp = tempdir().unwrap();
        let backend = Arc::new(TestBackend::with_test_data());
        let target = BackupTarget::local(temp.path());

        // Create backup
        let creator = BackupCreator::new(Arc::clone(&backend), target);
        let backup = creator.create_full_backup().await.unwrap();

        assert!(backup.is_valid());
        assert!(backup.file_path().unwrap().exists());

        // Create a new empty backend for restore
        let restore_backend = Arc::new(TestBackend::new());
        let restorer = BackupRestorer::new(Arc::clone(&restore_backend));

        // Restore
        let result = restorer.restore(&backup).await.unwrap();

        assert!(result.total_items > 0);
        assert!(result.namespaces_restored > 0);

        // Verify data was restored
        let job1 = restore_backend.get("jobs", "job1").await.unwrap();
        assert!(job1.is_some());
        assert_eq!(job1.unwrap(), b"test_job_1".to_vec());
    }

    #[tokio::test]
    async fn test_partial_restore() {
        let temp = tempdir().unwrap();
        let backend = Arc::new(TestBackend::with_test_data());
        let target = BackupTarget::local(temp.path());

        // Create backup
        let creator = BackupCreator::new(Arc::clone(&backend), target);
        let backup = creator.create_full_backup().await.unwrap();

        // Restore only jobs namespace
        let restore_backend = Arc::new(TestBackend::new());
        let restorer = BackupRestorer::new(Arc::clone(&restore_backend));

        let options = RestoreOptions::new().with_namespaces(vec!["jobs".to_string()]);
        let _result = restorer
            .restore_with_options(&backup, options)
            .await
            .unwrap();

        // Jobs should be restored
        let job1 = restore_backend.get("jobs", "job1").await.unwrap();
        assert!(job1.is_some());

        // Nodes should NOT be restored (not in namespace list)
        let node1 = restore_backend.get("nodes", "node1").await.unwrap();
        assert!(node1.is_none());
    }

    #[tokio::test]
    async fn test_dry_run_restore() {
        let temp = tempdir().unwrap();
        let backend = Arc::new(TestBackend::with_test_data());
        let target = BackupTarget::local(temp.path());

        // Create backup
        let creator = BackupCreator::new(Arc::clone(&backend), target);
        let backup = creator.create_full_backup().await.unwrap();

        // Dry run restore
        let restore_backend = Arc::new(TestBackend::new());
        let restorer = BackupRestorer::new(Arc::clone(&restore_backend));

        let options = RestoreOptions::new().with_dry_run();
        let result = restorer
            .restore_with_options(&backup, options)
            .await
            .unwrap();

        // Should report items that would be restored
        assert!(result.total_items > 0);

        // But data should NOT actually be restored
        let job1 = restore_backend.get("jobs", "job1").await.unwrap();
        assert!(job1.is_none());
    }

    #[test]
    fn test_backup_types() {
        assert_eq!("full".parse::<BackupType>().unwrap(), BackupType::Full);
        assert_eq!(
            "incremental".parse::<BackupType>().unwrap(),
            BackupType::Incremental
        );
        assert_eq!(
            "snapshot".parse::<BackupType>().unwrap(),
            BackupType::Snapshot
        );
    }

    #[test]
    fn test_backup_target_locations() {
        let local = BackupTarget::local("/backups");
        let location = local.location_for("test_backup");

        if let BackupLocation::Local { path } = location {
            assert!(path.contains("test_backup"));
            assert!(path.ends_with(".marabunta.bak.gz"));
        } else {
            panic!("Expected local location");
        }
    }

    #[test]
    fn test_retention_policy() {
        let policy = RetentionPolicy::with_max_count(5);

        let mut backups = Vec::new();
        for i in 0..10 {
            let mut backup = Backup::new(
                BackupType::Full,
                BackupLocation::Local {
                    path: format!("/tmp/backup{}.gz", i),
                },
            );
            backup.status = BackupStatus::Completed;
            backup.timestamp = chrono::Utc::now() - chrono::Duration::hours(i as i64);
            backups.push(backup);
        }

        let retained = policy.filter_to_retain(&backups);
        assert_eq!(retained.len(), 5);

        let to_delete = policy.filter_to_delete(&backups);
        assert_eq!(to_delete.len(), 5);
    }

    #[test]
    fn test_schedule_expressions() {
        // Every hour
        let hourly = ScheduleExpression::every_hours(1);
        assert_eq!(hourly.interval_minutes, Some(60));

        // Daily at 3 AM
        let daily = ScheduleExpression::daily_at(3);
        assert_eq!(daily.hour, Some(3));
        assert_eq!(daily.minute, Some(0));

        // Weekly on Sunday at 2 AM
        let weekly = ScheduleExpression::weekly_on(0, 2);
        assert_eq!(weekly.day_of_week, Some(0));
        assert_eq!(weekly.hour, Some(2));
    }
}
