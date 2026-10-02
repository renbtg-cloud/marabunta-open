// Marabunta - Licensed under the MIT License.
//! Backup scheduling and retention management
//!
//! This module provides scheduled backup functionality with configurable
//! cron-like scheduling and automatic retention policies.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Datelike, Timelike, Utc};
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::storage::persistence::PersistenceBackend;

use super::create::BackupCreator;
use super::errors::BackupError;
use super::types::{Backup, BackupId, BackupStatus, BackupType};

/// Schedule configuration for automatic backups
#[derive(Debug, Clone)]
pub struct BackupSchedule {
    /// Schedule expression (simplified cron-like)
    pub expression: ScheduleExpression,
    /// Type of backup to create
    pub backup_type: BackupType,
    /// Whether the schedule is enabled
    pub enabled: bool,
    /// Description of the schedule
    pub description: Option<String>,
}

impl BackupSchedule {
    /// Create a new backup schedule
    pub fn new(expression: ScheduleExpression, backup_type: BackupType) -> Self {
        Self {
            expression,
            backup_type,
            enabled: true,
            description: None,
        }
    }

    /// Set the schedule description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Disable the schedule
    pub fn disabled(mut self) -> Self {
        self.enabled = false;
        self
    }

    /// Check if the schedule matches a given time
    pub fn matches(&self, time: &DateTime<Utc>) -> bool {
        self.expression.matches(time)
    }

    /// Get the next scheduled time after the given time
    pub fn next_run(&self, after: &DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.expression.next_run(after)
    }
}

/// Simplified cron-like schedule expression
#[derive(Debug, Clone)]
pub struct ScheduleExpression {
    /// Hour of day (0-23), None means every hour
    pub hour: Option<u32>,
    /// Minute of hour (0-59), None means every minute
    pub minute: Option<u32>,
    /// Day of week (0=Sunday, 6=Saturday), None means every day
    pub day_of_week: Option<u32>,
    /// Day of month (1-31), None means every day
    pub day_of_month: Option<u32>,
    /// Interval in minutes (alternative to time-based scheduling)
    pub interval_minutes: Option<u32>,
}

impl ScheduleExpression {
    /// Create a schedule that runs every N minutes
    pub fn every_minutes(minutes: u32) -> Self {
        Self {
            hour: None,
            minute: None,
            day_of_week: None,
            day_of_month: None,
            interval_minutes: Some(minutes),
        }
    }

    /// Create a schedule that runs every N hours
    pub fn every_hours(hours: u32) -> Self {
        Self::every_minutes(hours * 60)
    }

    /// Create a daily schedule at a specific hour
    pub fn daily_at(hour: u32) -> Self {
        Self {
            hour: Some(hour),
            minute: Some(0),
            day_of_week: None,
            day_of_month: None,
            interval_minutes: None,
        }
    }

    /// Create a weekly schedule on a specific day and hour
    pub fn weekly_on(day_of_week: u32, hour: u32) -> Self {
        Self {
            hour: Some(hour),
            minute: Some(0),
            day_of_week: Some(day_of_week),
            day_of_month: None,
            interval_minutes: None,
        }
    }

    /// Create a monthly schedule on a specific day and hour
    pub fn monthly_on(day_of_month: u32, hour: u32) -> Self {
        Self {
            hour: Some(hour),
            minute: Some(0),
            day_of_week: None,
            day_of_month: Some(day_of_month),
            interval_minutes: None,
        }
    }

    /// Check if this expression matches the given time
    pub fn matches(&self, time: &DateTime<Utc>) -> bool {
        // Interval-based scheduling is handled differently
        if self.interval_minutes.is_some() {
            return false; // Interval matching requires last run time
        }

        // Check hour
        if let Some(hour) = self.hour {
            if time.hour() != hour {
                return false;
            }
        }

        // Check minute
        if let Some(minute) = self.minute {
            if time.minute() != minute {
                return false;
            }
        }

        // Check day of week
        if let Some(dow) = self.day_of_week {
            if time.weekday().num_days_from_sunday() != dow {
                return false;
            }
        }

        // Check day of month
        if let Some(dom) = self.day_of_month {
            if time.day() != dom {
                return false;
            }
        }

        true
    }

    /// Get the next run time after the given time
    pub fn next_run(&self, after: &DateTime<Utc>) -> Option<DateTime<Utc>> {
        if let Some(interval) = self.interval_minutes {
            return Some(*after + chrono::Duration::minutes(interval as i64));
        }

        // For time-based schedules, find the next matching time
        // This is a simplified implementation
        let mut candidate = *after + chrono::Duration::minutes(1);

        // Limit search to prevent infinite loops
        for _ in 0..525600 {
            // Max 1 year of minutes
            if self.matches(&candidate) {
                return Some(candidate);
            }
            candidate += chrono::Duration::minutes(1);
        }

        None
    }
}

/// Retention policy for automatic backup cleanup
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    /// Maximum number of backups to keep
    pub max_count: Option<usize>,
    /// Maximum age of backups to keep
    pub max_age: Option<Duration>,
    /// Minimum number of full backups to keep regardless of age
    pub min_full_backups: usize,
    /// Keep daily backups for this many days
    pub keep_daily_for_days: Option<u32>,
    /// Keep weekly backups for this many weeks
    pub keep_weekly_for_weeks: Option<u32>,
    /// Keep monthly backups for this many months
    pub keep_monthly_for_months: Option<u32>,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            max_count: Some(10),
            max_age: Some(Duration::from_secs(30 * 24 * 60 * 60)), // 30 days
            min_full_backups: 1,
            keep_daily_for_days: Some(7),
            keep_weekly_for_weeks: Some(4),
            keep_monthly_for_months: Some(3),
        }
    }
}

impl RetentionPolicy {
    /// Create a new retention policy with max count
    pub fn with_max_count(max_count: usize) -> Self {
        Self {
            max_count: Some(max_count),
            ..Default::default()
        }
    }

    /// Create a new retention policy with max age
    pub fn with_max_age(max_age: Duration) -> Self {
        Self {
            max_age: Some(max_age),
            ..Default::default()
        }
    }

    /// Determine which backups should be retained
    pub fn filter_to_retain<'a>(&self, backups: &'a [Backup]) -> Vec<&'a Backup> {
        let now = Utc::now();
        let mut retained: Vec<&Backup> = Vec::new();
        let mut full_backup_count = 0;

        // Sort by timestamp descending (newest first)
        let mut sorted: Vec<&Backup> = backups
            .iter()
            .filter(|b| b.status == BackupStatus::Completed)
            .collect();
        sorted.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

        for backup in sorted {
            let age = now
                .signed_duration_since(backup.timestamp)
                .to_std()
                .unwrap_or(Duration::ZERO);

            let mut should_keep = false;
            let mut reason = String::new();

            // Check max count
            if let Some(max_count) = self.max_count {
                if retained.len() < max_count {
                    should_keep = true;
                    reason = "within max count".to_string();
                }
            } else {
                should_keep = true;
            }

            // Check max age
            if let Some(max_age) = self.max_age {
                if age > max_age && !should_keep {
                    // Exceeds max age, but check minimum full backups
                    if backup.backup_type == BackupType::Full
                        && full_backup_count < self.min_full_backups
                    {
                        should_keep = true;
                        reason = "minimum full backup".to_string();
                    }
                }
            }

            // Keep minimum full backups regardless
            if backup.backup_type == BackupType::Full && full_backup_count < self.min_full_backups {
                should_keep = true;
                reason = "minimum full backup".to_string();
            }

            if should_keep {
                retained.push(backup);
                if backup.backup_type == BackupType::Full {
                    full_backup_count += 1;
                }
                debug!(
                    backup_id = %backup.id,
                    reason = %reason,
                    "Retaining backup"
                );
            }
        }

        retained
    }

    /// Get backups that should be deleted
    pub fn filter_to_delete<'a>(&self, backups: &'a [Backup]) -> Vec<&'a Backup> {
        let retained = self.filter_to_retain(backups);
        let retained_ids: std::collections::HashSet<_> = retained.iter().map(|b| &b.id).collect();

        backups
            .iter()
            .filter(|b| !retained_ids.contains(&b.id))
            .collect()
    }
}

/// Backup scheduler that manages automatic backups and retention
pub struct BackupScheduler<B: PersistenceBackend + 'static> {
    /// Backup creator
    creator: Arc<BackupCreator<B>>,
    /// Backup schedules
    schedules: RwLock<Vec<BackupSchedule>>,
    /// Retention policy
    retention_policy: RwLock<RetentionPolicy>,
    /// Backup history (recent backups)
    history: RwLock<VecDeque<Backup>>,
    /// Maximum history size
    max_history: usize,
    /// Last run times for each schedule
    last_runs: RwLock<Vec<Option<DateTime<Utc>>>>,
    /// Shutdown signal sender
    #[allow(dead_code)]
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl<B: PersistenceBackend + 'static> BackupScheduler<B> {
    /// Create a new backup scheduler
    pub fn new(creator: BackupCreator<B>) -> Self {
        Self {
            creator: Arc::new(creator),
            schedules: RwLock::new(Vec::new()),
            retention_policy: RwLock::new(RetentionPolicy::default()),
            history: RwLock::new(VecDeque::new()),
            max_history: 100,
            last_runs: RwLock::new(Vec::new()),
            shutdown_tx: None,
        }
    }

    /// Add a backup schedule
    pub fn add_schedule(&self, schedule: BackupSchedule) {
        let mut schedules = self.schedules.write();
        let mut last_runs = self.last_runs.write();
        schedules.push(schedule);
        last_runs.push(None);
    }

    /// Set the retention policy
    pub fn set_retention_policy(&self, policy: RetentionPolicy) {
        *self.retention_policy.write() = policy;
    }

    /// Get the current retention policy
    pub fn retention_policy(&self) -> RetentionPolicy {
        self.retention_policy.read().clone()
    }

    /// Get all schedules
    pub fn schedules(&self) -> Vec<BackupSchedule> {
        self.schedules.read().clone()
    }

    /// Get backup history
    pub fn history(&self) -> Vec<Backup> {
        self.history.read().iter().cloned().collect()
    }

    /// Add a backup to history
    fn add_to_history(&self, backup: Backup) {
        let mut history = self.history.write();
        if history.len() >= self.max_history {
            history.pop_front();
        }
        history.push_back(backup);
    }

    /// Run a single backup based on schedule index
    pub async fn run_scheduled_backup(&self, schedule_index: usize) -> Result<Backup, BackupError> {
        let schedule = {
            let schedules = self.schedules.read();
            schedules
                .get(schedule_index)
                .cloned()
                .ok_or_else(|| BackupError::Config("Invalid schedule index".to_string()))?
        };

        if !schedule.enabled {
            return Err(BackupError::Config("Schedule is disabled".to_string()));
        }

        info!(
            schedule_index = schedule_index,
            backup_type = %schedule.backup_type,
            "Running scheduled backup"
        );

        let backup = self
            .creator
            .create_backup(schedule.backup_type, None)
            .await?;

        // Update last run time
        {
            let mut last_runs = self.last_runs.write();
            if schedule_index < last_runs.len() {
                last_runs[schedule_index] = Some(Utc::now());
            }
        }

        self.add_to_history(backup.clone());

        Ok(backup)
    }

    /// Run a manual backup
    pub async fn run_manual_backup(&self, backup_type: BackupType) -> Result<Backup, BackupError> {
        info!(backup_type = %backup_type, "Running manual backup");

        let backup = self.creator.create_backup(backup_type, None).await?;
        self.add_to_history(backup.clone());

        Ok(backup)
    }

    /// Check if any schedules should run and execute them
    pub async fn check_and_run(&self) -> Vec<Result<Backup, BackupError>> {
        let now = Utc::now();
        let mut results = Vec::new();

        let schedules = self.schedules.read().clone();
        let last_runs = self.last_runs.read().clone();

        for (index, schedule) in schedules.iter().enumerate() {
            if !schedule.enabled {
                continue;
            }

            let should_run = if let Some(interval) = schedule.expression.interval_minutes {
                // Interval-based: check if enough time has passed
                match last_runs.get(index).and_then(|&r| r) {
                    Some(last) => {
                        let elapsed = now.signed_duration_since(last);
                        elapsed.num_minutes() >= interval as i64
                    }
                    None => true, // Never run, should run now
                }
            } else {
                // Time-based: check if current time matches
                schedule.matches(&now)
            };

            if should_run {
                results.push(self.run_scheduled_backup(index).await);
            }
        }

        results
    }

    /// Apply retention policy and get backups to delete
    pub fn get_backups_to_delete(&self) -> Vec<Backup> {
        let history = self.history.read();
        let backups: Vec<Backup> = history.iter().cloned().collect();
        let policy = self.retention_policy.read();

        policy
            .filter_to_delete(&backups)
            .into_iter()
            .cloned()
            .collect()
    }

    /// Clean up old backups based on retention policy
    pub async fn cleanup_old_backups(&self) -> Result<Vec<BackupId>, BackupError> {
        let to_delete = self.get_backups_to_delete();
        let mut deleted_ids = Vec::new();

        for backup in to_delete {
            info!(backup_id = %backup.id, "Deleting backup per retention policy");

            // Delete the backup file
            if let Some(path) = backup.file_path() {
                if path.exists() {
                    if let Err(e) = std::fs::remove_file(&path) {
                        warn!(
                            backup_id = %backup.id,
                            error = %e,
                            "Failed to delete backup file"
                        );
                        continue;
                    }
                }
            }

            deleted_ids.push(backup.id.clone());

            // Remove from history
            let mut history = self.history.write();
            history.retain(|b| b.id != backup.id);
        }

        Ok(deleted_ids)
    }

    /// Start the scheduler loop (runs in background)
    pub async fn start(self: Arc<Self>, check_interval: Duration) -> mpsc::Sender<()> {
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

        let scheduler = Arc::clone(&self);
        tokio::spawn(async move {
            info!("Backup scheduler started");

            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        info!("Backup scheduler shutting down");
                        break;
                    }
                    _ = tokio::time::sleep(check_interval) => {
                        // Check and run scheduled backups
                        let results = scheduler.check_and_run().await;
                        for result in results {
                            match result {
                                Ok(backup) => {
                                    info!(
                                        backup_id = %backup.id,
                                        "Scheduled backup completed successfully"
                                    );
                                }
                                Err(e) => {
                                    error!(error = %e, "Scheduled backup failed");
                                }
                            }
                        }

                        // Clean up old backups
                        if let Err(e) = scheduler.cleanup_old_backups().await {
                            error!(error = %e, "Backup cleanup failed");
                        }
                    }
                }
            }
        });

        shutdown_tx
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup::types::{BackupLocation, BackupTarget};
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

    #[test]
    fn test_schedule_expression_every_minutes() {
        let expr = ScheduleExpression::every_minutes(30);
        assert_eq!(expr.interval_minutes, Some(30));

        let now = Utc::now();
        let next = expr.next_run(&now).unwrap();
        assert_eq!((next - now).num_minutes(), 30);
    }

    #[test]
    fn test_schedule_expression_daily() {
        let expr = ScheduleExpression::daily_at(3);
        assert_eq!(expr.hour, Some(3));
        assert_eq!(expr.minute, Some(0));
        assert!(expr.day_of_week.is_none());
    }

    #[test]
    fn test_schedule_expression_weekly() {
        let expr = ScheduleExpression::weekly_on(0, 2); // Sunday at 2 AM
        assert_eq!(expr.day_of_week, Some(0));
        assert_eq!(expr.hour, Some(2));
    }

    #[test]
    fn test_schedule_expression_matches() {
        let expr = ScheduleExpression::daily_at(14); // 2 PM

        // Create a time at 2 PM
        let time = chrono::NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(14, 0, 0)
            .unwrap()
            .and_utc();

        assert!(expr.matches(&time));

        // Create a time at 3 PM
        let time_3pm = chrono::NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(15, 0, 0)
            .unwrap()
            .and_utc();

        assert!(!expr.matches(&time_3pm));
    }

    #[test]
    fn test_retention_policy_default() {
        let policy = RetentionPolicy::default();
        assert_eq!(policy.max_count, Some(10));
        assert_eq!(policy.min_full_backups, 1);
    }

    #[test]
    fn test_retention_policy_filter() {
        let policy = RetentionPolicy::with_max_count(3);

        let mut backups = Vec::new();
        for i in 0..5 {
            let mut backup = Backup::new(
                BackupType::Full,
                BackupLocation::Local {
                    path: format!("/tmp/backup{}.gz", i),
                },
            );
            backup.status = BackupStatus::Completed;
            // Space them out in time
            backup.timestamp = Utc::now() - chrono::Duration::hours(i as i64);
            backups.push(backup);
        }

        let retained = policy.filter_to_retain(&backups);
        assert_eq!(retained.len(), 3);

        let to_delete = policy.filter_to_delete(&backups);
        assert_eq!(to_delete.len(), 2);
    }

    #[test]
    fn test_retention_policy_min_full_backups() {
        let mut policy = RetentionPolicy::with_max_count(2);
        policy.min_full_backups = 3;

        let mut backups = Vec::new();
        for i in 0..5 {
            let mut backup = Backup::new(
                BackupType::Full,
                BackupLocation::Local {
                    path: format!("/tmp/backup{}.gz", i),
                },
            );
            backup.status = BackupStatus::Completed;
            backup.timestamp = Utc::now() - chrono::Duration::hours(i as i64);
            backups.push(backup);
        }

        let retained = policy.filter_to_retain(&backups);
        // Should keep at least min_full_backups (3) even though max_count is 2
        assert!(retained.len() >= 3);
    }

    #[tokio::test]
    async fn test_scheduler_add_schedule() {
        let temp = tempdir().unwrap();
        let backend = Arc::new(MemoryBackend::new());
        let target = BackupTarget::local(temp.path());
        let creator = BackupCreator::new(backend, target);

        let scheduler = BackupScheduler::new(creator);

        scheduler.add_schedule(BackupSchedule::new(
            ScheduleExpression::every_hours(1),
            BackupType::Full,
        ));

        assert_eq!(scheduler.schedules().len(), 1);
    }

    #[tokio::test]
    async fn test_scheduler_manual_backup() {
        let temp = tempdir().unwrap();
        let backend = Arc::new(MemoryBackend::new());
        let target = BackupTarget::local(temp.path());
        let creator = BackupCreator::new(backend, target);

        let scheduler = BackupScheduler::new(creator);
        let backup = scheduler.run_manual_backup(BackupType::Full).await.unwrap();

        assert!(backup.is_valid());
        assert_eq!(scheduler.history().len(), 1);
    }

    #[tokio::test]
    async fn test_scheduler_history_limit() {
        let temp = tempdir().unwrap();
        let backend = Arc::new(MemoryBackend::new());
        let target = BackupTarget::local(temp.path());
        let creator = BackupCreator::new(backend, target);

        let mut scheduler = BackupScheduler::new(creator);
        scheduler.max_history = 3;

        for _ in 0..5 {
            scheduler.run_manual_backup(BackupType::Full).await.unwrap();
        }

        assert_eq!(scheduler.history().len(), 3);
    }

    #[test]
    fn test_backup_schedule_disabled() {
        let schedule =
            BackupSchedule::new(ScheduleExpression::every_hours(1), BackupType::Full).disabled();

        assert!(!schedule.enabled);
    }

    #[test]
    fn test_backup_schedule_with_description() {
        let schedule = BackupSchedule::new(ScheduleExpression::daily_at(3), BackupType::Full)
            .with_description("Daily 3 AM backup");

        assert_eq!(schedule.description, Some("Daily 3 AM backup".to_string()));
    }
}
