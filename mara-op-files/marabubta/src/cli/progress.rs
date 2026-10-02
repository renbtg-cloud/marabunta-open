// Marabunta - Licensed under the MIT License.
//! Progress bars and spinners for long-running CLI operations
//!
//! Provides visual feedback for operations like job submission, file uploads,
//! and long-running commands.
//!
//! # Example
//!
//! ```rust,ignore
//! use marabunta_compute::cli::progress::{ProgressTracker, MultiProgress};
//!
//! // Single progress bar
//! let progress = ProgressTracker::new_bar(100, "Processing tasks");
//! for i in 0..100 {
//!     progress.inc(1);
//! }
//! progress.finish("Done!");
//!
//! // Multi-progress for parallel operations
//! let multi = MultiProgress::new();
//! let bar1 = multi.add_bar(50, "Task 1");
//! let bar2 = multi.add_bar(50, "Task 2");
//! ```

use console::style;
use indicatif::{
    MultiProgress as IndicatifMultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle,
};
use std::sync::Arc;
use std::time::Duration;

// ─────────────────────────────────────────────────────────────────────────────
// PROGRESS STYLE PRESETS
// ─────────────────────────────────────────────────────────────────────────────

/// Predefined progress bar styles
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProgressStylePreset {
    /// Default bar style with percentage
    #[default]
    Default,
    /// Download/upload style with bytes
    Download,
    /// Spinner for indefinite operations
    Spinner,
    /// Task execution style with count
    Tasks,
    /// Minimal style (just percentage)
    Minimal,
    /// Verbose style with ETA and speed
    Verbose,
}

impl ProgressStylePreset {
    /// Get the indicatif ProgressStyle for this preset
    pub fn to_style(&self) -> ProgressStyle {
        match self {
            ProgressStylePreset::Default => ProgressStyle::default_bar()
                .template("{spinner:.green} {msg} [{bar:40.cyan/blue}] {pos}/{len} ({percent}%)")
                .unwrap()
                .progress_chars("=>-"),

            ProgressStylePreset::Download => ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} {msg} [{bar:30.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec})",
                )
                .unwrap()
                .progress_chars("=>-"),

            ProgressStylePreset::Spinner => ProgressStyle::default_spinner()
                .template("{spinner:.green} {msg}")
                .unwrap(),

            ProgressStylePreset::Tasks => ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} {msg}\n    [{bar:50.cyan/blue}] {pos}/{len} tasks ({percent}%)",
                )
                .unwrap()
                .progress_chars("=>-"),

            ProgressStylePreset::Minimal => ProgressStyle::default_bar()
                .template("{bar:40.cyan/blue} {percent}%")
                .unwrap()
                .progress_chars("=>-"),

            ProgressStylePreset::Verbose => ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} {msg}\n    [{bar:40.cyan/blue}] {pos}/{len} ({percent}%)\n    ETA: {eta} | Elapsed: {elapsed_precise} | Speed: {per_sec}",
                )
                .unwrap()
                .progress_chars("=>-"),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PROGRESS TRACKER
// ─────────────────────────────────────────────────────────────────────────────

/// A wrapper around indicatif's ProgressBar with convenience methods
pub struct ProgressTracker {
    bar: ProgressBar,
    /// Whether to show the progress bar (can be disabled for non-TTY)
    enabled: bool,
}

impl ProgressTracker {
    /// Create a new progress bar with the given total and message
    pub fn new_bar(total: u64, message: impl Into<String>) -> Self {
        let bar = ProgressBar::new(total);
        bar.set_style(ProgressStylePreset::Default.to_style());
        bar.set_message(message.into());
        bar.enable_steady_tick(Duration::from_millis(100));

        Self { bar, enabled: true }
    }

    /// Create a new progress bar with a specific style preset
    pub fn new_bar_styled(total: u64, message: impl Into<String>, style: ProgressStylePreset) -> Self {
        let bar = ProgressBar::new(total);
        bar.set_style(style.to_style());
        bar.set_message(message.into());
        bar.enable_steady_tick(Duration::from_millis(100));

        Self { bar, enabled: true }
    }

    /// Create a new spinner for indefinite operations
    pub fn new_spinner(message: impl Into<String>) -> Self {
        let bar = ProgressBar::new_spinner();
        bar.set_style(ProgressStylePreset::Spinner.to_style());
        bar.set_message(message.into());
        bar.enable_steady_tick(Duration::from_millis(100));

        Self { bar, enabled: true }
    }

    /// Create a hidden progress bar (for non-TTY environments)
    pub fn hidden() -> Self {
        let bar = ProgressBar::hidden();
        Self {
            bar,
            enabled: false,
        }
    }

    /// Check if running in a terminal
    pub fn is_terminal() -> bool {
        std::env::var("TERM")
            .map(|t| !t.is_empty() && t != "dumb")
            .unwrap_or(false)
            && std::env::var("CI").is_err()
            && std::env::var("NO_PROGRESS").is_err()
    }

    /// Create appropriate tracker based on terminal detection
    pub fn auto(total: u64, message: impl Into<String>) -> Self {
        if Self::is_terminal() {
            Self::new_bar(total, message)
        } else {
            let msg = message.into();
            println!("{}", msg);
            Self::hidden()
        }
    }

    /// Create appropriate spinner based on terminal detection
    pub fn auto_spinner(message: impl Into<String>) -> Self {
        if Self::is_terminal() {
            Self::new_spinner(message)
        } else {
            let msg = message.into();
            println!("{}", msg);
            Self::hidden()
        }
    }

    /// Set the progress bar style
    pub fn set_style(&self, style: ProgressStylePreset) {
        if self.enabled {
            self.bar.set_style(style.to_style());
        }
    }

    /// Set the message
    pub fn set_message(&self, message: impl Into<String>) {
        if self.enabled {
            self.bar.set_message(message.into());
        }
    }

    /// Set the position
    pub fn set_position(&self, pos: u64) {
        if self.enabled {
            self.bar.set_position(pos);
        }
    }

    /// Increment the position by delta
    pub fn inc(&self, delta: u64) {
        if self.enabled {
            self.bar.inc(delta);
        }
    }

    /// Set the total length
    pub fn set_length(&self, len: u64) {
        if self.enabled {
            self.bar.set_length(len);
        }
    }

    /// Get current position
    pub fn position(&self) -> u64 {
        self.bar.position()
    }

    /// Get current length
    pub fn length(&self) -> Option<u64> {
        self.bar.length()
    }

    /// Check if finished
    pub fn is_finished(&self) -> bool {
        self.bar.is_finished()
    }

    /// Finish the progress bar with a message
    pub fn finish(&self, message: impl Into<String>) {
        if self.enabled {
            self.bar.finish_with_message(message.into());
        }
    }

    /// Finish and clear the progress bar
    pub fn finish_and_clear(&self) {
        if self.enabled {
            self.bar.finish_and_clear();
        }
    }

    /// Finish the progress bar (without message change)
    pub fn finish_silent(&self) {
        if self.enabled {
            self.bar.finish();
        }
    }

    /// Abandon the progress bar (mark as incomplete)
    pub fn abandon(&self, message: impl Into<String>) {
        if self.enabled {
            self.bar.abandon_with_message(message.into());
        }
    }

    /// Suspend the progress bar for printing
    pub fn suspend<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        self.bar.suspend(f)
    }

    /// Get the underlying ProgressBar (for advanced usage)
    pub fn inner(&self) -> &ProgressBar {
        &self.bar
    }
}

impl Drop for ProgressTracker {
    fn drop(&mut self) {
        if self.enabled && !self.bar.is_finished() {
            self.bar.finish_and_clear();
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MULTI-PROGRESS
// ─────────────────────────────────────────────────────────────────────────────

/// Manager for multiple concurrent progress bars
pub struct MultiProgress {
    multi: Arc<IndicatifMultiProgress>,
    enabled: bool,
}

impl MultiProgress {
    /// Create a new multi-progress manager
    pub fn new() -> Self {
        let multi = IndicatifMultiProgress::new();
        let enabled = ProgressTracker::is_terminal();

        if !enabled {
            multi.set_draw_target(ProgressDrawTarget::hidden());
        }

        Self {
            multi: Arc::new(multi),
            enabled,
        }
    }

    /// Add a progress bar to the multi-progress
    pub fn add_bar(&self, total: u64, message: impl Into<String>) -> ProgressTracker {
        let bar = self.multi.add(ProgressBar::new(total));
        bar.set_style(ProgressStylePreset::Default.to_style());
        bar.set_message(message.into());
        bar.enable_steady_tick(Duration::from_millis(100));

        ProgressTracker {
            bar,
            enabled: self.enabled,
        }
    }

    /// Add a spinner to the multi-progress
    pub fn add_spinner(&self, message: impl Into<String>) -> ProgressTracker {
        let bar = self.multi.add(ProgressBar::new_spinner());
        bar.set_style(ProgressStylePreset::Spinner.to_style());
        bar.set_message(message.into());
        bar.enable_steady_tick(Duration::from_millis(100));

        ProgressTracker {
            bar,
            enabled: self.enabled,
        }
    }

    /// Add a progress bar with custom style
    pub fn add_styled(
        &self,
        total: u64,
        message: impl Into<String>,
        style: ProgressStylePreset,
    ) -> ProgressTracker {
        let bar = self.multi.add(ProgressBar::new(total));
        bar.set_style(style.to_style());
        bar.set_message(message.into());
        bar.enable_steady_tick(Duration::from_millis(100));

        ProgressTracker {
            bar,
            enabled: self.enabled,
        }
    }

    /// Insert a progress bar at a specific position
    pub fn insert(&self, index: usize, total: u64, message: impl Into<String>) -> ProgressTracker {
        let bar = self.multi.insert(index, ProgressBar::new(total));
        bar.set_style(ProgressStylePreset::Default.to_style());
        bar.set_message(message.into());
        bar.enable_steady_tick(Duration::from_millis(100));

        ProgressTracker {
            bar,
            enabled: self.enabled,
        }
    }

    /// Suspend the multi-progress for printing
    pub fn suspend<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        self.multi.suspend(f)
    }

    /// Print a line above the progress bars
    pub fn println(&self, msg: impl AsRef<str>) -> std::io::Result<()> {
        self.multi.println(msg)
    }

    /// Clear all progress bars
    pub fn clear(&self) -> std::io::Result<()> {
        self.multi.clear()
    }

    /// Check if this multi-progress is visible
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

impl Default for MultiProgress {
    fn default() -> Self {
        Self::new()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// JOB PROGRESS TRACKER
// ─────────────────────────────────────────────────────────────────────────────

/// Specialized progress tracker for job monitoring
pub struct JobProgressTracker {
    multi: MultiProgress,
    main_bar: ProgressTracker,
    task_bar: Option<ProgressTracker>,
    job_id: String,
}

impl JobProgressTracker {
    /// Create a new job progress tracker
    pub fn new(job_id: impl Into<String>, total_tasks: u64) -> Self {
        let job_id = job_id.into();
        let multi = MultiProgress::new();

        let main_bar = multi.add_styled(
            100,
            format!("Job {}", &job_id[..12.min(job_id.len())]),
            ProgressStylePreset::Verbose,
        );

        let task_bar = if total_tasks > 0 {
            Some(multi.add_styled(
                total_tasks,
                "Tasks",
                ProgressStylePreset::Tasks,
            ))
        } else {
            None
        };

        Self {
            multi,
            main_bar,
            task_bar,
            job_id,
        }
    }

    /// Update progress from job status
    pub fn update(&self, progress: f64, completed: u32, total: u32, running: u32, failed: u32) {
        // Update main progress bar (percentage)
        let percent = (progress * 100.0) as u64;
        self.main_bar.set_position(percent);

        let status = if failed > 0 {
            format!(
                "Job {} - {} completed, {} running, {} failed",
                &self.job_id[..12.min(self.job_id.len())],
                style(completed).green(),
                style(running).yellow(),
                style(failed).red()
            )
        } else {
            format!(
                "Job {} - {} completed, {} running",
                &self.job_id[..12.min(self.job_id.len())],
                style(completed).green(),
                style(running).yellow()
            )
        };
        self.main_bar.set_message(status);

        // Update task bar if present
        if let Some(ref task_bar) = self.task_bar {
            task_bar.set_length(total as u64);
            task_bar.set_position(completed as u64);
        }
    }

    /// Update with Monte Carlo estimate
    pub fn update_monte_carlo(&self, mean: f64, std_error: f64, converged: bool) {
        let converge_status = if converged {
            style("CONVERGED").green().bold()
        } else {
            style("running").yellow()
        };

        self.multi
            .println(format!(
                "    Estimate: {} +/- {} [{}]",
                style(format!("{:.6}", mean)).cyan(),
                style(format!("{:.6}", std_error)).dim(),
                converge_status
            ))
            .ok();
    }

    /// Finish with success
    pub fn finish_success(&self) {
        self.main_bar.finish(format!(
            "{} Job {} completed!",
            style("").green(),
            &self.job_id[..12.min(self.job_id.len())]
        ));
        if let Some(ref task_bar) = self.task_bar {
            task_bar.finish_and_clear();
        }
    }

    /// Finish with failure
    pub fn finish_failed(&self, error: Option<&str>) {
        let msg = if let Some(err) = error {
            format!(
                "{} Job {} failed: {}",
                style("").red(),
                &self.job_id[..12.min(self.job_id.len())],
                err
            )
        } else {
            format!(
                "{} Job {} failed",
                style("").red(),
                &self.job_id[..12.min(self.job_id.len())]
            )
        };
        self.main_bar.abandon(msg);
        if let Some(ref task_bar) = self.task_bar {
            task_bar.finish_and_clear();
        }
    }

    /// Finish with cancellation
    pub fn finish_cancelled(&self) {
        self.main_bar.abandon(format!(
            "{} Job {} cancelled",
            style("").yellow(),
            &self.job_id[..12.min(self.job_id.len())]
        ));
        if let Some(ref task_bar) = self.task_bar {
            task_bar.finish_and_clear();
        }
    }

    /// Print a message while keeping progress visible
    pub fn println(&self, msg: impl AsRef<str>) -> std::io::Result<()> {
        self.multi.println(msg)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// UPLOAD PROGRESS
// ─────────────────────────────────────────────────────────────────────────────

/// Progress tracker for file uploads
pub struct UploadProgressTracker {
    bar: ProgressTracker,
    filename: String,
}

impl UploadProgressTracker {
    /// Create a new upload progress tracker
    pub fn new(filename: impl Into<String>, total_bytes: u64) -> Self {
        let filename = filename.into();
        let bar = ProgressTracker::new_bar_styled(
            total_bytes,
            format!("Uploading {}", &filename),
            ProgressStylePreset::Download,
        );

        Self { bar, filename }
    }

    /// Update bytes uploaded
    pub fn set_bytes(&self, bytes: u64) {
        self.bar.set_position(bytes);
    }

    /// Increment bytes uploaded
    pub fn inc_bytes(&self, bytes: u64) {
        self.bar.inc(bytes);
    }

    /// Finish the upload
    pub fn finish(&self) {
        self.bar.finish(format!(
            "{} Uploaded {}",
            style("").green(),
            &self.filename
        ));
    }

    /// Fail the upload
    pub fn fail(&self, error: &str) {
        self.bar.abandon(format!(
            "{} Failed to upload {}: {}",
            style("").red(),
            &self.filename,
            error
        ));
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TESTS
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_tracker_basic() {
        let tracker = ProgressTracker::new_bar(100, "Test");
        tracker.inc(50);
        assert_eq!(tracker.position(), 50);
        tracker.finish("Done");
        assert!(tracker.is_finished());
    }

    #[test]
    fn test_progress_tracker_hidden() {
        let tracker = ProgressTracker::hidden();
        tracker.inc(50);
        tracker.finish("Done");
    }

    #[test]
    fn test_multi_progress() {
        let multi = MultiProgress::new();
        let bar1 = multi.add_bar(100, "Bar 1");
        let bar2 = multi.add_bar(100, "Bar 2");

        bar1.inc(25);
        bar2.inc(50);

        bar1.finish("Done 1");
        bar2.finish("Done 2");
    }

    #[test]
    fn test_style_presets() {
        // Just verify styles compile
        let _ = ProgressStylePreset::Default.to_style();
        let _ = ProgressStylePreset::Download.to_style();
        let _ = ProgressStylePreset::Spinner.to_style();
        let _ = ProgressStylePreset::Tasks.to_style();
        let _ = ProgressStylePreset::Minimal.to_style();
        let _ = ProgressStylePreset::Verbose.to_style();
    }
}
