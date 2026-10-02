// Marabunta - Licensed under the MIT License.
//! Victim selection algorithms for the preemption engine

use chrono::{Duration, Utc};

use super::task_state::RunningTask;
use super::types::{PreemptionConstraints, ResourceUsage, VictimSelector};

/// Weights for scoring preemption candidates
#[derive(Debug, Clone)]
pub struct ScoringWeights {
    /// Weight for priority (lower priority = higher score)
    pub priority_weight: f64,
    /// Weight for runtime (shorter runtime = higher score)
    pub runtime_weight: f64,
    /// Weight for checkpoint age (recent checkpoint = higher score)
    pub checkpoint_age_weight: f64,
    /// Weight for resource match (better resource match = higher score)
    pub resource_match_weight: f64,
    /// Weight for preemption count (fewer preemptions = higher score)
    pub preemption_count_weight: f64,
}

impl Default for ScoringWeights {
    fn default() -> Self {
        Self {
            priority_weight: 1.0,
            runtime_weight: 0.5,
            checkpoint_age_weight: 0.3,
            resource_match_weight: 0.4,
            preemption_count_weight: 0.2,
        }
    }
}

impl ScoringWeights {
    /// Create weights optimized for minimizing work loss
    pub fn minimize_work_loss() -> Self {
        Self {
            priority_weight: 0.3,
            runtime_weight: 1.0,
            checkpoint_age_weight: 1.0,
            resource_match_weight: 0.2,
            preemption_count_weight: 0.4,
        }
    }

    /// Create weights optimized for priority enforcement
    pub fn priority_focused() -> Self {
        Self {
            priority_weight: 2.0,
            runtime_weight: 0.2,
            checkpoint_age_weight: 0.2,
            resource_match_weight: 0.3,
            preemption_count_weight: 0.1,
        }
    }

    /// Create weights optimized for resource efficiency
    pub fn resource_focused() -> Self {
        Self {
            priority_weight: 0.3,
            runtime_weight: 0.3,
            checkpoint_age_weight: 0.3,
            resource_match_weight: 1.5,
            preemption_count_weight: 0.3,
        }
    }
}

/// Scorer for selecting victims for preemption
pub struct VictimScorer {
    weights: ScoringWeights,
}

impl VictimScorer {
    /// Create a new victim scorer with the given weights
    pub fn new(weights: ScoringWeights) -> Self {
        Self { weights }
    }

    /// Create a victim scorer with default weights
    pub fn with_default_weights() -> Self {
        Self::new(ScoringWeights::default())
    }

    /// Score a task as a preemption candidate
    /// Higher scores indicate better candidates for preemption
    pub fn score(
        &self,
        task: &RunningTask,
        needed_resources: &ResourceUsage,
        selector: &VictimSelector,
    ) -> f64 {
        self.combine_scores(task, selector, needed_resources)
    }

    /// Select best victims to free required resources
    /// Returns tasks sorted by score (highest = best candidate)
    pub fn select_victims(
        &self,
        tasks: &[RunningTask],
        needed_resources: &ResourceUsage,
        selector: &VictimSelector,
        constraints: &PreemptionConstraints,
    ) -> Vec<(RunningTask, f64)> {
        let now = Utc::now();

        // Filter and score tasks
        let mut candidates: Vec<(RunningTask, f64)> = tasks
            .iter()
            .filter(|task| self.is_eligible(task, constraints, now))
            .map(|task| {
                let score = self.score(task, needed_resources, selector);
                (task.clone(), score)
            })
            .collect();

        // Sort by score descending (highest score = best candidate)
        candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // Select minimum set to satisfy resource requirements
        self.select_minimum_set(candidates, needed_resources)
    }

    /// Check if a task is eligible for preemption given constraints
    fn is_eligible(
        &self,
        task: &RunningTask,
        constraints: &PreemptionConstraints,
        now: chrono::DateTime<Utc>,
    ) -> bool {
        // Must be preemptible
        if !task.preemptible {
            return false;
        }

        // Check minimum runtime
        if let Some(min_runtime) = constraints.min_runtime {
            if task.runtime() < min_runtime {
                return false;
            }
        }

        // Check max preemptions
        if let Some(max_preemptions) = constraints.max_preemptions {
            if task.preemption_count >= max_preemptions {
                return false;
            }
        }

        // Check cooldown
        if let Some(cooldown) = constraints.cooldown {
            if task.is_in_cooldown(cooldown) {
                return false;
            }
        }

        // Check protected jobs
        for pattern in &constraints.protected_jobs {
            if self.matches_pattern(&task.job_id, pattern) {
                return false;
            }
        }

        // Check protected nodes
        for pattern in &constraints.protected_nodes {
            if self.matches_pattern(&task.node_id, pattern) {
                return false;
            }
        }

        // Check blackout windows
        for window in &constraints.blackout_windows {
            if window.is_active(now) {
                return false;
            }
        }

        true
    }

    /// Simple pattern matching (supports * as wildcard)
    fn matches_pattern(&self, value: &str, pattern: &str) -> bool {
        if pattern == "*" {
            return true;
        }
        if pattern.starts_with('*') && pattern.ends_with('*') {
            let inner = &pattern[1..pattern.len() - 1];
            return value.contains(inner);
        }
        if let Some(suffix) = pattern.strip_prefix('*') {
            return value.ends_with(suffix);
        }
        if let Some(prefix) = pattern.strip_suffix('*') {
            return value.starts_with(prefix);
        }
        value == pattern
    }

    /// Select minimum set of tasks to free required resources
    fn select_minimum_set(
        &self,
        candidates: Vec<(RunningTask, f64)>,
        needed: &ResourceUsage,
    ) -> Vec<(RunningTask, f64)> {
        let mut selected = Vec::new();
        let mut freed = ResourceUsage::zero();

        for (task, score) in candidates {
            if freed.can_satisfy(needed) {
                break;
            }
            freed.add(&task.resources);
            selected.push((task, score));
        }

        selected
    }

    /// Score based on lowest priority
    fn score_lowest_priority(&self, task: &RunningTask) -> f64 {
        // Normalize priority to 0-1 range (assuming priority 0-100)
        // Lower priority = higher score
        let normalized = (100 - task.priority.max(0).min(100)) as f64 / 100.0;
        normalized * self.weights.priority_weight
    }

    /// Score based on shortest running time
    fn score_shortest_running(&self, task: &RunningTask) -> f64 {
        // Shorter runtime = higher score
        // Use inverse of runtime in hours (capped at 24 hours)
        let hours = task.runtime().num_minutes() as f64 / 60.0;
        let capped_hours = hours.min(24.0);
        let normalized = 1.0 - (capped_hours / 24.0);
        normalized * self.weights.runtime_weight
    }

    /// Score based on longest running time
    fn score_longest_running(&self, task: &RunningTask) -> f64 {
        // Longer runtime = higher score
        let hours = task.runtime().num_minutes() as f64 / 60.0;
        let capped_hours = hours.min(24.0);
        let normalized = capped_hours / 24.0;
        normalized * self.weights.runtime_weight
    }

    /// Score based on most recent checkpoint
    fn score_most_recent_checkpoint(&self, task: &RunningTask) -> f64 {
        // More recent checkpoint = higher score (less work lost)
        if let Some(ref checkpoint) = task.last_checkpoint {
            let age_minutes = checkpoint.age().num_minutes() as f64;
            // Cap at 60 minutes, normalize
            let capped_age = age_minutes.min(60.0);
            let normalized = 1.0 - (capped_age / 60.0);
            normalized * self.weights.checkpoint_age_weight
        } else {
            // No checkpoint = lowest score for this factor
            0.0
        }
    }

    /// Score based on resource match
    fn score_resource_match(&self, task: &RunningTask, needed: &ResourceUsage) -> f64 {
        // Better match between task resources and needed resources = higher score
        // We want to minimize wasted resources (freeing too much)
        let task_res = &task.resources;

        let cpu_ratio = if needed.cpu_cores > 0.0 {
            (task_res.cpu_cores / needed.cpu_cores).min(2.0) / 2.0
        } else {
            0.5
        };

        let mem_ratio = if needed.memory_gb > 0.0 {
            (task_res.memory_gb / needed.memory_gb).min(2.0) / 2.0
        } else {
            0.5
        };

        let gpu_ratio = if needed.gpu_count > 0 {
            (task_res.gpu_count as f64 / needed.gpu_count as f64).min(2.0) / 2.0
        } else {
            0.5
        };

        let disk_ratio = if needed.disk_gb > 0.0 {
            (task_res.disk_gb / needed.disk_gb).min(2.0) / 2.0
        } else {
            0.5
        };

        // Average of ratios, penalizing both over and under
        let avg_ratio = (cpu_ratio + mem_ratio + gpu_ratio + disk_ratio) / 4.0;
        // Convert to score: 0.5 (perfect match) = 1.0 score, deviation reduces score
        let match_quality = 1.0 - (avg_ratio - 0.5).abs() * 2.0;

        match_quality.max(0.0) * self.weights.resource_match_weight
    }

    /// Score based on preemption count
    fn score_preemption_count(&self, task: &RunningTask) -> f64 {
        // Fewer preemptions = higher score (fairness)
        let normalized = 1.0 - (task.preemption_count as f64 / 10.0).min(1.0);
        normalized * self.weights.preemption_count_weight
    }

    /// Score based on time estimate overage
    fn score_over_time_estimate(&self, task: &RunningTask) -> f64 {
        if task.is_over_time_estimate() {
            // Over estimate = good candidate
            if let Some(estimate) = task.time_estimate {
                let overage = task.runtime().num_seconds() as f64 / estimate.num_seconds() as f64;
                (overage - 1.0).min(1.0) // Cap at 2x overage
            } else {
                0.0
            }
        } else {
            0.0
        }
    }

    /// Score for job type matching
    fn score_job_type(&self, task: &RunningTask, target_types: &[String]) -> f64 {
        if let Some(ref job_type) = task.job_type {
            if target_types.iter().any(|t| t == job_type) {
                1.0
            } else {
                0.0
            }
        } else {
            0.0
        }
    }

    /// Score for domain matching
    fn score_from_domain(&self, task: &RunningTask, target_domain: &str) -> f64 {
        if task.authority_domain == target_domain {
            1.0
        } else {
            0.0
        }
    }

    /// Combine scores based on VictimSelector
    fn combine_scores(
        &self,
        task: &RunningTask,
        selector: &VictimSelector,
        needed: &ResourceUsage,
    ) -> f64 {
        match selector {
            VictimSelector::LowestPriority => {
                self.score_lowest_priority(task)
                    + self.score_preemption_count(task) * 0.5
                    + self.score_resource_match(task, needed) * 0.3
            }

            VictimSelector::ShortestRunning => {
                self.score_shortest_running(task)
                    + self.score_most_recent_checkpoint(task) * 0.5
                    + self.score_resource_match(task, needed) * 0.3
            }

            VictimSelector::LongestRunning => {
                self.score_longest_running(task)
                    + self.score_preemption_count(task) * 0.3
                    + self.score_resource_match(task, needed) * 0.3
            }

            VictimSelector::MostRecentCheckpoint => {
                self.score_most_recent_checkpoint(task)
                    + self.score_lowest_priority(task) * 0.3
                    + self.score_resource_match(task, needed) * 0.3
            }

            VictimSelector::JobType(types) => {
                self.score_job_type(task, types) * 2.0
                    + self.score_lowest_priority(task) * 0.5
                    + self.score_resource_match(task, needed) * 0.3
            }

            VictimSelector::FromDomain(domain) => {
                self.score_from_domain(task, domain) * 2.0
                    + self.score_lowest_priority(task) * 0.5
                    + self.score_resource_match(task, needed) * 0.3
            }

            VictimSelector::OverTimeEstimate => {
                self.score_over_time_estimate(task) * 2.0
                    + self.score_lowest_priority(task) * 0.5
                    + self.score_resource_match(task, needed) * 0.3
            }

            VictimSelector::Custom { score_function } => {
                // Custom scoring would be implemented via a registry
                // For now, fall back to priority-based
                tracing::warn!(
                    "Custom score function '{}' not implemented, using priority-based",
                    score_function
                );
                self.score_lowest_priority(task) + self.score_resource_match(task, needed) * 0.5
            }

            VictimSelector::Cascade(selectors) => {
                // Try each selector, return first non-zero score
                for selector in selectors {
                    let score = self.combine_scores(task, selector, needed);
                    if score > 0.0 {
                        return score;
                    }
                }
                0.0
            }

            VictimSelector::Weighted { selectors } => {
                // Weighted combination of scores
                let mut total_score = 0.0;
                let mut total_weight = 0.0;

                for (selector, weight) in selectors {
                    total_score += self.combine_scores(task, selector, needed) * weight;
                    total_weight += weight;
                }

                if total_weight > 0.0 {
                    total_score / total_weight
                } else {
                    0.0
                }
            }
            VictimSelector::BpfNegotiator { payload } => {
                use crate::preemption::bpf_arena::{BpfMarketArena, ThermodynamicTelemetry};
                                
                let telemetry = ThermodynamicTelemetry {
                    thermal_celsius: 65.0, // Simulated real-time sensor reading
                    available_memory_mb: 8192,
                    current_spot_price_mmx: 1.5,
                    network_latency_ms: 12,
                };
                
                let arena = BpfMarketArena::new("local_node".into(), telemetry);
                match arena.conduct_trial(payload, task) {
                    Ok(true) => 1.0, // BPF Lawyer mathematically won. Evict the victim.
                    Ok(false) => 0.0, // BPF Lawyer lost the argument. Victim is protected.
                    Err(_) => 0.0, // Fail-safe: if BPF faults, preserve the running task.
                }
            }
        }
    }
}

/// Helper to estimate recomputation time for a task
pub fn estimate_recomputation(task: &RunningTask) -> Duration {
    if let Some(ref checkpoint) = task.last_checkpoint {
        // Time since last checkpoint is work that would need to be redone
        checkpoint.age()
    } else {
        // No checkpoint means all work is lost
        task.runtime()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::preemption::task_state::CheckpointInfo;

    fn create_test_task(task_id: &str, priority: i32, cpu: f64, memory: f64) -> RunningTask {
        RunningTask::new(
            task_id,
            "job-1",
            "node-1",
            priority,
            ResourceUsage::new(cpu, memory, 0, 10.0),
        )
    }

    #[test]
    fn test_score_lowest_priority() {
        let scorer = VictimScorer::with_default_weights();

        let low_priority = create_test_task("task-1", 10, 2.0, 4.0);
        let high_priority = create_test_task("task-2", 90, 2.0, 4.0);

        let low_score = scorer.score_lowest_priority(&low_priority);
        let high_score = scorer.score_lowest_priority(&high_priority);

        // Lower priority should have higher score
        assert!(low_score > high_score);
    }

    #[test]
    fn test_score_resource_match() {
        let scorer = VictimScorer::with_default_weights();

        let needed = ResourceUsage::new(4.0, 8.0, 0, 10.0);

        let perfect_match = create_test_task("task-1", 50, 4.0, 8.0);
        let under_match = create_test_task("task-2", 50, 2.0, 4.0);
        let over_match = create_test_task("task-3", 50, 8.0, 16.0);

        let perfect_score = scorer.score_resource_match(&perfect_match, &needed);
        let under_score = scorer.score_resource_match(&under_match, &needed);
        let over_score = scorer.score_resource_match(&over_match, &needed);

        // Perfect match should have higher score than mismatches
        assert!(perfect_score >= under_score);
        assert!(perfect_score >= over_score);
    }

    #[test]
    fn test_select_victims() {
        let scorer = VictimScorer::with_default_weights();

        let tasks = vec![
            create_test_task("task-1", 10, 2.0, 4.0),
            create_test_task("task-2", 50, 2.0, 4.0),
            create_test_task("task-3", 90, 2.0, 4.0),
        ];

        let needed = ResourceUsage::new(3.0, 6.0, 0, 15.0);
        let constraints = PreemptionConstraints::default();

        let victims = scorer.select_victims(
            &tasks,
            &needed,
            &VictimSelector::LowestPriority,
            &constraints,
        );

        // Should select low priority tasks first
        assert!(!victims.is_empty());
        assert_eq!(victims[0].0.task_id, "task-1");
    }

    #[test]
    fn test_constraints_filtering() {
        let scorer = VictimScorer::with_default_weights();

        let mut task = create_test_task("task-1", 10, 2.0, 4.0);
        task.preemption_count = 5;

        let tasks = vec![task];
        let needed = ResourceUsage::new(2.0, 4.0, 0, 10.0);

        let constraints = PreemptionConstraints::default().with_max_preemptions(3);

        let victims = scorer.select_victims(
            &tasks,
            &needed,
            &VictimSelector::LowestPriority,
            &constraints,
        );

        // Task should be filtered out due to max preemptions constraint
        assert!(victims.is_empty());
    }

    #[test]
    fn test_checkpoint_scoring() {
        let scorer = VictimScorer::with_default_weights();

        let mut task_with_checkpoint = create_test_task("task-1", 50, 2.0, 4.0);
        let checkpoint = CheckpointInfo::new("ckpt-1", 1024, "/storage/ckpt-1");
        task_with_checkpoint.last_checkpoint = Some(checkpoint);

        let task_without_checkpoint = create_test_task("task-2", 50, 2.0, 4.0);

        let with_score = scorer.score_most_recent_checkpoint(&task_with_checkpoint);
        let without_score = scorer.score_most_recent_checkpoint(&task_without_checkpoint);

        // Task with checkpoint should have higher score
        assert!(with_score > without_score);
    }

    #[test]
    fn test_pattern_matching() {
        let scorer = VictimScorer::with_default_weights();

        assert!(scorer.matches_pattern("job-123", "job-*"));
        assert!(scorer.matches_pattern("test-job", "*-job"));
        assert!(scorer.matches_pattern("job-123-test", "*123*"));
        assert!(scorer.matches_pattern("anything", "*"));
        assert!(scorer.matches_pattern("exact", "exact"));
        assert!(!scorer.matches_pattern("different", "exact"));
    }

    #[test]
    fn test_weighted_selector() {
        let scorer = VictimScorer::with_default_weights();

        let task = create_test_task("task-1", 30, 2.0, 4.0);
        let needed = ResourceUsage::new(2.0, 4.0, 0, 10.0);

        let weighted_selector = VictimSelector::Weighted {
            selectors: vec![
                (VictimSelector::LowestPriority, 0.7),
                (VictimSelector::ShortestRunning, 0.3),
            ],
        };

        let score = scorer.score(&task, &needed, &weighted_selector);
        assert!(score > 0.0);
    }

    #[test]
    fn test_cascade_selector() {
        let scorer = VictimScorer::with_default_weights();

        let mut task = create_test_task("task-1", 30, 2.0, 4.0);
        task.job_type = Some("batch".to_string());

        let needed = ResourceUsage::new(2.0, 4.0, 0, 10.0);

        let cascade_selector = VictimSelector::Cascade(vec![
            VictimSelector::JobType(vec!["batch".to_string()]),
            VictimSelector::LowestPriority,
        ]);

        let score = scorer.score(&task, &needed, &cascade_selector);
        assert!(score > 0.0);
    }
}
