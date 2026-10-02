// Marabunta - Licensed under the MIT License.
//! Business metrics for Marabunta Compute
//!
//! Provides high-level business metrics including throughput rates,
//! queue depths, SLA compliance, and resource utilization metrics.

use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::histogram::{buckets, Labels, LabeledHistogram};
use super::summary::{LabeledSummary, SummaryConfig};

/// Business metrics collection for the Marabunta cluster
#[derive(Debug)]
pub struct BusinessMetrics {
    /// Job throughput metrics
    pub jobs: JobMetrics,
    /// Task throughput metrics
    pub tasks: TaskMetrics,
    /// Queue metrics
    pub queues: QueueMetrics,
    /// Worker metrics
    pub workers: WorkerMetrics,
    /// SLA metrics
    pub sla: SlaMetrics,
    /// Resource metrics
    pub resources: ResourceMetrics,
}

impl BusinessMetrics {
    /// Create a new business metrics collection
    pub fn new() -> Self {
        Self {
            jobs: JobMetrics::new(),
            tasks: TaskMetrics::new(),
            queues: QueueMetrics::new(),
            workers: WorkerMetrics::new(),
            sla: SlaMetrics::new(),
            resources: ResourceMetrics::new(),
        }
    }

    /// Encode all business metrics in Prometheus format
    pub fn prometheus_encode(&self) -> String {
        let mut output = String::new();
        output.push_str(&self.jobs.prometheus_encode());
        output.push_str(&self.tasks.prometheus_encode());
        output.push_str(&self.queues.prometheus_encode());
        output.push_str(&self.workers.prometheus_encode());
        output.push_str(&self.sla.prometheus_encode());
        output.push_str(&self.resources.prometheus_encode());
        output
    }
}

impl Default for BusinessMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Job Metrics
// =============================================================================

/// Job throughput and status metrics
#[derive(Debug)]
pub struct JobMetrics {
    /// Total jobs submitted
    submitted_total: AtomicU64,
    /// Jobs by status
    by_status: RwLock<HashMap<String, AtomicU64>>,
    /// Jobs per second rate calculator
    rate_calculator: RateCalculator,
    /// Job duration histogram
    duration_histogram: LabeledHistogram,
    /// Job queue time (time from submission to start)
    queue_time_summary: LabeledSummary,
}

impl JobMetrics {
    /// Create new job metrics
    pub fn new() -> Self {
        Self {
            submitted_total: AtomicU64::new(0),
            by_status: RwLock::new(HashMap::new()),
            rate_calculator: RateCalculator::new(Duration::from_secs(60)),
            duration_histogram: LabeledHistogram::new(
                "marabunta_job_duration_seconds",
                "Job execution duration in seconds",
                buckets::TASK_DURATION,
            ),
            queue_time_summary: LabeledSummary::with_config(
                "marabunta_job_queue_time_seconds",
                "Time jobs spend waiting in queue",
                SummaryConfig::with_quantiles(&[0.5, 0.9, 0.95, 0.99])
                    .max_age(Duration::from_secs(300)),
            ),
        }
    }

    /// Record a job submission
    pub fn record_submission(&self) {
        self.submitted_total.fetch_add(1, Ordering::Relaxed);
        self.rate_calculator.record();
        self.inc_status("submitted");
    }

    /// Record a job status change
    pub fn record_status_change(&self, status: &str) {
        self.inc_status(status);
    }

    /// Record job completion with duration
    pub fn record_completion(&self, duration_secs: f64, job_type: &str) {
        let labels = Labels::new(&[("job_type", job_type)]);
        let _ = self.duration_histogram.observe(labels, duration_secs);
        self.inc_status("completed");
    }

    /// Record job failure
    pub fn record_failure(&self, job_type: &str, error_type: &str) {
        let _ = error_type; // Could add to labels
        let _ = job_type;
        self.inc_status("failed");
    }

    /// Record queue time for a job
    pub fn record_queue_time(&self, queue_time_secs: f64, priority: &str) {
        let labels = Labels::new(&[("priority", priority)]);
        let _ = self.queue_time_summary.observe(labels, queue_time_secs);
    }

    /// Get current jobs per second rate
    pub fn jobs_per_second(&self) -> f64 {
        self.rate_calculator.rate()
    }

    /// Get total submitted jobs
    pub fn total_submitted(&self) -> u64 {
        self.submitted_total.load(Ordering::Relaxed)
    }

    /// Get count by status
    pub fn count_by_status(&self, status: &str) -> u64 {
        self.by_status
            .read()
            .get(status)
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    fn inc_status(&self, status: &str) {
        let statuses = self.by_status.read();
        if let Some(counter) = statuses.get(status) {
            counter.fetch_add(1, Ordering::Relaxed);
            return;
        }
        drop(statuses);

        let mut statuses = self.by_status.write();
        statuses
            .entry(status.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Encode in Prometheus format
    pub fn prometheus_encode(&self) -> String {
        let mut output = String::new();

        // Jobs submitted total
        output.push_str("# HELP marabunta_jobs_submitted_total Total number of jobs submitted\n");
        output.push_str("# TYPE marabunta_jobs_submitted_total counter\n");
        output.push_str(&format!(
            "marabunta_jobs_submitted_total {}\n",
            self.submitted_total.load(Ordering::Relaxed)
        ));

        // Jobs by status
        output.push_str("# HELP marabunta_jobs_by_status_total Jobs by status\n");
        output.push_str("# TYPE marabunta_jobs_by_status_total counter\n");
        for (status, count) in self.by_status.read().iter() {
            output.push_str(&format!(
                "marabunta_jobs_by_status_total{{status=\"{}\"}} {}\n",
                status,
                count.load(Ordering::Relaxed)
            ));
        }

        // Jobs per second rate
        output.push_str("# HELP marabunta_jobs_per_second Current job submission rate\n");
        output.push_str("# TYPE marabunta_jobs_per_second gauge\n");
        output.push_str(&format!("marabunta_jobs_per_second {:.2}\n", self.jobs_per_second()));

        // Duration histogram
        output.push_str(&self.duration_histogram.prometheus_encode());

        // Queue time summary
        output.push_str(&self.queue_time_summary.prometheus_encode());

        output
    }
}

impl Default for JobMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Task Metrics
// =============================================================================

/// Task throughput metrics
#[derive(Debug)]
pub struct TaskMetrics {
    /// Total tasks executed
    executed_total: AtomicU64,
    /// Tasks by status
    by_status: RwLock<HashMap<String, AtomicU64>>,
    /// Task throughput rate
    rate_calculator: RateCalculator,
    /// Task execution time histogram
    execution_time: LabeledHistogram,
    /// Task scheduling latency
    scheduling_latency: LabeledSummary,
    /// Active tasks gauge
    active_tasks: AtomicU64,
}

impl TaskMetrics {
    /// Create new task metrics
    pub fn new() -> Self {
        Self {
            executed_total: AtomicU64::new(0),
            by_status: RwLock::new(HashMap::new()),
            rate_calculator: RateCalculator::new(Duration::from_secs(60)),
            execution_time: LabeledHistogram::new(
                "marabunta_task_execution_seconds",
                "Task execution time in seconds",
                buckets::TASK_DURATION,
            ),
            scheduling_latency: LabeledSummary::with_config(
                "marabunta_task_scheduling_latency_seconds",
                "Time to schedule a task",
                SummaryConfig::with_quantiles(&[0.5, 0.9, 0.95, 0.99])
                    .max_age(Duration::from_secs(300)),
            ),
            active_tasks: AtomicU64::new(0),
        }
    }

    /// Record task start
    pub fn record_start(&self) {
        self.active_tasks.fetch_add(1, Ordering::Relaxed);
        self.inc_status("running");
    }

    /// Record task completion
    pub fn record_completion(&self, duration_secs: f64, task_type: &str) {
        self.executed_total.fetch_add(1, Ordering::Relaxed);
        self.active_tasks.fetch_sub(1, Ordering::Relaxed);
        self.rate_calculator.record();
        self.inc_status("completed");

        let labels = Labels::new(&[("task_type", task_type)]);
        let _ = self.execution_time.observe(labels, duration_secs);
    }

    /// Record task failure
    pub fn record_failure(&self, task_type: &str) {
        self.active_tasks.fetch_sub(1, Ordering::Relaxed);
        self.inc_status("failed");
        let _ = task_type;
    }

    /// Record scheduling latency
    pub fn record_scheduling_latency(&self, latency_secs: f64, node_type: &str) {
        let labels = Labels::new(&[("node_type", node_type)]);
        let _ = self.scheduling_latency.observe(labels, latency_secs);
    }

    /// Get current tasks per second rate
    pub fn tasks_per_second(&self) -> f64 {
        self.rate_calculator.rate()
    }

    /// Get total executed tasks
    pub fn total_executed(&self) -> u64 {
        self.executed_total.load(Ordering::Relaxed)
    }

    /// Get active task count
    pub fn active(&self) -> u64 {
        self.active_tasks.load(Ordering::Relaxed)
    }

    fn inc_status(&self, status: &str) {
        let statuses = self.by_status.read();
        if let Some(counter) = statuses.get(status) {
            counter.fetch_add(1, Ordering::Relaxed);
            return;
        }
        drop(statuses);

        let mut statuses = self.by_status.write();
        statuses
            .entry(status.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Encode in Prometheus format
    pub fn prometheus_encode(&self) -> String {
        let mut output = String::new();

        // Tasks executed total
        output.push_str("# HELP marabunta_tasks_executed_total Total number of tasks executed\n");
        output.push_str("# TYPE marabunta_tasks_executed_total counter\n");
        output.push_str(&format!(
            "marabunta_tasks_executed_total {}\n",
            self.executed_total.load(Ordering::Relaxed)
        ));

        // Active tasks
        output.push_str("# HELP marabunta_tasks_active Current number of active tasks\n");
        output.push_str("# TYPE marabunta_tasks_active gauge\n");
        output.push_str(&format!(
            "marabunta_tasks_active {}\n",
            self.active_tasks.load(Ordering::Relaxed)
        ));

        // Tasks per second
        output.push_str("# HELP marabunta_tasks_per_second Current task completion rate\n");
        output.push_str("# TYPE marabunta_tasks_per_second gauge\n");
        output.push_str(&format!("marabunta_tasks_per_second {:.2}\n", self.tasks_per_second()));

        // Tasks by status
        output.push_str("# HELP marabunta_tasks_by_status_total Tasks by status\n");
        output.push_str("# TYPE marabunta_tasks_by_status_total counter\n");
        for (status, count) in self.by_status.read().iter() {
            output.push_str(&format!(
                "marabunta_tasks_by_status_total{{status=\"{}\"}} {}\n",
                status,
                count.load(Ordering::Relaxed)
            ));
        }

        // Execution time histogram
        output.push_str(&self.execution_time.prometheus_encode());

        // Scheduling latency summary
        output.push_str(&self.scheduling_latency.prometheus_encode());

        output
    }
}

impl Default for TaskMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Queue Metrics
// =============================================================================

/// Queue depth and wait time metrics
#[derive(Debug)]
pub struct QueueMetrics {
    /// Queue depths by queue name
    depths: RwLock<HashMap<String, AtomicU64>>,
    /// Queue wait time histogram
    wait_time: LabeledHistogram,
    /// High water marks
    high_water_marks: RwLock<HashMap<String, u64>>,
}

impl QueueMetrics {
    /// Create new queue metrics
    pub fn new() -> Self {
        Self {
            depths: RwLock::new(HashMap::new()),
            wait_time: LabeledHistogram::new(
                "marabunta_queue_wait_seconds",
                "Time items spend waiting in queue",
                buckets::QUEUE_WAIT,
            ),
            high_water_marks: RwLock::new(HashMap::new()),
        }
    }

    /// Set queue depth for a queue
    pub fn set_depth(&self, queue_name: &str, depth: u64) {
        {
            let depths = self.depths.read();
            if let Some(counter) = depths.get(queue_name) {
                counter.store(depth, Ordering::Relaxed);
                // Update high water mark
                let mut marks = self.high_water_marks.write();
                let mark = marks.entry(queue_name.to_string()).or_insert(0);
                if depth > *mark {
                    *mark = depth;
                }
                return;
            }
        }

        let mut depths = self.depths.write();
        depths
            .entry(queue_name.to_string())
            .or_insert_with(|| AtomicU64::new(depth));

        let mut marks = self.high_water_marks.write();
        marks.insert(queue_name.to_string(), depth);
    }

    /// Increment queue depth
    pub fn increment(&self, queue_name: &str) {
        let depths = self.depths.read();
        if let Some(counter) = depths.get(queue_name) {
            let new_depth = counter.fetch_add(1, Ordering::Relaxed) + 1;
            let mut marks = self.high_water_marks.write();
            let mark = marks.entry(queue_name.to_string()).or_insert(0);
            if new_depth > *mark {
                *mark = new_depth;
            }
            return;
        }
        drop(depths);

        let mut depths = self.depths.write();
        depths
            .entry(queue_name.to_string())
            .or_insert_with(|| AtomicU64::new(1));
    }

    /// Decrement queue depth
    pub fn decrement(&self, queue_name: &str) {
        let depths = self.depths.read();
        if let Some(counter) = depths.get(queue_name) {
            counter.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Record wait time for an item
    pub fn record_wait_time(&self, queue_name: &str, wait_secs: f64) {
        let labels = Labels::new(&[("queue", queue_name)]);
        let _ = self.wait_time.observe(labels, wait_secs);
    }

    /// Get queue depth
    pub fn depth(&self, queue_name: &str) -> u64 {
        self.depths
            .read()
            .get(queue_name)
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Get total queue depth across all queues
    pub fn total_depth(&self) -> u64 {
        self.depths
            .read()
            .values()
            .map(|c| c.load(Ordering::Relaxed))
            .sum()
    }

    /// Get high water mark for a queue
    pub fn high_water_mark(&self, queue_name: &str) -> u64 {
        self.high_water_marks
            .read()
            .get(queue_name)
            .copied()
            .unwrap_or(0)
    }

    /// Encode in Prometheus format
    pub fn prometheus_encode(&self) -> String {
        let mut output = String::new();

        // Queue depth
        output.push_str("# HELP marabunta_queue_depth Current queue depth\n");
        output.push_str("# TYPE marabunta_queue_depth gauge\n");
        for (name, depth) in self.depths.read().iter() {
            output.push_str(&format!(
                "marabunta_queue_depth{{queue=\"{}\"}} {}\n",
                name,
                depth.load(Ordering::Relaxed)
            ));
        }

        // Total queue depth
        output.push_str("# HELP marabunta_queue_depth_total Total queue depth across all queues\n");
        output.push_str("# TYPE marabunta_queue_depth_total gauge\n");
        output.push_str(&format!("marabunta_queue_depth_total {}\n", self.total_depth()));

        // High water marks
        output.push_str("# HELP marabunta_queue_high_water_mark Maximum observed queue depth\n");
        output.push_str("# TYPE marabunta_queue_high_water_mark gauge\n");
        for (name, mark) in self.high_water_marks.read().iter() {
            output.push_str(&format!(
                "marabunta_queue_high_water_mark{{queue=\"{}\"}} {}\n",
                name, mark
            ));
        }

        // Wait time histogram
        output.push_str(&self.wait_time.prometheus_encode());

        output
    }
}

impl Default for QueueMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Worker Metrics
// =============================================================================

/// Worker availability and utilization metrics
#[derive(Debug)]
pub struct WorkerMetrics {
    /// Total workers
    total: AtomicU64,
    /// Workers by status
    by_status: RwLock<HashMap<String, AtomicU64>>,
    /// Worker utilization (0-100)
    utilization: RwLock<HashMap<String, f64>>,
    /// Worker task capacity
    capacity: AtomicU64,
    /// Currently used capacity
    used_capacity: AtomicU64,
}

impl WorkerMetrics {
    /// Create new worker metrics
    pub fn new() -> Self {
        Self {
            total: AtomicU64::new(0),
            by_status: RwLock::new(HashMap::new()),
            utilization: RwLock::new(HashMap::new()),
            capacity: AtomicU64::new(0),
            used_capacity: AtomicU64::new(0),
        }
    }

    /// Set total worker count
    pub fn set_total(&self, count: u64) {
        self.total.store(count, Ordering::Relaxed);
    }

    /// Set worker count by status
    pub fn set_by_status(&self, status: &str, count: u64) {
        let statuses = self.by_status.read();
        if let Some(counter) = statuses.get(status) {
            counter.store(count, Ordering::Relaxed);
            return;
        }
        drop(statuses);

        let mut statuses = self.by_status.write();
        statuses
            .entry(status.to_string())
            .or_insert_with(|| AtomicU64::new(count));
    }

    /// Set utilization for a worker
    pub fn set_utilization(&self, worker_id: &str, utilization: f64) {
        self.utilization
            .write()
            .insert(worker_id.to_string(), utilization);
    }

    /// Set total capacity
    pub fn set_capacity(&self, total: u64, used: u64) {
        self.capacity.store(total, Ordering::Relaxed);
        self.used_capacity.store(used, Ordering::Relaxed);
    }

    /// Get total workers
    pub fn total(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }

    /// Get worker count by status
    pub fn by_status(&self, status: &str) -> u64 {
        self.by_status
            .read()
            .get(status)
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Get average utilization
    pub fn average_utilization(&self) -> f64 {
        let utils = self.utilization.read();
        if utils.is_empty() {
            return 0.0;
        }
        utils.values().sum::<f64>() / utils.len() as f64
    }

    /// Get capacity utilization percentage
    pub fn capacity_utilization(&self) -> f64 {
        let capacity = self.capacity.load(Ordering::Relaxed);
        if capacity == 0 {
            return 0.0;
        }
        self.used_capacity.load(Ordering::Relaxed) as f64 / capacity as f64 * 100.0
    }

    /// Encode in Prometheus format
    pub fn prometheus_encode(&self) -> String {
        let mut output = String::new();

        // Total workers
        output.push_str("# HELP marabunta_workers_total Total number of workers\n");
        output.push_str("# TYPE marabunta_workers_total gauge\n");
        output.push_str(&format!(
            "marabunta_workers_total {}\n",
            self.total.load(Ordering::Relaxed)
        ));

        // Workers by status
        output.push_str("# HELP marabunta_workers_by_status Workers by status\n");
        output.push_str("# TYPE marabunta_workers_by_status gauge\n");
        for (status, count) in self.by_status.read().iter() {
            output.push_str(&format!(
                "marabunta_workers_by_status{{status=\"{}\"}} {}\n",
                status,
                count.load(Ordering::Relaxed)
            ));
        }

        // Average utilization
        output.push_str("# HELP marabunta_workers_utilization_percent Average worker utilization\n");
        output.push_str("# TYPE marabunta_workers_utilization_percent gauge\n");
        output.push_str(&format!(
            "marabunta_workers_utilization_percent {:.2}\n",
            self.average_utilization()
        ));

        // Capacity
        output.push_str("# HELP marabunta_workers_capacity_total Total worker capacity\n");
        output.push_str("# TYPE marabunta_workers_capacity_total gauge\n");
        output.push_str(&format!(
            "marabunta_workers_capacity_total {}\n",
            self.capacity.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP marabunta_workers_capacity_used Used worker capacity\n");
        output.push_str("# TYPE marabunta_workers_capacity_used gauge\n");
        output.push_str(&format!(
            "marabunta_workers_capacity_used {}\n",
            self.used_capacity.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP marabunta_workers_capacity_utilization_percent Capacity utilization percentage\n");
        output.push_str("# TYPE marabunta_workers_capacity_utilization_percent gauge\n");
        output.push_str(&format!(
            "marabunta_workers_capacity_utilization_percent {:.2}\n",
            self.capacity_utilization()
        ));

        output
    }
}

impl Default for WorkerMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// SLA Metrics
// =============================================================================

/// SLA compliance metrics
#[derive(Debug)]
pub struct SlaMetrics {
    /// Jobs completed within SLA
    within_sla: AtomicU64,
    /// Jobs that violated SLA
    violated_sla: AtomicU64,
    /// SLA violations by type
    violations_by_type: RwLock<HashMap<String, AtomicU64>>,
    /// Current SLA compliance rate (updated periodically)
    compliance_rate: RwLock<f64>,
}

impl SlaMetrics {
    /// Create new SLA metrics
    pub fn new() -> Self {
        Self {
            within_sla: AtomicU64::new(0),
            violated_sla: AtomicU64::new(0),
            violations_by_type: RwLock::new(HashMap::new()),
            compliance_rate: RwLock::new(100.0),
        }
    }

    /// Record SLA compliance
    pub fn record_within_sla(&self) {
        self.within_sla.fetch_add(1, Ordering::Relaxed);
        self.update_compliance_rate();
    }

    /// Record SLA violation
    pub fn record_violation(&self, violation_type: &str) {
        self.violated_sla.fetch_add(1, Ordering::Relaxed);

        {
            let violations = self.violations_by_type.read();
            if let Some(counter) = violations.get(violation_type) {
                counter.fetch_add(1, Ordering::Relaxed);
                self.update_compliance_rate();
                return;
            }
        }

        let mut violations = self.violations_by_type.write();
        violations
            .entry(violation_type.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);

        self.update_compliance_rate();
    }

    fn update_compliance_rate(&self) {
        let within = self.within_sla.load(Ordering::Relaxed);
        let violated = self.violated_sla.load(Ordering::Relaxed);
        let total = within + violated;

        let rate = if total == 0 {
            100.0
        } else {
            within as f64 / total as f64 * 100.0
        };

        *self.compliance_rate.write() = rate;
    }

    /// Get current compliance rate
    pub fn compliance_rate(&self) -> f64 {
        *self.compliance_rate.read()
    }

    /// Get total violations
    pub fn total_violations(&self) -> u64 {
        self.violated_sla.load(Ordering::Relaxed)
    }

    /// Get violations by type
    pub fn violations_by_type(&self, violation_type: &str) -> u64 {
        self.violations_by_type
            .read()
            .get(violation_type)
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Encode in Prometheus format
    pub fn prometheus_encode(&self) -> String {
        let mut output = String::new();

        // SLA compliance
        output.push_str("# HELP marabunta_sla_within_total Jobs completed within SLA\n");
        output.push_str("# TYPE marabunta_sla_within_total counter\n");
        output.push_str(&format!(
            "marabunta_sla_within_total {}\n",
            self.within_sla.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP marabunta_sla_violated_total Jobs that violated SLA\n");
        output.push_str("# TYPE marabunta_sla_violated_total counter\n");
        output.push_str(&format!(
            "marabunta_sla_violated_total {}\n",
            self.violated_sla.load(Ordering::Relaxed)
        ));

        // Violations by type
        output.push_str("# HELP marabunta_sla_violations_by_type SLA violations by type\n");
        output.push_str("# TYPE marabunta_sla_violations_by_type counter\n");
        for (vtype, count) in self.violations_by_type.read().iter() {
            output.push_str(&format!(
                "marabunta_sla_violations_by_type{{type=\"{}\"}} {}\n",
                vtype,
                count.load(Ordering::Relaxed)
            ));
        }

        // Compliance rate
        output.push_str("# HELP marabunta_sla_compliance_percent Current SLA compliance percentage\n");
        output.push_str("# TYPE marabunta_sla_compliance_percent gauge\n");
        output.push_str(&format!(
            "marabunta_sla_compliance_percent {:.2}\n",
            self.compliance_rate()
        ));

        output
    }
}

impl Default for SlaMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Resource Metrics
// =============================================================================

/// Resource utilization metrics
#[derive(Debug)]
pub struct ResourceMetrics {
    /// CPU utilization by node
    cpu_utilization: RwLock<HashMap<String, f64>>,
    /// Memory utilization by node
    memory_utilization: RwLock<HashMap<String, f64>>,
    /// GPU utilization by node
    gpu_utilization: RwLock<HashMap<String, f64>>,
    /// Network bandwidth usage
    network_bandwidth: RwLock<HashMap<String, (u64, u64)>>, // (rx, tx) bytes
}

impl ResourceMetrics {
    /// Create new resource metrics
    pub fn new() -> Self {
        Self {
            cpu_utilization: RwLock::new(HashMap::new()),
            memory_utilization: RwLock::new(HashMap::new()),
            gpu_utilization: RwLock::new(HashMap::new()),
            network_bandwidth: RwLock::new(HashMap::new()),
        }
    }

    /// Set CPU utilization for a node
    pub fn set_cpu(&self, node_id: &str, utilization: f64) {
        self.cpu_utilization
            .write()
            .insert(node_id.to_string(), utilization);
    }

    /// Set memory utilization for a node
    pub fn set_memory(&self, node_id: &str, utilization: f64) {
        self.memory_utilization
            .write()
            .insert(node_id.to_string(), utilization);
    }

    /// Set GPU utilization for a node
    pub fn set_gpu(&self, node_id: &str, utilization: f64) {
        self.gpu_utilization
            .write()
            .insert(node_id.to_string(), utilization);
    }

    /// Set network bandwidth for a node
    pub fn set_network(&self, node_id: &str, rx_bytes: u64, tx_bytes: u64) {
        self.network_bandwidth
            .write()
            .insert(node_id.to_string(), (rx_bytes, tx_bytes));
    }

    /// Get average CPU utilization
    pub fn avg_cpu(&self) -> f64 {
        let cpu = self.cpu_utilization.read();
        if cpu.is_empty() {
            return 0.0;
        }
        cpu.values().sum::<f64>() / cpu.len() as f64
    }

    /// Get average memory utilization
    pub fn avg_memory(&self) -> f64 {
        let mem = self.memory_utilization.read();
        if mem.is_empty() {
            return 0.0;
        }
        mem.values().sum::<f64>() / mem.len() as f64
    }

    /// Get average GPU utilization
    pub fn avg_gpu(&self) -> f64 {
        let gpu = self.gpu_utilization.read();
        if gpu.is_empty() {
            return 0.0;
        }
        gpu.values().sum::<f64>() / gpu.len() as f64
    }

    /// Encode in Prometheus format
    pub fn prometheus_encode(&self) -> String {
        let mut output = String::new();

        // CPU utilization
        output.push_str("# HELP marabunta_resource_cpu_utilization CPU utilization percentage\n");
        output.push_str("# TYPE marabunta_resource_cpu_utilization gauge\n");
        for (node, util) in self.cpu_utilization.read().iter() {
            output.push_str(&format!(
                "marabunta_resource_cpu_utilization{{node=\"{}\"}} {:.2}\n",
                node, util
            ));
        }
        output.push_str(&format!(
            "marabunta_resource_cpu_utilization_avg {:.2}\n",
            self.avg_cpu()
        ));

        // Memory utilization
        output.push_str("# HELP marabunta_resource_memory_utilization Memory utilization percentage\n");
        output.push_str("# TYPE marabunta_resource_memory_utilization gauge\n");
        for (node, util) in self.memory_utilization.read().iter() {
            output.push_str(&format!(
                "marabunta_resource_memory_utilization{{node=\"{}\"}} {:.2}\n",
                node, util
            ));
        }
        output.push_str(&format!(
            "marabunta_resource_memory_utilization_avg {:.2}\n",
            self.avg_memory()
        ));

        // GPU utilization
        output.push_str("# HELP marabunta_resource_gpu_utilization GPU utilization percentage\n");
        output.push_str("# TYPE marabunta_resource_gpu_utilization gauge\n");
        for (node, util) in self.gpu_utilization.read().iter() {
            output.push_str(&format!(
                "marabunta_resource_gpu_utilization{{node=\"{}\"}} {:.2}\n",
                node, util
            ));
        }
        if !self.gpu_utilization.read().is_empty() {
            output.push_str(&format!(
                "marabunta_resource_gpu_utilization_avg {:.2}\n",
                self.avg_gpu()
            ));
        }

        // Network bandwidth
        output.push_str("# HELP marabunta_resource_network_rx_bytes Network bytes received\n");
        output.push_str("# TYPE marabunta_resource_network_rx_bytes counter\n");
        output.push_str("# HELP marabunta_resource_network_tx_bytes Network bytes transmitted\n");
        output.push_str("# TYPE marabunta_resource_network_tx_bytes counter\n");
        for (node, (rx, tx)) in self.network_bandwidth.read().iter() {
            output.push_str(&format!(
                "marabunta_resource_network_rx_bytes{{node=\"{}\"}} {}\n",
                node, rx
            ));
            output.push_str(&format!(
                "marabunta_resource_network_tx_bytes{{node=\"{}\"}} {}\n",
                node, tx
            ));
        }

        output
    }
}

impl Default for ResourceMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Rate Calculator
// =============================================================================

/// Calculates rate of events over a sliding window
#[derive(Debug)]
struct RateCalculator {
    /// Event timestamps within the window
    events: RwLock<VecDeque<Instant>>,
    /// Window duration
    window: Duration,
}

impl RateCalculator {
    fn new(window: Duration) -> Self {
        Self {
            events: RwLock::new(VecDeque::new()),
            window,
        }
    }

    fn record(&self) {
        let now = Instant::now();
        let cutoff = now - self.window;

        let mut events = self.events.write();
        events.push_back(now);

        // Remove old events
        while events.front().map(|t| *t < cutoff).unwrap_or(false) {
            events.pop_front();
        }
    }

    fn rate(&self) -> f64 {
        let now = Instant::now();
        let cutoff = now - self.window;

        let events = self.events.read();
        let count = events.iter().filter(|t| **t >= cutoff).count();

        count as f64 / self.window.as_secs_f64()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_job_metrics() {
        let metrics = JobMetrics::new();

        metrics.record_submission();
        metrics.record_submission();
        metrics.record_completion(10.5, "batch");
        metrics.record_failure("batch", "timeout");

        assert_eq!(metrics.total_submitted(), 2);
        assert_eq!(metrics.count_by_status("submitted"), 2);
        assert_eq!(metrics.count_by_status("completed"), 1);
        assert_eq!(metrics.count_by_status("failed"), 1);
    }

    #[test]
    fn test_task_metrics() {
        let metrics = TaskMetrics::new();

        metrics.record_start();
        metrics.record_start();
        assert_eq!(metrics.active(), 2);

        metrics.record_completion(5.0, "compute");
        assert_eq!(metrics.active(), 1);
        assert_eq!(metrics.total_executed(), 1);

        metrics.record_failure("compute");
        assert_eq!(metrics.active(), 0);
    }

    #[test]
    fn test_queue_metrics() {
        let metrics = QueueMetrics::new();

        metrics.set_depth("default", 10);
        assert_eq!(metrics.depth("default"), 10);

        metrics.increment("default");
        assert_eq!(metrics.depth("default"), 11);
        assert_eq!(metrics.high_water_mark("default"), 11);

        metrics.decrement("default");
        assert_eq!(metrics.depth("default"), 10);
        assert_eq!(metrics.high_water_mark("default"), 11);
    }

    #[test]
    fn test_worker_metrics() {
        let metrics = WorkerMetrics::new();

        metrics.set_total(10);
        metrics.set_by_status("ready", 8);
        metrics.set_by_status("busy", 2);

        assert_eq!(metrics.total(), 10);
        assert_eq!(metrics.by_status("ready"), 8);
        assert_eq!(metrics.by_status("busy"), 2);

        metrics.set_utilization("worker-1", 50.0);
        metrics.set_utilization("worker-2", 70.0);
        assert!((metrics.average_utilization() - 60.0).abs() < 0.1);

        metrics.set_capacity(100, 60);
        assert!((metrics.capacity_utilization() - 60.0).abs() < 0.1);
    }

    #[test]
    fn test_sla_metrics() {
        let metrics = SlaMetrics::new();

        for _ in 0..90 {
            metrics.record_within_sla();
        }
        for _ in 0..10 {
            metrics.record_violation("timeout");
        }

        assert!((metrics.compliance_rate() - 90.0).abs() < 0.1);
        assert_eq!(metrics.total_violations(), 10);
        assert_eq!(metrics.violations_by_type("timeout"), 10);
    }

    #[test]
    fn test_resource_metrics() {
        let metrics = ResourceMetrics::new();

        metrics.set_cpu("node-1", 50.0);
        metrics.set_cpu("node-2", 70.0);
        assert!((metrics.avg_cpu() - 60.0).abs() < 0.1);

        metrics.set_memory("node-1", 40.0);
        metrics.set_memory("node-2", 60.0);
        assert!((metrics.avg_memory() - 50.0).abs() < 0.1);
    }

    #[test]
    fn test_rate_calculator() {
        let calc = RateCalculator::new(Duration::from_millis(100));

        for _ in 0..10 {
            calc.record();
        }

        let rate = calc.rate();
        // 10 events in 100ms = 100 events/sec
        assert!(rate > 50.0 && rate < 200.0);
    }

    #[test]
    fn test_business_metrics_encoding() {
        let metrics = BusinessMetrics::new();

        metrics.jobs.record_submission();
        metrics.tasks.record_start();
        metrics.queues.set_depth("default", 5);
        metrics.workers.set_total(10);

        let encoded = metrics.prometheus_encode();
        assert!(encoded.contains("marabunta_jobs_submitted_total"));
        assert!(encoded.contains("marabunta_tasks_active"));
        assert!(encoded.contains("marabunta_queue_depth"));
        assert!(encoded.contains("marabunta_workers_total"));
    }
}
