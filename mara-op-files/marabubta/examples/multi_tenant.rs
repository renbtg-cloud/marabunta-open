// Marabunta - Licensed under the MIT License.
//! Multi-Tenant Job Submission Example
//!
//! This example demonstrates how to use Marabunta Compute's multi-tenancy features
//! for organizations with multiple teams and users. It shows how to:
//!
//! - Create and manage tenants (organizations, teams, users)
//! - Submit jobs with tenant context
//! - Apply tenant-specific quotas and policies
//! - Isolate resources between tenants
//!
//! # Multi-Tenancy Architecture
//!
//! ```text
//!                    +-------------------+
//!                    |   Organization    |
//!                    |    (Acme Corp)    |
//!                    +--------+----------+
//!                             |
//!            +----------------+----------------+
//!            |                |                |
//!     +------+------+  +------+------+  +------+------+
//!     |  Team: ML   |  | Team: Data  |  | Team: Infra |
//!     +------+------+  +------+------+  +------+------+
//!            |                |                |
//!        +---+---+        +---+---+        +---+---+
//!        |       |        |       |        |       |
//!      User1   User2    User3   User4    User5   User6
//! ```
//!
//! # Features
//!
//! - Hierarchical tenant organization
//! - Quota enforcement at each level
//! - Policy inheritance and overrides
//! - Isolated job queues per tenant
//! - Resource accounting and billing
//!
//! # Running this example
//!
//! ```bash
//! # Run locally for demonstration
//! cargo run --example multi_tenant
//! ```

use marabunta_compute::common::{Job, Task, TaskPayload};
use marabunta_compute::tenancy::{
    AuditLevel, BillingStatus, QuotaPeriod, Tenant, TenantBilling, TenantId, TenantLevel,
    TenantPolicies, TenantQuotas, TenantRole,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================================
// Multi-Tenant Job Submission
// ============================================================================

/// A job submission request with tenant context
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantJobSubmission {
    /// The tenant submitting the job
    pub tenant_id: TenantId,
    /// User ID of the submitter
    pub user_id: String,
    /// The job to submit
    pub job: Job,
    /// Requested priority (may be capped by quota)
    pub requested_priority: Option<u32>,
    /// Requested resources
    pub requested_resources: Option<ResourceRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceRequest {
    pub cpu_cores: Option<u32>,
    pub memory_mb: Option<u64>,
    pub gpu_count: Option<u32>,
    pub disk_gb: Option<u64>,
}

/// Result of a job submission attempt
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SubmissionResult {
    /// Job was accepted
    Accepted {
        job_id: String,
        effective_priority: u32,
        queue_position: u32,
    },
    /// Job was rejected due to quota
    QuotaExceeded {
        reason: String,
        current_usage: f64,
        limit: f64,
    },
    /// Job was rejected due to policy
    PolicyViolation { reason: String },
    /// Tenant is not authorized
    Unauthorized { reason: String },
}

// ============================================================================
// Tenant Manager (Simplified)
// ============================================================================

/// Simplified tenant manager for demonstration
pub struct TenantManager {
    tenants: HashMap<TenantId, Tenant>,
    usage: HashMap<TenantId, TenantUsage>,
}

#[derive(Debug, Clone, Default)]
pub struct TenantUsage {
    pub concurrent_jobs: u32,
    pub concurrent_tasks: u32,
    pub cpu_hours_used: f64,
    pub gpu_hours_used: f64,
    pub storage_used_gb: f64,
}

impl TenantManager {
    pub fn new() -> Self {
        Self {
            tenants: HashMap::new(),
            usage: HashMap::new(),
        }
    }

    /// Register a tenant
    pub fn register_tenant(&mut self, tenant: Tenant) {
        self.usage.insert(tenant.id, TenantUsage::default());
        self.tenants.insert(tenant.id, tenant);
    }

    /// Get a tenant by ID
    pub fn get_tenant(&self, id: &TenantId) -> Option<&Tenant> {
        self.tenants.get(id)
    }

    /// Get tenant usage
    pub fn get_usage(&self, id: &TenantId) -> Option<&TenantUsage> {
        self.usage.get(id)
    }

    /// Check if a user can submit jobs for a tenant
    pub fn can_submit(&self, tenant_id: &TenantId, user_id: &str) -> bool {
        if let Some(tenant) = self.tenants.get(tenant_id) {
            tenant.is_member(user_id) && tenant.is_active()
        } else {
            false
        }
    }

    /// Check quota for a job submission
    pub fn check_quota(
        &self,
        tenant_id: &TenantId,
        resources: &ResourceRequest,
    ) -> Result<(), String> {
        let tenant = self.tenants.get(tenant_id).ok_or("Tenant not found")?;
        let usage = self.usage.get(tenant_id).ok_or("Usage not tracked")?;

        // Check concurrent jobs
        if let Some(max_jobs) = tenant.quotas.max_concurrent_jobs {
            if usage.concurrent_jobs >= max_jobs {
                return Err(format!(
                    "Concurrent job limit exceeded: {}/{}",
                    usage.concurrent_jobs, max_jobs
                ));
            }
        }

        // Check storage
        if let Some(requested_disk) = resources.disk_gb {
            if let Some(max_storage) = tenant.quotas.max_storage_gb {
                if usage.storage_used_gb + requested_disk as f64 > max_storage {
                    return Err(format!(
                        "Storage quota exceeded: {:.1}/{:.1} GB",
                        usage.storage_used_gb, max_storage
                    ));
                }
            }
        }

        Ok(())
    }

    /// Check policy for a job submission
    pub fn check_policy(&self, tenant_id: &TenantId, job: &Job) -> Result<(), String> {
        let tenant = self.tenants.get(tenant_id).ok_or("Tenant not found")?;

        // Check region restrictions
        if let Some(region) = job.metadata.get("region").and_then(|r| r.as_str()) {
            if !tenant.policies.is_region_allowed(region) {
                return Err(format!(
                    "Region '{}' is not allowed for this tenant",
                    region
                ));
            }
        }

        // Check priority ceiling
        if let Some(max_priority) = tenant.quotas.max_priority {
            if job.priority > max_priority {
                return Err(format!(
                    "Priority {} exceeds maximum allowed {}",
                    job.priority, max_priority
                ));
            }
        }

        Ok(())
    }

    /// Submit a job with tenant context
    pub fn submit_job(&mut self, submission: TenantJobSubmission) -> SubmissionResult {
        // Check authorization
        if !self.can_submit(&submission.tenant_id, &submission.user_id) {
            return SubmissionResult::Unauthorized {
                reason: format!(
                    "User {} is not authorized to submit jobs for tenant {}",
                    submission.user_id, submission.tenant_id
                ),
            };
        }

        // Check quota
        let resources = submission.requested_resources.unwrap_or(ResourceRequest {
            cpu_cores: Some(1),
            memory_mb: Some(1024),
            gpu_count: None,
            disk_gb: Some(1),
        });

        if let Err(reason) = self.check_quota(&submission.tenant_id, &resources) {
            let usage = self.get_usage(&submission.tenant_id).unwrap();
            return SubmissionResult::QuotaExceeded {
                reason: reason.clone(),
                current_usage: usage.concurrent_jobs as f64,
                limit: self
                    .get_tenant(&submission.tenant_id)
                    .unwrap()
                    .quotas
                    .max_concurrent_jobs
                    .unwrap_or(0) as f64,
            };
        }

        // Check policy
        if let Err(reason) = self.check_policy(&submission.tenant_id, &submission.job) {
            return SubmissionResult::PolicyViolation { reason };
        }

        // Apply priority ceiling
        let tenant = self.get_tenant(&submission.tenant_id).unwrap();
        let effective_priority = submission
            .requested_priority
            .unwrap_or(submission.job.priority)
            .min(tenant.quotas.max_priority.unwrap_or(u32::MAX));

        // Update usage
        if let Some(usage) = self.usage.get_mut(&submission.tenant_id) {
            usage.concurrent_jobs += 1;
            if let Some(disk) = resources.disk_gb {
                usage.storage_used_gb += disk as f64;
            }
        }

        SubmissionResult::Accepted {
            job_id: submission.job.id.to_string(),
            effective_priority,
            queue_position: 1, // Simplified
        }
    }

    /// Complete a job (update usage)
    pub fn complete_job(&mut self, tenant_id: &TenantId, cpu_hours: f64, gpu_hours: f64) {
        if let Some(usage) = self.usage.get_mut(tenant_id) {
            usage.concurrent_jobs = usage.concurrent_jobs.saturating_sub(1);
            usage.cpu_hours_used += cpu_hours;
            usage.gpu_hours_used += gpu_hours;
        }
    }
}

impl Default for TenantManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tenant Hierarchy Builder
// ============================================================================

/// Build a complete organization hierarchy
pub fn build_organization_hierarchy() -> (TenantManager, TenantId) {
    let mut manager = TenantManager::new();

    // Create organization
    let org = Tenant::new_organization("Acme Corporation", "acme")
        .with_description("Acme Corp - Leading provider of everything")
        .with_contact_email("admin@acme.com")
        .with_quotas(TenantQuotas {
            max_concurrent_jobs: Some(1000),
            max_concurrent_tasks: Some(10000),
            max_cpu_hours: Some(100000.0),
            max_gpu_hours: Some(10000.0),
            max_storage_gb: Some(10000.0),
            max_network_egress_gb: Some(5000.0),
            custom_limits: HashMap::new(),
            max_priority: Some(1000),
            period: QuotaPeriod::Monthly,
        })
        .with_policies(TenantPolicies {
            allow_phantom_nodes: true,
            allow_infrastructure_nodes: true,
            required_sla_tier: None,
            allowed_regions: vec![],
            blocked_regions: vec!["cn-north-1".to_string()],
            data_retention_days: Some(365),
            require_encryption: true,
            audit_level: AuditLevel::Verbose,
            custom_policies: HashMap::new(),
        })
        .with_billing(TenantBilling {
            plan_id: "enterprise".to_string(),
            cost_center: Some("ACME-HQ-001".to_string()),
            billing_email: Some("billing@acme.com".to_string()),
            status: BillingStatus::Active,
            credit_balance: 50000.0,
            current_period_usage: 0.0,
        });

    let org_id = org.id;
    manager.register_tenant(org);

    // Create ML team
    let ml_team = Tenant::new_team("Machine Learning Team", "ml-team", org_id)
        .with_description("AI/ML research and development")
        .with_quotas(TenantQuotas {
            max_concurrent_jobs: Some(200),
            max_concurrent_tasks: Some(2000),
            max_cpu_hours: Some(20000.0),
            max_gpu_hours: Some(5000.0),
            max_storage_gb: Some(2000.0),
            max_network_egress_gb: Some(1000.0),
            custom_limits: HashMap::new(),
            max_priority: Some(800),
            period: QuotaPeriod::Monthly,
        });

    let ml_team_id = ml_team.id;
    manager.register_tenant(ml_team);

    // Create Data Engineering team
    let data_team = Tenant::new_team("Data Engineering Team", "data-team", org_id)
        .with_description("Data pipelines and analytics")
        .with_quotas(TenantQuotas {
            max_concurrent_jobs: Some(500),
            max_concurrent_tasks: Some(5000),
            max_cpu_hours: Some(50000.0),
            max_gpu_hours: Some(1000.0),
            max_storage_gb: Some(5000.0),
            max_network_egress_gb: Some(3000.0),
            custom_limits: HashMap::new(),
            max_priority: Some(600),
            period: QuotaPeriod::Monthly,
        });

    let data_team_id = data_team.id;
    manager.register_tenant(data_team);

    // Create users under ML team
    let mut alice = Tenant::new_user("Alice (ML Lead)", "alice", ml_team_id, "alice@acme.com")
        .with_quotas(TenantQuotas {
            max_concurrent_jobs: Some(50),
            max_concurrent_tasks: Some(500),
            max_cpu_hours: Some(5000.0),
            max_gpu_hours: Some(2000.0),
            max_storage_gb: Some(500.0),
            max_network_egress_gb: Some(200.0),
            custom_limits: HashMap::new(),
            max_priority: Some(700),
            period: QuotaPeriod::Monthly,
        });
    alice.add_admin("alice@acme.com");
    let alice_id = alice.id;
    manager.register_tenant(alice);

    let mut bob = Tenant::new_user("Bob (ML Engineer)", "bob", ml_team_id, "bob@acme.com")
        .with_quotas(TenantQuotas {
            max_concurrent_jobs: Some(20),
            max_concurrent_tasks: Some(200),
            max_cpu_hours: Some(2000.0),
            max_gpu_hours: Some(500.0),
            max_storage_gb: Some(200.0),
            max_network_egress_gb: Some(100.0),
            custom_limits: HashMap::new(),
            max_priority: Some(500),
            period: QuotaPeriod::Monthly,
        });
    bob.add_member("bob@acme.com");
    manager.register_tenant(bob);

    // Create users under Data team
    let mut charlie = Tenant::new_user(
        "Charlie (Data Lead)",
        "charlie",
        data_team_id,
        "charlie@acme.com",
    )
    .with_quotas(TenantQuotas {
        max_concurrent_jobs: Some(100),
        max_concurrent_tasks: Some(1000),
        max_cpu_hours: Some(10000.0),
        max_gpu_hours: Some(200.0),
        max_storage_gb: Some(1000.0),
        max_network_egress_gb: Some(500.0),
        custom_limits: HashMap::new(),
        max_priority: Some(500),
        period: QuotaPeriod::Monthly,
    });
    charlie.add_admin("charlie@acme.com");
    manager.register_tenant(charlie);

    (manager, alice_id)
}

// ============================================================================
// Demo Functions
// ============================================================================

/// Create a sample ML training job
fn create_ml_training_job(tenant_id: TenantId, user_id: &str) -> TenantJobSubmission {
    let mut job = Job::with_tenant("gpt-fine-tuning", tenant_id);
    job.set_submitted_by(user_id);
    job.priority = 600;
    job.metadata = serde_json::json!({
        "model": "gpt-3.5",
        "dataset": "custom-qa",
        "epochs": 10,
        "region": "us-east-1"
    });

    // Add training task
    let task = Task::new(
        job.id,
        TaskPayload::Function {
            name: "train_model".to_string(),
            input: serde_json::to_vec(&job.metadata).unwrap(),
        },
    );
    job.tasks.push(task.id);

    TenantJobSubmission {
        tenant_id,
        user_id: user_id.to_string(),
        job,
        requested_priority: Some(600),
        requested_resources: Some(ResourceRequest {
            cpu_cores: Some(8),
            memory_mb: Some(32768),
            gpu_count: Some(4),
            disk_gb: Some(100),
        }),
    }
}

/// Create a sample data pipeline job
fn create_data_pipeline_job(tenant_id: TenantId, user_id: &str) -> TenantJobSubmission {
    let mut job = Job::with_tenant("daily-etl", tenant_id);
    job.set_submitted_by(user_id);
    job.priority = 400;
    job.metadata = serde_json::json!({
        "pipeline": "sales-aggregation",
        "date": "2024-01-15",
        "region": "us-west-2"
    });

    let task = Task::new(
        job.id,
        TaskPayload::Shell {
            command: "spark-submit".to_string(),
            args: vec!["--master".to_string(), "yarn".to_string()],
        },
    );
    job.tasks.push(task.id);

    TenantJobSubmission {
        tenant_id,
        user_id: user_id.to_string(),
        job,
        requested_priority: Some(400),
        requested_resources: Some(ResourceRequest {
            cpu_cores: Some(16),
            memory_mb: Some(65536),
            gpu_count: None,
            disk_gb: Some(500),
        }),
    }
}

/// Try to submit a job to a blocked region
fn create_blocked_region_job(tenant_id: TenantId, user_id: &str) -> TenantJobSubmission {
    let mut job = Job::with_tenant("china-data-sync", tenant_id);
    job.set_submitted_by(user_id);
    job.priority = 200;
    job.metadata = serde_json::json!({
        "region": "cn-north-1"  // This region is blocked
    });

    let task = Task::new(
        job.id,
        TaskPayload::Shell {
            command: "sync".to_string(),
            args: vec![],
        },
    );
    job.tasks.push(task.id);

    TenantJobSubmission {
        tenant_id,
        user_id: user_id.to_string(),
        job,
        requested_priority: None,
        requested_resources: None,
    }
}

// ============================================================================
// Main Entry Point
// ============================================================================

fn main() {
    println!("=== Marabunta Compute: Multi-Tenant Job Submission ===\n");

    // Build organization hierarchy
    let (mut manager, alice_id) = build_organization_hierarchy();

    // Display tenant hierarchy
    println!("--- Tenant Hierarchy ---");
    for (id, tenant) in &manager.tenants {
        let indent = match tenant.level {
            TenantLevel::Organization => "",
            TenantLevel::Team => "  ",
            TenantLevel::User => "    ",
        };
        println!(
            "{}{} ({:?}) - {}",
            indent, tenant.name, tenant.level, tenant.id
        );
        println!(
            "{}  Quotas: {} concurrent jobs, {} CPU hours/month",
            indent,
            tenant.quotas.max_concurrent_jobs.unwrap_or(0),
            tenant.quotas.max_cpu_hours.unwrap_or(0.0)
        );
    }
    println!();

    // Demo 1: Successful ML job submission
    println!("--- Demo 1: ML Training Job Submission ---");
    let ml_job = create_ml_training_job(alice_id, "alice@acme.com");
    println!("Submitting job: {}", ml_job.job.name);
    println!("  Tenant: {}", ml_job.tenant_id);
    println!("  User: {}", ml_job.user_id);
    println!("  Requested priority: {:?}", ml_job.requested_priority);

    match manager.submit_job(ml_job) {
        SubmissionResult::Accepted {
            job_id,
            effective_priority,
            queue_position,
        } => {
            println!("  Result: ACCEPTED");
            println!("    Job ID: {}", job_id);
            println!("    Effective priority: {}", effective_priority);
            println!("    Queue position: {}", queue_position);
        }
        SubmissionResult::QuotaExceeded {
            reason,
            current_usage,
            limit,
        } => {
            println!("  Result: QUOTA EXCEEDED");
            println!("    Reason: {}", reason);
            println!("    Usage: {}/{}", current_usage, limit);
        }
        SubmissionResult::PolicyViolation { reason } => {
            println!("  Result: POLICY VIOLATION");
            println!("    Reason: {}", reason);
        }
        SubmissionResult::Unauthorized { reason } => {
            println!("  Result: UNAUTHORIZED");
            println!("    Reason: {}", reason);
        }
    }
    println!();

    // Demo 2: Blocked region job
    println!("--- Demo 2: Blocked Region Job ---");
    let blocked_job = create_blocked_region_job(alice_id, "alice@acme.com");
    println!("Submitting job: {}", blocked_job.job.name);
    println!("  Target region: cn-north-1 (blocked by policy)");

    match manager.submit_job(blocked_job) {
        SubmissionResult::PolicyViolation { reason } => {
            println!("  Result: POLICY VIOLATION (expected)");
            println!("    Reason: {}", reason);
        }
        other => {
            println!("  Result: {:?}", other);
        }
    }
    println!();

    // Demo 3: Unauthorized user
    println!("--- Demo 3: Unauthorized User ---");
    let unauthorized_job = create_ml_training_job(alice_id, "eve@hacker.com");
    println!("Submitting job as unauthorized user: eve@hacker.com");

    match manager.submit_job(unauthorized_job) {
        SubmissionResult::Unauthorized { reason } => {
            println!("  Result: UNAUTHORIZED (expected)");
            println!("    Reason: {}", reason);
        }
        other => {
            println!("  Result: {:?}", other);
        }
    }
    println!();

    // Demo 4: Multiple jobs to test quota
    println!("--- Demo 4: Quota Exhaustion ---");
    println!("Submitting multiple jobs to test quota enforcement...");

    // Create a tenant with very low quota for demo
    let mut limited_tenant =
        Tenant::new_user("Limited User", "limited", alice_id, "limited@acme.com").with_quotas(
            TenantQuotas {
                max_concurrent_jobs: Some(2),
                max_concurrent_tasks: Some(10),
                max_cpu_hours: Some(10.0),
                max_gpu_hours: Some(1.0),
                max_storage_gb: Some(5.0),
                max_network_egress_gb: Some(1.0),
                custom_limits: HashMap::new(),
                max_priority: Some(100),
                period: QuotaPeriod::Monthly,
            },
        );
    limited_tenant.add_member("limited@acme.com");
    let limited_id = limited_tenant.id;
    manager.register_tenant(limited_tenant);

    for i in 1..=4 {
        let mut job = Job::with_tenant(format!("job-{}", i), limited_id);
        job.set_submitted_by("limited@acme.com");
        job.priority = 50;

        let submission = TenantJobSubmission {
            tenant_id: limited_id,
            user_id: "limited@acme.com".to_string(),
            job,
            requested_priority: None,
            requested_resources: Some(ResourceRequest {
                cpu_cores: Some(1),
                memory_mb: Some(1024),
                gpu_count: None,
                disk_gb: Some(1),
            }),
        };

        print!("  Job {}: ", i);
        match manager.submit_job(submission) {
            SubmissionResult::Accepted { .. } => println!("ACCEPTED"),
            SubmissionResult::QuotaExceeded { reason, .. } => {
                println!("QUOTA EXCEEDED - {}", reason)
            }
            other => println!("{:?}", other),
        }
    }
    println!();

    // Show usage summary
    println!("--- Usage Summary ---");
    for (id, usage) in &manager.usage {
        if let Some(tenant) = manager.get_tenant(id) {
            if usage.concurrent_jobs > 0 || usage.cpu_hours_used > 0.0 {
                println!("{} ({}):", tenant.name, tenant.level);
                println!("  Concurrent jobs: {}", usage.concurrent_jobs);
                println!("  CPU hours used: {:.1}", usage.cpu_hours_used);
                println!("  GPU hours used: {:.1}", usage.gpu_hours_used);
                println!("  Storage used: {:.1} GB", usage.storage_used_gb);
            }
        }
    }
    println!();

    // Show JSON example
    println!("--- JSON Job Submission Example ---");
    let example_job = create_ml_training_job(alice_id, "alice@acme.com");
    let json = serde_json::to_string_pretty(&example_job).unwrap();
    println!("{}", json);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tenant_creation() {
        let org = Tenant::new_organization("Test Org", "test-org");
        assert_eq!(org.level, TenantLevel::Organization);
        assert!(org.parent_id.is_none());
    }

    #[test]
    fn test_tenant_hierarchy() {
        let (manager, alice_id) = build_organization_hierarchy();

        let alice = manager.get_tenant(&alice_id).unwrap();
        assert_eq!(alice.level, TenantLevel::User);
        assert!(alice.parent_id.is_some());
    }

    #[test]
    fn test_quota_check() {
        let (manager, alice_id) = build_organization_hierarchy();

        let resources = ResourceRequest {
            cpu_cores: Some(4),
            memory_mb: Some(16384),
            gpu_count: Some(1),
            disk_gb: Some(50),
        };

        assert!(manager.check_quota(&alice_id, &resources).is_ok());
    }

    #[test]
    fn test_policy_blocked_region() {
        let (manager, alice_id) = build_organization_hierarchy();

        let mut job = Job::with_tenant("test-job", alice_id);
        job.metadata = serde_json::json!({"region": "cn-north-1"});

        assert!(manager.check_policy(&alice_id, &job).is_err());
    }

    #[test]
    fn test_unauthorized_submission() {
        let (mut manager, alice_id) = build_organization_hierarchy();

        let submission = TenantJobSubmission {
            tenant_id: alice_id,
            user_id: "unauthorized@example.com".to_string(),
            job: Job::with_tenant("test", alice_id),
            requested_priority: None,
            requested_resources: None,
        };

        match manager.submit_job(submission) {
            SubmissionResult::Unauthorized { .. } => {}
            _ => panic!("Expected Unauthorized result"),
        }
    }
}
