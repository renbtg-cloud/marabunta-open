// Marabunta - Licensed under the MIT License.
//! Integration tests for Coordinator startup and basic operations
//!
//! These tests verify that the coordinator can:
//! - Start up with in-memory storage
//! - Respond to health checks
//! - Accept and process job submissions
//! - Track job status
//! - Register and manage nodes

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tower::ServiceExt;

use crate::common::types::*;
use crate::coordinator::api::{create_router, ApiState, MastersCapacityResponse};
use crate::coordinator::master_connection::MasterRegistry;
use crate::coordinator::persistence::MemoryPersistenceBackend;
use crate::coordinator::state::{ClusterState, ClusterStats};
use crate::protocol::coordinator_master::{CoordinatorToMaster, MasterCapacity};

// ============== Local Test Types (serializable versions of API types) ==============

/// Job submission request (serializable version for tests)
#[derive(Debug, Serialize)]
struct TestSubmitJobRequest {
    name: String,
    tasks: Vec<TestTaskSpec>,
    #[serde(default)]
    priority: u32,
    #[serde(default)]
    metadata: serde_json::Value,
    #[serde(default = "default_validate_dag")]
    validate_dag: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    failed_dependency_policy: Option<String>,
}

fn default_validate_dag() -> bool {
    true
}

/// Task specification for tests
#[derive(Debug, Serialize)]
struct TestTaskSpec {
    payload: TaskPayload,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(default)]
    depends_on_indices: Vec<usize>,
    #[serde(default)]
    depends_on_names: Vec<String>,
}

/// Job submission response (deserializable version for tests)
#[derive(Debug, Deserialize)]
struct TestSubmitJobResponse {
    job_id: JobId,
    task_count: usize,
    #[serde(default)]
    has_dependencies: bool,
    #[serde(default)]
    task_ids: Vec<String>,
}

/// Job status response (deserializable version for tests)
#[derive(Debug, Deserialize)]
struct TestJobStatusResponse {
    job_id: JobId,
    status: JobStatus,
    total_tasks: usize,
    completed_tasks: usize,
    failed_tasks: usize,
    running_tasks: usize,
    #[serde(default)]
    waiting_tasks: usize,
    #[serde(default)]
    skipped_tasks: usize,
    #[serde(default)]
    has_dependencies: bool,
}

/// Job details response
#[derive(Debug, Deserialize)]
struct TestJobDetails {
    job: Job,
    tasks: Vec<Task>,
}

/// Master registration request
#[derive(Debug, Serialize)]
struct TestRegisterMasterRequest {
    address: String,
    region: String,
}

// ============== Test Harness ==============

/// Test harness for coordinator integration tests
///
/// Provides a fully configured coordinator with in-memory storage
/// and mock components for testing.
struct TestHarness {
    api_state: Arc<ApiState>,
    router: axum::Router,
}

impl TestHarness {
    /// Create a new test harness with in-memory backends
    fn new() -> Self {
        let cluster = Arc::new(ClusterState::<MemoryPersistenceBackend>::new());
        let master_registry = Arc::new(MasterRegistry::new(Duration::from_secs(30)));

        let api_state = Arc::new(ApiState {
            cluster,
            master_registry,
        });

        let router = create_router(api_state.clone());

        Self { api_state, router }
    }

    /// Get the cluster state for direct manipulation
    fn cluster(&self) -> &Arc<ClusterState<MemoryPersistenceBackend>> {
        &self.api_state.cluster
    }

    /// Get the master registry for direct manipulation
    fn master_registry(&self) -> &Arc<MasterRegistry> {
        &self.api_state.master_registry
    }

    /// Make a GET request to the API
    async fn get(&self, uri: &str) -> axum::response::Response {
        let request = Request::builder()
            .method("GET")
            .uri(uri)
            .body(Body::empty())
            .unwrap();

        self.router.clone().oneshot(request).await.unwrap()
    }

    /// Make a POST request with JSON body
    async fn post_json<T: Serialize>(&self, uri: &str, body: &T) -> axum::response::Response {
        let body_str = serde_json::to_string(body).unwrap();
        self.post_raw(uri, body_str).await
    }

    /// Make a POST request with raw JSON string
    async fn post_raw(&self, uri: &str, body: String) -> axum::response::Response {
        let request = Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();

        self.router.clone().oneshot(request).await.unwrap()
    }

    /// Register a mock master that captures messages
    async fn register_mock_master(
        &self,
        master_id: &str,
        region: &str,
    ) -> mpsc::Receiver<CoordinatorToMaster> {
        let (tx, rx) = mpsc::channel(100);
        let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        let capacity = MasterCapacity::new(100, 1000);

        self.master_registry()
            .register_master(
                master_id.to_string(),
                region.to_string(),
                capacity,
                addr,
                tx,
            )
            .await
            .unwrap();

        rx
    }
}

/// Extract JSON body from response
async fn json_body<T: serde::de::DeserializeOwned>(response: axum::response::Response) -> T {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

// ============== Test: Coordinator Startup with In-Memory Storage ==============

#[tokio::test]
async fn test_coordinator_startup_with_memory_storage() {
    // Create coordinator with in-memory storage
    let harness = TestHarness::new();

    // Verify cluster state is initialized
    let stats = harness.cluster().get_stats();
    assert_eq!(stats.total_masters, 0);
    assert_eq!(stats.total_jobs, 0);
    assert_eq!(stats.pending_jobs, 0);
    assert_eq!(stats.running_jobs, 0);

    // Verify master registry is initialized
    assert_eq!(harness.master_registry().count(), 0);
    assert_eq!(harness.master_registry().healthy_count(), 0);
}

#[tokio::test]
async fn test_coordinator_startup_persistence_not_required() {
    // Verify coordinator can start without persistence backend
    let cluster = Arc::new(ClusterState::<MemoryPersistenceBackend>::new());
    let master_registry = Arc::new(MasterRegistry::new(Duration::from_secs(30)));

    let _api_state = Arc::new(ApiState {
        cluster: cluster.clone(),
        master_registry,
    });

    // Verify cluster state works without persistence
    let job = Job::new("test-job");
    let job_id = job.id;
    let tasks = vec![Task::new(
        job_id,
        TaskPayload::Shell {
            command: "echo".to_string(),
            args: vec!["hello".to_string()],
        },
    )];

    let submitted_id = cluster.submit_job(job, tasks);
    assert_eq!(submitted_id, job_id);

    let retrieved = cluster.get_job(job_id);
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().name, "test-job");
}

// ============== Test: Health Endpoint ==============

#[tokio::test]
async fn test_health_endpoint_returns_ok() {
    let harness = TestHarness::new();

    let response = harness.get("/health").await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(&body[..], b"OK");
}

#[tokio::test]
async fn test_health_endpoint_is_fast() {
    let harness = TestHarness::new();

    let start = std::time::Instant::now();
    let response = harness.get("/health").await;
    let duration = start.elapsed();

    assert_eq!(response.status(), StatusCode::OK);
    // Health check should respond in under 100ms (generous for CI environments)
    assert!(
        duration < Duration::from_millis(100),
        "Health check took too long: {:?}",
        duration
    );
}

// ============== Test: Job Submission via API ==============

#[tokio::test]
async fn test_submit_job_via_api() {
    let harness = TestHarness::new();

    let req = TestSubmitJobRequest {
        name: "test-job".to_string(),
        tasks: vec![TestTaskSpec {
            payload: TaskPayload::Shell {
                command: "echo".to_string(),
                args: vec!["hello".to_string()],
            },
            name: Some("echo-task".to_string()),
            depends_on_indices: vec![],
            depends_on_names: vec![],
        }],
        priority: 5,
        metadata: serde_json::json!({"env": "test"}),
        validate_dag: true,
        failed_dependency_policy: None,
    };

    let response = harness.post_json("/job", &req).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body: TestSubmitJobResponse = json_body(response).await;
    assert_eq!(body.task_count, 1);
    assert!(!body.has_dependencies);
    assert_eq!(body.task_ids.len(), 1);

    // Verify job was stored in cluster state
    let job = harness.cluster().get_job(body.job_id);
    assert!(job.is_some());
    let job = job.unwrap();
    assert_eq!(job.name, "test-job");
    assert_eq!(job.priority, 5);
}

#[tokio::test]
async fn test_submit_job_with_multiple_tasks() {
    let harness = TestHarness::new();

    let req = TestSubmitJobRequest {
        name: "multi-task-job".to_string(),
        tasks: vec![
            TestTaskSpec {
                payload: TaskPayload::Shell {
                    command: "echo".to_string(),
                    args: vec!["task1".to_string()],
                },
                name: Some("task-1".to_string()),
                depends_on_indices: vec![],
                depends_on_names: vec![],
            },
            TestTaskSpec {
                payload: TaskPayload::Shell {
                    command: "echo".to_string(),
                    args: vec!["task2".to_string()],
                },
                name: Some("task-2".to_string()),
                depends_on_indices: vec![],
                depends_on_names: vec![],
            },
            TestTaskSpec {
                payload: TaskPayload::Shell {
                    command: "echo".to_string(),
                    args: vec!["task3".to_string()],
                },
                name: Some("task-3".to_string()),
                depends_on_indices: vec![],
                depends_on_names: vec![],
            },
        ],
        priority: 10,
        metadata: serde_json::Value::Null,
        validate_dag: true,
        failed_dependency_policy: None,
    };

    let response = harness.post_json("/job", &req).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body: TestSubmitJobResponse = json_body(response).await;
    assert_eq!(body.task_count, 3);
    assert_eq!(body.task_ids.len(), 3);

    // Verify tasks are stored
    let tasks = harness.cluster().get_tasks_for_job(body.job_id);
    assert_eq!(tasks.len(), 3);
}

#[tokio::test]
async fn test_submit_job_with_dependencies() {
    let harness = TestHarness::new();

    let req = TestSubmitJobRequest {
        name: "dag-job".to_string(),
        tasks: vec![
            TestTaskSpec {
                payload: TaskPayload::Shell {
                    command: "prepare".to_string(),
                    args: vec![],
                },
                name: Some("prepare".to_string()),
                depends_on_indices: vec![],
                depends_on_names: vec![],
            },
            TestTaskSpec {
                payload: TaskPayload::Shell {
                    command: "process".to_string(),
                    args: vec![],
                },
                name: Some("process".to_string()),
                depends_on_indices: vec![0], // depends on prepare
                depends_on_names: vec![],
            },
            TestTaskSpec {
                payload: TaskPayload::Shell {
                    command: "finalize".to_string(),
                    args: vec![],
                },
                name: Some("finalize".to_string()),
                depends_on_indices: vec![],
                depends_on_names: vec!["process".to_string()], // depends on process by name
            },
        ],
        priority: 0,
        metadata: serde_json::Value::Null,
        validate_dag: true,
        failed_dependency_policy: None,
    };

    let response = harness.post_json("/job", &req).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body: TestSubmitJobResponse = json_body(response).await;
    assert!(body.has_dependencies);
    assert_eq!(body.task_count, 3);

    // Verify task states
    let tasks = harness.cluster().get_tasks_for_job(body.job_id);
    let prepare = tasks
        .iter()
        .find(|t| t.name.as_deref() == Some("prepare"))
        .unwrap();
    let process = tasks
        .iter()
        .find(|t| t.name.as_deref() == Some("process"))
        .unwrap();
    let finalize = tasks
        .iter()
        .find(|t| t.name.as_deref() == Some("finalize"))
        .unwrap();

    // prepare has no dependencies, should be Ready
    assert_eq!(prepare.state, TaskState::Ready);
    assert!(prepare.depends_on.is_empty());

    // process depends on prepare, should be Waiting
    assert_eq!(process.state, TaskState::Waiting);
    assert_eq!(process.depends_on.len(), 1);

    // finalize depends on process, should be Waiting
    assert_eq!(finalize.state, TaskState::Waiting);
    assert_eq!(finalize.depends_on.len(), 1);
}

#[tokio::test]
async fn test_submit_job_with_circular_dependency_fails() {
    let harness = TestHarness::new();

    // Create a job with circular dependencies: A -> B -> C -> A
    let req = TestSubmitJobRequest {
        name: "circular-job".to_string(),
        tasks: vec![
            TestTaskSpec {
                payload: TaskPayload::Shell {
                    command: "a".to_string(),
                    args: vec![],
                },
                name: Some("A".to_string()),
                depends_on_indices: vec![],
                depends_on_names: vec!["C".to_string()], // A depends on C (circular!)
            },
            TestTaskSpec {
                payload: TaskPayload::Shell {
                    command: "b".to_string(),
                    args: vec![],
                },
                name: Some("B".to_string()),
                depends_on_indices: vec![],
                depends_on_names: vec!["A".to_string()], // B depends on A
            },
            TestTaskSpec {
                payload: TaskPayload::Shell {
                    command: "c".to_string(),
                    args: vec![],
                },
                name: Some("C".to_string()),
                depends_on_indices: vec![],
                depends_on_names: vec!["B".to_string()], // C depends on B
            },
        ],
        priority: 0,
        metadata: serde_json::Value::Null,
        validate_dag: true,
        failed_dependency_policy: None,
    };

    let response = harness.post_json("/job", &req).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_submit_job_with_invalid_dependency_index_fails() {
    let harness = TestHarness::new();

    let req = TestSubmitJobRequest {
        name: "bad-index-job".to_string(),
        tasks: vec![TestTaskSpec {
            payload: TaskPayload::Shell {
                command: "echo".to_string(),
                args: vec![],
            },
            name: None,
            depends_on_indices: vec![999], // Invalid index
            depends_on_names: vec![],
        }],
        priority: 0,
        metadata: serde_json::Value::Null,
        validate_dag: true,
        failed_dependency_policy: None,
    };

    let response = harness.post_json("/job", &req).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

// ============== Test: Job Status Retrieval ==============

#[tokio::test]
async fn test_get_job_status() {
    let harness = TestHarness::new();

    // Submit a job first
    let req = TestSubmitJobRequest {
        name: "status-test-job".to_string(),
        tasks: vec![
            TestTaskSpec {
                payload: TaskPayload::Shell {
                    command: "echo".to_string(),
                    args: vec!["1".to_string()],
                },
                name: None,
                depends_on_indices: vec![],
                depends_on_names: vec![],
            },
            TestTaskSpec {
                payload: TaskPayload::Shell {
                    command: "echo".to_string(),
                    args: vec!["2".to_string()],
                },
                name: None,
                depends_on_indices: vec![],
                depends_on_names: vec![],
            },
        ],
        priority: 0,
        metadata: serde_json::Value::Null,
        validate_dag: true,
        failed_dependency_policy: None,
    };

    let submit_response = harness.post_json("/job", &req).await;
    let submit_body: TestSubmitJobResponse = json_body(submit_response).await;

    // Get job status
    let status_uri = format!("/job/{}/status", submit_body.job_id.0);
    let response = harness.get(&status_uri).await;
    assert_eq!(response.status(), StatusCode::OK);

    let status: TestJobStatusResponse = json_body(response).await;
    assert_eq!(status.job_id, submit_body.job_id);
    assert_eq!(status.total_tasks, 2);
    assert_eq!(status.completed_tasks, 0);
    assert_eq!(status.failed_tasks, 0);
    assert_eq!(status.running_tasks, 0);
}

#[tokio::test]
async fn test_get_job_details() {
    let harness = TestHarness::new();

    // Submit a job
    let req = TestSubmitJobRequest {
        name: "details-test-job".to_string(),
        tasks: vec![TestTaskSpec {
            payload: TaskPayload::Python {
                script: "print('hello')".to_string(),
                args: vec![],
            },
            name: Some("python-task".to_string()),
            depends_on_indices: vec![],
            depends_on_names: vec![],
        }],
        priority: 7,
        metadata: serde_json::json!({"version": "1.0"}),
        validate_dag: true,
        failed_dependency_policy: None,
    };

    let submit_response = harness.post_json("/job", &req).await;
    let submit_body: TestSubmitJobResponse = json_body(submit_response).await;

    // Get job details
    let details_uri = format!("/job/{}", submit_body.job_id.0);
    let response = harness.get(&details_uri).await;
    assert_eq!(response.status(), StatusCode::OK);

    let details: TestJobDetails = json_body(response).await;
    assert_eq!(details.job.name, "details-test-job");
    assert_eq!(details.job.priority, 7);
    assert_eq!(details.tasks.len(), 1);
    assert_eq!(details.tasks[0].name, Some("python-task".to_string()));
}

#[tokio::test]
async fn test_get_nonexistent_job_returns_404() {
    let harness = TestHarness::new();

    let response = harness
        .get("/job/00000000-0000-0000-0000-000000000000")
        .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let response = harness
        .get("/job/00000000-0000-0000-0000-000000000000/status")
        .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_get_job_with_invalid_uuid_returns_400() {
    let harness = TestHarness::new();

    let response = harness.get("/job/not-a-valid-uuid").await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

// ============== Test: Node Registration ==============

#[tokio::test]
async fn test_register_master_via_api() {
    let harness = TestHarness::new();

    let req = TestRegisterMasterRequest {
        address: "192.168.1.100:9000".to_string(),
        region: "us-west-2".to_string(),
    };

    let response = harness.post_json("/master/register", &req).await;
    assert_eq!(response.status(), StatusCode::OK);

    let master_info: MasterInfo = json_body(response).await;
    assert_eq!(master_info.address, "192.168.1.100:9000");
    assert_eq!(master_info.status, MasterStatus::Ready);
    assert_eq!(master_info.worker_count, 0);
    assert_eq!(master_info.pending_tasks, 0);

    // Verify master is in cluster state
    let stats = harness.cluster().get_stats();
    assert_eq!(stats.total_masters, 1);
    assert_eq!(stats.healthy_masters, 1);
}

#[tokio::test]
async fn test_register_multiple_masters() {
    let harness = TestHarness::new();

    // Register first master
    let req1 = TestRegisterMasterRequest {
        address: "master1:9000".to_string(),
        region: "us-east-1".to_string(),
    };
    let response1 = harness.post_json("/master/register", &req1).await;
    assert_eq!(response1.status(), StatusCode::OK);

    // Register second master in different region
    let req2 = TestRegisterMasterRequest {
        address: "master2:9000".to_string(),
        region: "us-west-2".to_string(),
    };
    let response2 = harness.post_json("/master/register", &req2).await;
    assert_eq!(response2.status(), StatusCode::OK);

    // Verify both masters are registered
    let stats = harness.cluster().get_stats();
    assert_eq!(stats.total_masters, 2);
    assert_eq!(stats.healthy_masters, 2);

    // List masters
    let list_response = harness.get("/masters").await;
    assert_eq!(list_response.status(), StatusCode::OK);

    let masters: Vec<MasterInfo> = json_body(list_response).await;
    assert_eq!(masters.len(), 2);
}

#[tokio::test]
async fn test_job_assignment_to_master() {
    let harness = TestHarness::new();

    // Register a master in the registry (with channel)
    let _rx = harness.register_mock_master("master-1", "us-east-1").await;

    // Also register in cluster state
    let master_info = MasterInfo {
        id: MasterId::new(),
        address: "master-1:9000".to_string(),
        region: RegionId::from_name("us-east-1"),
        status: MasterStatus::Ready,
        worker_count: 0,
        pending_tasks: 0,
        last_heartbeat: chrono::Utc::now(),
    };
    harness.cluster().register_master(master_info.clone());

    // Submit a job
    let req = TestSubmitJobRequest {
        name: "assigned-job".to_string(),
        tasks: vec![TestTaskSpec {
            payload: TaskPayload::Shell {
                command: "echo".to_string(),
                args: vec!["hello".to_string()],
            },
            name: None,
            depends_on_indices: vec![],
            depends_on_names: vec![],
        }],
        priority: 0,
        metadata: serde_json::Value::Null,
        validate_dag: true,
        failed_dependency_policy: None,
    };

    let response = harness.post_json("/job", &req).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body: TestSubmitJobResponse = json_body(response).await;

    // Verify job is in cluster state
    let job = harness.cluster().get_job(body.job_id);
    assert!(job.is_some());
}

// ============== Test: Cluster Status ==============

#[tokio::test]
async fn test_cluster_status_endpoint() {
    let harness = TestHarness::new();

    // Get initial status
    let response = harness.get("/cluster/status").await;
    assert_eq!(response.status(), StatusCode::OK);

    let stats: ClusterStats = json_body(response).await;
    assert_eq!(stats.total_masters, 0);
    assert_eq!(stats.total_jobs, 0);

    // Submit some jobs
    for i in 0..3 {
        let req = TestSubmitJobRequest {
            name: format!("job-{}", i),
            tasks: vec![TestTaskSpec {
                payload: TaskPayload::Shell {
                    command: "echo".to_string(),
                    args: vec![],
                },
                name: None,
                depends_on_indices: vec![],
                depends_on_names: vec![],
            }],
            priority: 0,
            metadata: serde_json::Value::Null,
            validate_dag: true,
            failed_dependency_policy: None,
        };
        harness.post_json("/job", &req).await;
    }

    // Get updated status
    let response = harness.get("/cluster/status").await;
    let stats: ClusterStats = json_body(response).await;
    assert_eq!(stats.total_jobs, 3);
    assert_eq!(stats.pending_jobs, 3);
}

// ============== Test: Concurrent Operations ==============

#[tokio::test]
async fn test_concurrent_job_submissions() {
    let harness = Arc::new(TestHarness::new());

    // Submit multiple jobs concurrently
    let mut handles = vec![];
    for i in 0..10 {
        let harness = harness.clone();
        handles.push(tokio::spawn(async move {
            let req = TestSubmitJobRequest {
                name: format!("concurrent-job-{}", i),
                tasks: vec![TestTaskSpec {
                    payload: TaskPayload::Shell {
                        command: format!("task-{}", i),
                        args: vec![],
                    },
                    name: None,
                    depends_on_indices: vec![],
                    depends_on_names: vec![],
                }],
                priority: i as u32,
                metadata: serde_json::Value::Null,
                validate_dag: true,
                failed_dependency_policy: None,
            };

            let body_str = serde_json::to_string(&req).unwrap();
            let request = Request::builder()
                .method("POST")
                .uri("/job")
                .header("content-type", "application/json")
                .body(Body::from(body_str))
                .unwrap();

            harness.router.clone().oneshot(request).await.unwrap()
        }));
    }

    // Wait for all submissions
    for handle in handles {
        let response = handle.await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    // Verify all jobs were created
    let stats = harness.cluster().get_stats();
    assert_eq!(stats.total_jobs, 10);
}

// ============== Test: Job State Transitions ==============

#[tokio::test]
async fn test_job_status_update() {
    let harness = TestHarness::new();

    // Submit a job
    let req = TestSubmitJobRequest {
        name: "status-transition-job".to_string(),
        tasks: vec![TestTaskSpec {
            payload: TaskPayload::Shell {
                command: "echo".to_string(),
                args: vec![],
            },
            name: None,
            depends_on_indices: vec![],
            depends_on_names: vec![],
        }],
        priority: 0,
        metadata: serde_json::Value::Null,
        validate_dag: true,
        failed_dependency_policy: None,
    };

    let response = harness.post_json("/job", &req).await;
    let body: TestSubmitJobResponse = json_body(response).await;

    // Initial status should be Pending
    let job = harness.cluster().get_job(body.job_id).unwrap();
    assert_eq!(job.status, JobStatus::Pending);

    // Update to Running
    harness
        .cluster()
        .update_job_status(body.job_id, JobStatus::Running);
    let job = harness.cluster().get_job(body.job_id).unwrap();
    assert_eq!(job.status, JobStatus::Running);
    assert!(job.started_at.is_some());

    // Update to Completed
    harness
        .cluster()
        .update_job_status(body.job_id, JobStatus::Completed);
    let job = harness.cluster().get_job(body.job_id).unwrap();
    assert_eq!(job.status, JobStatus::Completed);
    assert!(job.completed_at.is_some());
}

// ============== Test: Task Operations ==============

#[tokio::test]
async fn test_task_status_update() {
    let harness = TestHarness::new();

    // Submit a job with tasks
    let req = TestSubmitJobRequest {
        name: "task-status-job".to_string(),
        tasks: vec![
            TestTaskSpec {
                payload: TaskPayload::Shell {
                    command: "task1".to_string(),
                    args: vec![],
                },
                name: Some("task-1".to_string()),
                depends_on_indices: vec![],
                depends_on_names: vec![],
            },
            TestTaskSpec {
                payload: TaskPayload::Shell {
                    command: "task2".to_string(),
                    args: vec![],
                },
                name: Some("task-2".to_string()),
                depends_on_indices: vec![],
                depends_on_names: vec![],
            },
        ],
        priority: 0,
        metadata: serde_json::Value::Null,
        validate_dag: true,
        failed_dependency_policy: None,
    };

    let response = harness.post_json("/job", &req).await;
    let body: TestSubmitJobResponse = json_body(response).await;

    // Get tasks
    let tasks = harness.cluster().get_tasks_for_job(body.job_id);
    assert_eq!(tasks.len(), 2);

    let task_id = tasks[0].id;

    // Update task status
    harness
        .cluster()
        .update_task_status(task_id, TaskStatus::Running);
    let task = harness.cluster().get_task(task_id).unwrap();
    assert_eq!(task.status, TaskStatus::Running);
    assert!(task.started_at.is_some());

    harness
        .cluster()
        .update_task_status(task_id, TaskStatus::Completed);
    let task = harness.cluster().get_task(task_id).unwrap();
    assert_eq!(task.status, TaskStatus::Completed);
    assert!(task.completed_at.is_some());
}

// ============== Test: Python Task Payload ==============

#[tokio::test]
async fn test_submit_python_task() {
    let harness = TestHarness::new();

    let req = TestSubmitJobRequest {
        name: "python-job".to_string(),
        tasks: vec![TestTaskSpec {
            payload: TaskPayload::Python {
                script: r#"
import sys
print("Hello from Python!")
result = sum(range(100))
print(f"Sum: {result}")
"#
                .to_string(),
                args: vec!["--verbose".to_string()],
            },
            name: Some("python-calc".to_string()),
            depends_on_indices: vec![],
            depends_on_names: vec![],
        }],
        priority: 0,
        metadata: serde_json::Value::Null,
        validate_dag: true,
        failed_dependency_policy: None,
    };

    let response = harness.post_json("/job", &req).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body: TestSubmitJobResponse = json_body(response).await;
    assert_eq!(body.task_count, 1);

    // Verify task payload
    let tasks = harness.cluster().get_tasks_for_job(body.job_id);
    match &tasks[0].payload {
        TaskPayload::Python { script, args } => {
            assert!(script.contains("Hello from Python"));
            assert_eq!(args, &vec!["--verbose".to_string()]);
        }
        _ => panic!("Expected Python payload"),
    }
}

// ============== Test: Monte Carlo Task Payload ==============

#[tokio::test]
async fn test_submit_monte_carlo_task() {
    let harness = TestHarness::new();

    let req = TestSubmitJobRequest {
        name: "monte-carlo-job".to_string(),
        tasks: vec![TestTaskSpec {
            payload: TaskPayload::MonteCarlo {
                seed: 42,
                iterations: 1_000_000,
                params: serde_json::json!({
                    "simulation_type": "pi_estimation",
                    "precision": "high"
                }),
            },
            name: Some("pi-estimation".to_string()),
            depends_on_indices: vec![],
            depends_on_names: vec![],
        }],
        priority: 0,
        metadata: serde_json::Value::Null,
        validate_dag: true,
        failed_dependency_policy: None,
    };

    let response = harness.post_json("/job", &req).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body: TestSubmitJobResponse = json_body(response).await;
    let tasks = harness.cluster().get_tasks_for_job(body.job_id);
    match &tasks[0].payload {
        TaskPayload::MonteCarlo {
            seed,
            iterations,
            params,
        } => {
            assert_eq!(*seed, 42);
            assert_eq!(*iterations, 1_000_000);
            assert_eq!(params["simulation_type"], "pi_estimation");
        }
        _ => panic!("Expected MonteCarlo payload"),
    }
}

// ============== Test: Parameter Sweep Task Payload ==============

#[tokio::test]
async fn test_submit_parameter_sweep_tasks() {
    let harness = TestHarness::new();

    // Create parameter sweep tasks
    let tasks: Vec<TestTaskSpec> = (0..5)
        .map(|i| TestTaskSpec {
            payload: TaskPayload::ParameterSweep {
                param_name: "learning_rate".to_string(),
                param_value: serde_json::json!(0.001 * (i + 1) as f64),
                base_config: serde_json::json!({
                    "model": "neural_network",
                    "epochs": 100,
                    "batch_size": 32
                }),
            },
            name: Some(format!("sweep-{}", i)),
            depends_on_indices: vec![],
            depends_on_names: vec![],
        })
        .collect();

    let req = TestSubmitJobRequest {
        name: "parameter-sweep-job".to_string(),
        tasks,
        priority: 0,
        metadata: serde_json::Value::Null,
        validate_dag: true,
        failed_dependency_policy: None,
    };

    let response = harness.post_json("/job", &req).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body: TestSubmitJobResponse = json_body(response).await;
    assert_eq!(body.task_count, 5);
}

// ============== Test: Masters Capacity ==============

#[tokio::test]
async fn test_masters_capacity_endpoint() {
    let harness = TestHarness::new();

    // Initially no masters
    let response = harness.get("/masters/capacity").await;
    assert_eq!(response.status(), StatusCode::OK);

    let capacity: MastersCapacityResponse = json_body(response).await;
    assert_eq!(capacity.total_masters, 0);
    assert_eq!(capacity.healthy_masters, 0);

    // Register some masters in the registry
    let _rx1 = harness.register_mock_master("master-1", "us-east-1").await;
    let _rx2 = harness.register_mock_master("master-2", "us-west-2").await;

    // Check capacity
    let response = harness.get("/masters/capacity").await;
    let capacity: MastersCapacityResponse = json_body(response).await;
    assert_eq!(capacity.total_masters, 2);
    assert_eq!(capacity.healthy_masters, 2);
    assert_eq!(capacity.total_job_capacity, 200); // 2 masters * 100 max_jobs each
    assert_eq!(capacity.total_worker_capacity, 2000); // 2 masters * 1000 max_workers each
}
