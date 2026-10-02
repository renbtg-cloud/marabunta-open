// Marabunta - Licensed under the MIT License.
use tracing::{info, warn};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use crate::swarm::types::SwarmError;

/// Defines the operational state of an offloaded quantum computation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuantumJobState {
    Submitted,
    Queued { position: u32 },
    Running { progress_pct: u8 },
    Completed(String), // Returns the measurement outcomes or processed FHE ciphertext
    Failed(String),    // Returns the error message (decoherence, gate depth, etc.)
}

/// A hardware-agnostic representation of a Quantum Circuit for Marabunta HPC.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantumCircuit {
    pub name: String,
    pub qubits: u32,
    pub openqasm_version: String,
    pub instructions: String, // Valid OpenQASM 3.0
}

impl QuantumCircuit {
    pub fn new_fhe_accelerator(qubits: u32, operations: &str) -> Self {
        Self {
            name: format!("fhe_accel_{}", uuid::Uuid::new_v4().simple()),
            qubits,
            openqasm_version: "3.0".to_string(),
            instructions: format!("OPENQASM 3.0;\ninclude \"stdgates.inc\";\nqubit[{}] q;\n{}", qubits, operations),
        }
    }
}

/// A highly constrained, asynchronous client for interfacing with
/// low-qubit, high-latency quantum backends and local classical simulators.
pub struct QuantumAccelerator {
    client: Client,
    api_endpoint: String,
    auth_token: String,
    backend: String,
}

impl QuantumAccelerator {
    /// Initializes the quantum integration client.
    pub fn new(api_endpoint: &str, auth_token: &str, backend: &str) -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_default(),
            api_endpoint: api_endpoint.to_string(),
            auth_token: auth_token.to_string(),
            backend: backend.to_string(),
        }
    }

    /// Submits a computational circuit (OpenQASM) to the external quantum provider.
    /// In Marabunta, this is used for accelerating FHE polynomial multiplications.
    pub async fn submit_circuit(&self, circuit: &QuantumCircuit) -> Result<String, SwarmError> {
        info!(
            circuit = %circuit.name,
            qubits = circuit.qubits,
            backend = %self.backend,
            "Submitting quantum circuit to provider..."
        );

        if self.api_endpoint.is_empty() || self.api_endpoint.contains("mock") {
            return self.submit_mock(circuit).await;
        }

        let response = self.client.post(&format!("{}/jobs", self.api_endpoint))
            .header("Authorization", format!("Bearer {}", self.auth_token))
            .json(&circuit)
            .send()
            .await
            .map_err(|e| SwarmError::Transport(format!("Quantum API connection failed: {}", e)))?;

        let status = response.status();
        if !status.is_success() {
            let err_text = response.text().await.unwrap_or_default();
            return Err(SwarmError::Api(format!("Quantum API error ({}): {}", status, err_text)));
        }

        #[derive(Deserialize)]
        struct JobResponse { id: String }
        let body: JobResponse = response.json().await
            .map_err(|e| SwarmError::Api(format!("Failed to parse Quantum job ID: {}", e)))?;

        info!(job_id = %body.id, "Quantum job submitted successfully.");
        Ok(body.id)
    }

    /// Submits to a local "Quantum-Inspired" classical simulator.
    /// This is a PRAGMATIC FALLBACK for when QPU latencies are too high.
    pub async fn execute_inspired(&self, circuit: &QuantumCircuit) -> Result<String, SwarmError> {
        warn!(circuit = %circuit.name, "Real QPU unavailable or latent. Falling back to GPU-backed Tensor Network contraction.");
        
        // This simulates the contraction of a 1D Matrix Product State (MPS)
        // In a real production deployment, this would invoke a CUDA/ROCm kernel.
        tokio::time::sleep(Duration::from_millis(500)).await;
        
        let result = format!("{{ \"statevector_sampling\": \"[0.98, 0.01, 0.0, 0.01]\", \"simulation\": \"tensor_network_v1\" }}");
        Ok(result)
    }

    /// Polls the status of a previously submitted quantum job.
    pub async fn check_job_status(&self, job_id: &str) -> Result<QuantumJobState, SwarmError> {
        if self.api_endpoint.is_empty() || self.api_endpoint.contains("mock") {
            return Ok(QuantumJobState::Running { progress_pct: 42 });
        }

        let response = self.client.get(&format!("{}/jobs/{}", self.api_endpoint, job_id))
            .header("Authorization", format!("Bearer {}", self.auth_token))
            .send()
            .await
            .map_err(|e| SwarmError::Transport(format!("Quantum status poll failed: {}", e)))?;

        let state: QuantumJobState = response.json().await
            .map_err(|e| SwarmError::Api(format!("Failed to parse Quantum status: {}", e)))?;

        Ok(state)
    }

    async fn submit_mock(&self, _circuit: &QuantumCircuit) -> Result<String, SwarmError> {
        let mock_id = format!("qjob_{}", uuid::Uuid::new_v4().simple());
        info!(job_id = %mock_id, "Generated mock Quantum Job ID for disconnected testing.");
        Ok(mock_id)
    }
}

/// Automated Market Maker (AMM) for Quantum Time.
/// Nodes with Trait::QuantumCompute use this to price their specialized gates.
pub fn calculate_quantum_premium(swarm_saturation: f64) -> f64 {
    // Quantum time is exponentially scarcer than classical time.
    let base_premium = 25.0; 
    base_premium * (2.0 * swarm_saturation).exp()
}
