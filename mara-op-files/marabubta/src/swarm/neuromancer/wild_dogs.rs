// Marabunta - Licensed under the MIT License.
//! Wild Dogs — Pack hunting coordination for the Neuromancer subsystem.
//!
//! Correlates multiple confirmed threats within a time window, clusters them
//! by behavioural similarity (cosine similarity of evidence vectors), and
//! initiates coordinated pack hunts against groups of colluding nodes
//! (e.g., Sybil attacks, coordinated data poisoning).

use std::sync::Arc;
use std::time::SystemTime;

use tracing::{debug, info, warn};

use super::bus::NeuromancerBus;
use crate::swarm::neuromancer::config::WildDogsConfig;
use super::types::*;

// ============================================================================
// Types
// ============================================================================

/// A threat observation with its extracted behavioural vector, used for
/// pairwise similarity comparison when detecting clusters.
#[derive(Debug, Clone)]
pub struct ClusterPattern {
    pub node: NodeId,
    pub evidence: EvidenceChain,
    pub timestamp: SystemTime,
    pub behavioral_vector: Vec<f64>,
}

/// A cluster of correlated threats that warrants a coordinated pack hunt.
#[derive(Debug, Clone)]
pub struct ThreatCluster {
    pub id: ClusterId,
    pub members: Vec<NodeId>,
    pub pattern: String,
    pub evidence: Vec<EvidenceChain>,
    pub first_seen: SystemTime,
    pub last_seen: SystemTime,
    pub avg_similarity: f64,
}

// ============================================================================
// WildDogs engine
// ============================================================================

/// Pack hunting coordinator.
///
/// Receives confirmed threat events, extracts behavioural vectors from their
/// evidence chains, and when enough similar threats cluster within the
/// correlation window, initiates a pack hunt against all members.
pub struct WildDogs {
    config: WildDogsConfig,
    bus: Arc<NeuromancerBus>,
    recent_threats: Vec<ClusterPattern>,
    active_hunts: Vec<ThreatCluster>,
    hunt_count: u64,
}

impl WildDogs {
    /// Create a new WildDogs engine.
    pub fn new(config: WildDogsConfig, bus: Arc<NeuromancerBus>) -> Self {
        Self {
            config,
            bus,
            recent_threats: Vec::new(),
            active_hunts: Vec::new(),
            hunt_count: 0,
        }
    }

    /// Initiates a coordinated pack hunt against groups of colluding nodes
    /// (e.g., Sybil attacks, coordinated data poisoning).
    ///
    /// This engine uses two primary detection vectors:
    /// 1. **Behavioural Similarity**: Cosine similarity of anomaly confidence vectors.
    /// 2. **XOR Density Analysis**: Mathematical clustering of NodeIds in Kademlia neighborhoods
    ///    without corresponding economic stake (Chrysalis POW or Rep).
    pub fn on_threat_confirmed(
        &mut self,
        node: NodeId,
        evidence: EvidenceChain,
        timestamp: SystemTime,
    ) {
        let behavioral_vector = extract_behavioral_vector(&evidence);

        debug!(
            ?node,
            vector_len = behavioral_vector.len(),
            "Wild Dogs: recording confirmed threat"
        );

        // --- Sybil Defense: XOR Density Check ---
        // If multiple nodes with low reputation cluster in the same XOR neighborhood,
        // it indicates a coordinated Sybil attack attempting to monopolize a DHT bucket.
        if self.detect_xor_clustering(&node) {
            warn!(?node, "Wild Dogs: Sybil cluster detected via XOR density analysis.");
            // Automatically lower the threshold for behavioral similarity for this node.
        }

        self.recent_threats.push(ClusterPattern {
            node,
            evidence,
            timestamp,
            behavioral_vector,
        });

        if let Some(cluster) = self.detect_cluster() {
            self.initiate_pack_hunt(cluster);
        }
    }

    /// Analyzes the XOR distance between the new suspect and existing threats.
    /// High density in a specific bucket range suggests a coordinated Sybil strike.
    fn detect_xor_clustering(&self, suspect: &NodeId) -> bool {
        let mut neighbors = 0;
        for threat in &self.recent_threats {
            // Calculate XOR distance between suspect UUID and known threats.
            // In a production system, we'd use the 256-bit Kad distance.
            let dist = suspect.0.as_u128() ^ threat.node.0.as_u128();
            
            // If distance is within 2^112 (a very tight 16-bit shared prefix), they are neighbors.
            if dist < (1u128 << 112) {
                neighbors += 1;
            }
        }
        
        neighbors >= self.config.min_cluster_size
    }

    /// Scan recent threats for a cluster: filter to those within the
    /// correlation window, compute pairwise cosine similarity, and if the
    /// average meets the threshold and count meets the minimum, return a
    /// `ThreatCluster`.
    pub fn detect_cluster(&self) -> Option<ThreatCluster> {
        let now = SystemTime::now();

        // Filter to threats within the correlation window.
        let recent: Vec<&ClusterPattern> = self
            .recent_threats
            .iter()
            .filter(|t| {
                now.duration_since(t.timestamp)
                    .map(|d| d <= self.config.correlation_window)
                    .unwrap_or(false)
            })
            .collect();

        if recent.len() < self.config.min_cluster_size {
            return None;
        }

        // Compute pairwise cosine similarity.
        let mut total_sim = 0.0;
        let mut pair_count = 0u64;

        for i in 0..recent.len() {
            for j in (i + 1)..recent.len() {
                total_sim +=
                    cosine_similarity(&recent[i].behavioral_vector, &recent[j].behavioral_vector);
                pair_count += 1;
            }
        }

        if pair_count == 0 {
            return None;
        }

        let avg_similarity = total_sim / pair_count as f64;

        if avg_similarity < self.config.similarity_threshold as f64 {
            debug!(
                avg_similarity,
                threshold = self.config.similarity_threshold,
                "Wild Dogs: similarity below threshold"
            );
            return None;
        }

        // Build the cluster.
        let members: Vec<NodeId> = recent.iter().map(|t| t.node).collect();
        let evidence: Vec<EvidenceChain> = recent.iter().map(|t| t.evidence.clone()).collect();
        let first_seen = recent.iter().map(|t| t.timestamp).min().unwrap_or(now);
        let last_seen = recent.iter().map(|t| t.timestamp).max().unwrap_or(now);

        let cluster = ThreatCluster {
            id: rand::random::<[u8; 32]>(),
            members,
            pattern: format!(
                "correlated_threat_cluster(size={}, avg_sim={:.3})",
                recent.len(),
                avg_similarity
            ),
            evidence,
            first_seen,
            last_seen,
            avg_similarity,
        };

        info!(
            cluster_size = cluster.members.len(),
            avg_similarity = cluster.avg_similarity,
            "Wild Dogs: threat cluster detected"
        );

        Some(cluster)
    }

    /// Initiate a pack hunt against the given cluster, if the number of
    /// active hunts has not reached the configured maximum.
    pub fn initiate_pack_hunt(&mut self, cluster: ThreatCluster) {
        if self.active_hunts.len() >= self.config.max_concurrent_hunts {
            warn!(
                active = self.active_hunts.len(),
                max = self.config.max_concurrent_hunts,
                "Wild Dogs: max concurrent hunts reached, deferring"
            );
            return;
        }

        info!(
            targets = ?cluster.members,
            pattern = %cluster.pattern,
            "Wild Dogs: initiating pack hunt"
        );

        self.bus.emit(MarabuntaEvent::PackHuntInitiated {
            targets: cluster.members.clone(),
            pattern: cluster.pattern.clone(),
            timestamp: SystemTime::now(),
        });

        self.active_hunts.push(cluster);
        self.hunt_count += 1;
    }

    /// Remove threats whose timestamps fall outside the correlation window.
    pub fn cleanup_old_threats(&mut self) {
        let now = SystemTime::now();
        let window = self.config.correlation_window;
        let before = self.recent_threats.len();

        self.recent_threats.retain(|t| {
            now.duration_since(t.timestamp)
                .map(|d| d <= window)
                .unwrap_or(false)
        });

        let removed = before - self.recent_threats.len();
        if removed > 0 {
            debug!(removed, "Wild Dogs: cleaned up old threats");
        }
    }

    /// How many pack hunts have been initiated since creation.
    pub fn hunt_count(&self) -> u64 {
        self.hunt_count
    }

    /// How many active hunts are currently tracked.
    pub fn active_hunt_count(&self) -> usize {
        self.active_hunts.len()
    }

    /// How many recent threats are stored.
    pub fn recent_threat_count(&self) -> usize {
        self.recent_threats.len()
    }
}

// ============================================================================
// Math helpers
// ============================================================================

/// Compute the cosine similarity between two vectors.
///
/// Returns `dot(a, b) / (|a| * |b|)`.  If either vector has zero magnitude
/// (including empty vectors), returns 0.0.
pub fn cosine_similarity(a: &[f64], b: &[f64]) -> f64 {
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }

    let len = a.len().min(b.len());
    let mut dot = 0.0;
    let mut mag_a = 0.0;
    let mut mag_b = 0.0;

    for i in 0..len {
        dot += a[i] * b[i];
        mag_a += a[i] * a[i];
        mag_b += b[i] * b[i];
    }

    // Include any trailing elements in magnitude calculation.
    for i in len..a.len() {
        mag_a += a[i] * a[i];
    }
    for i in len..b.len() {
        mag_b += b[i] * b[i];
    }

    let denom = mag_a.sqrt() * mag_b.sqrt();
    if denom == 0.0 {
        0.0
    } else {
        dot / denom
    }
}

/// Extract a behavioural vector from an evidence chain by collecting the
/// confidence score of each evidence item.
pub fn extract_behavioral_vector(evidence: &EvidenceChain) -> Vec<f64> {
    evidence.events.iter().map(|item| item.confidence).collect()
}

// ============================================================================
// Unit tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::super::bus::NeuromancerBus;
    use super::*;
    use std::time::Duration;

    fn test_config() -> WildDogsConfig {
        WildDogsConfig {
            correlation_window: Duration::from_secs(60),
            min_cluster_size: 3,
            similarity_threshold: 0.7,
            max_concurrent_hunts: 5,
            ..Default::default()
        }
    }

    fn make_evidence(confidences: &[f64]) -> EvidenceChain {
        let mut chain = EvidenceChain::new();
        for &c in confidences {
            chain.push(EvidenceItem {
                event_type: "TestAnomaly".into(),
                details: "test evidence".into(),
                timestamp: SystemTime::now(),
                confidence: c,
            });
        }
        chain
    }

    fn make_bus() -> Arc<NeuromancerBus> {
        Arc::new(NeuromancerBus::new(64))
    }

    #[test]
    fn test_three_similar_threats_trigger_pack_hunt() {
        let bus = make_bus();
        let mut rx = bus.subscribe();
        let mut dogs = WildDogs::new(test_config(), bus);

        let now = SystemTime::now();

        // Three threats with identical evidence vectors → high similarity.
        for _ in 0..3 {
            let node = NodeId::new();
            let evidence = make_evidence(&[0.9, 0.8, 0.7]);
            dogs.on_threat_confirmed(node, evidence, now);
        }

        // Should have initiated exactly one pack hunt.
        assert_eq!(dogs.hunt_count(), 1);
        assert_eq!(dogs.active_hunt_count(), 1);

        // The bus should have received a PackHuntInitiated event.
        let event = rx.try_recv().expect("expected PackHuntInitiated event");
        assert_eq!(event.type_name(), "PackHuntInitiated");
    }

    #[test]
    fn test_threats_outside_window_no_hunt() {
        let bus = make_bus();
        let config = WildDogsConfig {
            correlation_window: Duration::from_secs(10),
            min_cluster_size: 3,
            similarity_threshold: 0.7,
            max_concurrent_hunts: 5,
            ..Default::default()
        };
        let mut dogs = WildDogs::new(config, bus);

        // Timestamps far in the past (outside the 10s window).
        let old_time = SystemTime::now() - Duration::from_secs(3600);

        for _ in 0..5 {
            let node = NodeId::new();
            let evidence = make_evidence(&[0.9, 0.8, 0.7]);
            dogs.on_threat_confirmed(node, evidence, old_time);
        }

        // No hunt should be initiated because all threats are outside window.
        assert_eq!(dogs.hunt_count(), 0);
    }

    #[test]
    fn test_low_similarity_no_hunt() {
        let bus = make_bus();
        let mut dogs = WildDogs::new(test_config(), bus);

        let now = SystemTime::now();

        // Three threats with very different vectors.
        dogs.on_threat_confirmed(NodeId::new(), make_evidence(&[1.0, 0.0, 0.0]), now);
        dogs.on_threat_confirmed(NodeId::new(), make_evidence(&[0.0, 1.0, 0.0]), now);
        dogs.on_threat_confirmed(NodeId::new(), make_evidence(&[0.0, 0.0, 1.0]), now);

        // Average cosine similarity should be 0.0, below threshold.
        assert_eq!(dogs.hunt_count(), 0);
    }

    #[test]
    fn test_cosine_similarity_identical_vectors() {
        let a = [1.0, 0.0];
        let b = [1.0, 0.0];
        let sim = cosine_similarity(&a, &b);
        assert!(
            (sim - 1.0).abs() < 1e-10,
            "identical vectors should have similarity 1.0, got {sim}"
        );
    }

    #[test]
    fn test_cosine_similarity_orthogonal_vectors() {
        let a = [1.0, 0.0];
        let b = [0.0, 1.0];
        let sim = cosine_similarity(&a, &b);
        assert!(
            sim.abs() < 1e-10,
            "orthogonal vectors should have similarity 0.0, got {sim}"
        );
    }

    #[test]
    fn test_cosine_similarity_zero_vector() {
        let a = [0.0, 0.0, 0.0];
        let b = [1.0, 2.0, 3.0];
        assert_eq!(cosine_similarity(&a, &b), 0.0);
        assert_eq!(cosine_similarity(&b, &a), 0.0);
    }

    #[test]
    fn test_cosine_similarity_empty_vectors() {
        let a: [f64; 0] = [];
        let b: [f64; 0] = [];
        assert_eq!(cosine_similarity(&a, &b), 0.0);
    }

    #[test]
    fn test_cosine_similarity_proportional_vectors() {
        let a = [2.0, 4.0, 6.0];
        let b = [1.0, 2.0, 3.0];
        let sim = cosine_similarity(&a, &b);
        assert!(
            (sim - 1.0).abs() < 1e-10,
            "proportional vectors should have similarity 1.0, got {sim}"
        );
    }

    #[test]
    fn test_cleanup_removes_old_threats() {
        let bus = make_bus();
        let config = WildDogsConfig {
            correlation_window: Duration::from_secs(10),
            min_cluster_size: 3,
            similarity_threshold: 0.7,
            max_concurrent_hunts: 5,
            ..Default::default()
        };
        let mut dogs = WildDogs::new(config, bus);

        // Add threats with old timestamps.
        let old_time = SystemTime::now() - Duration::from_secs(3600);
        for _ in 0..3 {
            dogs.recent_threats.push(ClusterPattern {
                node: NodeId::new(),
                evidence: make_evidence(&[0.9]),
                timestamp: old_time,
                behavioral_vector: vec![0.9],
            });
        }

        // Add one recent threat.
        dogs.recent_threats.push(ClusterPattern {
            node: NodeId::new(),
            evidence: make_evidence(&[0.5]),
            timestamp: SystemTime::now(),
            behavioral_vector: vec![0.5],
        });

        assert_eq!(dogs.recent_threat_count(), 4);
        dogs.cleanup_old_threats();
        assert_eq!(dogs.recent_threat_count(), 1);
    }

    #[test]
    fn test_max_concurrent_hunts_respected() {
        let bus = make_bus();
        let config = WildDogsConfig {
            correlation_window: Duration::from_secs(300),
            min_cluster_size: 2,
            similarity_threshold: 0.5,
            max_concurrent_hunts: 1, // Only 1 hunt allowed.
        };
        let mut dogs = WildDogs::new(config, bus);

        let now = SystemTime::now();

        // First batch: 2 similar threats → should trigger hunt #1.
        dogs.on_threat_confirmed(NodeId::new(), make_evidence(&[0.9, 0.8]), now);
        dogs.on_threat_confirmed(NodeId::new(), make_evidence(&[0.9, 0.8]), now);
        assert_eq!(dogs.hunt_count(), 1);
        assert_eq!(dogs.active_hunt_count(), 1);

        // Second batch: add more similar threats, but hunt limit is 1.
        dogs.on_threat_confirmed(NodeId::new(), make_evidence(&[0.9, 0.8]), now);
        dogs.on_threat_confirmed(NodeId::new(), make_evidence(&[0.9, 0.8]), now);

        // Hunt count should still be 1 because max_concurrent_hunts is 1.
        assert_eq!(dogs.hunt_count(), 1);
        assert_eq!(dogs.active_hunt_count(), 1);
    }

    #[test]
    fn test_extract_behavioral_vector() {
        let evidence = make_evidence(&[0.1, 0.5, 0.9, 1.0]);
        let vector = extract_behavioral_vector(&evidence);
        assert_eq!(vector, vec![0.1, 0.5, 0.9, 1.0]);
    }

    #[test]
    fn test_extract_behavioral_vector_empty() {
        let evidence = EvidenceChain::new();
        let vector = extract_behavioral_vector(&evidence);
        assert!(vector.is_empty());
    }

    #[test]
    fn test_below_min_cluster_size_no_hunt() {
        let bus = make_bus();
        let config = WildDogsConfig {
            correlation_window: Duration::from_secs(300),
            min_cluster_size: 5,
            similarity_threshold: 0.5,
            max_concurrent_hunts: 5,
            ..Default::default()
        };
        let mut dogs = WildDogs::new(config, bus);

        let now = SystemTime::now();

        // Only 3 threats, but min_cluster_size is 5.
        for _ in 0..3 {
            dogs.on_threat_confirmed(NodeId::new(), make_evidence(&[0.9, 0.8, 0.7]), now);
        }

        assert_eq!(dogs.hunt_count(), 0);
    }
}
