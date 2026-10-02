// Marabunta - Licensed under the MIT License.
use dashmap::DashMap;

use crate::highestsec::constraints::{CustodyRecord, NonexportTracking};
use crate::highestsec::jurisdiction::{JurisdictionSet, StaticAllianceResolver};
use crate::highestsec::plugin::JobId;
use crate::highestsec::types::CountryCode;

// ---------------------------------------------------------------------------
// NonexportViolation / NonexportViolationType
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct NonexportViolation {
    pub job_id: String,
    pub violation_type: NonexportViolationType,
    pub jurisdiction: CountryCode,
    pub taint_sources: Vec<JobId>,
}

#[derive(Debug)]
pub enum NonexportViolationType {
    UnauthorizedProcessing,
    UnauthorizedTransit,
}

impl std::fmt::Display for NonexportViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.violation_type {
            NonexportViolationType::UnauthorizedProcessing => {
                write!(
                    f,
                    "NONEXPORT violation: unauthorized processing of job {} in jurisdiction {}",
                    self.job_id, self.jurisdiction
                )
            }
            NonexportViolationType::UnauthorizedTransit => {
                write!(
                    f,
                    "NONEXPORT violation: unauthorized transit of job {} through jurisdiction {}",
                    self.job_id, self.jurisdiction
                )
            }
        }
    }
}

// ---------------------------------------------------------------------------
// NonexportEngine
// ---------------------------------------------------------------------------

/// NONEXPORT enforcement engine.
///
/// Tracks NONEXPORT-taint propagation across jobs and enforces that only
/// authorized jurisdictions may process or transit NONEXPORT-controlled data.
///
/// The engine maintains per-input tracking records keyed by
/// `"<job_id>:<input_id>"` and a per-job merged tracking record.
pub struct NonexportEngine {
    /// Per-input tracking: key = "<job_id>:<input_id>"
    inputs: DashMap<String, NonexportTracking>,
    /// Per-job merged tracking
    tracking: DashMap<String, NonexportTracking>,
    /// Agreement resolver for checking jurisdiction membership.
    resolver: StaticAllianceResolver,
}

impl Default for NonexportEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl NonexportEngine {
    pub fn new() -> Self {
        Self {
            inputs: DashMap::new(),
            tracking: DashMap::new(),
            resolver: StaticAllianceResolver,
        }
    }

    /// Register an input's NONEXPORT tracking metadata for a job.
    pub fn register_input(&self, job_id: &str, input_id: &str, tracking: NonexportTracking) {
        let key = format!("{}:{}", job_id, input_id);
        self.inputs.insert(key, tracking);
    }

    /// Compute the merged NONEXPORT tracking for a job's output based on the
    /// specified input IDs.
    ///
    /// Rules:
    /// - If ANY registered input for the job is tainted, the output is tainted.
    /// - `approved_jurisdictions` = INTERSECTION of all **tainted** inputs'
    ///   approved jurisdictions.
    /// - `taint_source` = union of all tainted inputs' taint sources.
    /// - `control_category` = first non-None category found.
    /// - `custody_chain` = concatenation of all inputs' custody chains.
    pub fn compute_output_nonexport(&self, job_id: &str, input_ids: &[String]) -> NonexportTracking {
        let mut any_tainted = false;
        let mut approved: Option<JurisdictionSet> = None;
        let mut sources: Vec<JobId> = Vec::new();
        let mut category: Option<String> = None;
        let mut custody: Vec<CustodyRecord> = Vec::new();

        for input_id in input_ids {
            let key = format!("{}:{}", job_id, input_id);
            if let Some(entry) = self.inputs.get(&key) {
                let t = entry.value();
                custody.extend(t.custody_chain.iter().cloned());

                if t.is_tainted {
                    any_tainted = true;

                    // Intersection of approved jurisdictions across tainted inputs.
                    approved = Some(match approved {
                        None => t.approved_jurisdictions.clone(),
                        Some(existing) => existing.intersect(&t.approved_jurisdictions),
                    });

                    // Union of taint sources.
                    for src in &t.taint_source {
                        if !sources.contains(src) {
                            sources.push(src.clone());
                        }
                    }

                    if category.is_none() {
                        category = t.control_category.clone();
                    }
                }
            }
        }

        let result = NonexportTracking {
            is_tainted: any_tainted,
            taint_source: sources,
            approved_jurisdictions: approved.unwrap_or(JurisdictionSet::Any),
            control_category: category,
            custody_chain: custody,
        };

        // Store the merged tracking for later enforcement checks.
        self.tracking.insert(job_id.to_string(), result.clone());
        result
    }

    /// Check whether a node in `node_jurisdiction` is authorized to process
    /// data for the given job.
    pub fn check_node_authorized(
        &self,
        job_id: &str,
        node_jurisdiction: &CountryCode,
    ) -> Result<(), NonexportViolation> {
        let Some(tracking) = self.tracking.get(job_id) else {
            // No tracking record => no NONEXPORT constraints => allowed.
            return Ok(());
        };

        if !tracking.is_tainted {
            return Ok(());
        }

        if tracking
            .approved_jurisdictions
            .contains(node_jurisdiction, &self.resolver)
        {
            Ok(())
        } else {
            Err(NonexportViolation {
                job_id: job_id.to_string(),
                violation_type: NonexportViolationType::UnauthorizedProcessing,
                jurisdiction: *node_jurisdiction,
                taint_sources: tracking.taint_source.clone(),
            })
        }
    }

    /// Check whether data for the given job may transit through
    /// `transit_jurisdiction`.
    pub fn check_transit_authorized(
        &self,
        job_id: &str,
        transit_jurisdiction: &CountryCode,
    ) -> Result<(), NonexportViolation> {
        let Some(tracking) = self.tracking.get(job_id) else {
            return Ok(());
        };

        if !tracking.is_tainted {
            return Ok(());
        }

        if tracking
            .approved_jurisdictions
            .contains(transit_jurisdiction, &self.resolver)
        {
            Ok(())
        } else {
            Err(NonexportViolation {
                job_id: job_id.to_string(),
                violation_type: NonexportViolationType::UnauthorizedTransit,
                jurisdiction: *transit_jurisdiction,
                taint_sources: tracking.taint_source.clone(),
            })
        }
    }

    /// Append a custody record to a job's merged tracking.
    pub fn record_custody(&self, job_id: &str, record: CustodyRecord) {
        self.tracking
            .entry(job_id.to_string())
            .and_modify(|t| {
                t.custody_chain.push(record.clone());
            })
            .or_insert_with(|| NonexportTracking {
                is_tainted: false,
                taint_source: Vec::new(),
                approved_jurisdictions: JurisdictionSet::Any,
                control_category: None,
                custody_chain: vec![record],
            });
    }

    /// Return the current merged NONEXPORT tracking for a job, if any.
    pub fn get_tracking(&self, job_id: &str) -> Option<NonexportTracking> {
        self.tracking.get(job_id).map(|e| e.value().clone())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use crate::highestsec::constraints::CustodyOperation;

    fn clean_tracking() -> NonexportTracking {
        NonexportTracking::clean()
    }

    fn tainted_tracking(source: &str, jurisdictions: Vec<CountryCode>) -> NonexportTracking {
        NonexportTracking {
            is_tainted: true,
            taint_source: vec![source.into()],
            approved_jurisdictions: JurisdictionSet::Only(jurisdictions),
            control_category: Some("EXPORT_CATEGORY_A-XI".into()),
            custody_chain: Vec::new(),
        }
    }

    fn make_custody_record(
        node_id: Vec<u8>,
        jurisdiction: CountryCode,
        operation: CustodyOperation,
    ) -> CustodyRecord {
        CustodyRecord {
            node_id,
            jurisdiction,
            proof_level: 2,
            timestamp: Utc::now().to_rfc3339(),
            operation,
        }
    }

    // ========================================================================
    // Taint propagation
    // ========================================================================

    #[test]
    fn test_clean_input_stays_clean() {
        let engine = NonexportEngine::new();
        engine.register_input("job-1", "in-a", clean_tracking());
        engine.register_input("job-1", "in-b", clean_tracking());

        let output = engine.compute_output_nonexport(
            "job-1",
            &["in-a".into(), "in-b".into()],
        );

        assert!(!output.is_tainted);
        assert!(output.taint_source.is_empty());
    }

    #[test]
    fn test_tainted_propagates() {
        let engine = NonexportEngine::new();
        engine.register_input(
            "job-1",
            "in-a",
            tainted_tracking("src-1", vec![CountryCode::US, CountryCode::GB]),
        );

        let output = engine.compute_output_nonexport("job-1", &["in-a".into()]);

        assert!(output.is_tainted);
        assert_eq!(output.taint_source, vec!["src-1".to_string()]);
    }

    #[test]
    fn test_mixed_inputs_tainted_wins() {
        let engine = NonexportEngine::new();
        engine.register_input("job-1", "clean", clean_tracking());
        engine.register_input(
            "job-1",
            "dirty",
            tainted_tracking("src-x", vec![CountryCode::US]),
        );

        let output = engine.compute_output_nonexport(
            "job-1",
            &["clean".into(), "dirty".into()],
        );

        assert!(output.is_tainted);
        assert_eq!(output.taint_source, vec!["src-x".to_string()]);
    }

    // ========================================================================
    // Jurisdiction intersection
    // ========================================================================

    #[test]
    fn test_jurisdiction_intersection() {
        let engine = NonexportEngine::new();

        // Input A approves US, GB, CA.
        engine.register_input(
            "job-1",
            "in-a",
            tainted_tracking("src-a", vec![CountryCode::US, CountryCode::GB, CountryCode::CA]),
        );
        // Input B approves US, CA, AU.
        engine.register_input(
            "job-1",
            "in-b",
            tainted_tracking("src-b", vec![CountryCode::US, CountryCode::CA, CountryCode::AU]),
        );

        let output = engine.compute_output_nonexport(
            "job-1",
            &["in-a".into(), "in-b".into()],
        );

        assert!(output.is_tainted);

        let resolver = StaticAllianceResolver;
        // Intersection: US, CA.
        assert!(output.approved_jurisdictions.contains(&CountryCode::US, &resolver));
        assert!(output.approved_jurisdictions.contains(&CountryCode::CA, &resolver));
        assert!(!output.approved_jurisdictions.contains(&CountryCode::GB, &resolver));
        assert!(!output.approved_jurisdictions.contains(&CountryCode::AU, &resolver));

        // Taint sources should be unioned.
        assert!(output.taint_source.contains(&"src-a".to_string()));
        assert!(output.taint_source.contains(&"src-b".to_string()));
    }

    // ========================================================================
    // Node authorization
    // ========================================================================

    #[test]
    fn test_node_authorized() {
        let engine = NonexportEngine::new();
        engine.register_input(
            "job-1",
            "in-a",
            tainted_tracking("src", vec![CountryCode::US, CountryCode::GB]),
        );
        engine.compute_output_nonexport("job-1", &["in-a".into()]);

        assert!(engine.check_node_authorized("job-1", &CountryCode::US).is_ok());
        assert!(engine.check_node_authorized("job-1", &CountryCode::GB).is_ok());
    }

    #[test]
    fn test_node_unauthorized() {
        let engine = NonexportEngine::new();
        engine.register_input(
            "job-1",
            "in-a",
            tainted_tracking("src", vec![CountryCode::US, CountryCode::GB]),
        );
        engine.compute_output_nonexport("job-1", &["in-a".into()]);

        let result = engine.check_node_authorized("job-1", &CountryCode::FR);
        assert!(result.is_err());
        let violation = result.unwrap_err();
        assert_eq!(violation.jurisdiction, CountryCode::FR);
        assert!(matches!(
            violation.violation_type,
            NonexportViolationType::UnauthorizedProcessing
        ));
    }

    #[test]
    fn test_node_authorized_no_tracking() {
        let engine = NonexportEngine::new();
        // No tracking registered => always allowed.
        assert!(engine.check_node_authorized("unknown-job", &CountryCode::FR).is_ok());
    }

    #[test]
    fn test_node_authorized_clean_data() {
        let engine = NonexportEngine::new();
        engine.register_input("job-1", "in-a", clean_tracking());
        engine.compute_output_nonexport("job-1", &["in-a".into()]);

        // Clean data => any jurisdiction allowed.
        assert!(engine.check_node_authorized("job-1", &CountryCode::FR).is_ok());
    }

    // ========================================================================
    // Transit authorization
    // ========================================================================

    #[test]
    fn test_transit_authorized() {
        let engine = NonexportEngine::new();
        engine.register_input(
            "job-1",
            "in-a",
            tainted_tracking("src", vec![CountryCode::US, CountryCode::GB]),
        );
        engine.compute_output_nonexport("job-1", &["in-a".into()]);

        assert!(engine.check_transit_authorized("job-1", &CountryCode::US).is_ok());
        assert!(engine.check_transit_authorized("job-1", &CountryCode::GB).is_ok());
    }

    #[test]
    fn test_transit_unauthorized() {
        let engine = NonexportEngine::new();
        engine.register_input(
            "job-1",
            "in-a",
            tainted_tracking("src", vec![CountryCode::US]),
        );
        engine.compute_output_nonexport("job-1", &["in-a".into()]);

        let result = engine.check_transit_authorized("job-1", &CountryCode::DE);
        assert!(result.is_err());
        let violation = result.unwrap_err();
        assert_eq!(violation.jurisdiction, CountryCode::DE);
        assert!(matches!(
            violation.violation_type,
            NonexportViolationType::UnauthorizedTransit
        ));
    }

    #[test]
    fn test_transit_authorized_clean_data() {
        let engine = NonexportEngine::new();
        engine.register_input("job-1", "in-a", clean_tracking());
        engine.compute_output_nonexport("job-1", &["in-a".into()]);

        assert!(engine.check_transit_authorized("job-1", &CountryCode::FR).is_ok());
    }

    // ========================================================================
    // Custody chain
    // ========================================================================

    #[test]
    fn test_custody_chain_records() {
        let engine = NonexportEngine::new();
        engine.register_input(
            "job-1",
            "in-a",
            tainted_tracking("src", vec![CountryCode::US]),
        );
        engine.compute_output_nonexport("job-1", &["in-a".into()]);

        engine.record_custody(
            "job-1",
            make_custody_record(vec![1], CountryCode::US, CustodyOperation::Received),
        );
        engine.record_custody(
            "job-1",
            make_custody_record(vec![2], CountryCode::US, CustodyOperation::Executed),
        );
        engine.record_custody(
            "job-1",
            make_custody_record(vec![3], CountryCode::US, CustodyOperation::Forwarded),
        );

        let tracking = engine.get_tracking("job-1").unwrap();
        assert_eq!(tracking.custody_chain.len(), 3);
        assert_eq!(tracking.custody_chain[0].node_id, vec![1]);
        assert_eq!(tracking.custody_chain[1].node_id, vec![2]);
        assert_eq!(tracking.custody_chain[2].node_id, vec![3]);
    }

    #[test]
    fn test_custody_record_creates_tracking_if_absent() {
        let engine = NonexportEngine::new();

        // No prior tracking for "new-job"
        engine.record_custody(
            "new-job",
            make_custody_record(vec![10], CountryCode::FR, CustodyOperation::Received),
        );

        let tracking = engine.get_tracking("new-job").unwrap();
        assert!(!tracking.is_tainted);
        assert_eq!(tracking.custody_chain.len(), 1);
    }

    // ========================================================================
    // get_tracking
    // ========================================================================

    #[test]
    fn test_get_tracking_none() {
        let engine = NonexportEngine::new();
        assert!(engine.get_tracking("nonexistent").is_none());
    }

    #[test]
    fn test_get_tracking_after_compute() {
        let engine = NonexportEngine::new();
        engine.register_input(
            "job-1",
            "in-a",
            tainted_tracking("src-1", vec![CountryCode::US]),
        );
        engine.compute_output_nonexport("job-1", &["in-a".into()]);

        let tracking = engine.get_tracking("job-1");
        assert!(tracking.is_some());
        let t = tracking.unwrap();
        assert!(t.is_tainted);
        assert_eq!(t.taint_source, vec!["src-1".to_string()]);
    }

    // ========================================================================
    // Edge cases
    // ========================================================================

    #[test]
    fn test_compute_with_nonexistent_input_ids() {
        let engine = NonexportEngine::new();
        engine.register_input("job-1", "exists", clean_tracking());

        // Reference an input that was never registered.
        let output = engine.compute_output_nonexport(
            "job-1",
            &["exists".into(), "missing".into()],
        );

        // Should still work -- missing inputs are just skipped.
        assert!(!output.is_tainted);
    }

    #[test]
    fn test_compute_empty_input_ids() {
        let engine = NonexportEngine::new();
        let output = engine.compute_output_nonexport("job-1", &[]);
        assert!(!output.is_tainted);
    }

    #[test]
    fn test_multiple_jobs_independent() {
        let engine = NonexportEngine::new();

        engine.register_input(
            "job-1",
            "in-a",
            tainted_tracking("src-1", vec![CountryCode::US]),
        );
        engine.register_input("job-2", "in-a", clean_tracking());

        engine.compute_output_nonexport("job-1", &["in-a".into()]);
        engine.compute_output_nonexport("job-2", &["in-a".into()]);

        // job-1 is tainted, job-2 is clean.
        assert!(engine.check_node_authorized("job-1", &CountryCode::FR).is_err());
        assert!(engine.check_node_authorized("job-2", &CountryCode::FR).is_ok());
    }

    #[test]
    fn test_taint_sources_deduplicated() {
        let engine = NonexportEngine::new();

        // Two inputs with overlapping taint sources.
        let mut t1 = tainted_tracking("src-common", vec![CountryCode::US]);
        t1.taint_source.push("src-a".into());

        let mut t2 = tainted_tracking("src-common", vec![CountryCode::US]);
        t2.taint_source.push("src-b".into());

        engine.register_input("job-1", "in-a", t1);
        engine.register_input("job-1", "in-b", t2);

        let output = engine.compute_output_nonexport(
            "job-1",
            &["in-a".into(), "in-b".into()],
        );

        // "src-common" should appear only once.
        let count = output
            .taint_source
            .iter()
            .filter(|s| *s == "src-common")
            .count();
        assert_eq!(count, 1);
        assert!(output.taint_source.contains(&"src-a".to_string()));
        assert!(output.taint_source.contains(&"src-b".to_string()));
    }

    #[test]
    fn test_control_category_first_wins() {
        let engine = NonexportEngine::new();

        let mut t1 = tainted_tracking("src-1", vec![CountryCode::US]);
        t1.control_category = Some("EXPORT_CATEGORY_A-XI".into());

        let mut t2 = tainted_tracking("src-2", vec![CountryCode::US]);
        t2.control_category = Some("EAR99".into());

        engine.register_input("job-1", "in-a", t1);
        engine.register_input("job-1", "in-b", t2);

        let output = engine.compute_output_nonexport(
            "job-1",
            &["in-a".into(), "in-b".into()],
        );

        // First category encountered wins.
        assert_eq!(output.control_category, Some("EXPORT_CATEGORY_A-XI".into()));
    }

    #[test]
    fn test_custody_chains_from_inputs_concatenated() {
        let engine = NonexportEngine::new();

        let mut t1 = tainted_tracking("src-1", vec![CountryCode::US]);
        t1.custody_chain.push(make_custody_record(
            vec![1],
            CountryCode::US,
            CustodyOperation::Received,
        ));

        let mut t2 = tainted_tracking("src-2", vec![CountryCode::US]);
        t2.custody_chain.push(make_custody_record(
            vec![2],
            CountryCode::US,
            CustodyOperation::Executed,
        ));

        engine.register_input("job-1", "in-a", t1);
        engine.register_input("job-1", "in-b", t2);

        let output = engine.compute_output_nonexport(
            "job-1",
            &["in-a".into(), "in-b".into()],
        );

        assert_eq!(output.custody_chain.len(), 2);
    }
}
