// Marabunta - Licensed under the MIT License.
use serde::{Serialize, Deserialize};
use crate::highestsec::types::CountryCode;
use crate::highestsec::jurisdiction::JurisdictionSet;
use crate::highestsec::classification::ClassificationLevel;
use crate::highestsec::plugin::JobId;

/// A single data constraint describing jurisdictional or sovereignty requirements.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DataConstraint {
    /// Data may only be decrypted within these jurisdictions.
    DecryptionBoundary(JurisdictionSet),
    /// Data may transit through these jurisdictions.
    TransitAllowed(JurisdictionSet),
    /// Data must NOT transit through these jurisdictions.
    TransitDenied(JurisdictionSet),
    /// Results may only be consolidated (aggregated) within these jurisdictions.
    ConsolidationBoundary(JurisdictionSet),
    /// Cryptographic keys must reside in these jurisdictions.
    KeyResidency(JurisdictionSet),
    /// NONEXPORT tainted data: originated in a specific jurisdiction and may only go
    /// to approved recipients.
    NonexportTainted {
        original_jurisdiction: CountryCode,
        approved_recipients: JurisdictionSet,
    },
    /// Temporal jurisdiction: constraints that apply under a specific legal
    /// regime at a given point in time, allowing nesting.
    TemporalJurisdiction {
        jurisdiction: CountryCode,
        applicable_law_date: String,
        constraints_at_time: Box<DataConstraint>,
    },
}

/// A composed set of constraints representing the high-water-mark union of all
/// input constraint sets for a particular data pipeline.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ConstraintSet {
    /// Jurisdictions where decryption is permitted (intersection of inputs).
    pub decryption_boundary: Option<JurisdictionSet>,
    /// Jurisdictions where transit is allowed.
    pub transit_allowed: Option<JurisdictionSet>,
    /// Jurisdictions where transit is denied (union of inputs).
    pub transit_denied: Option<JurisdictionSet>,
    /// Jurisdictions where consolidation is permitted (intersection of inputs).
    pub consolidation_boundary: Option<JurisdictionSet>,
    /// Jurisdictions where keys must reside (intersection of inputs).
    pub key_residency: Option<JurisdictionSet>,
    /// NONEXPORT tracking state.
    pub nonexport: Option<NonexportTracking>,
    /// Classification level (max of inputs).
    pub classification: ClassificationLevel,
    /// Compartments (union of inputs, deduplicated).
    pub compartments: Vec<String>,
}

/// Tracks NONEXPORT taint propagation through the compute pipeline.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NonexportTracking {
    /// Whether data is NONEXPORT-tainted.
    pub is_tainted: bool,
    /// The job IDs that introduced the taint.
    pub taint_source: Vec<JobId>,
    /// Jurisdictions approved to receive NONEXPORT-controlled data.
    pub approved_jurisdictions: JurisdictionSet,
    /// NONEXPORT control category (e.g., EXPORT_CATEGORY_A category).
    pub control_category: Option<String>,
    /// Chain of custody records.
    pub custody_chain: Vec<CustodyRecord>,
}

/// A record of data custody at a particular node and time.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CustodyRecord {
    /// Node that held custody.
    pub node_id: Vec<u8>,
    /// Jurisdiction of the node.
    pub jurisdiction: CountryCode,
    /// Proof level established.
    pub proof_level: u8,
    /// ISO 8601 timestamp.
    pub timestamp: String,
    /// What operation was performed.
    pub operation: CustodyOperation,
}

/// Operations that generate custody records.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CustodyOperation {
    Received,
    Executed,
    Forwarded,
    Consolidated,
    Delivered,
}

impl NonexportTracking {
    /// Create a clean (untainted) NONEXPORT tracking record.
    pub fn clean() -> Self {
        Self {
            is_tainted: false,
            taint_source: Vec::new(),
            approved_jurisdictions: JurisdictionSet::Any,
            control_category: None,
            custody_chain: Vec::new(),
        }
    }

    /// Whether this data is NONEXPORT-tainted.
    pub fn is_tainted(&self) -> bool {
        self.is_tainted
    }
}

/// Compose multiple constraint sets using high-water-mark rules:
///
/// 1. `decryption_boundary`: INTERSECTION of all inputs.
/// 2. `transit_denied`: UNION of all inputs.
/// 3. `consolidation_boundary`: INTERSECTION of all inputs.
/// 4. `key_residency`: INTERSECTION of all inputs.
/// 5. `nonexport`: if ANY input is tainted, output is tainted;
///    `approved_jurisdictions` = INTERSECTION of all tainted inputs;
///    `taint_source` = union of all sources; `custody_chain` = concatenated.
/// 6. `classification`: MAX of all inputs.
/// 7. `compartments`: UNION of all inputs, deduplicated.
pub fn compose_constraints(inputs: &[ConstraintSet]) -> ConstraintSet {
    if inputs.is_empty() {
        return ConstraintSet::default();
    }

    let mut decryption_boundary: Option<JurisdictionSet> = None;
    let mut transit_allowed: Option<JurisdictionSet> = None;
    let mut transit_denied: Option<JurisdictionSet> = None;
    let mut consolidation_boundary: Option<JurisdictionSet> = None;
    let mut key_residency: Option<JurisdictionSet> = None;
    let mut classification = ClassificationLevel::Unclassified;
    let mut compartments: Vec<String> = Vec::new();

    // NONEXPORT: collect all tracking records, then merge.
    let mut any_tainted = false;
    let mut nonexport_approved: Option<JurisdictionSet> = None;
    let mut nonexport_sources: Vec<JobId> = Vec::new();
    let mut nonexport_custody: Vec<CustodyRecord> = Vec::new();
    let mut nonexport_category: Option<String> = None;
    let mut has_any_nonexport = false;

    for input in inputs {
        // 1. Decryption boundary: INTERSECTION.
        if let Some(ref db) = input.decryption_boundary {
            decryption_boundary = Some(match decryption_boundary {
                None => db.clone(),
                Some(existing) => existing.intersect(db),
            });
        }

        // Transit allowed: INTERSECTION (more restrictive).
        if let Some(ref ta) = input.transit_allowed {
            transit_allowed = Some(match transit_allowed {
                None => ta.clone(),
                Some(existing) => existing.intersect(ta),
            });
        }

        // 2. Transit denied: UNION (more restrictive).
        if let Some(ref td) = input.transit_denied {
            transit_denied = Some(match transit_denied {
                None => td.clone(),
                Some(existing) => existing.union(td),
            });
        }

        // 3. Consolidation boundary: INTERSECTION.
        if let Some(ref cb) = input.consolidation_boundary {
            consolidation_boundary = Some(match consolidation_boundary {
                None => cb.clone(),
                Some(existing) => existing.intersect(cb),
            });
        }

        // 4. Key residency: INTERSECTION.
        if let Some(ref kr) = input.key_residency {
            key_residency = Some(match key_residency {
                None => kr.clone(),
                Some(existing) => existing.intersect(kr),
            });
        }

        // 5. NONEXPORT.
        if let Some(ref nonexport) = input.nonexport {
            has_any_nonexport = true;
            if nonexport.is_tainted {
                any_tainted = true;
                nonexport_approved = Some(match nonexport_approved {
                    None => nonexport.approved_jurisdictions.clone(),
                    Some(existing) => existing.intersect(&nonexport.approved_jurisdictions),
                });
                for src in &nonexport.taint_source {
                    if !nonexport_sources.contains(src) {
                        nonexport_sources.push(src.clone());
                    }
                }
                if nonexport_category.is_none() {
                    nonexport_category = nonexport.control_category.clone();
                }
            }
            nonexport_custody.extend(nonexport.custody_chain.iter().cloned());
        }

        // 6. Classification: MAX.
        if input.classification > classification {
            classification = input.classification;
        }

        // 7. Compartments: UNION (deduplicated).
        for c in &input.compartments {
            if !compartments.contains(c) {
                compartments.push(c.clone());
            }
        }
    }

    let nonexport = if has_any_nonexport {
        Some(NonexportTracking {
            is_tainted: any_tainted,
            taint_source: nonexport_sources,
            approved_jurisdictions: nonexport_approved.unwrap_or(JurisdictionSet::Any),
            control_category: nonexport_category,
            custody_chain: nonexport_custody,
        })
    } else {
        None
    };

    ConstraintSet {
        decryption_boundary,
        transit_allowed,
        transit_denied,
        consolidation_boundary,
        key_residency,
        nonexport,
        classification,
        compartments,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::highestsec::jurisdiction::{JurisdictionSet, StaticAllianceResolver, AllianceResolver};
    use crate::highestsec::types::CountryCode;

    fn resolver() -> StaticAllianceResolver {
        StaticAllianceResolver
    }

    #[test]
    fn test_compose_empty_inputs() {
        let result = compose_constraints(&[]);
        assert!(result.decryption_boundary.is_none());
        assert!(result.transit_denied.is_none());
        assert!(result.consolidation_boundary.is_none());
        assert!(result.key_residency.is_none());
        assert!(result.nonexport.is_none());
        assert_eq!(result.classification, ClassificationLevel::Unclassified);
        assert!(result.compartments.is_empty());
    }

    #[test]
    fn test_compose_single_input() {
        let input = ConstraintSet {
            decryption_boundary: Some(JurisdictionSet::Only(vec![
                CountryCode::FR,
                CountryCode::DE,
            ])),
            transit_denied: Some(JurisdictionSet::Only(vec![CountryCode::US])),
            classification: ClassificationLevel::Secret,
            compartments: vec!["ALPHA".into()],
            ..Default::default()
        };

        let result = compose_constraints(&[input]);
        let r = resolver();

        let db = result.decryption_boundary.unwrap();
        assert!(db.contains(&CountryCode::FR, &r));
        assert!(db.contains(&CountryCode::DE, &r));
        assert!(!db.contains(&CountryCode::ES, &r));

        let td = result.transit_denied.unwrap();
        assert!(td.contains(&CountryCode::US, &r));
        assert!(!td.contains(&CountryCode::FR, &r));

        assert_eq!(result.classification, ClassificationLevel::Secret);
        assert_eq!(result.compartments, vec!["ALPHA".to_string()]);
    }

    #[test]
    fn test_decryption_boundary_intersection() {
        let a = ConstraintSet {
            decryption_boundary: Some(JurisdictionSet::Only(vec![
                CountryCode::FR,
                CountryCode::DE,
                CountryCode::ES,
            ])),
            ..Default::default()
        };
        let b = ConstraintSet {
            decryption_boundary: Some(JurisdictionSet::Only(vec![
                CountryCode::DE,
                CountryCode::ES,
                CountryCode::IT,
            ])),
            ..Default::default()
        };

        let result = compose_constraints(&[a, b]);
        let r = resolver();
        let db = result.decryption_boundary.unwrap();

        // Intersection: DE, ES.
        assert!(db.contains(&CountryCode::DE, &r));
        assert!(db.contains(&CountryCode::ES, &r));
        assert!(!db.contains(&CountryCode::FR, &r));
        assert!(!db.contains(&CountryCode::IT, &r));
    }

    #[test]
    fn test_transit_denied_union() {
        let a = ConstraintSet {
            transit_denied: Some(JurisdictionSet::Only(vec![CountryCode::US])),
            ..Default::default()
        };
        let b = ConstraintSet {
            transit_denied: Some(JurisdictionSet::Only(vec![CountryCode::GB])),
            ..Default::default()
        };

        let result = compose_constraints(&[a, b]);
        let r = resolver();
        let td = result.transit_denied.unwrap();

        // Union: US, GB.
        assert!(td.contains(&CountryCode::US, &r));
        assert!(td.contains(&CountryCode::GB, &r));
        assert!(!td.contains(&CountryCode::FR, &r));
    }

    #[test]
    fn test_classification_max() {
        let inputs = vec![
            ConstraintSet {
                classification: ClassificationLevel::Restricted,
                ..Default::default()
            },
            ConstraintSet {
                classification: ClassificationLevel::Unclassified,
                ..Default::default()
            },
            ConstraintSet {
                classification: ClassificationLevel::Secret,
                ..Default::default()
            },
            ConstraintSet {
                classification: ClassificationLevel::Confidential,
                ..Default::default()
            },
        ];

        let result = compose_constraints(&inputs);
        assert_eq!(result.classification, ClassificationLevel::Secret);
    }

    #[test]
    fn test_compartments_union_deduplicated() {
        let a = ConstraintSet {
            compartments: vec!["ALPHA".into(), "BRAVO".into()],
            ..Default::default()
        };
        let b = ConstraintSet {
            compartments: vec!["BRAVO".into(), "CHARLIE".into()],
            ..Default::default()
        };

        let result = compose_constraints(&[a, b]);
        assert_eq!(result.compartments.len(), 3);
        assert!(result.compartments.contains(&"ALPHA".to_string()));
        assert!(result.compartments.contains(&"BRAVO".to_string()));
        assert!(result.compartments.contains(&"CHARLIE".to_string()));
    }

    #[test]
    fn test_nonexport_any_taint_propagates() {
        let clean = ConstraintSet {
            nonexport: Some(NonexportTracking::clean()),
            ..Default::default()
        };
        let tainted = ConstraintSet {
            nonexport: Some(NonexportTracking {
                is_tainted: true,
                taint_source: vec!["job-1".into()],
                approved_jurisdictions: JurisdictionSet::Only(vec![
                    CountryCode::US,
                    CountryCode::GB,
                ]),
                control_category: Some("EXPORT_CATEGORY_A-XI".into()),
                custody_chain: vec![CustodyRecord {
                    node_id: vec![1, 2, 3],
                    jurisdiction: CountryCode::US,
                    proof_level: 3,
                    timestamp: "2025-01-01T00:00:00Z".into(),
                    operation: CustodyOperation::Received,
                }],
            }),
            ..Default::default()
        };

        let result = compose_constraints(&[clean, tainted]);
        let nonexport = result.nonexport.unwrap();
        assert!(nonexport.is_tainted());
        assert_eq!(nonexport.taint_source, vec!["job-1".to_string()]);
        assert_eq!(nonexport.control_category, Some("EXPORT_CATEGORY_A-XI".into()));
    }

    #[test]
    fn test_nonexport_jurisdiction_intersection() {
        let a = ConstraintSet {
            nonexport: Some(NonexportTracking {
                is_tainted: true,
                taint_source: vec!["job-a".into()],
                approved_jurisdictions: JurisdictionSet::Only(vec![
                    CountryCode::US,
                    CountryCode::GB,
                    CountryCode::CA,
                ]),
                control_category: None,
                custody_chain: Vec::new(),
            }),
            ..Default::default()
        };
        let b = ConstraintSet {
            nonexport: Some(NonexportTracking {
                is_tainted: true,
                taint_source: vec!["job-b".into()],
                approved_jurisdictions: JurisdictionSet::Only(vec![
                    CountryCode::US,
                    CountryCode::CA,
                    CountryCode::AU,
                ]),
                control_category: None,
                custody_chain: Vec::new(),
            }),
            ..Default::default()
        };

        let result = compose_constraints(&[a, b]);
        let nonexport = result.nonexport.unwrap();
        assert!(nonexport.is_tainted());
        let r = resolver();
        // Intersection: US, CA.
        assert!(nonexport.approved_jurisdictions.contains(&CountryCode::US, &r));
        assert!(nonexport.approved_jurisdictions.contains(&CountryCode::CA, &r));
        assert!(!nonexport.approved_jurisdictions.contains(&CountryCode::GB, &r));
        assert!(!nonexport.approved_jurisdictions.contains(&CountryCode::AU, &r));

        // Sources should be unioned.
        assert!(nonexport.taint_source.contains(&"job-a".to_string()));
        assert!(nonexport.taint_source.contains(&"job-b".to_string()));
    }

    #[test]
    fn test_nonexport_clean() {
        let nonexport = NonexportTracking::clean();
        assert!(!nonexport.is_tainted());
        assert!(nonexport.taint_source.is_empty());
        assert!(nonexport.custody_chain.is_empty());
        assert!(nonexport.control_category.is_none());

        let r = resolver();
        // Any jurisdiction should be in the clean approved set.
        assert!(nonexport.approved_jurisdictions.contains(&CountryCode::US, &r));
        assert!(nonexport.approved_jurisdictions.contains(&CountryCode::FR, &r));
    }

    #[test]
    fn test_custody_chain_concatenation() {
        let a = ConstraintSet {
            nonexport: Some(NonexportTracking {
                is_tainted: true,
                taint_source: vec!["j1".into()],
                approved_jurisdictions: JurisdictionSet::Any,
                control_category: None,
                custody_chain: vec![
                    CustodyRecord {
                        node_id: vec![1],
                        jurisdiction: CountryCode::FR,
                        proof_level: 2,
                        timestamp: "2025-01-01T00:00:00Z".into(),
                        operation: CustodyOperation::Received,
                    },
                    CustodyRecord {
                        node_id: vec![2],
                        jurisdiction: CountryCode::FR,
                        proof_level: 2,
                        timestamp: "2025-01-01T01:00:00Z".into(),
                        operation: CustodyOperation::Executed,
                    },
                ],
            }),
            ..Default::default()
        };
        let b = ConstraintSet {
            nonexport: Some(NonexportTracking {
                is_tainted: true,
                taint_source: vec!["j2".into()],
                approved_jurisdictions: JurisdictionSet::Any,
                control_category: None,
                custody_chain: vec![CustodyRecord {
                    node_id: vec![3],
                    jurisdiction: CountryCode::DE,
                    proof_level: 3,
                    timestamp: "2025-01-01T02:00:00Z".into(),
                    operation: CustodyOperation::Forwarded,
                }],
            }),
            ..Default::default()
        };

        let result = compose_constraints(&[a, b]);
        let nonexport = result.nonexport.unwrap();
        assert_eq!(nonexport.custody_chain.len(), 3);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let cs = ConstraintSet {
            decryption_boundary: Some(JurisdictionSet::Only(vec![CountryCode::FR])),
            transit_denied: Some(JurisdictionSet::Only(vec![CountryCode::US])),
            consolidation_boundary: Some(JurisdictionSet::Only(vec![
                CountryCode::FR,
                CountryCode::DE,
            ])),
            key_residency: Some(JurisdictionSet::Only(vec![CountryCode::FR])),
            transit_allowed: Some(JurisdictionSet::Only(vec![
                CountryCode::FR,
                CountryCode::DE,
                CountryCode::ES,
            ])),
            nonexport: Some(NonexportTracking {
                is_tainted: true,
                taint_source: vec!["job-x".into()],
                approved_jurisdictions: JurisdictionSet::Only(vec![CountryCode::US]),
                control_category: Some("CAT-V".into()),
                custody_chain: vec![CustodyRecord {
                    node_id: vec![10, 20],
                    jurisdiction: CountryCode::US,
                    proof_level: 3,
                    timestamp: "2025-06-01T00:00:00Z".into(),
                    operation: CustodyOperation::Delivered,
                }],
            }),
            classification: ClassificationLevel::Secret,
            compartments: vec!["ALPHA".into(), "BRAVO".into()],
        };

        let json = serde_json::to_string(&cs).unwrap();
        let cs2: ConstraintSet = serde_json::from_str(&json).unwrap();

        assert_eq!(cs2.classification, ClassificationLevel::Secret);
        assert_eq!(cs2.compartments.len(), 2);
        assert!(cs2.nonexport.as_ref().unwrap().is_tainted());
        assert!(cs2.decryption_boundary.is_some());
        assert!(cs2.transit_denied.is_some());
    }

    #[test]
    fn test_temporal_jurisdiction_nesting() {
        let constraint = DataConstraint::TemporalJurisdiction {
            jurisdiction: CountryCode::DE,
            applicable_law_date: "2024-01-01".into(),
            constraints_at_time: Box::new(DataConstraint::TemporalJurisdiction {
                jurisdiction: CountryCode::FR,
                applicable_law_date: "2020-06-15".into(),
                constraints_at_time: Box::new(DataConstraint::DecryptionBoundary(
                    JurisdictionSet::Only(vec![CountryCode::FR]),
                )),
            }),
        };

        let json = serde_json::to_string(&constraint).unwrap();
        let c2: DataConstraint = serde_json::from_str(&json).unwrap();

        // Verify the outer layer.
        match &c2 {
            DataConstraint::TemporalJurisdiction {
                jurisdiction,
                applicable_law_date,
                constraints_at_time,
            } => {
                assert_eq!(*jurisdiction, CountryCode::DE);
                assert_eq!(applicable_law_date, "2024-01-01");
                // Verify the inner layer.
                match constraints_at_time.as_ref() {
                    DataConstraint::TemporalJurisdiction {
                        jurisdiction: inner_j,
                        constraints_at_time: innermost,
                        ..
                    } => {
                        assert_eq!(*inner_j, CountryCode::FR);
                        match innermost.as_ref() {
                            DataConstraint::DecryptionBoundary(_) => {} // expected
                            other => panic!("expected DecryptionBoundary, got {:?}", other),
                        }
                    }
                    other => panic!("expected nested TemporalJurisdiction, got {:?}", other),
                }
            }
            other => panic!("expected TemporalJurisdiction, got {:?}", other),
        }
    }

    #[test]
    fn test_data_constraint_variants_serialize() {
        let constraints = vec![
            DataConstraint::DecryptionBoundary(JurisdictionSet::Only(vec![CountryCode::FR])),
            DataConstraint::TransitAllowed(JurisdictionSet::Any),
            DataConstraint::TransitDenied(JurisdictionSet::Only(vec![CountryCode::US])),
            DataConstraint::ConsolidationBoundary(JurisdictionSet::Only(vec![
                CountryCode::FR,
                CountryCode::DE,
            ])),
            DataConstraint::KeyResidency(JurisdictionSet::Only(vec![CountryCode::FR])),
            DataConstraint::NonexportTainted {
                original_jurisdiction: CountryCode::US,
                approved_recipients: JurisdictionSet::Only(vec![
                    CountryCode::US,
                    CountryCode::GB,
                ]),
            },
        ];

        for c in &constraints {
            let json = serde_json::to_string(c).unwrap();
            let _c2: DataConstraint = serde_json::from_str(&json).unwrap();
        }
    }

    #[test]
    fn test_custody_operations_serialize() {
        let ops = vec![
            CustodyOperation::Received,
            CustodyOperation::Executed,
            CustodyOperation::Forwarded,
            CustodyOperation::Consolidated,
            CustodyOperation::Delivered,
        ];

        for op in &ops {
            let json = serde_json::to_string(op).unwrap();
            let _op2: CustodyOperation = serde_json::from_str(&json).unwrap();
        }
    }

    #[test]
    fn test_key_residency_intersection() {
        let a = ConstraintSet {
            key_residency: Some(JurisdictionSet::Only(vec![
                CountryCode::FR,
                CountryCode::DE,
                CountryCode::NL,
            ])),
            ..Default::default()
        };
        let b = ConstraintSet {
            key_residency: Some(JurisdictionSet::Only(vec![
                CountryCode::DE,
                CountryCode::NL,
                CountryCode::BE,
            ])),
            ..Default::default()
        };

        let result = compose_constraints(&[a, b]);
        let r = resolver();
        let kr = result.key_residency.unwrap();
        assert!(kr.contains(&CountryCode::DE, &r));
        assert!(kr.contains(&CountryCode::NL, &r));
        assert!(!kr.contains(&CountryCode::FR, &r));
        assert!(!kr.contains(&CountryCode::BE, &r));
    }

    #[test]
    fn test_consolidation_boundary_intersection() {
        let a = ConstraintSet {
            consolidation_boundary: Some(JurisdictionSet::Only(vec![
                CountryCode::FR,
                CountryCode::DE,
            ])),
            ..Default::default()
        };
        let b = ConstraintSet {
            consolidation_boundary: Some(JurisdictionSet::Only(vec![
                CountryCode::FR,
                CountryCode::ES,
            ])),
            ..Default::default()
        };

        let result = compose_constraints(&[a, b]);
        let r = resolver();
        let cb = result.consolidation_boundary.unwrap();
        assert!(cb.contains(&CountryCode::FR, &r));
        assert!(!cb.contains(&CountryCode::DE, &r));
        assert!(!cb.contains(&CountryCode::ES, &r));
    }

    #[test]
    fn test_no_nonexport_when_none_provided() {
        let a = ConstraintSet {
            classification: ClassificationLevel::Restricted,
            ..Default::default()
        };
        let b = ConstraintSet {
            classification: ClassificationLevel::Confidential,
            ..Default::default()
        };

        let result = compose_constraints(&[a, b]);
        assert!(result.nonexport.is_none());
    }

    #[test]
    fn test_compose_preserves_none_fields() {
        // If no input sets a particular boundary, the output should also be None.
        let a = ConstraintSet {
            classification: ClassificationLevel::Restricted,
            compartments: vec!["X".into()],
            ..Default::default()
        };

        let result = compose_constraints(&[a]);
        assert!(result.decryption_boundary.is_none());
        assert!(result.transit_denied.is_none());
        assert!(result.consolidation_boundary.is_none());
        assert!(result.key_residency.is_none());
        assert!(result.transit_allowed.is_none());
        assert_eq!(result.classification, ClassificationLevel::Restricted);
        assert_eq!(result.compartments, vec!["X".to_string()]);
    }
}



/// Represents an Algebraic Intermediate Representation (AIR) constraint 
/// for Zero-Knowledge STARK polynomial evaluation.
/// Ensures the `wasm32-wasi` payload executes with absolute mathematical determinism.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ExecutionTraceConstraint {
    /// Validates the Program Counter (PC) transitions according to the RISC-V ISA.
    /// E.g., $PC_{next} = PC_{current} + 4$ for sequential instructions.
    ValidPcTransition {
        current_pc: u64,
        next_pc: u64,
        instruction_opcode: u32,
    },
    /// Ensures that any memory access falls within the strict 32-bit linear bounds 
    /// of the WASM sandbox, preventing buffer overflows in the execution trace.
    LinearMemoryBounds {
        accessed_ptr: u64,
        max_allocated_pages: u32,
    },
    /// Enforces Deterministic Fuel Metering (The Halting Problem bypass).
    /// The trace must prove the fuel counter strictly monotonically decreased.
    MonotonicFuelConsumption {
        fuel_initial: u64,
        fuel_remaining: u64,
    },
    /// Asserts that a hypercall (e.g., `path_open`) perfectly matches the 
    /// deterministic response recorded in the `.mrb-dump` Flight Data Recorder.
    IsomorphicJournalMatch {
        hypercall_id: u32,
        expected_tape_hash: [u8; 32],
        actual_trace_hash: [u8; 32],
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StarkPolynomialCommitment {
    pub execution_constraints: Vec<ExecutionTraceConstraint>,
    pub data_constraints: Vec<DataConstraint>,
    pub final_state_root: [u8; 32],
}
