// Marabunta - Licensed under the MIT License.
use std::net::IpAddr;

use crate::highestsec::jurisdiction_proof::GeoResult;
use crate::highestsec::types::CountryCode;

/// Trait for IP geolocation providers. Implementations query external
/// services (MaxMind, IP-API, etc.) and return a `GeoResult`.
pub trait GeoProvider: Send + Sync {
    fn name(&self) -> &str;
    fn lookup(&self, ip: IpAddr) -> Result<GeoResult, GeoError>;
}

/// Errors that a geolocation provider can produce.
#[derive(Debug)]
pub enum GeoError {
    ProviderUnavailable(String),
    RateLimited,
    InvalidIp,
}

impl std::fmt::Display for GeoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProviderUnavailable(msg) => write!(f, "provider unavailable: {}", msg),
            Self::RateLimited => write!(f, "rate limited"),
            Self::InvalidIp => write!(f, "invalid IP address"),
        }
    }
}

/// Multi-provider geolocation verifier. Queries all registered providers
/// and determines whether the claimed country matches by consensus.
pub struct GeolocationVerifier {
    providers: Vec<Box<dyn GeoProvider>>,
    consensus_threshold: f64,
}

impl GeolocationVerifier {
    /// Create a new verifier with the given providers and a consensus
    /// threshold in `[0.0, 1.0]`. E.g. 0.5 means >50% of providers
    /// must agree for a `Confirmed` verdict.
    pub fn new(providers: Vec<Box<dyn GeoProvider>>, threshold: f64) -> Self {
        Self {
            providers,
            consensus_threshold: threshold,
        }
    }

    /// Verify an IP address against a claimed country.
    ///
    /// 1. Query every provider for the IP.
    /// 2. Count how many successful results agree with `claimed_country`.
    /// 3. Produce a `GeoVerification` with the appropriate verdict.
    pub fn verify(&self, ip: IpAddr, claimed_country: &CountryCode) -> GeoVerification {
        let mut results: Vec<GeoResult> = Vec::new();

        for provider in &self.providers {
            if let Ok(result) = provider.lookup(ip) {
                results.push(result);
            }
        }

        if results.is_empty() {
            return GeoVerification {
                claimed: *claimed_country,
                results,
                agreement_ratio: 0.0,
                verdict: GeoVerdict::InsufficientProviders,
            };
        }

        let agree_count = results
            .iter()
            .filter(|r| r.country == *claimed_country)
            .count();
        let agreement_ratio = agree_count as f64 / results.len() as f64;

        let verdict = if agreement_ratio >= self.consensus_threshold {
            GeoVerdict::Confirmed
        } else if agree_count > 0 {
            GeoVerdict::Suspicious
        } else {
            GeoVerdict::Denied
        };

        GeoVerification {
            claimed: *claimed_country,
            results,
            agreement_ratio,
            verdict,
        }
    }

    /// Returns the number of registered providers.
    pub fn provider_count(&self) -> usize {
        self.providers.len()
    }
}

/// Result of a multi-provider geolocation verification.
pub struct GeoVerification {
    pub claimed: CountryCode,
    pub results: Vec<GeoResult>,
    pub agreement_ratio: f64,
    pub verdict: GeoVerdict,
}

/// Verdict from the geolocation consensus check.
#[derive(Debug, Clone, PartialEq)]
pub enum GeoVerdict {
    /// All (or enough) providers confirm the claimed country.
    Confirmed,
    /// Some providers agree, but not enough for full consensus.
    Suspicious,
    /// No provider agrees with the claimed country.
    Denied,
    /// Not enough providers responded to form a verdict.
    InsufficientProviders,
}

impl std::fmt::Display for GeoVerdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Confirmed => write!(f, "Confirmed"),
            Self::Suspicious => write!(f, "Suspicious"),
            Self::Denied => write!(f, "Denied"),
            Self::InsufficientProviders => write!(f, "InsufficientProviders"),
        }
    }
}

/// A reference point used by the latency triangulator — a known server
/// at a known geographic location.
pub struct ReferencePoint {
    pub name: String,
    pub ip: IpAddr,
    pub location: (f64, f64), // (latitude, longitude)
}

/// Latency-based triangulation verifier. Uses known reference points
/// and round-trip-time measurements to check whether claimed location
/// is physically plausible.
pub struct LatencyTriangulator {
    pub reference_points: Vec<ReferencePoint>,
}

/// Verdict from latency triangulation.
#[derive(Debug, PartialEq)]
pub enum LatencyVerdict {
    /// All measured RTTs are consistent with the claimed location.
    Consistent,
    /// At least one RTT is too low for the physical distance.
    Inconsistent {
        expected_max_ms: f64,
        measured_ms: f64,
    },
    /// Not enough matching reference points to make a determination.
    InsufficientData,
}

impl LatencyTriangulator {
    pub fn new(reference_points: Vec<ReferencePoint>) -> Self {
        Self { reference_points }
    }

    /// Check measured round-trip times against the claimed location.
    ///
    /// For each reference point that has a matching RTT measurement:
    /// 1. Compute the great-circle distance to the claimed location.
    /// 2. Compute max plausible one-way time: `distance_km / 200.0` ms
    ///    (speed of light in fiber is roughly 200 km/ms).
    /// 3. Max RTT = 2 * one-way time.
    /// 4. Apply a 1.5x margin for routing overhead.
    /// 5. If the measured RTT is *less* than `max_rtt / 1.5` (i.e.
    ///    impossibly fast for the claimed distance), mark as inconsistent.
    ///    This detects VPN/proxy: the *real* location is closer than claimed.
    ///
    /// Actually, the more useful check is: if measured RTT far *exceeds*
    /// what the distance predicts, the node might be further away than
    /// claimed. We check: `measured > max_rtt * 1.5`.
    pub fn verify(
        &self,
        claimed_location: (f64, f64),
        rtts: &[(String, std::time::Duration)],
    ) -> LatencyVerdict {
        let mut matched = 0u32;

        for rp in &self.reference_points {
            // Find the RTT measurement for this reference point by name.
            let Some((_name, duration)) = rtts.iter().find(|(name, _)| name == &rp.name) else {
                continue;
            };

            matched += 1;
            let measured_ms = duration.as_secs_f64() * 1000.0;

            let distance_km = haversine_km(claimed_location, rp.location);

            // One-way propagation time at ~200 km/ms in fiber.
            let one_way_ms = distance_km / 200.0;
            // Round-trip.
            let expected_max_rtt_ms = 2.0 * one_way_ms;
            // Apply 1.5x margin to account for routing inefficiency.
            let threshold_ms = expected_max_rtt_ms * 1.5;

            // If the measured RTT vastly exceeds what physics allows for
            // the claimed distance, the node is likely further away.
            if measured_ms > threshold_ms && threshold_ms > 0.0 {
                return LatencyVerdict::Inconsistent {
                    expected_max_ms: threshold_ms,
                    measured_ms,
                };
            }
        }

        if matched == 0 {
            LatencyVerdict::InsufficientData
        } else {
            LatencyVerdict::Consistent
        }
    }
}

/// Haversine formula: great-circle distance in kilometers between two
/// (latitude, longitude) points given in degrees.
fn haversine_km(a: (f64, f64), b: (f64, f64)) -> f64 {
    let r = 6371.0; // Earth radius in km
    let d_lat = (b.0 - a.0).to_radians();
    let d_lon = (b.1 - a.1).to_radians();
    let lat1 = a.0.to_radians();
    let lat2 = b.0.to_radians();

    let h = (d_lat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (d_lon / 2.0).sin().powi(2);
    2.0 * r * h.sqrt().asin()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    // ---- Mock GeoProvider ----

    struct MockGeoProvider {
        name: String,
        country: CountryCode,
    }

    impl MockGeoProvider {
        fn new(name: &str, country: CountryCode) -> Self {
            Self {
                name: name.into(),
                country,
            }
        }
    }

    impl GeoProvider for MockGeoProvider {
        fn name(&self) -> &str {
            &self.name
        }

        fn lookup(&self, _ip: IpAddr) -> Result<GeoResult, GeoError> {
            Ok(GeoResult {
                country: self.country,
                region: None,
                city: None,
                latitude: 48.0,
                longitude: 2.0,
                provider: self.name.clone(),
                confidence: 0.9,
                queried_at: "2026-01-15T12:00:00Z".into(),
            })
        }
    }

    /// A provider that always fails.
    struct FailingGeoProvider {
        name: String,
    }

    impl FailingGeoProvider {
        fn new(name: &str) -> Self {
            Self { name: name.into() }
        }
    }

    impl GeoProvider for FailingGeoProvider {
        fn name(&self) -> &str {
            &self.name
        }

        fn lookup(&self, _ip: IpAddr) -> Result<GeoResult, GeoError> {
            Err(GeoError::ProviderUnavailable("down".into()))
        }
    }

    fn test_ip() -> IpAddr {
        "93.184.216.34".parse().unwrap()
    }

    // ---- GeolocationVerifier tests ----

    #[test]
    fn test_geo_consensus_confirmed_both_agree() {
        let providers: Vec<Box<dyn GeoProvider>> = vec![
            Box::new(MockGeoProvider::new("provider-a", CountryCode::FR)),
            Box::new(MockGeoProvider::new("provider-b", CountryCode::FR)),
        ];
        let verifier = GeolocationVerifier::new(providers, 0.5);
        let result = verifier.verify(test_ip(), &CountryCode::FR);

        assert_eq!(result.verdict, GeoVerdict::Confirmed);
        assert_eq!(result.agreement_ratio, 1.0);
        assert_eq!(result.results.len(), 2);
        assert_eq!(result.claimed, CountryCode::FR);
    }

    #[test]
    fn test_geo_consensus_suspicious_half_agree() {
        // Threshold is 0.75, so 1/2 = 0.5 < 0.75 but agree_count > 0 => Suspicious.
        let providers: Vec<Box<dyn GeoProvider>> = vec![
            Box::new(MockGeoProvider::new("provider-a", CountryCode::FR)),
            Box::new(MockGeoProvider::new("provider-b", CountryCode::DE)),
        ];
        let verifier = GeolocationVerifier::new(providers, 0.75);
        let result = verifier.verify(test_ip(), &CountryCode::FR);

        assert_eq!(result.verdict, GeoVerdict::Suspicious);
        assert_eq!(result.agreement_ratio, 0.5);
    }

    #[test]
    fn test_geo_consensus_denied_none_agree() {
        let providers: Vec<Box<dyn GeoProvider>> = vec![
            Box::new(MockGeoProvider::new("provider-a", CountryCode::DE)),
            Box::new(MockGeoProvider::new("provider-b", CountryCode::DE)),
        ];
        let verifier = GeolocationVerifier::new(providers, 0.5);
        let result = verifier.verify(test_ip(), &CountryCode::FR);

        assert_eq!(result.verdict, GeoVerdict::Denied);
        assert_eq!(result.agreement_ratio, 0.0);
    }

    #[test]
    fn test_geo_insufficient_providers_all_fail() {
        let providers: Vec<Box<dyn GeoProvider>> = vec![
            Box::new(FailingGeoProvider::new("fail-a")),
            Box::new(FailingGeoProvider::new("fail-b")),
        ];
        let verifier = GeolocationVerifier::new(providers, 0.5);
        let result = verifier.verify(test_ip(), &CountryCode::FR);

        assert_eq!(result.verdict, GeoVerdict::InsufficientProviders);
        assert_eq!(result.results.len(), 0);
    }

    #[test]
    fn test_geo_insufficient_providers_no_providers() {
        let providers: Vec<Box<dyn GeoProvider>> = vec![];
        let verifier = GeolocationVerifier::new(providers, 0.5);
        let result = verifier.verify(test_ip(), &CountryCode::FR);

        assert_eq!(result.verdict, GeoVerdict::InsufficientProviders);
    }

    #[test]
    fn test_geo_consensus_confirmed_one_fails_one_agrees() {
        // One provider fails, one agrees. 1/1 successful = 100% agreement.
        let providers: Vec<Box<dyn GeoProvider>> = vec![
            Box::new(FailingGeoProvider::new("fail-a")),
            Box::new(MockGeoProvider::new("provider-b", CountryCode::FR)),
        ];
        let verifier = GeolocationVerifier::new(providers, 0.5);
        let result = verifier.verify(test_ip(), &CountryCode::FR);

        assert_eq!(result.verdict, GeoVerdict::Confirmed);
        assert_eq!(result.results.len(), 1);
    }

    #[test]
    fn test_geo_three_providers_two_agree() {
        // 2/3 agree at threshold 0.5 => Confirmed.
        let providers: Vec<Box<dyn GeoProvider>> = vec![
            Box::new(MockGeoProvider::new("a", CountryCode::FR)),
            Box::new(MockGeoProvider::new("b", CountryCode::FR)),
            Box::new(MockGeoProvider::new("c", CountryCode::DE)),
        ];
        let verifier = GeolocationVerifier::new(providers, 0.5);
        let result = verifier.verify(test_ip(), &CountryCode::FR);

        assert_eq!(result.verdict, GeoVerdict::Confirmed);
        assert!((result.agreement_ratio - 2.0 / 3.0).abs() < 0.001);
    }

    #[test]
    fn test_geo_provider_count() {
        let providers: Vec<Box<dyn GeoProvider>> = vec![
            Box::new(MockGeoProvider::new("a", CountryCode::FR)),
            Box::new(MockGeoProvider::new("b", CountryCode::DE)),
        ];
        let verifier = GeolocationVerifier::new(providers, 0.5);
        assert_eq!(verifier.provider_count(), 2);
    }

    #[test]
    fn test_geo_verdict_display() {
        assert_eq!(GeoVerdict::Confirmed.to_string(), "Confirmed");
        assert_eq!(GeoVerdict::Suspicious.to_string(), "Suspicious");
        assert_eq!(GeoVerdict::Denied.to_string(), "Denied");
        assert_eq!(
            GeoVerdict::InsufficientProviders.to_string(),
            "InsufficientProviders"
        );
    }

    #[test]
    fn test_geo_error_display() {
        let e1 = GeoError::ProviderUnavailable("timeout".into());
        assert!(e1.to_string().contains("timeout"));

        let e2 = GeoError::RateLimited;
        assert_eq!(e2.to_string(), "rate limited");

        let e3 = GeoError::InvalidIp;
        assert_eq!(e3.to_string(), "invalid IP address");
    }

    // ---- Haversine tests ----

    #[test]
    fn test_haversine_paris_to_berlin() {
        // Paris (48.8566, 2.3522) to Berlin (52.52, 13.405) ~ 878 km
        let dist = haversine_km((48.8566, 2.3522), (52.52, 13.405));
        assert!(dist > 850.0 && dist < 900.0, "distance was {}", dist);
    }

    #[test]
    fn test_haversine_same_point() {
        let dist = haversine_km((48.8566, 2.3522), (48.8566, 2.3522));
        assert!(dist < 0.001, "distance should be ~0, was {}", dist);
    }

    #[test]
    fn test_haversine_antipodal() {
        // Opposite sides of the earth: ~20,000 km.
        let dist = haversine_km((0.0, 0.0), (0.0, 180.0));
        assert!(
            dist > 20_000.0 && dist < 20_100.0,
            "distance was {}",
            dist
        );
    }

    // ---- LatencyTriangulator tests ----

    #[test]
    fn test_latency_consistent() {
        // Claimed: Paris. Reference: Berlin (~878 km).
        // Expected max RTT at 200 km/ms: 2 * 878/200 = 8.78 ms.
        // With 1.5x margin: ~13.17 ms. Measured: 10 ms => consistent.
        let triangulator = LatencyTriangulator::new(vec![ReferencePoint {
            name: "berlin".into(),
            ip: "1.2.3.4".parse().unwrap(),
            location: (52.52, 13.405),
        }]);

        let rtts = vec![("berlin".into(), Duration::from_millis(10))];
        let verdict = triangulator.verify((48.8566, 2.3522), &rtts);
        assert_eq!(verdict, LatencyVerdict::Consistent);
    }

    #[test]
    fn test_latency_inconsistent() {
        // Claimed: Paris. Reference: Berlin (~878 km).
        // Expected max RTT with 1.5x margin: ~13.17 ms.
        // Measured: 100 ms => way too high, node is further away than claimed.
        let triangulator = LatencyTriangulator::new(vec![ReferencePoint {
            name: "berlin".into(),
            ip: "1.2.3.4".parse().unwrap(),
            location: (52.52, 13.405),
        }]);

        let rtts = vec![("berlin".into(), Duration::from_millis(100))];
        let verdict = triangulator.verify((48.8566, 2.3522), &rtts);
        match verdict {
            LatencyVerdict::Inconsistent {
                expected_max_ms,
                measured_ms,
            } => {
                assert!(expected_max_ms > 10.0 && expected_max_ms < 20.0);
                assert!((measured_ms - 100.0).abs() < 0.1);
            }
            other => panic!("expected Inconsistent, got {:?}", other),
        }
    }

    #[test]
    fn test_latency_insufficient_data_no_matching_ref() {
        let triangulator = LatencyTriangulator::new(vec![ReferencePoint {
            name: "berlin".into(),
            ip: "1.2.3.4".parse().unwrap(),
            location: (52.52, 13.405),
        }]);

        // RTT is for a different reference point name.
        let rtts = vec![("london".into(), Duration::from_millis(5))];
        let verdict = triangulator.verify((48.8566, 2.3522), &rtts);
        assert_eq!(verdict, LatencyVerdict::InsufficientData);
    }

    #[test]
    fn test_latency_insufficient_data_no_refs() {
        let triangulator = LatencyTriangulator::new(vec![]);
        let rtts = vec![("anything".into(), Duration::from_millis(5))];
        let verdict = triangulator.verify((48.8566, 2.3522), &rtts);
        assert_eq!(verdict, LatencyVerdict::InsufficientData);
    }

    #[test]
    fn test_latency_multiple_refs_one_inconsistent() {
        // Two reference points: berlin and london. Berlin is inconsistent.
        let triangulator = LatencyTriangulator::new(vec![
            ReferencePoint {
                name: "berlin".into(),
                ip: "1.2.3.4".parse().unwrap(),
                location: (52.52, 13.405),
            },
            ReferencePoint {
                name: "london".into(),
                ip: "5.6.7.8".parse().unwrap(),
                location: (51.5074, -0.1278),
            },
        ]);

        // Berlin: ~878 km from Paris => max ~13 ms with margin.
        // London: ~344 km from Paris => max ~5.16 ms with margin.
        // Berlin measured=5ms (ok), London measured=50ms (too high).
        let rtts = vec![
            ("berlin".into(), Duration::from_millis(5)),
            ("london".into(), Duration::from_millis(50)),
        ];
        let verdict = triangulator.verify((48.8566, 2.3522), &rtts);
        match verdict {
            LatencyVerdict::Inconsistent { .. } => {} // expected
            other => panic!("expected Inconsistent, got {:?}", other),
        }
    }

    #[test]
    fn test_latency_very_close_reference() {
        // Reference point is essentially at the same location as claimed.
        // Distance ~0, so threshold ~0. Any non-zero RTT is fine because
        // the threshold is computed as max_rtt * 1.5 which will be ~0,
        // but we need to ensure we don't false-positive for very small distances.
        // With distance ~0, threshold ~0, so even 1ms will be > threshold.
        // This is expected behavior: a reference at the same location should
        // detect any measurable latency. In practice, co-located references
        // would not be used.
        let triangulator = LatencyTriangulator::new(vec![ReferencePoint {
            name: "colocated".into(),
            ip: "10.0.0.1".parse().unwrap(),
            location: (48.8566, 2.3522),
        }]);

        let rtts = vec![("colocated".into(), Duration::from_millis(1))];
        let verdict = triangulator.verify((48.8566, 2.3522), &rtts);
        // Co-located reference has threshold=0 and the code guards with
        // `threshold > 0.0` to avoid false positives, so this returns Consistent.
        match verdict {
            LatencyVerdict::Consistent => {} // expected: threshold is 0 so guard prevents flag
            other => panic!("expected Consistent for co-located (guard), got {:?}", other),
        }
    }
}
