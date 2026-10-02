// Marabunta - Licensed under the MIT License.
//! Host Environment Scanner for the Global Dependency Resolver.
//! 
//! Executes on daemon boot to catalog native proprietary software (Excel, Postgres, Python)
//! and populates the node's Kademlia CapabilityVector Bloom Filter.

use std::process::Command;
use tracing::{info, debug};
use crate::marabunta::gossip::CapabilityVector;

pub struct HostEnvironmentScanner;

impl HostEnvironmentScanner {
    /// Scans the local OS environment for registered enterprise tools.
    /// Hashes confirmed binaries and injects them into a CapabilityVector.
    pub fn generate_capability_vector() -> CapabilityVector {
        info!("MARABUNTA SCANNER: Profiling native host dependencies...");
        
        let mut vector = CapabilityVector::default();
        
        let target_binaries = vec![

            ("python3", "python3"),
            ("psql", "postgres"),
            ("excel.exe", "msexcel"),
            ("ffmpeg", "ffmpeg"),
            ("sqlite3", "sqlite"),
            ("mfcobol", "microfocus_cics"),
            ("zdt", "ibm_zdt"),
        ];

        let mut discovered = 0;

        for (binary, capability_name) in target_binaries {
            // Use standard `which` on Unix-like systems.
            // On Windows, this would logically fall back to `where` or Registry checks.
            let output = Command::new("which")
                .arg(binary)
                .output();

            if let Ok(result) = output {
                if result.status.success() {
                    debug!("MARABUNTA SCANNER: Discovered {} at execution layer.", binary);
                    Self::inject_into_bloom_filter(&mut vector, capability_name);
                    discovered += 1;
                }
            }
        }
        
        info!("MARABUNTA SCANNER: Profile complete. Injected {} native capabilities into Kademlia Bloom Filter.", discovered);
        vector
    }

    /// Simulates injecting a capability hash into the 256-bit Bloom Filter array.
    fn inject_into_bloom_filter(vector: &mut CapabilityVector, capability_name: &str) {
        let hash = blake3::hash(capability_name.as_bytes());
        let hash_bytes = hash.as_bytes();
        
        // Fast bitwise union to embed the capability
        for i in 0..32 {
            vector.software_bloom_filter[i] |= hash_bytes[i];
        }
    }
}
