<!-- Marabunta - Licensed under the MIT License.
# Pillar 9.1: Mantis IDE Integration (Rust-Rover Marabunta Plugin)

## Architecture
Physical Kotlin/JNI bridge. Pulls WASI journals from the Swarm (via Pillar 6 APIs)
and provides deterministic replay of failed executions locally inside Rust-Rover.

## Features
- **Crash Snapshot Integration**: Instantly pull a 50MB WASI memory snapshot and step backwards in time.
- **Swarm Telemetry Overlay**: Visual markers on lines of code causing widespread node eviction.
- **Live JCL Bidding**: Submit Bids to the spot market directly from the IDE.