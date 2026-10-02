<!-- Marabunta - Licensed under the MIT License.
# Marabunta CICS Plugin

A CICS-compatible transaction processing plugin for the Marabunta Swarm.
Implements TN3270 terminal protocol, EXEC CICS command interpretation,
VSAM file access, temporary/transient data queues, and WASM-based program
execution via wazero.

## Architecture

- **TN3270 server**: TCP listener on port 3270 for 3270 terminal emulators
- **Transaction executor**: EXEC CICS command interpreter with EIB context
- **VSAM**: Key-Sequenced (KSDS), Entry-Sequenced (ESDS), Relative Record (RRDS), AIX
- **Queues**: Temporary Storage (TS) and Transient Data (TD) via swarm pub/sub
- **WASM runtime**: wazero-based execution with host function bindings

## Key Format

- VSAM KSDS: `cics:vsam:{dataset}:ksds:{key}`
- VSAM ESDS: `cics:vsam:{dataset}:esds:{rba:020d}`
- TS Queue: `cics:ts:{queue}`
- TD Queue: `cics:td:{queue}`

## Build

```bash
make build
```

## Run

```bash
./marabunta-cics --swarm-addr /tmp/marabunta.sock --listen :3270
```
