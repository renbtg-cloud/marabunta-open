<!-- Marabunta - Licensed under the MIT License.
# Marabunta Kafka Plugin

A Kafka-compatible message broker plugin for the Marabunta Swarm. Implements the
Kafka binary protocol so standard Kafka clients can connect directly.

## Architecture

- **Kafka binary protocol**: Accepts Produce, Fetch, Metadata, and other Kafka requests
- **Broker**: Core topic/partition management with hash-based partition routing
- **Storage**: Append-only log backed by swarm distributed storage with batch writes
- **Consumer groups**: Coordinated via swarm pub/sub for rebalancing
- **Retention**: Hourly enforcement of time-based and size-based retention policies

## Key Format

- Records: `kafka:{topic}:{partition}:{offset:020d}`
- Metadata: `kafka:{topic}:meta`
- Offsets: `kafka:{topic}:{group}:offsets`

## Build

```bash
make build
```

## Run

```bash
./marabunta-kafka --swarm-addr /tmp/marabunta.sock --listen :9092
```
