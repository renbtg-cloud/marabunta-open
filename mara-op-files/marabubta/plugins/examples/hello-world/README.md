<!-- Marabunta - Licensed under the MIT License.
# Hello-World Plugin

A minimal Marabunta Swarm plugin that demonstrates registration, echo handling, and health checks.

## What It Does

1. Connects to the swarm host via Unix socket or TCP.
2. Registers as `hello-world` with the `CanExecute` trait.
3. Listens for incoming messages:
   - **HealthReq** -- responds with healthy status.
   - **HandleReq** -- echoes the payload back unchanged.
   - **StartReq** -- acknowledges startup.
   - **StopReq** -- performs a clean shutdown.

## Usage

```bash
# Via Unix socket
python hello_world.py --socket /tmp/marabunta/plugins/hello.sock

# Via TCP
python hello_world.py --host 127.0.0.1 --port 4201
```

## Dependencies

- Python 3.8+
- No external packages required (uses the shared `swarm_client` library).

## Wire Protocol

All messages use the standard Marabunta plugin wire protocol:
- 4-byte big-endian length prefix + JSON body.
- Envelope format: `{"type": "<Variant>", "payload": { ... }}`.
