<!-- Marabunta - Licensed under the MIT License.
# Redis-Compatible Swarm Plugin

A full Redis-compatible TCP server that stores all data in the Marabunta Swarm's distributed storage. Any standard Redis client library can connect and issue familiar commands.

## Supported Commands

| Category    | Commands                                           |
|-------------|----------------------------------------------------|
| Strings     | GET, SET, SETNX, SETEX, MGET, MSET                |
| Numeric     | INCR, DECR, INCRBY, DECRBY                        |
| Keys        | DEL, EXISTS, KEYS, EXPIRE, TTL, PTTL              |
| Pub/Sub     | PUBLISH, SUBSCRIBE, UNSUBSCRIBE                   |
| Server      | PING, ECHO, INFO, COMMAND, QUIT                   |

## Design

### Wire Protocol

The plugin speaks the RESP (Redis Serialization Protocol) to Redis clients and the Marabunta plugin wire protocol (4-byte BE length + JSON) to the swarm host.

### Key Prefix

All keys are stored in the swarm with the prefix `redis:` to avoid collisions with other plugins.

### Spec Compliance

| Spec   | Feature                       | Implementation                                          |
|--------|-------------------------------|---------------------------------------------------------|
| R.3.1  | Pipeline support              | Batch commands parsed together, responses buffered      |
| R.3.2  | Pub/Sub via swarm Subscribe   | Push-based events from swarm SubscribeEvt, no polling   |
| R.3.3  | MGET/MSET batching            | Single logical round-trip via grouped storage calls     |
| R.3.4  | TTL                           | Lazy deletion on GET + background cleanup heartbeat     |
| R.3.5  | Atomic INCR/SETNX             | Optimistic locking via version CAS (compare-and-swap)   |

## Usage

```bash
# Connect to swarm via Unix socket
python redis_plugin.py --swarm-socket /tmp/marabunta/plugins/redis.sock

# Connect to swarm via TCP
python redis_plugin.py --swarm-host 127.0.0.1 --swarm-port 4201

# Custom Redis listen port
python redis_plugin.py --port 6380

# Then connect with any Redis client:
redis-cli -p 6379
> SET hello world
OK
> GET hello
"world"
```

## Testing

```bash
# Run unit tests
python -m pytest test_redis_plugin.py -v

# Or with unittest directly
python test_redis_plugin.py
```

## Dependencies

- Python 3.8+
- No external packages required (pure stdlib + shared swarm_client).
- Optional: `redis` Python package for integration testing with a real Redis client.

## Architecture

```
Redis Client (redis-cli, Jedis, etc.)
    |
    | RESP protocol (TCP :6379)
    v
+-------------------+
|   RedisServer     |  asyncio TCP server
|   RESPParser      |  incremental RESP parser
|   RedisHandler    |  command dispatch
+-------------------+
    |
    | PluginWireMessage (4-byte BE + JSON)
    v
+-------------------+
| SwarmConnection   |  shared Python client library
+-------------------+
    |
    | Unix socket or TCP
    v
+-------------------+
| Marabunta Swarm    |  distributed storage, pub/sub, discovery
+-------------------+
```
