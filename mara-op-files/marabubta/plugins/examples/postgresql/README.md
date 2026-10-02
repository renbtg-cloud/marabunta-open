<!-- Marabunta - Licensed under the MIT License.
# Marabunta PostgreSQL Plugin

A distributed PostgreSQL-compatible plugin for the Marabunta Swarm. Exposes a
pgwire-compatible endpoint that clients can connect to using any PostgreSQL
driver (psql, pgx, JDBC, etc.).

## Architecture

- **pgwire server**: Accepts PostgreSQL wire protocol connections
- **SQL parser**: Uses pg_query_go to parse SQL into ASTs
- **Query planner**: Routes queries to single shards, scatters across all, or handles joins
- **Executor**: Dispatches to local SQLite shards or scatter across the swarm
- **Storage**: SQLite-backed shard manager with connection pooling
- **Catalog**: Tracks schema, tables, and indexes; syncs via swarm pub/sub

## Key Format

- Data: `pg:{instance}:{table}:shard_{id}:data`
- Catalog: `pg:{instance}:catalog`

## Build

```bash
make build
```

## Run

```bash
./marabunta-postgres --swarm-addr /tmp/marabunta.sock --listen :5432 --instance default
```
