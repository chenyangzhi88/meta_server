# meta_server

`meta_server` is a Raft-backed metadata service for node registration and tablet routing.

## Configuration

The server now loads startup topology from `meta.toml` in the current directory by default.
You can point to another file with `META_CONFIG=/path/to/node.toml`.

Supported environment overrides:

- `META_CONFIG`
- `META_NODE_ID`
- `META_LISTEN_ADDR`
- `META_STORAGE_DIR`
- `META_LEADER_HINT`
- `META_VOTERS`
- `META_PEERS`
- `META_GROUP_ID`
- `META_RECONCILE_INTERVAL_MS`
- `META_HEARTBEAT_TIMEOUT_SECS`
- `META_HTTP_LISTEN_ADDR`

Example config:

```toml
[node]
id = 1
listen_addr = "127.0.0.1:7001"
advertise_addr = "127.0.0.1:7001"
storage_dir = "./meta_data_1"

[cluster]
group_id = "meta-cluster-1"
voters = [1, 2, 3]

[reconciler]
interval_ms = 1000
heartbeat_timeout_secs = 15

[observability]
http_listen_addr = "127.0.0.1:7101"

[[peers]]
id = 2
addr = "127.0.0.1:7002"

[[peers]]
id = 3
addr = "127.0.0.1:7003"
```

Three sample node configs are under `conf/`.

## Run

Start a three-node cluster with:

```bash
META_CONFIG=conf/node1.toml cargo run
META_CONFIG=conf/node2.toml cargo run
META_CONFIG=conf/node3.toml cargo run
```

Or use the helper script:

```bash
./scripts/start-three-node.sh
```

When `http_listen_addr` is configured, the built-in observability gateway is available at `/`
with JSON APIs under `/api/*`.

The HTML observability page includes:

- tablet search
- node state / route state / command status filters
- retry buttons for failed `LoadTablet` commands
- clear buttons for operator alerts

Manual HTTP actions:

- `POST /api/scheduler/commands/:command_id/retry`
- `POST /api/tablets/:tablet_id/alerts/clear`

## Leader Scheduling

Leader-only tablet migration is now exposed through gRPC:

- `BeginTabletMigration`
- `CompleteTabletMigration`

These RPCs call the built-in scheduler and persist the resulting route changes through Raft.

The background reconciler runs only on the current Raft leader. In the current minimal version,
if a cache node misses heartbeats longer than the configured timeout, the leader marks it `Down`
and fails over its tablets to another `Up` cache node.
