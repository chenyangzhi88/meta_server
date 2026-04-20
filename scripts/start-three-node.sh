#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
}

trap cleanup EXIT INT TERM

mkdir -p logs

echo "building meta_server"
cargo build

echo "starting meta node 1 with conf/node1.toml"
META_CONFIG=conf/node1.toml ./target/debug/meta_server >logs/node1.log 2>&1 &
PIDS+=("$!")

echo "starting meta node 2 with conf/node2.toml"
META_CONFIG=conf/node2.toml ./target/debug/meta_server >logs/node2.log 2>&1 &
PIDS+=("$!")

echo "starting meta node 3 with conf/node3.toml"
META_CONFIG=conf/node3.toml ./target/debug/meta_server >logs/node3.log 2>&1 &
PIDS+=("$!")

echo "cluster processes: ${PIDS[*]}"
echo "logs:"
echo "  logs/node1.log"
echo "  logs/node2.log"
echo "  logs/node3.log"
echo "press Ctrl-C to stop all nodes"

wait
