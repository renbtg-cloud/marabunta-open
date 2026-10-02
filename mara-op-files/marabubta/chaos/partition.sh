#!/usr/bin/env bash
# Marabunta - Licensed under the MIT License.
# Create a network partition isolating specified node IDs.
set -euo pipefail

API="${SWARM_API:-http://localhost:8080}"
PARTITION_ID="${1:-demo-partition}"
shift || true

# Remaining args are node IDs.
NODE_IDS="[]"
if [ $# -gt 0 ]; then
  NODE_IDS=$(printf '%s\n' "$@" | jq -R . | jq -s .)
fi

echo "=== Creating partition: $PARTITION_ID ==="
RESULT=$(curl -sS -X POST "$API/api/v1/chaos/partition" \
  -H "Content-Type: application/json" \
  -d "{\"partition_id\": \"$PARTITION_ID\", \"node_ids\": $NODE_IDS}")

echo "$RESULT" | jq .
