#!/usr/bin/env bash
# Marabunta - Licensed under the MIT License.
# Kill all nodes in a specific "region" (simulated via partition).
set -euo pipefail

API="${SWARM_API:-http://localhost:8080}"
REGION="${1:-us-east}"

echo "=== Killing all nodes in region: $REGION ==="

# Get all node IDs (in production, filter by region metadata).
NODES=$(curl -sS "$API/api/v1/chaos/status" | jq -r '.killed_nodes // [] | .[]')

# For demo purposes, kill 10 nodes and label it as a "region failure".
RESULT=$(curl -sS -X POST "$API/api/v1/chaos/kill" \
  -H "Content-Type: application/json" \
  -d "{\"count\": 10}")

echo "$RESULT" | jq .
echo "Region $REGION simulated failure complete."
