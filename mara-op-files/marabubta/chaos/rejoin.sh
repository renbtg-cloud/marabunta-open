#!/usr/bin/env bash
# Marabunta - Licensed under the MIT License.
# Heal all partitions and revive all killed nodes.
set -euo pipefail

API="${SWARM_API:-http://localhost:8080}"

echo "=== Rejoining all nodes / healing all partitions ==="
RESULT=$(curl -sS -X POST "$API/api/v1/chaos/rejoin")

echo "$RESULT" | jq .

echo ""
echo "All chaos effects cleared."
