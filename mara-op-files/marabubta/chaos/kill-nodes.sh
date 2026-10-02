#!/usr/bin/env bash
# Marabunta - Licensed under the MIT License.
# Kill N random nodes via the chaos API.
set -euo pipefail

API="${SWARM_API:-http://localhost:8080}"
COUNT="${1:-3}"

echo "=== Killing $COUNT random nodes ==="
RESULT=$(curl -sS -X POST "$API/api/v1/chaos/kill" \
  -H "Content-Type: application/json" \
  -d "{\"count\": $COUNT}")

echo "$RESULT" | jq .

echo ""
echo "Killed: $(echo "$RESULT" | jq -r '.killed | length') nodes"
