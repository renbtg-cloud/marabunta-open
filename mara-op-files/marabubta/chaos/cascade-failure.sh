#!/usr/bin/env bash
# Marabunta - Licensed under the MIT License.
# Start a cascade failure: kill N nodes one at a time with delay.
set -euo pipefail

API="${SWARM_API:-http://localhost:8080}"
COUNT="${1:-5}"
DELAY_MS="${2:-2000}"

echo "=== Starting cascade failure: $COUNT nodes, ${DELAY_MS}ms delay ==="
RESULT=$(curl -sS -X POST "$API/api/v1/chaos/cascade" \
  -H "Content-Type: application/json" \
  -d "{\"count\": $COUNT, \"delay_ms\": $DELAY_MS}")

echo "$RESULT" | jq .

echo ""
echo "Cascade started. Monitor with: $0/../rejoin.sh (or check status)"
