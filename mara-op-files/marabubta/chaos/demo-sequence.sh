#!/usr/bin/env bash
# Marabunta - Licensed under the MIT License.
# Full demo chaos sequence for Mauricio's presentation.
#
# Stages:
# 1. Kill 3 random nodes — show self-healing
# 2. Wait, show recovery
# 3. Network partition — show isolation detection
# 4. Wait, show partition effects on the cube
# 5. Cascade failure — dramatic waterfall
# 6. Rejoin — everything heals
set -euo pipefail

API="${SWARM_API:-http://localhost:8080}"
PAUSE="${1:-8}"

echo "=================================================="
echo "  Marabunta Swarm — Chaos Demo Sequence"
echo "=================================================="
echo ""

# Stage 1: Kill nodes.
echo ">>> Stage 1: Killing 3 random nodes..."
curl -sS -X POST "$API/api/v1/chaos/kill" \
  -H "Content-Type: application/json" \
  -d '{"count": 3}' | jq .
echo ""
echo "    [Watch the dashboard: nodes turn red, chunks re-replicate]"
echo "    Waiting ${PAUSE}s..."
sleep "$PAUSE"

# Stage 2: Show status.
echo ""
echo ">>> Stage 2: Current chaos status:"
curl -sS "$API/api/v1/chaos/status" | jq '{killed_count, partitions: (.partitions | length), cascade_active}'
echo ""
sleep 2

# Stage 3: Network partition.
echo ">>> Stage 3: Creating network partition..."
curl -sS -X POST "$API/api/v1/chaos/partition" \
  -H "Content-Type: application/json" \
  -d '{"partition_id": "demo-split", "node_ids": []}' | jq .
echo ""
echo "    [Watch: isolated nodes lose gossip, health degrades]"
echo "    Waiting ${PAUSE}s..."
sleep "$PAUSE"

# Stage 4: Cascade failure.
echo ""
echo ">>> Stage 4: Cascade failure — 5 nodes, 2s apart..."
curl -sS -X POST "$API/api/v1/chaos/cascade" \
  -H "Content-Type: application/json" \
  -d '{"count": 5, "delay_ms": 2000}' | jq .
echo ""
echo "    [Watch: nodes die one by one, re-replication arcs appear]"
echo "    Waiting $((PAUSE * 2))s for cascade to complete..."
sleep "$((PAUSE * 2))"

# Stage 5: Status check.
echo ""
echo ">>> Stage 5: Post-chaos status:"
curl -sS "$API/api/v1/chaos/status" | jq '{killed_count, partitions: (.partitions | length), cascade_active}'
echo ""
sleep 2

# Stage 6: Rejoin.
echo ">>> Stage 6: Healing — rejoining all nodes..."
curl -sS -X POST "$API/api/v1/chaos/rejoin" | jq .
echo ""
echo "    [Watch: all nodes revive, green indicators return]"
echo ""
echo "=================================================="
echo "  Demo sequence complete!"
echo "=================================================="
