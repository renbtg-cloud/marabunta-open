#!/usr/bin/env bash
# Marabunta - Licensed under the MIT License.
# Scale the worker pool up or down.
set -euo pipefail

DESIRED="${1:-50}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TF_DIR="$SCRIPT_DIR/../terraform"

echo "=== Scaling worker pool to $DESIRED nodes ==="

cd "$TF_DIR"
terraform apply -var="scale_count=$DESIRED" -auto-approve

echo "=== Scale pool updated to $DESIRED ==="
