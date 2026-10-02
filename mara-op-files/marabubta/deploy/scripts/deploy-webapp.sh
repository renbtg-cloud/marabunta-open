#!/usr/bin/env bash
# Marabunta - Licensed under the MIT License.
# Build the webapp and deploy to core-0.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WEBAPP_DIR="$REPO_ROOT/financial-demo-webapp"

echo "=== Building webapp ==="
cd "$WEBAPP_DIR"
npm ci
npm run build

echo ""
echo "=== Deploying webapp via Ansible ==="
cd "$REPO_ROOT/deploy/ansible"
ansible-playbook -i inventory.yml playbook-deploy.yml --tags webapp

echo ""
echo "=== Webapp deployed ==="
