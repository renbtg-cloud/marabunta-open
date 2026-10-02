#!/usr/bin/env bash
# Marabunta - Licensed under the MIT License.
# Build the swarm binary in release mode and deploy via Ansible.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ANSIBLE_DIR="$REPO_ROOT/deploy/ansible"

echo "=== Building swarm binary (release) ==="
cd "$REPO_ROOT"
cargo build --release --bin swarm

echo ""
echo "Binary size: $(du -h target/release/swarm | cut -f1)"

echo ""
echo "=== Deploying via Ansible ==="
cd "$ANSIBLE_DIR"
ansible-playbook -i inventory.yml playbook-deploy.yml

echo ""
echo "=== Deployment complete ==="
