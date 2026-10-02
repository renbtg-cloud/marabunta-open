#!/usr/bin/env bash
# Marabunta - Licensed under the MIT License.
# Provision core infrastructure using Terraform.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TF_DIR="$SCRIPT_DIR/../terraform"

echo "=== Provisioning Marabunta Swarm Core Infrastructure ==="

cd "$TF_DIR"

terraform init -upgrade
terraform plan -out=plan.tfplan
terraform apply plan.tfplan

echo ""
echo "=== Core IPs ==="
terraform output -json core_public_ips | jq -r '.[]'
echo ""
echo "API Endpoint: $(terraform output -raw api_endpoint)"
echo "Dashboard URL: $(terraform output -raw dashboard_url)"
