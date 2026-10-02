#!/usr/bin/env bash
# Marabunta - Licensed under the MIT License.
#
# build-dashboard.sh — Build the React dashboard and compile it into the marabunta-swarm binary
#
# Prerequisites:
#   1. Node.js 18+ and npm installed
#   2. Rust toolchain installed
#
# Usage:
#   ./scripts/build-dashboard.sh           # Build dashboard + release binary
#   ./scripts/build-dashboard.sh dashboard # Build only the dashboard (no cargo build)
#   ./scripts/build-dashboard.sh cargo     # Build only the Rust binary (assumes dist/ exists)
#
# Output:
#   dashboard/dist/          — Built static assets (embedded via rust-embed)
#   target/release/marabunta-swarm — Release binary with embedded dashboard
#

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DASHBOARD_DIR="$REPO_ROOT/dashboard"
MODE="${1:-all}"

build_dashboard() {
    echo "==> Installing dashboard dependencies..."
    cd "$DASHBOARD_DIR"
    npm ci --prefer-offline

    echo "==> Building dashboard (vite)..."
    npx vite build

    echo "==> Dashboard built:"
    ls -lh "$DASHBOARD_DIR/dist/index.html"
    ls -lh "$DASHBOARD_DIR/dist/assets/"
}

build_cargo() {
    echo "==> Building marabunta-swarm (release)..."
    cd "$REPO_ROOT"
    cargo build --release --bin marabunta-swarm

    echo "==> Binary ready:"
    ls -lh "$REPO_ROOT/target/release/marabunta-swarm"
}

case "$MODE" in
    dashboard)
        build_dashboard
        ;;
    cargo)
        build_cargo
        ;;
    all|"")
        build_dashboard
        echo ""
        build_cargo
        ;;
    *)
        echo "Usage: $0 [dashboard|cargo|all]"
        exit 1
        ;;
esac

echo ""
echo "Done."
