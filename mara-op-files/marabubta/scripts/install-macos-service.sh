#!/bin/bash
# Marabunta - Licensed under the MIT License.
set -euo pipefail

# ============================================================================
# Marabunta Compute Swarm Node - macOS Service Installer
#
# Installs the swarm node as a launchd service.
#   - Root:     system-wide daemon in /Library/LaunchDaemons/
#   - Non-root: user-level agent in ~/Library/LaunchAgents/
#
# Usage:
#   sudo ./install-macos-service.sh          # system-wide
#   ./install-macos-service.sh               # user-level
#   ./install-macos-service.sh --binary /path/to/marabunta-swarm
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PLIST_SRC="${SCRIPT_DIR}/../packaging/macos/com.marabuntacompute.swarm.plist"
LABEL="com.marabuntacompute.swarm"

# Defaults
BINARY_PATH="/usr/local/bin/marabunta-swarm"
LISTEN_PORT=4200
API_PORT=4201
BOOTSTRAP_SERVERS=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --binary)
            BINARY_PATH="$2"
            shift 2
            ;;
        --listen-port)
            LISTEN_PORT="$2"
            shift 2
            ;;
        --api-port)
            API_PORT="$2"
            shift 2
            ;;
        --bootstrap)
            BOOTSTRAP_SERVERS="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --binary PATH        Path to marabunta-swarm binary (default: /usr/local/bin/marabunta-swarm)"
            echo "  --listen-port PORT   Gossip listen port (default: 4200)"
            echo "  --api-port PORT      HTTP API port (default: 4201)"
            echo "  --bootstrap ADDRS    Comma-separated bootstrap addresses"
            echo "  --help               Show this help"
            echo ""
            echo "Run with sudo for system-wide installation, without for user-level."
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

echo "============================================"
echo " Marabunta Compute Swarm Node - macOS Installer"
echo "============================================"
echo ""

# -------------------------------------------------------------------------
# Determine installation mode
# -------------------------------------------------------------------------
if [[ "$(id -u)" -eq 0 ]]; then
    MODE="system"
    PLIST_DIR="/Library/LaunchDaemons"
    CONFIG_DIR="/usr/local/etc/marabunta"
    LOG_DIR="/usr/local/var/log/marabunta"
    DATA_DIR="/var/lib/marabunta"
    echo "Mode: system-wide (LaunchDaemon)"
else
    MODE="user"
    PLIST_DIR="${HOME}/Library/LaunchAgents"
    CONFIG_DIR="${HOME}/Library/Application Support/MarabuntaCompute/config"
    LOG_DIR="${HOME}/Library/Logs/MarabuntaCompute"
    DATA_DIR="${HOME}/Library/Application Support/MarabuntaCompute/data"
    echo "Mode: user-level (LaunchAgent)"
fi

PLIST_DEST="${PLIST_DIR}/${LABEL}.plist"
CONFIG_FILE="${CONFIG_DIR}/swarm.toml"

echo "Plist:  ${PLIST_DEST}"
echo "Config: ${CONFIG_FILE}"
echo "Logs:   ${LOG_DIR}"
echo "Data:   ${DATA_DIR}"
echo ""

# -------------------------------------------------------------------------
# 1. Validate prerequisites
# -------------------------------------------------------------------------
if [[ ! -f "${PLIST_SRC}" ]]; then
    echo "ERROR: Plist template not found at ${PLIST_SRC}" >&2
    echo "  Make sure you run this script from the repository root." >&2
    exit 1
fi

if [[ ! -x "${BINARY_PATH}" ]]; then
    # Try to find it in the build output
    BUILD_BINARY="${SCRIPT_DIR}/../target/release/marabunta-swarm"
    if [[ -x "${BUILD_BINARY}" ]]; then
        echo "Binary not found at ${BINARY_PATH}, but found build output."
        echo "Copying ${BUILD_BINARY} -> ${BINARY_PATH}"
        if [[ "${MODE}" == "system" ]]; then
            cp "${BUILD_BINARY}" "${BINARY_PATH}"
            chmod 755 "${BINARY_PATH}"
        else
            echo "WARNING: Cannot copy to ${BINARY_PATH} without root." >&2
            echo "  Either run with sudo or specify --binary with a user-writable path." >&2
            exit 1
        fi
    else
        echo "WARNING: Binary not found at ${BINARY_PATH}" >&2
        echo "  Build with: cargo build --release --bin marabunta-swarm" >&2
        echo "  Then copy to ${BINARY_PATH} or pass --binary /path/to/binary" >&2
        exit 1
    fi
fi

echo "[1/5] Binary validated: ${BINARY_PATH}"

# -------------------------------------------------------------------------
# 2. Create directories
# -------------------------------------------------------------------------
mkdir -p "${CONFIG_DIR}" "${LOG_DIR}" "${DATA_DIR}" "${PLIST_DIR}"

if [[ "${MODE}" == "system" ]]; then
    # Ensure the marabunta user/group owns the data directories.
    # On macOS, we use _marabunta if it exists, otherwise fall back to current user.
    if dscl . -read /Users/_marabunta &>/dev/null; then
        chown -R _marabunta:staff "${DATA_DIR}" "${LOG_DIR}"
    fi
fi

echo "[2/5] Directories created"

# -------------------------------------------------------------------------
# 3. Generate default config if not present
# -------------------------------------------------------------------------
if [[ ! -f "${CONFIG_FILE}" ]]; then
    BOOTSTRAP_TOML="bootstrap_servers = []"
    if [[ -n "${BOOTSTRAP_SERVERS}" ]]; then
        # Convert comma-separated to TOML array
        IFS=',' read -ra SERVERS <<< "${BOOTSTRAP_SERVERS}"
        QUOTED=""
        for s in "${SERVERS[@]}"; do
            s="$(echo "$s" | xargs)"  # trim whitespace
            if [[ -n "${QUOTED}" ]]; then
                QUOTED="${QUOTED}, "
            fi
            QUOTED="${QUOTED}\"${s}\""
        done
        BOOTSTRAP_TOML="bootstrap_servers = [${QUOTED}]"
    fi

    cat > "${CONFIG_FILE}" <<TOML
# Marabunta Compute Swarm Node Configuration
# Generated by install-macos-service.sh on $(date "+%Y-%m-%d %H:%M:%S")
#
# See https://docs.marabunta-compute.io/config for full reference.

# Network
listen_addr = "0.0.0.0:${LISTEN_PORT}"
${BOOTSTRAP_TOML}

# Gossip protocol
gossip_interval = "1s"
gossip_fanout = 3

# Failure detection
suspect_threshold = "10s"
dead_threshold = "30s"

# Work execution
max_concurrent_chunks = 4
max_load = 0.8
chunk_timeout = "300s"

# Identity persistence (keeps NodeId stable across restarts)
identity_file = "${DATA_DIR}/identity.json"

# State persistence
state_dir = "${DATA_DIR}/state"

# Organic swarm
max_collectives = 3
bid_window = "5s"
min_capability_match = 0.5
TOML

    echo "[3/5] Default config written to: ${CONFIG_FILE}"
else
    echo "[3/5] Config already exists: ${CONFIG_FILE} (skipped)"
fi

# -------------------------------------------------------------------------
# 4. Install the plist
# -------------------------------------------------------------------------
# Unload existing service first (ignore errors if not loaded).
launchctl unload -w "${PLIST_DEST}" 2>/dev/null || true

# Generate the plist from template, adjusting paths for user-level installs.
if [[ "${MODE}" == "user" ]]; then
    # For user-level, rewrite paths in the plist template.
    sed \
        -e "s|/usr/local/bin/marabunta-swarm|${BINARY_PATH}|g" \
        -e "s|/usr/local/etc/marabunta/swarm.toml|${CONFIG_FILE}|g" \
        -e "s|/var/lib/marabunta|${DATA_DIR}|g" \
        -e "s|/usr/local/var/log/marabunta/swarm.log|${LOG_DIR}/swarm.log|g" \
        -e "s|/usr/local/var/log/marabunta/swarm.err|${LOG_DIR}/swarm.err|g" \
        "${PLIST_SRC}" > "${PLIST_DEST}"
else
    cp "${PLIST_SRC}" "${PLIST_DEST}"
fi

# Set correct ownership and permissions for the plist.
if [[ "${MODE}" == "system" ]]; then
    chown root:wheel "${PLIST_DEST}"
    chmod 644 "${PLIST_DEST}"
else
    chmod 644 "${PLIST_DEST}"
fi

echo "[4/5] Plist installed to: ${PLIST_DEST}"

# -------------------------------------------------------------------------
# 5. Load and start the service
# -------------------------------------------------------------------------
launchctl load -w "${PLIST_DEST}"

# Give it a moment to start.
sleep 2

if launchctl list | grep -q "${LABEL}"; then
    echo "[5/5] Service loaded and running"
else
    echo "[5/5] Service loaded (check logs if it did not start)"
fi

echo ""
echo "============================================"
echo " Installation Complete"
echo "============================================"
echo ""
echo "  Label       : ${LABEL}"
echo "  Binary      : ${BINARY_PATH}"
echo "  Config      : ${CONFIG_FILE}"
echo "  Data Dir    : ${DATA_DIR}"
echo "  Logs (out)  : ${LOG_DIR}/swarm.log"
echo "  Logs (err)  : ${LOG_DIR}/swarm.err"
echo ""
echo "Useful commands:"
echo "  View status : launchctl list | grep ${LABEL}"
echo "  Stop        : launchctl unload -w ${PLIST_DEST}"
echo "  Start       : launchctl load -w ${PLIST_DEST}"
echo "  View logs   : tail -f ${LOG_DIR}/swarm.log"
echo "  Uninstall   : $(dirname "$0")/uninstall-macos-service.sh"
echo ""
