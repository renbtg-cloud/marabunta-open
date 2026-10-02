#!/bin/bash
# Marabunta - Licensed under the MIT License.
set -euo pipefail

# ============================================================================
# Marabunta Compute Swarm Node - macOS Service Uninstaller
#
# Removes the launchd service. Optionally removes data, config, and log files.
#
# Usage:
#   sudo ./uninstall-macos-service.sh              # system-wide
#   ./uninstall-macos-service.sh                    # user-level
#   ./uninstall-macos-service.sh --remove-data      # also delete data/config/logs
# ============================================================================

LABEL="com.marabuntacompute.swarm"
REMOVE_DATA=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --remove-data)
            REMOVE_DATA=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --remove-data    Also remove configuration, data, and log directories"
            echo "  --help           Show this help"
            echo ""
            echo "Run with sudo if the service was installed system-wide."
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

echo "============================================"
echo " Marabunta Compute Swarm Node - macOS Uninstaller"
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
else
    MODE="user"
    PLIST_DIR="${HOME}/Library/LaunchAgents"
    CONFIG_DIR="${HOME}/Library/Application Support/MarabuntaCompute/config"
    LOG_DIR="${HOME}/Library/Logs/MarabuntaCompute"
    DATA_DIR="${HOME}/Library/Application Support/MarabuntaCompute/data"
fi

PLIST_PATH="${PLIST_DIR}/${LABEL}.plist"

# -------------------------------------------------------------------------
# 1. Unload the service
# -------------------------------------------------------------------------
if [[ -f "${PLIST_PATH}" ]]; then
    echo "[1/3] Unloading service..."
    launchctl unload -w "${PLIST_PATH}" 2>/dev/null || true
    sleep 1
    echo "  Service unloaded."
else
    echo "[1/3] Plist not found at ${PLIST_PATH}, service may not be installed."
fi

# -------------------------------------------------------------------------
# 2. Remove the plist
# -------------------------------------------------------------------------
if [[ -f "${PLIST_PATH}" ]]; then
    rm -f "${PLIST_PATH}"
    echo "[2/3] Plist removed: ${PLIST_PATH}"
else
    echo "[2/3] Plist already removed (skipped)"
fi

# -------------------------------------------------------------------------
# 3. Optionally remove data directories
# -------------------------------------------------------------------------
if [[ "${REMOVE_DATA}" == "true" ]]; then
    for DIR in "${CONFIG_DIR}" "${LOG_DIR}" "${DATA_DIR}"; do
        if [[ -d "${DIR}" ]]; then
            rm -rf "${DIR}"
            echo "  Removed: ${DIR}"
        fi
    done
    echo "[3/3] Data directories removed"
else
    echo "[3/3] Data directories preserved"
    echo "  Config : ${CONFIG_DIR}"
    echo "  Logs   : ${LOG_DIR}"
    echo "  Data   : ${DATA_DIR}"
    echo "  Use --remove-data to delete these directories."
fi

echo ""
echo "============================================"
echo " Uninstallation Complete"
echo "============================================"
echo ""
