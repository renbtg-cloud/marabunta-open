#!/bin/bash
# Marabunta - Licensed under the MIT License.
# Marabunta Compute Systemd Service Installer
# Run as root: sudo ./install-services.sh

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Check root
if [[ $EUID -ne 0 ]]; then
   log_error "This script must be run as root"
   exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log_info "Installing Marabunta Compute systemd services..."

# Create marabunta user if it doesn't exist
if ! id "marabunta" &>/dev/null; then
    log_info "Creating marabunta user..."
    useradd -r -s /bin/false -d /var/lib/marabunta -c "Marabunta Compute" marabunta
fi

# Create directories
log_info "Creating directories..."
mkdir -p /var/lib/marabunta /var/log/marabunta /etc/marabunta
chown -R marabunta:marabunta /var/lib/marabunta /var/log/marabunta
chmod 750 /var/lib/marabunta /var/log/marabunta
chmod 755 /etc/marabunta

# Copy service files
log_info "Installing service files..."
cp "${SCRIPT_DIR}/marabunta-coordinator.service" /etc/systemd/system/
cp "${SCRIPT_DIR}/marabunta-master.service" /etc/systemd/system/
cp "${SCRIPT_DIR}/marabunta-worker.service" /etc/systemd/system/
cp "${SCRIPT_DIR}/marabunta-worker@.service" /etc/systemd/system/

# Copy environment files (don't overwrite existing)
for env_file in coordinator master worker; do
    if [[ ! -f "/etc/marabunta/${env_file}.env" ]]; then
        log_info "Creating /etc/marabunta/${env_file}.env..."
        cp "${SCRIPT_DIR}/${env_file}.env.example" "/etc/marabunta/${env_file}.env"
    else
        log_warn "/etc/marabunta/${env_file}.env exists, skipping"
    fi
done

# Reload systemd
log_info "Reloading systemd daemon..."
systemctl daemon-reload

# Print instructions
echo ""
log_info "Installation complete!"
echo ""
echo "Next steps:"
echo "  1. Edit configuration files in /etc/marabunta/"
echo "  2. Start services:"
echo "     sudo systemctl start marabunta-coordinator"
echo "     sudo systemctl start marabunta-master"
echo "     sudo systemctl start marabunta-worker"
echo ""
echo "  3. Enable on boot:"
echo "     sudo systemctl enable marabunta-coordinator marabunta-master marabunta-worker"
echo ""
echo "  4. Check status:"
echo "     sudo systemctl status marabunta-coordinator"
echo ""
