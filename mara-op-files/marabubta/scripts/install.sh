#!/bin/bash
# Marabunta - Licensed under the MIT License.
# Marabunta Compute Installation Script
#
# Usage:
#   curl -fsSL https://marabunta-compute.io/install.sh | bash
#   wget -qO- https://marabunta-compute.io/install.sh | bash
#
# Options:
#   --version VERSION    Install specific version (default: latest)
#   --prefix PATH        Installation prefix (default: /usr/local)
#   --no-systemd         Skip systemd service installation
#   --component NAME     Install specific component (coordinator|master|worker|cli|all)
#
# Environment variables:
#   MARABUNTA_VERSION         Version to install
#   MARABUNTA_PREFIX          Installation prefix
#   MARABUNTA_NO_SYSTEMD      Skip systemd installation

set -e

# Configuration
REPO="marabunta-compute/marabunta-compute"
BINARY_PREFIX="marabunta"
DEFAULT_VERSION="latest"
DEFAULT_PREFIX="/usr/local"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Logging functions
log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_step() { echo -e "${BLUE}==>${NC} $1"; }

# Detect OS and architecture
detect_platform() {
    OS=$(uname -s | tr '[:upper:]' '[:lower:]')
    ARCH=$(uname -m)

    case "$OS" in
        linux)
            OS="linux"
            ;;
        darwin)
            OS="darwin"
            ;;
        mingw*|msys*|cygwin*)
            OS="windows"
            ;;
        *)
            log_error "Unsupported operating system: $OS"
            exit 1
            ;;
    esac

    case "$ARCH" in
        x86_64|amd64)
            ARCH="x86_64"
            ;;
        aarch64|arm64)
            ARCH="aarch64"
            ;;
        armv7l)
            ARCH="armv7"
            ;;
        *)
            log_error "Unsupported architecture: $ARCH"
            exit 1
            ;;
    esac

    PLATFORM="${OS}-${ARCH}"
    log_info "Detected platform: $PLATFORM"
}

# Check for required tools
check_dependencies() {
    local missing=()

    for cmd in curl tar; do
        if ! command -v "$cmd" &> /dev/null; then
            missing+=("$cmd")
        fi
    done

    if [[ ${#missing[@]} -gt 0 ]]; then
        log_error "Missing required tools: ${missing[*]}"
        log_info "Please install them and try again"
        exit 1
    fi
}

# Get latest version from GitHub
get_latest_version() {
    local latest
    latest=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name"' | sed -E 's/.*"([^"]+)".*/\1/')
    echo "${latest#v}"  # Remove 'v' prefix if present
}

# Download and extract binary
download_binary() {
    local version="$1"
    local platform="$2"
    local prefix="$3"

    local download_url="https://github.com/${REPO}/releases/download/v${version}/marabunta-compute-${version}-${platform}.tar.gz"
    local tmp_dir=$(mktemp -d)

    log_step "Downloading Marabunta Compute v${version} for ${platform}..."

    if ! curl -fsSL "$download_url" -o "${tmp_dir}/marabunta-compute.tar.gz"; then
        log_error "Failed to download from: $download_url"
        rm -rf "$tmp_dir"
        exit 1
    fi

    log_step "Extracting binaries..."
    tar -xzf "${tmp_dir}/marabunta-compute.tar.gz" -C "$tmp_dir"

    log_step "Installing to ${prefix}/bin..."

    # Create bin directory if needed
    if [[ ! -d "${prefix}/bin" ]]; then
        sudo mkdir -p "${prefix}/bin"
    fi

    # Install binaries
    for binary in marabunta marabunta-coordinator marabunta-master marabunta-worker; do
        if [[ -f "${tmp_dir}/${binary}" ]]; then
            sudo install -m 755 "${tmp_dir}/${binary}" "${prefix}/bin/${binary}"
            log_info "Installed: ${prefix}/bin/${binary}"
        fi
    done

    # Cleanup
    rm -rf "$tmp_dir"
}

# Install systemd services
install_systemd() {
    if [[ "$OS" != "linux" ]]; then
        log_warn "Systemd services are only available on Linux"
        return
    fi

    if ! command -v systemctl &> /dev/null; then
        log_warn "systemctl not found, skipping systemd installation"
        return
    fi

    log_step "Installing systemd services..."

    # Create marabunta user
    if ! id "marabunta" &>/dev/null; then
        sudo useradd -r -s /bin/false -d /var/lib/marabunta -c "Marabunta Compute" marabunta 2>/dev/null || true
    fi

    # Create directories
    sudo mkdir -p /var/lib/marabunta /var/log/marabunta /etc/marabunta
    sudo chown -R marabunta:marabunta /var/lib/marabunta /var/log/marabunta

    # Download and install service files
    local service_url="https://raw.githubusercontent.com/${REPO}/main/packaging/systemd"

    for service in marabunta-coordinator marabunta-master marabunta-worker; do
        curl -fsSL "${service_url}/${service}.service" | sudo tee "/etc/systemd/system/${service}.service" > /dev/null
    done

    # Download template service
    curl -fsSL "${service_url}/marabunta-worker@.service" | sudo tee "/etc/systemd/system/marabunta-worker@.service" > /dev/null

    sudo systemctl daemon-reload

    log_info "Systemd services installed"
}

# Create configuration files
create_configs() {
    log_step "Creating default configuration..."

    sudo mkdir -p /etc/marabunta

    # Coordinator config
    if [[ ! -f /etc/marabunta/coordinator.toml ]]; then
        cat <<'EOF' | sudo tee /etc/marabunta/coordinator.toml > /dev/null
[server]
bind = "0.0.0.0:8080"

[cluster]
name = "marabunta-cluster"
heartbeat_interval = "10s"

[storage]
data_dir = "/var/lib/marabunta"

[logging]
level = "info"
EOF
    fi

    # Master config
    if [[ ! -f /etc/marabunta/master.toml ]]; then
        cat <<'EOF' | sudo tee /etc/marabunta/master.toml > /dev/null
[server]
bind = "0.0.0.0:9000"

[coordinator]
url = "http://localhost:8080"

[scheduler]
algorithm = "priority"

[storage]
data_dir = "/var/lib/marabunta"

[logging]
level = "info"
EOF
    fi

    # Worker config
    if [[ ! -f /etc/marabunta/worker.toml ]]; then
        cat <<'EOF' | sudo tee /etc/marabunta/worker.toml > /dev/null
[master]
url = "http://localhost:9000"

[coordinator]
url = "http://localhost:8080"

[worker]
cores = 0
heartbeat_interval = "10s"

[storage]
data_dir = "/var/lib/marabunta"

[logging]
level = "info"
EOF
    fi

    log_info "Configuration files created in /etc/marabunta/"
}

# Print completion message
print_success() {
    echo ""
    echo -e "${GREEN}======================================${NC}"
    echo -e "${GREEN} Marabunta Compute installed successfully! ${NC}"
    echo -e "${GREEN}======================================${NC}"
    echo ""
    echo "Binaries installed to: ${PREFIX}/bin/"
    echo ""
    echo "Quick start:"
    echo "  marabunta --help              # Show CLI help"
    echo "  marabunta status              # Check cluster status"
    echo ""

    if [[ "$OS" == "linux" ]] && [[ -z "$NO_SYSTEMD" ]]; then
        echo "Start services:"
        echo "  sudo systemctl start marabunta-coordinator"
        echo "  sudo systemctl start marabunta-master"
        echo "  sudo systemctl start marabunta-worker"
        echo ""
        echo "Enable on boot:"
        echo "  sudo systemctl enable marabunta-coordinator marabunta-master marabunta-worker"
        echo ""
    fi

    echo "Documentation: https://docs.marabunta-compute.io"
}

# Parse arguments
parse_args() {
    VERSION="${MARABUNTA_VERSION:-$DEFAULT_VERSION}"
    PREFIX="${MARABUNTA_PREFIX:-$DEFAULT_PREFIX}"
    NO_SYSTEMD="${MARABUNTA_NO_SYSTEMD:-}"
    COMPONENT="all"

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --version)
                VERSION="$2"
                shift 2
                ;;
            --prefix)
                PREFIX="$2"
                shift 2
                ;;
            --no-systemd)
                NO_SYSTEMD=1
                shift
                ;;
            --component)
                COMPONENT="$2"
                shift 2
                ;;
            --help|-h)
                echo "Marabunta Compute Installation Script"
                echo ""
                echo "Usage: $0 [OPTIONS]"
                echo ""
                echo "Options:"
                echo "  --version VERSION    Install specific version (default: latest)"
                echo "  --prefix PATH        Installation prefix (default: /usr/local)"
                echo "  --no-systemd         Skip systemd service installation"
                echo "  --component NAME     Install specific component"
                echo "  --help               Show this help message"
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                exit 1
                ;;
        esac
    done
}

# Main
main() {
    echo ""
    echo -e "${CYAN}Marabunta Compute Installer${NC}"
    echo ""

    parse_args "$@"
    check_dependencies
    detect_platform

    # Get version
    if [[ "$VERSION" == "latest" ]]; then
        VERSION=$(get_latest_version)
        if [[ -z "$VERSION" ]]; then
            log_error "Could not determine latest version"
            exit 1
        fi
    fi

    log_info "Installing Marabunta Compute v${VERSION}"

    # Download and install
    download_binary "$VERSION" "$PLATFORM" "$PREFIX"

    # Create configs
    create_configs

    # Install systemd services
    if [[ -z "$NO_SYSTEMD" ]]; then
        install_systemd
    fi

    print_success
}

main "$@"
