#!/bin/bash
# Marabunta - Licensed under the MIT License.
#
# Build script for native Rust libraries
#
# Prerequisites:
#   - Rust toolchain with cross-compilation targets
#   - Android NDK installed
#   - cargo-ndk (cargo install cargo-ndk)
#
# Usage:
#   ./build_native.sh [debug|release]
#

set -e

# Configuration
BUILD_TYPE="${1:-release}"
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ANDROID_DIR="$PROJECT_ROOT/android"
JNI_LIBS_DIR="$ANDROID_DIR/app/src/main/jniLibs"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Building Marabunta Worker Native Libraries${NC}"
echo "Build type: $BUILD_TYPE"
echo "Project root: $PROJECT_ROOT"

# Check for required tools
check_tool() {
    if ! command -v "$1" &> /dev/null; then
        echo -e "${RED}Error: $1 is not installed${NC}"
        echo "Install with: $2"
        exit 1
    fi
}

check_tool rustup "https://rustup.rs"
check_tool cargo "https://rustup.rs"

# Check for cargo-ndk (optional but recommended)
if ! command -v cargo-ndk &> /dev/null; then
    echo -e "${YELLOW}Warning: cargo-ndk not installed${NC}"
    echo "Install with: cargo install cargo-ndk"
    echo "Falling back to manual cross-compilation..."
    USE_CARGO_NDK=false
else
    USE_CARGO_NDK=true
fi

# Add Android targets if not already added
echo "Adding Android targets..."
rustup target add aarch64-linux-android 2>/dev/null || true
rustup target add armv7-linux-androideabi 2>/dev/null || true
rustup target add i686-linux-android 2>/dev/null || true
rustup target add x86_64-linux-android 2>/dev/null || true

# Create jniLibs directories
mkdir -p "$JNI_LIBS_DIR/arm64-v8a"
mkdir -p "$JNI_LIBS_DIR/armeabi-v7a"
mkdir -p "$JNI_LIBS_DIR/x86"
mkdir -p "$JNI_LIBS_DIR/x86_64"

# Build function
build_target() {
    local target=$1
    local output_dir=$2
    local lib_name="libmarabunta_compute.so"

    echo -e "${GREEN}Building for $target...${NC}"

    if [ "$USE_CARGO_NDK" = true ]; then
        # Use cargo-ndk for easier cross-compilation
        cd "$PROJECT_ROOT"
        if [ "$BUILD_TYPE" = "release" ]; then
            cargo ndk -t "$target" build --release
        else
            cargo ndk -t "$target" build
        fi
    else
        # Manual cross-compilation (requires NDK setup in cargo config)
        cd "$PROJECT_ROOT"
        if [ "$BUILD_TYPE" = "release" ]; then
            cargo build --target "$target" --release
        else
            cargo build --target "$target"
        fi
    fi

    # Copy library to jniLibs
    local build_dir="$PROJECT_ROOT/target/$target"
    if [ "$BUILD_TYPE" = "release" ]; then
        build_dir="$build_dir/release"
    else
        build_dir="$build_dir/debug"
    fi

    if [ -f "$build_dir/$lib_name" ]; then
        cp "$build_dir/$lib_name" "$JNI_LIBS_DIR/$output_dir/libmarabunta_worker.so"
        echo -e "${GREEN}Copied $lib_name to $output_dir${NC}"
    else
        echo -e "${YELLOW}Warning: Library not found at $build_dir/$lib_name${NC}"
    fi
}

# Build for each architecture
echo ""
echo "Building for ARM64 (aarch64)..."
build_target "aarch64-linux-android" "arm64-v8a"

echo ""
echo "Building for ARM32 (armv7)..."
build_target "armv7-linux-androideabi" "armeabi-v7a"

echo ""
echo "Building for x86..."
build_target "i686-linux-android" "x86"

echo ""
echo "Building for x86_64..."
build_target "x86_64-linux-android" "x86_64"

echo ""
echo -e "${GREEN}Native library build complete!${NC}"
echo ""
echo "Libraries copied to:"
ls -la "$JNI_LIBS_DIR"/*/libmarabunta_worker.so 2>/dev/null || echo "No libraries found"

echo ""
echo "To build the APK, run:"
echo "  cd $ANDROID_DIR && ./gradlew assembleRelease"
