#!/usr/bin/env bash
# Marabunta - Licensed under the MIT License.
#
# android-build.sh — Cross-compile Marabunta Worker native library for Android
#
# Prerequisites:
#   1. Android NDK r25+ installed (via Android Studio or standalone)
#   2. Rust Android targets installed:
#        rustup target add aarch64-linux-android armv7-linux-androideabi \
#                          x86_64-linux-android i686-linux-android
#   3. cargo-ndk installed:
#        cargo install cargo-ndk
#
# Usage:
#   ./scripts/android-build.sh            # Build all ABIs (release)
#   ./scripts/android-build.sh debug      # Build all ABIs (debug)
#   ./scripts/android-build.sh arm64      # Build only arm64-v8a (release)
#
# Output:
#   android/app/src/main/jniLibs/<abi>/libmarabunta_worker.so
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
JNILIBS_DIR="$PROJECT_ROOT/android/app/src/main/jniLibs"

# Build profile
PROFILE="${1:-release}"
case "$PROFILE" in
    debug)   CARGO_FLAGS="" ; PROFILE_DIR="debug" ;;
    release) CARGO_FLAGS="--release" ; PROFILE_DIR="release" ;;
    arm64)   CARGO_FLAGS="--release" ; PROFILE_DIR="release" ; ABIS="arm64-v8a" ;;
    arm32)   CARGO_FLAGS="--release" ; PROFILE_DIR="release" ; ABIS="armeabi-v7a" ;;
    x86_64)  CARGO_FLAGS="--release" ; PROFILE_DIR="release" ; ABIS="x86_64" ;;
    x86)     CARGO_FLAGS="--release" ; PROFILE_DIR="release" ; ABIS="x86" ;;
    *)       echo "Usage: $0 [debug|release|arm64|arm32|x86_64|x86]" ; exit 1 ;;
esac

# Default: all ABIs
ABIS="${ABIS:-arm64-v8a armeabi-v7a x86_64 x86}"

# ABI → Rust target mapping
declare -A ABI_TO_TARGET=(
    ["arm64-v8a"]="aarch64-linux-android"
    ["armeabi-v7a"]="armv7-linux-androideabi"
    ["x86_64"]="x86_64-linux-android"
    ["x86"]="i686-linux-android"
)

# Verify ANDROID_NDK_HOME
if [ -z "${ANDROID_NDK_HOME:-}" ]; then
    # Try common locations
    for candidate in \
        "$HOME/Android/Sdk/ndk/"* \
        "$HOME/Library/Android/sdk/ndk/"* \
        "/opt/android-ndk/"* \
        "/usr/local/android-ndk/"*; do
        if [ -d "$candidate" ]; then
            ANDROID_NDK_HOME="$candidate"
            break
        fi
    done
fi

if [ -z "${ANDROID_NDK_HOME:-}" ]; then
    echo "ERROR: ANDROID_NDK_HOME not set and NDK not found in standard locations."
    echo ""
    echo "Install the NDK via Android Studio (SDK Manager → SDK Tools → NDK)"
    echo "or set ANDROID_NDK_HOME to the NDK root directory."
    exit 1
fi

echo "Using NDK: $ANDROID_NDK_HOME"
echo "Profile:   $PROFILE_DIR"
echo "ABIs:      $ABIS"
echo ""

# Verify cargo-ndk is installed
if ! command -v cargo-ndk &>/dev/null; then
    echo "ERROR: cargo-ndk not found. Install with: cargo install cargo-ndk"
    exit 1
fi

# Verify Rust targets are installed
for abi in $ABIS; do
    target="${ABI_TO_TARGET[$abi]}"
    if ! rustup target list --installed | grep -q "$target"; then
        echo "Installing Rust target: $target"
        rustup target add "$target"
    fi
done

# Build each ABI
cd "$PROJECT_ROOT"

for abi in $ABIS; do
    target="${ABI_TO_TARGET[$abi]}"
    echo "========================================="
    echo "Building for $abi ($target)..."
    echo "========================================="

    # Minimum Android API level
    # API 21 = Android 5.0 (our minSdk)
    cargo ndk \
        --target "$target" \
        --platform 21 \
        -- build \
        --lib \
        $CARGO_FLAGS

    # Copy the shared library to jniLibs
    mkdir -p "$JNILIBS_DIR/$abi"
    src="$PROJECT_ROOT/target/$target/$PROFILE_DIR/libmarabunta_worker.so"

    if [ ! -f "$src" ]; then
        # cargo-ndk may output to a different location
        src="$PROJECT_ROOT/target/$target/$PROFILE_DIR/libmarabunta_compute.so"
    fi

    if [ -f "$src" ]; then
        cp "$src" "$JNILIBS_DIR/$abi/libmarabunta_worker.so"
        echo "  → $JNILIBS_DIR/$abi/libmarabunta_worker.so"

        # Print size
        size=$(du -h "$JNILIBS_DIR/$abi/libmarabunta_worker.so" | cut -f1)
        echo "  → Size: $size"
    else
        echo "  WARNING: Build output not found at $src"
        echo "  Checking for alternative names..."
        ls -la "$PROJECT_ROOT/target/$target/$PROFILE_DIR/"lib*.so 2>/dev/null || true
    fi

    echo ""
done

# Strip debug symbols from release builds for smaller APK
if [ "$PROFILE_DIR" = "release" ]; then
    echo "Stripping debug symbols..."
    for abi in $ABIS; do
        target="${ABI_TO_TARGET[$abi]}"
        lib="$JNILIBS_DIR/$abi/libmarabunta_worker.so"
        if [ -f "$lib" ]; then
            # Find the NDK strip tool
            strip_tool=$(find "$ANDROID_NDK_HOME" -name "*-strip" -path "*/$target/*" | head -1)
            if [ -z "$strip_tool" ]; then
                # Try llvm-strip (newer NDKs)
                strip_tool=$(find "$ANDROID_NDK_HOME" -name "llvm-strip" | head -1)
            fi

            if [ -n "$strip_tool" ]; then
                before=$(du -h "$lib" | cut -f1)
                "$strip_tool" "$lib"
                after=$(du -h "$lib" | cut -f1)
                echo "  $abi: $before → $after"
            else
                echo "  WARNING: strip tool not found for $abi"
            fi
        fi
    done
    echo ""
fi

# Summary
echo "========================================="
echo "Build complete!"
echo "========================================="
echo ""
echo "Native libraries:"
for abi in $ABIS; do
    lib="$JNILIBS_DIR/$abi/libmarabunta_worker.so"
    if [ -f "$lib" ]; then
        size=$(du -h "$lib" | cut -f1)
        echo "  $abi: $size ($lib)"
    else
        echo "  $abi: NOT BUILT"
    fi
done
echo ""
echo "Next steps:"
echo "  1. cd android/"
echo "  2. ./gradlew assembleDebug   (or assembleRelease)"
echo "  3. adb install app/build/outputs/apk/debug/app-debug.apk"
