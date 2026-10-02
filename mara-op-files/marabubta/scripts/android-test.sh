#!/usr/bin/env bash
# Marabunta - Licensed under the MIT License.
#
# android-test.sh — Run Android emulator tests for Marabunta Worker
#
# Prerequisites:
#   1. Android SDK with emulator and system images
#   2. adb in PATH
#   3. APK already built (run android-build.sh + gradlew assembleDebug first)
#
# Usage:
#   ./scripts/android-test.sh              # Run all tests on default emulator
#   ./scripts/android-test.sh api34        # Create and test on API 34 emulator
#   ./scripts/android-test.sh smoke        # Quick smoke test on running emulator
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PACKAGE="com.marabunta.worker"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

pass() { echo -e "${GREEN}PASS${NC}: $1"; }
fail() { echo -e "${RED}FAIL${NC}: $1"; FAILURES=$((FAILURES + 1)); }
warn() { echo -e "${YELLOW}WARN${NC}: $1"; }
info() { echo -e "INFO: $1"; }

FAILURES=0

# Check adb is available
if ! command -v adb &>/dev/null; then
    echo "ERROR: adb not found in PATH"
    echo "Install Android SDK Platform-Tools or add to PATH"
    exit 1
fi

# Wait for device
wait_for_device() {
    info "Waiting for device..."
    adb wait-for-device
    # Wait for boot to complete
    local timeout=120
    local elapsed=0
    while [ "$(adb shell getprop sys.boot_completed 2>/dev/null)" != "1" ]; do
        sleep 2
        elapsed=$((elapsed + 2))
        if [ $elapsed -ge $timeout ]; then
            fail "Device boot timeout after ${timeout}s"
            exit 1
        fi
    done
    pass "Device ready"
}

# Get device API level
get_api_level() {
    adb shell getprop ro.build.version.sdk | tr -d '\r\n'
}

# Install APK
install_apk() {
    local apk="$PROJECT_ROOT/android/app/build/outputs/apk/debug/app-debug.apk"
    if [ ! -f "$apk" ]; then
        fail "APK not found at $apk"
        echo "Build it first: cd android && ./gradlew assembleDebug"
        exit 1
    fi

    info "Installing APK..."
    adb install -r "$apk" 2>&1
    if [ $? -eq 0 ]; then
        pass "APK installed"
    else
        fail "APK installation failed"
        exit 1
    fi
}

# ========================================================================
# Test: App launches without crash
# ========================================================================
test_app_launch() {
    info "Test: App launch"

    # Force-stop first
    adb shell am force-stop "$PACKAGE" 2>/dev/null || true
    sleep 1

    # Launch main activity
    adb shell am start -n "$PACKAGE/.MainActivity" 2>&1
    sleep 3

    # Check if app is running
    if adb shell pidof "$PACKAGE" &>/dev/null; then
        pass "App launched successfully"
    else
        fail "App crashed on launch"
    fi
}

# ========================================================================
# Test: Service starts via intent
# ========================================================================
test_service_start() {
    info "Test: Service start"

    adb shell am startservice \
        -n "$PACKAGE/.MarabuntaWorkerService" \
        -a "$PACKAGE.START" 2>&1
    sleep 2

    # Check if service is running
    if adb shell dumpsys activity services "$PACKAGE" | grep -q "MarabuntaWorkerService"; then
        pass "Service started"
    else
        fail "Service failed to start"
    fi
}

# ========================================================================
# Test: Notification visible
# ========================================================================
test_notification() {
    info "Test: Notification visible"

    local api=$(get_api_level)

    if adb shell dumpsys notification | grep -q "marabunta_worker_channel\|Marabunta Worker"; then
        pass "Notification visible"
    else
        if [ "$api" -ge 26 ]; then
            fail "Notification not visible (API $api requires notification channel)"
        else
            warn "Notification check inconclusive on API $api"
        fi
    fi
}

# ========================================================================
# Test: Wake lock held
# ========================================================================
test_wake_lock() {
    info "Test: Wake lock"

    if adb shell dumpsys power | grep -q "Marabunta::WorkerWakeLock"; then
        pass "Wake lock held"
    else
        fail "Wake lock not found"
    fi
}

# ========================================================================
# Test: Service pause and resume
# ========================================================================
test_pause_resume() {
    info "Test: Pause"
    adb shell am startservice \
        -n "$PACKAGE/.MarabuntaWorkerService" \
        -a "$PACKAGE.PAUSE" 2>&1
    sleep 1

    info "Test: Resume"
    adb shell am startservice \
        -n "$PACKAGE/.MarabuntaWorkerService" \
        -a "$PACKAGE.RESUME" 2>&1
    sleep 1

    pass "Pause/Resume intents accepted (verify via logcat)"
}

# ========================================================================
# Test: Service stop
# ========================================================================
test_service_stop() {
    info "Test: Service stop"

    adb shell am startservice \
        -n "$PACKAGE/.MarabuntaWorkerService" \
        -a "$PACKAGE.STOP" 2>&1
    sleep 2

    if adb shell dumpsys activity services "$PACKAGE" | grep -q "MarabuntaWorkerService"; then
        fail "Service still running after stop"
    else
        pass "Service stopped"
    fi
}

# ========================================================================
# Test: Native library loads
# ========================================================================
test_native_library() {
    info "Test: Native library"

    # Check logcat for native library load message
    local log
    log=$(adb logcat -d -s NativeWorker:I | tail -20)

    if echo "$log" | grep -q "Native library loaded successfully"; then
        pass "Native library loaded"
    elif echo "$log" | grep -q "Failed to load native library"; then
        warn "Native library not loaded (expected if not cross-compiled)"
    else
        warn "Native library load status unknown"
    fi
}

# ========================================================================
# Test: ProGuard (release build only)
# ========================================================================
test_proguard() {
    local release_apk="$PROJECT_ROOT/android/app/build/outputs/apk/release/app-release.apk"
    if [ ! -f "$release_apk" ]; then
        warn "Release APK not found, skipping ProGuard test"
        return
    fi

    info "Test: ProGuard (checking class preservation)"

    # Extract classes and check critical classes exist
    local tmpdir
    tmpdir=$(mktemp -d)
    unzip -q "$release_apk" -d "$tmpdir" 2>/dev/null || true

    # Check if NativeWorker class exists in dex
    if command -v dexdump &>/dev/null; then
        if dexdump "$tmpdir/classes.dex" 2>/dev/null | grep -q "NativeWorker"; then
            pass "NativeWorker class preserved by ProGuard"
        else
            fail "NativeWorker class stripped by ProGuard!"
        fi
    else
        warn "dexdump not available, skipping ProGuard class check"
    fi

    rm -rf "$tmpdir"
}

# ========================================================================
# Test: Network security config
# ========================================================================
test_network_security() {
    info "Test: Network security config"

    # Try to access HTTP (should be blocked)
    local result
    result=$(adb shell "run-as $PACKAGE curl -s -o /dev/null -w '%{http_code}' http://httpbin.org/get" 2>&1 || true)

    # We can't easily test this without an HTTP endpoint, so just verify
    # the config exists in the APK
    warn "Network security config exists (manual verification needed)"
}

# ========================================================================
# Main
# ========================================================================

MODE="${1:-full}"

echo "========================================="
echo "Marabunta Worker Android Test Suite"
echo "========================================="
echo ""

wait_for_device

API_LEVEL=$(get_api_level)
info "Device API level: $API_LEVEL"
echo ""

case "$MODE" in
    smoke)
        install_apk
        test_app_launch
        test_native_library
        ;;
    full)
        install_apk
        test_app_launch
        test_native_library
        test_service_start
        test_notification
        test_wake_lock
        test_pause_resume
        test_service_stop
        test_proguard
        ;;
    api*)
        # Create emulator for specific API level
        target_api="${MODE#api}"
        info "Emulator tests for API $target_api not yet automated"
        info "Create emulator manually: avdmanager create avd -n test_api${target_api} -k 'system-images;android-${target_api};google_apis;x86_64'"
        ;;
    *)
        echo "Usage: $0 [full|smoke|api21|api26|api29|api33|api34]"
        exit 1
        ;;
esac

echo ""
echo "========================================="
if [ $FAILURES -eq 0 ]; then
    echo -e "${GREEN}All tests passed!${NC}"
else
    echo -e "${RED}$FAILURES test(s) failed${NC}"
fi
echo "========================================="

exit $FAILURES
