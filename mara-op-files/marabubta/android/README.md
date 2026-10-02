<!-- Marabunta - Licensed under the MIT License.
# Marabunta Worker Android App

A production-grade Android app that runs as a foreground service on old Android phones, contributing idle computing power to the Marabunta Compute network.

## Features

- **Foreground Service**: Runs continuously without being killed by Android's memory management
- **Battery-Aware**: Only computes when device is plugged in (configurable for battery mode)
- **Idle Detection**: Only computes when device is not in use (screen off or locked)
- **Auto-Start**: Automatically resumes after device reboot
- **Native Performance**: Uses Rust code via JNI for actual computation
- **Low Memory Footprint**: Designed for older devices with limited resources
- **Android 5.0+**: Supports API level 21 and above for maximum device coverage

## Project Structure

```
android/
├── app/
│   ├── src/
│   │   └── main/
│   │       ├── java/com/marabunta/worker/
│   │       │   ├── MarabuntaWorkerApp.java      # Application class
│   │       │   ├── MarabuntaWorkerService.java  # Foreground service
│   │       │   ├── MainActivity.java       # Main UI
│   │       │   ├── NativeWorker.java       # JNI bridge to Rust
│   │       │   ├── PowerMonitor.java       # Battery/charging state
│   │       │   ├── IdleDetector.java       # Screen/idle detection
│   │       │   ├── NotificationHelper.java # Notification management
│   │       │   ├── BootReceiver.java       # Auto-start on boot
│   │       │   └── PowerStateReceiver.java # Power state changes
│   │       ├── res/
│   │       │   ├── layout/activity_main.xml
│   │       │   ├── drawable/...
│   │       │   └── values/...
│   │       └── AndroidManifest.xml
│   ├── build.gradle
│   └── proguard-rules.pro
├── build.gradle
├── settings.gradle
├── gradle.properties
└── build_native.sh
```

## Prerequisites

### For building the APK:
- Java JDK 11 or higher
- Android SDK (API 33)
- Android NDK (for native libraries)
- Gradle 8.2+

### For building native libraries:
- Rust toolchain (rustup)
- Android cross-compilation targets
- cargo-ndk (recommended)

## Setup

### 1. Install Android targets for Rust

```bash
rustup target add aarch64-linux-android
rustup target add armv7-linux-androideabi
rustup target add i686-linux-android
rustup target add x86_64-linux-android
```

### 2. Install cargo-ndk (recommended)

```bash
cargo install cargo-ndk
```

### 3. Configure local.properties

Copy `local.properties.example` to `local.properties` and update paths:

```properties
sdk.dir=/path/to/Android/Sdk
ndk.dir=/path/to/Android/Sdk/ndk/25.2.9519653
```

## Building

### Build native libraries

```bash
# Release build
./build_native.sh release

# Debug build
./build_native.sh debug
```

### Build APK

```bash
# Debug APK
./gradlew assembleDebug

# Release APK (requires signing configuration)
./gradlew assembleRelease
```

The APK will be at `app/build/outputs/apk/`.

## Architecture

### Service Lifecycle

```
BootReceiver/PowerStateReceiver
         |
         v
   startForegroundService()
         |
         v
   MarabuntaWorkerService.onCreate()
   - Create notification channel
   - Start foreground with notification
   - Acquire wake lock
   - Initialize PowerMonitor, IdleDetector
   - Initialize NativeWorker (JNI)
   - Start worker thread
         |
         v
   Worker Loop:
   while (running) {
       if (canCompute()) {
           NativeWorker.executeNextTask()
           Update notification
       } else {
           Wait and check again
       }
   }
```

### Computation Conditions

The worker only computes when ALL conditions are met:
1. **Not paused** by user
2. **Power condition** met:
   - Plugged in (AC, USB, or Wireless), OR
   - Battery mode enabled AND battery > 90%
3. **Device idle**:
   - Screen off, OR
   - Screen on but device locked

### Native Worker (Rust)

The `NativeWorker` class bridges Java to Rust via JNI:

```java
// Java side
NativeWorker worker = new NativeWorker();
worker.initialize(coordinatorUrl, orgKey);
TaskResult result = worker.executeNextTask();
```

```rust
// Rust side (android_jni.rs)
#[no_mangle]
pub extern "system" fn Java_com_marabunta_worker_NativeWorker_executeNextTask(
    env: JNIEnv,
    _class: JClass,
) -> jobject {
    // Fetch task from coordinator
    // Execute in sandbox
    // Return result to Java
}
```

## Permissions

| Permission | Purpose |
|------------|---------|
| INTERNET | Connect to Marabunta coordinator |
| FOREGROUND_SERVICE | Run as foreground service |
| WAKE_LOCK | Keep CPU running while computing |
| RECEIVE_BOOT_COMPLETED | Auto-start after reboot |
| REQUEST_IGNORE_BATTERY_OPTIMIZATIONS | Request Doze exemption |
| POST_NOTIFICATIONS | Show status notification (Android 13+) |

## Battery Optimization

For reliable background operation, the app requests exemption from Android's battery optimization (Doze mode). This allows:
- Keeping CPU active during computation
- Maintaining coordinator connection
- Continuing work when screen is off

Users are prompted to grant this exemption on first launch.

## Testing

### Mock Mode

When the native library is not available, `NativeWorker` provides mock implementations for testing:

```java
NativeWorker worker = new NativeWorker();
worker.initializeMock("http://mock", "key");
TaskResult result = worker.executeNextTaskMock();
```

### ADB Testing

```bash
# Install APK
adb install -r app/build/outputs/apk/debug/app-debug.apk

# View logs
adb logcat -s MarabuntaWorkerService:V MarabuntaWorkerApp:V NativeWorker:V

# Simulate power events
adb shell dumpsys battery set ac 1  # Plug in
adb shell dumpsys battery set ac 0  # Unplug

# Check service status
adb shell dumpsys activity services com.marabunta.worker
```

## Troubleshooting

### Native library not loading

- Ensure libraries are in the correct `jniLibs/` directories
- Check that library name matches `System.loadLibrary()` call
- Verify ABI compatibility with target device

### Service being killed

- Check battery optimization exemption
- Ensure notification channel is configured
- Verify foreground service is started correctly

### Not computing

- Check power state (must be plugged in or battery mode enabled)
- Check idle state (screen must be off or locked)
- Check logs for errors

## License

MIT License - see project root for details.
