<!-- Marabunta - Licensed under the MIT License.
# Marabunta Worker — Google Play Store Foreground Service Justification

## Foreground Service Type: `specialUse`

### Subtype: `voluntary_distributed_computing`

---

## What the app does

Marabunta Worker is a **voluntary distributed computing** application. Users
opt-in to donate their device's idle compute capacity to scientific and
engineering workloads (Monte Carlo simulations, parameter sweeps, shell
scripts, Python scripts). The app connects to a coordinator server,
receives small compute tasks, executes them, and returns results.

Think of it as "BOINC / Folding@Home for mobile devices."

---

## Why a foreground service is required

The app performs long-running computation on behalf of the user. This
computation:

1. **Must survive the app being closed** — users start the service and
   close the app. The service continues computing in the background.
2. **Requires CPU wakefulness** — the partial wake lock keeps the CPU
   running while the screen is off.
3. **Is user-initiated and user-controlled** — the user explicitly
   starts computing via the UI. The persistent notification provides
   Pause and Stop actions at all times.

A foreground service is the only Android mechanism that satisfies all
three requirements. WorkManager and JobScheduler are insufficient because:
- They cannot hold wake locks for extended compute.
- They have strict execution time limits (10-15 minutes).
- They cannot be reliably kept alive across process death.

---

## Why `specialUse` and not another type

| Type | Why it doesn't fit |
|------|-------------------|
| `dataSync` | No user data is being synced. The app processes coordinator-assigned compute tasks, not user content. Google's Android 14 restrictions explicitly discourage `dataSync` for non-sync workloads. |
| `location` | No location data is used. |
| `camera` / `microphone` | No media capture. |
| `health` | No health sensors. |
| `connectedDevice` | No Bluetooth / USB peripherals. |
| `remoteMessaging` | No messaging functionality. |
| `mediaPlayback` | No media is played. |
| `phoneCall` | No phone call functionality. |
| `mediaProjection` | No screen capture. |
| `shortService` | Compute tasks can run for hours. |

The `specialUse` type with subtype `voluntary_distributed_computing`
accurately describes the app's purpose.

---

## User protections

The app includes multiple safeguards to respect the user's device:

### Battery protection
- **Default: only computes when plugged in** (charging)
- Optional: compute on battery only above 90% charge, never below 20%
- Battery temperature monitoring pauses compute if device gets hot

### Thermal protection
- Android `PowerManager.THERMAL_STATUS_*` API (API 29+)
- Fallback: battery temperature monitoring for older devices
- Compute pauses at MODERATE throttle, stops at SEVERE

### Idle-only compute
- Only computes when **screen is off** or **device is locked**
- Computation pauses immediately when the user picks up the device
- Doze mode is respected

### Network awareness
- Monitors connectivity state in real-time
- Pauses compute when offline
- Detects WiFi vs cellular (can restrict to WiFi-only)

### User control
- **Persistent notification** with Pause and Stop actions
- **In-app controls** for start/stop/pause
- **Settings** for CPU limit (%), memory limit (MB), battery policy
- Service can be stopped at any time with zero data loss

### Resource limits
- CPU usage capped (default 80%, user-configurable)
- Memory usage capped (default 256 MB, user-configurable)
- Android 2-thread Tokio runtime (mobile-friendly)

---

## Permissions justification

| Permission | Purpose |
|-----------|---------|
| `INTERNET` | Connect to coordinator server for task assignment and result reporting |
| `ACCESS_NETWORK_STATE` | Detect WiFi vs cellular, pause when offline |
| `FOREGROUND_SERVICE` | Required for long-running compute service |
| `FOREGROUND_SERVICE_SPECIAL_USE` | Required for `specialUse` type on API 34+ |
| `WAKE_LOCK` | Keep CPU running during computation while screen is off |
| `RECEIVE_BOOT_COMPLETED` | Auto-restart service after device reboot (user-configurable) |
| `REQUEST_IGNORE_BATTERY_OPTIMIZATIONS` | Prevent Android from killing the compute service during active tasks |
| `POST_NOTIFICATIONS` | Required for foreground service notification on Android 13+ |

---

## Data safety

See `DATA_SAFETY.md` for the complete data safety declaration.

**Summary:**
- No personal data is collected
- No location, contacts, or media accessed
- Device identifier (anonymous node ID) used for task routing only
- Compute results are task outputs (scientific data), not user data
- All network traffic uses TLS encryption

---

## Contact

For Play Store review questions:
- App name: Marabunta Worker
- Package: `com.marabunta.worker`
- Developer: Marabunta Compute Project
