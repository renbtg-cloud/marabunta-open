// Marabunta - Licensed under the MIT License.
package com.marabunta.worker;

import android.content.Context;
import android.os.Build;
import android.os.PowerManager;
import android.util.Log;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Monitors device thermal state and throttles computation when hot.
 * Uses PowerManager thermal API on Android 10+ with battery temp fallback.
 *
 * Thermal management rules:
 * - NOMINAL / LIGHT: Full compute allowed
 * - MODERATE: Reduce CPU usage (throttle)
 * - SEVERE: Stop all computation immediately
 * - CRITICAL / EMERGENCY / SHUTDOWN: Stop and alert user
 *
 * On older devices (pre-Android 10), battery temperature from PowerMonitor
 * is used as a proxy for thermal state:
 * - Below 35C: NOMINAL
 * - 35-40C: LIGHT
 * - 40-43C: MODERATE
 * - 43-45C: SEVERE
 * - 45-48C: CRITICAL
 * - 48-50C: EMERGENCY
 * - Above 50C: SHUTDOWN
 */
public class ThermalMonitor {

    private static final String TAG = "ThermalMonitor";

    // Battery temperature thresholds in tenths of a degree Celsius (for fallback)
    private static final int TEMP_LIGHT = 350;      // 35.0 C
    private static final int TEMP_MODERATE = 400;    // 40.0 C
    private static final int TEMP_SEVERE = 430;      // 43.0 C
    private static final int TEMP_CRITICAL = 450;    // 45.0 C
    private static final int TEMP_EMERGENCY = 480;   // 48.0 C
    private static final int TEMP_SHUTDOWN = 500;    // 50.0 C

    // Polling interval for battery temp fallback (30 seconds)
    private static final long POLL_INTERVAL_MS = 30 * 1000;

    /**
     * Thermal status levels matching PowerManager.THERMAL_STATUS_* constants
     * but available on all API levels.
     */
    public enum ThermalStatus {
        NOMINAL,
        LIGHT,
        MODERATE,
        SEVERE,
        CRITICAL,
        EMERGENCY,
        SHUTDOWN
    }

    private final Context context;
    private final PowerManager powerManager;
    private final PowerMonitor powerMonitor;  // For battery temp fallback

    // Current thermal state
    private volatile ThermalStatus currentStatus = ThermalStatus.NOMINAL;

    // Android 10+ thermal listener
    private Object thermalListener;  // PowerManager.OnThermalStatusChangedListener

    // Fallback: polling battery temperature
    private ScheduledExecutorService pollExecutor;
    private ScheduledFuture<?> pollFuture;

    private boolean started = false;

    /**
     * Create a ThermalMonitor.
     *
     * @param context Application context
     * @param powerMonitor PowerMonitor instance for battery temperature fallback
     */
    public ThermalMonitor(Context context, PowerMonitor powerMonitor) {
        this.context = context;
        this.powerManager = (PowerManager) context.getSystemService(Context.POWER_SERVICE);
        this.powerMonitor = powerMonitor;
    }

    /**
     * Start monitoring thermal state.
     */
    public void start() {
        if (started) {
            return;
        }

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            // Android 10+ (API 29): use thermal status listener
            registerThermalListener();
        } else {
            // Older devices: poll battery temperature from PowerMonitor
            startTemperaturePolling();
        }

        started = true;
        Log.i(TAG, "Thermal monitoring started");
    }

    /**
     * Stop monitoring thermal state.
     */
    public void stop() {
        if (!started) {
            return;
        }

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            unregisterThermalListener();
        } else {
            stopTemperaturePolling();
        }

        started = false;
        Log.i(TAG, "Thermal monitoring stopped");
    }

    /**
     * Register thermal status listener on Android 10+.
     */
    private void registerThermalListener() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            PowerManager.OnThermalStatusChangedListener listener =
                    new PowerManager.OnThermalStatusChangedListener() {
                @Override
                public void onThermalStatusChanged(int status) {
                    ThermalStatus previous = currentStatus;
                    currentStatus = mapPowerManagerStatus(status);

                    Log.d(TAG, "Thermal status changed: " + previous + " -> " + currentStatus);

                    if (previous != currentStatus) {
                        notifyNativeLayer();
                    }
                }
            };

            powerManager.addThermalStatusListener(listener);
            thermalListener = listener;

            // Get initial thermal status
            int initialStatus = powerManager.getCurrentThermalStatus();
            currentStatus = mapPowerManagerStatus(initialStatus);

            Log.i(TAG, "Thermal listener registered, initial status: " + currentStatus);
        }
    }

    /**
     * Unregister thermal status listener on Android 10+.
     */
    private void unregisterThermalListener() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q && thermalListener != null) {
            try {
                powerManager.removeThermalStatusListener(
                        (PowerManager.OnThermalStatusChangedListener) thermalListener);
                thermalListener = null;
                Log.i(TAG, "Thermal listener unregistered");
            } catch (Exception e) {
                Log.w(TAG, "Error unregistering thermal listener", e);
            }
        }
    }

    /**
     * Start polling battery temperature as a thermal proxy.
     */
    private void startTemperaturePolling() {
        pollExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ThermalMonitor-Poll");
            t.setDaemon(true);
            return t;
        });

        pollFuture = pollExecutor.scheduleAtFixedRate(() -> {
            try {
                ThermalStatus previous = currentStatus;
                currentStatus = estimateStatusFromBatteryTemp();

                if (previous != currentStatus) {
                    Log.d(TAG, "Thermal status changed (battery temp): " +
                          previous + " -> " + currentStatus +
                          " (temp=" + powerMonitor.getTemperatureCelsius() + "C)");
                    notifyNativeLayer();
                }
            } catch (Exception e) {
                Log.w(TAG, "Error polling battery temperature", e);
            }
        }, 0, POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);

        Log.i(TAG, "Battery temperature polling started");
    }

    /**
     * Stop polling battery temperature.
     */
    private void stopTemperaturePolling() {
        if (pollFuture != null) {
            pollFuture.cancel(false);
            pollFuture = null;
        }

        if (pollExecutor != null) {
            pollExecutor.shutdown();
            try {
                if (!pollExecutor.awaitTermination(2, TimeUnit.SECONDS)) {
                    pollExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                pollExecutor.shutdownNow();
            }
            pollExecutor = null;
        }

        Log.i(TAG, "Battery temperature polling stopped");
    }

    /**
     * Map PowerManager.THERMAL_STATUS_* constants to our ThermalStatus enum.
     */
    private ThermalStatus mapPowerManagerStatus(int status) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            switch (status) {
                case PowerManager.THERMAL_STATUS_NONE:
                    return ThermalStatus.NOMINAL;
                case PowerManager.THERMAL_STATUS_LIGHT:
                    return ThermalStatus.LIGHT;
                case PowerManager.THERMAL_STATUS_MODERATE:
                    return ThermalStatus.MODERATE;
                case PowerManager.THERMAL_STATUS_SEVERE:
                    return ThermalStatus.SEVERE;
                case PowerManager.THERMAL_STATUS_CRITICAL:
                    return ThermalStatus.CRITICAL;
                case PowerManager.THERMAL_STATUS_EMERGENCY:
                    return ThermalStatus.EMERGENCY;
                case PowerManager.THERMAL_STATUS_SHUTDOWN:
                    return ThermalStatus.SHUTDOWN;
                default:
                    Log.w(TAG, "Unknown thermal status: " + status);
                    return ThermalStatus.NOMINAL;
            }
        }
        return ThermalStatus.NOMINAL;
    }

    /**
     * Estimate thermal status from battery temperature.
     * Used as a fallback on devices without thermal API (pre-Android 10).
     */
    private ThermalStatus estimateStatusFromBatteryTemp() {
        // PowerMonitor stores temperature in tenths of a degree Celsius
        float tempCelsius = powerMonitor.getTemperatureCelsius();
        int tempTenths = (int) (tempCelsius * 10);

        if (tempTenths >= TEMP_SHUTDOWN) {
            return ThermalStatus.SHUTDOWN;
        } else if (tempTenths >= TEMP_EMERGENCY) {
            return ThermalStatus.EMERGENCY;
        } else if (tempTenths >= TEMP_CRITICAL) {
            return ThermalStatus.CRITICAL;
        } else if (tempTenths >= TEMP_SEVERE) {
            return ThermalStatus.SEVERE;
        } else if (tempTenths >= TEMP_MODERATE) {
            return ThermalStatus.MODERATE;
        } else if (tempTenths >= TEMP_LIGHT) {
            return ThermalStatus.LIGHT;
        } else {
            return ThermalStatus.NOMINAL;
        }
    }

    /**
     * Notify the native layer of thermal state changes.
     */
    private void notifyNativeLayer() {
        try {
            if (NativeWorker.isLibraryLoaded()) {
                NativeWorker.getInstance().setThermalState(currentStatus.ordinal());
            }
        } catch (Exception e) {
            Log.w(TAG, "Failed to notify native layer of thermal change", e);
        }
    }

    // ==================== Public API ====================

    /**
     * Check if computation is allowed at the current thermal level.
     * Only NOMINAL and LIGHT allow full computation.
     *
     * @return true if thermal state allows computation
     */
    public boolean canCompute() {
        return currentStatus == ThermalStatus.NOMINAL ||
               currentStatus == ThermalStatus.LIGHT;
    }

    /**
     * Check if computation should be throttled (reduced CPU %).
     * MODERATE thermal status means we should reduce workload but can continue.
     *
     * @return true if computation should be throttled
     */
    public boolean shouldThrottle() {
        return currentStatus == ThermalStatus.MODERATE;
    }

    /**
     * Check if computation must stop immediately.
     * SEVERE and above require immediate cessation of computation.
     *
     * @return true if computation must stop
     */
    public boolean mustStop() {
        return currentStatus == ThermalStatus.SEVERE ||
               currentStatus == ThermalStatus.CRITICAL ||
               currentStatus == ThermalStatus.EMERGENCY ||
               currentStatus == ThermalStatus.SHUTDOWN;
    }

    /**
     * Get the current thermal status.
     */
    public ThermalStatus getCurrentStatus() {
        return currentStatus;
    }

    /**
     * Get a summary string of the current thermal state.
     */
    public String getStatusSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append(currentStatus.name());

        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.Q) {
            sb.append(" (").append(powerMonitor.getTemperatureCelsius()).append("C)");
        }

        return sb.toString();
    }
}
