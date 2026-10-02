// Marabunta - Licensed under the MIT License.
package com.marabunta.worker;

import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.os.BatteryManager;
import android.os.Build;
import android.util.Log;

/**
 * Monitors power state to decide when to compute.
 *
 * Computation rules:
 * - ALWAYS compute if device is plugged in (AC, USB, or Wireless)
 * - OPTIONALLY compute on battery if:
 *   - User has enabled battery computing
 *   - Battery level is above 90%
 * - NEVER compute if battery below 20% (emergency reserve)
 *
 * This ensures we never drain the user's battery unexpectedly.
 */
public class PowerMonitor {

    private static final String TAG = "PowerMonitor";

    // Battery thresholds
    private static final int BATTERY_THRESHOLD_HIGH = 90;  // Can compute on battery above this
    private static final int BATTERY_THRESHOLD_LOW = 20;   // Never compute below this

    private final Context context;
    private final BatteryManager batteryManager;

    // Current state
    private volatile boolean isPluggedIn = false;
    private volatile int batteryPercent = 100;
    private volatile int chargingStatus = BatteryManager.BATTERY_STATUS_UNKNOWN;
    private volatile int pluggedType = 0;
    private volatile int temperature = 0;  // In tenths of a degree Celsius

    // Receiver for power state changes
    private final BroadcastReceiver powerReceiver = new BroadcastReceiver() {
        @Override
        public void onReceive(Context context, Intent intent) {
            updateStateFromIntent(intent);
        }
    };

    private boolean receiverRegistered = false;

    public PowerMonitor(Context context) {
        this.context = context;
        this.batteryManager = (BatteryManager) context.getSystemService(Context.BATTERY_SERVICE);

        // Register receiver for power changes
        registerReceiver();

        // Get initial state
        updateState();
    }

    /**
     * Register broadcast receiver for power state changes.
     */
    private void registerReceiver() {
        if (receiverRegistered) {
            return;
        }

        IntentFilter filter = new IntentFilter();
        filter.addAction(Intent.ACTION_BATTERY_CHANGED);
        filter.addAction(Intent.ACTION_POWER_CONNECTED);
        filter.addAction(Intent.ACTION_POWER_DISCONNECTED);

        // Use sticky broadcast to get current state immediately
        Intent batteryStatus = context.registerReceiver(powerReceiver, filter);
        if (batteryStatus != null) {
            updateStateFromIntent(batteryStatus);
        }

        receiverRegistered = true;
        Log.i(TAG, "Power receiver registered");
    }

    /**
     * Unregister the broadcast receiver.
     */
    public void unregister() {
        if (receiverRegistered) {
            try {
                context.unregisterReceiver(powerReceiver);
                receiverRegistered = false;
                Log.i(TAG, "Power receiver unregistered");
            } catch (Exception e) {
                Log.w(TAG, "Error unregistering receiver", e);
            }
        }
    }

    /**
     * Update state from battery changed intent.
     */
    private void updateStateFromIntent(Intent intent) {
        if (intent == null) {
            return;
        }

        // Get charging status
        chargingStatus = intent.getIntExtra(BatteryManager.EXTRA_STATUS,
                                           BatteryManager.BATTERY_STATUS_UNKNOWN);

        // Get plugged type
        pluggedType = intent.getIntExtra(BatteryManager.EXTRA_PLUGGED, 0);
        isPluggedIn = pluggedType != 0;

        // Get battery level
        int level = intent.getIntExtra(BatteryManager.EXTRA_LEVEL, -1);
        int scale = intent.getIntExtra(BatteryManager.EXTRA_SCALE, -1);
        if (level >= 0 && scale > 0) {
            batteryPercent = (level * 100) / scale;
        }

        // Get temperature
        temperature = intent.getIntExtra(BatteryManager.EXTRA_TEMPERATURE, 0);

        Log.d(TAG, "Power state updated: plugged=" + isPluggedIn +
              ", battery=" + batteryPercent + "%" +
              ", temp=" + (temperature / 10.0) + "C");
    }

    /**
     * Update state by querying battery manager directly.
     */
    private void updateState() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.LOLLIPOP) {
            batteryPercent = batteryManager.getIntProperty(BatteryManager.BATTERY_PROPERTY_CAPACITY);

            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
                isPluggedIn = batteryManager.isCharging();
            }
        }
    }

    // ==================== Public API ====================

    /**
     * Check if device is currently plugged in.
     */
    public boolean isPluggedIn() {
        return isPluggedIn;
    }

    /**
     * Check if device is charging (not just plugged in).
     */
    public boolean isCharging() {
        return chargingStatus == BatteryManager.BATTERY_STATUS_CHARGING ||
               chargingStatus == BatteryManager.BATTERY_STATUS_FULL;
    }

    /**
     * Get current battery percentage (0-100).
     */
    public int getBatteryPercent() {
        return batteryPercent;
    }

    /**
     * Get plugged type.
     *
     * @return One of: 0 (unplugged), BATTERY_PLUGGED_AC, BATTERY_PLUGGED_USB,
     *         BATTERY_PLUGGED_WIRELESS
     */
    public int getPluggedType() {
        return pluggedType;
    }

    /**
     * Get human-readable plugged type string.
     */
    public String getPluggedTypeString() {
        switch (pluggedType) {
            case BatteryManager.BATTERY_PLUGGED_AC:
                return "AC";
            case BatteryManager.BATTERY_PLUGGED_USB:
                return "USB";
            case BatteryManager.BATTERY_PLUGGED_WIRELESS:
                return "Wireless";
            default:
                return "Battery";
        }
    }

    /**
     * Get battery temperature in Celsius.
     */
    public float getTemperatureCelsius() {
        return temperature / 10.0f;
    }

    /**
     * Check if battery temperature is safe for computing.
     * Generally, batteries should stay below 45C.
     */
    public boolean isTemperatureSafe() {
        return temperature < 450;  // 45.0 C in tenths
    }

    /**
     * Check if we can compute based on power state.
     *
     * @param allowBattery If true, allow computing on battery when level > 90%
     * @return true if conditions allow computing
     */
    public boolean canCompute(boolean allowBattery) {
        // Always check temperature first
        if (!isTemperatureSafe()) {
            Log.w(TAG, "Temperature too high for computing: " + getTemperatureCelsius() + "C");
            return false;
        }

        // Emergency reserve - never compute below 20%
        if (batteryPercent < BATTERY_THRESHOLD_LOW) {
            return false;
        }

        // Plugged in - always OK
        if (isPluggedIn) {
            return true;
        }

        // Battery mode enabled and battery is high enough
        if (allowBattery && batteryPercent >= BATTERY_THRESHOLD_HIGH) {
            return true;
        }

        return false;
    }

    /**
     * Get estimated time remaining on battery.
     * Only available on Android 5.0+.
     *
     * @return Time remaining in milliseconds, or -1 if unavailable
     */
    public long getBatteryTimeRemainingMs() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.LOLLIPOP) {
            return batteryManager.computeChargeTimeRemaining();
        }
        return -1;
    }

    /**
     * Get a summary string of the current power state.
     */
    public String getStatusSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append(batteryPercent).append("% ");

        if (isPluggedIn) {
            sb.append("(").append(getPluggedTypeString()).append(")");
            if (isCharging()) {
                sb.append(" Charging");
            } else {
                sb.append(" Full");
            }
        } else {
            sb.append("(Battery)");
        }

        return sb.toString();
    }
}
