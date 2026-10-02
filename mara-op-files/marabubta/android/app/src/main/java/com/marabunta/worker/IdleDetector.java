// Marabunta - Licensed under the MIT License.
package com.marabunta.worker;

import android.app.KeyguardManager;
import android.app.usage.UsageStats;
import android.app.usage.UsageStatsManager;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.os.Build;
import android.os.PowerManager;
import android.util.Log;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Detects if the phone is idle (not being used).
 *
 * Idle detection criteria:
 * - Screen is off, OR
 * - Screen is on but device is locked
 * - No recent user interaction (if usage stats available)
 *
 * We only compute when the device is truly idle to:
 * - Preserve battery for user activities
 * - Avoid impacting user experience
 * - Respect user privacy (don't compute while actively using)
 */
public class IdleDetector {

    private static final String TAG = "IdleDetector";

    // How long without interaction to consider device idle (30 seconds)
    private static final long IDLE_THRESHOLD_MS = 30 * 1000;

    private final Context context;
    private final PowerManager powerManager;
    private final KeyguardManager keyguardManager;

    // Track last user interaction
    private final AtomicLong lastInteractionTime = new AtomicLong(System.currentTimeMillis());

    // Screen state tracking
    private volatile boolean isScreenOn = true;
    private volatile boolean isDeviceLocked = false;

    // Broadcast receiver for screen state
    private final BroadcastReceiver screenReceiver = new BroadcastReceiver() {
        @Override
        public void onReceive(Context context, Intent intent) {
            String action = intent.getAction();
            if (action == null) return;

            switch (action) {
                case Intent.ACTION_SCREEN_OFF:
                    isScreenOn = false;
                    Log.d(TAG, "Screen turned off");
                    break;

                case Intent.ACTION_SCREEN_ON:
                    isScreenOn = true;
                    lastInteractionTime.set(System.currentTimeMillis());
                    Log.d(TAG, "Screen turned on");
                    break;

                case Intent.ACTION_USER_PRESENT:
                    // User unlocked the device
                    isDeviceLocked = false;
                    lastInteractionTime.set(System.currentTimeMillis());
                    Log.d(TAG, "User present (unlocked)");
                    break;
            }

            updateLockedState();
        }
    };

    private boolean receiverRegistered = false;

    public IdleDetector(Context context) {
        this.context = context;
        this.powerManager = (PowerManager) context.getSystemService(Context.POWER_SERVICE);
        this.keyguardManager = (KeyguardManager) context.getSystemService(Context.KEYGUARD_SERVICE);

        // Register receiver
        registerReceiver();

        // Get initial state
        updateScreenState();
        updateLockedState();
    }

    /**
     * Register broadcast receiver for screen state changes.
     */
    private void registerReceiver() {
        if (receiverRegistered) {
            return;
        }

        IntentFilter filter = new IntentFilter();
        filter.addAction(Intent.ACTION_SCREEN_OFF);
        filter.addAction(Intent.ACTION_SCREEN_ON);
        filter.addAction(Intent.ACTION_USER_PRESENT);

        context.registerReceiver(screenReceiver, filter);
        receiverRegistered = true;
        Log.i(TAG, "Screen receiver registered");
    }

    /**
     * Unregister the broadcast receiver.
     */
    public void unregister() {
        if (receiverRegistered) {
            try {
                context.unregisterReceiver(screenReceiver);
                receiverRegistered = false;
                Log.i(TAG, "Screen receiver unregistered");
            } catch (Exception e) {
                Log.w(TAG, "Error unregistering receiver", e);
            }
        }
    }

    /**
     * Update screen on/off state.
     */
    private void updateScreenState() {
        // isInteractive() returns true if screen is on
        isScreenOn = powerManager.isInteractive();
    }

    /**
     * Update device locked state.
     */
    private void updateLockedState() {
        isDeviceLocked = keyguardManager.isKeyguardLocked();
    }

    // ==================== Public API ====================

    /**
     * Check if the device is currently idle.
     *
     * Idle means:
     * - Screen is off, OR
     * - Screen is on but device is locked
     *
     * @return true if device is idle
     */
    public boolean isIdle() {
        updateScreenState();
        updateLockedState();

        // Screen off = definitely idle
        if (!isScreenOn) {
            return true;
        }

        // Screen on but locked = probably idle
        if (isDeviceLocked) {
            return true;
        }

        // Screen on and unlocked = check for recent activity
        long timeSinceInteraction = System.currentTimeMillis() - lastInteractionTime.get();
        if (timeSinceInteraction > IDLE_THRESHOLD_MS) {
            // No interaction for a while, might be idle
            // But we should be cautious - user might just be reading
            return false;  // Conservative: assume not idle if unlocked
        }

        // Screen on, unlocked, recent interaction = not idle
        return false;
    }

    /**
     * Check if screen is currently off.
     */
    public boolean isScreenOff() {
        updateScreenState();
        return !isScreenOn;
    }

    /**
     * Check if screen is currently on.
     */
    public boolean isScreenOn() {
        updateScreenState();
        return isScreenOn;
    }

    /**
     * Check if device is locked.
     */
    public boolean isDeviceLocked() {
        updateLockedState();
        return isDeviceLocked;
    }

    /**
     * Record a user interaction.
     * Call this if you detect user activity.
     */
    public void recordInteraction() {
        lastInteractionTime.set(System.currentTimeMillis());
    }

    /**
     * Get time since last recorded interaction.
     *
     * @return Time in milliseconds
     */
    public long getTimeSinceInteractionMs() {
        return System.currentTimeMillis() - lastInteractionTime.get();
    }

    /**
     * Check if device is in Doze mode (Android 6.0+).
     *
     * Doze mode is a deep sleep state where the system restricts
     * network access and background processing. We should respect
     * this and not try to compute during Doze.
     *
     * @return true if device is in Doze mode
     */
    public boolean isInDozeMode() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            return powerManager.isDeviceIdleMode();
        }
        return false;
    }

    /**
     * Check if our app is on the Doze whitelist.
     *
     * If we're whitelisted, we can compute even during Doze.
     *
     * @return true if app is whitelisted
     */
    public boolean isIgnoringBatteryOptimizations() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            String packageName = context.getPackageName();
            return powerManager.isIgnoringBatteryOptimizations(packageName);
        }
        return true;  // No battery optimization before Android 6.0
    }

    /**
     * Get a summary of the current idle state.
     */
    public String getStatusSummary() {
        updateScreenState();
        updateLockedState();

        if (!isScreenOn) {
            return "Screen Off";
        } else if (isDeviceLocked) {
            return "Locked";
        } else {
            return "Active";
        }
    }

    /**
     * Get detailed idle analysis for debugging.
     */
    public String getDetailedStatus() {
        updateScreenState();
        updateLockedState();

        StringBuilder sb = new StringBuilder();
        sb.append("Screen: ").append(isScreenOn ? "ON" : "OFF").append("\n");
        sb.append("Locked: ").append(isDeviceLocked ? "YES" : "NO").append("\n");
        sb.append("Last interaction: ").append(getTimeSinceInteractionMs() / 1000).append("s ago\n");
        sb.append("Idle: ").append(isIdle() ? "YES" : "NO").append("\n");

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            sb.append("Doze mode: ").append(isInDozeMode() ? "YES" : "NO").append("\n");
            sb.append("Battery opt exempt: ").append(isIgnoringBatteryOptimizations() ? "YES" : "NO");
        }

        return sb.toString();
    }
}
