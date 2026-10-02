// Marabunta - Licensed under the MIT License.
package com.marabunta.worker;

import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.os.Build;
import android.util.Log;

/**
 * Broadcast receiver for power state changes.
 *
 * Monitors when the device is plugged in or unplugged to:
 * - Resume computing when plugged in
 * - Pause computing when unplugged (if battery mode disabled)
 *
 * This receiver is registered in the manifest for system-level events.
 */
public class PowerStateReceiver extends BroadcastReceiver {

    private static final String TAG = "PowerStateReceiver";

    @Override
    public void onReceive(Context context, Intent intent) {
        String action = intent.getAction();

        if (action == null) {
            return;
        }

        switch (action) {
            case Intent.ACTION_POWER_CONNECTED:
                Log.i(TAG, "Power connected");
                onPowerConnected(context);
                break;

            case Intent.ACTION_POWER_DISCONNECTED:
                Log.i(TAG, "Power disconnected");
                onPowerDisconnected(context);
                break;
        }
    }

    /**
     * Handle power connected event.
     *
     * If auto-start is enabled and service isn't running, start it.
     * If service is paused, resume it.
     */
    private void onPowerConnected(Context context) {
        MarabuntaWorkerApp app = (MarabuntaWorkerApp) context.getApplicationContext();

        if (!app.isAutoStartEnabled()) {
            Log.d(TAG, "Auto-start disabled, ignoring power connected");
            return;
        }

        // Start or resume the service
        Intent serviceIntent = new Intent(context, MarabuntaWorkerService.class);
        serviceIntent.setAction(MarabuntaWorkerService.ACTION_RESUME);

        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                context.startForegroundService(serviceIntent);
            } else {
                context.startService(serviceIntent);
            }
        } catch (Exception e) {
            Log.e(TAG, "Failed to start/resume service on power connect", e);
        }
    }

    /**
     * Handle power disconnected event.
     *
     * If battery mode is disabled, pause the service.
     * The service will check power state on its own, but this gives a faster response.
     */
    private void onPowerDisconnected(Context context) {
        MarabuntaWorkerApp app = (MarabuntaWorkerApp) context.getApplicationContext();

        if (app.canComputeOnBattery()) {
            Log.d(TAG, "Battery mode enabled, continuing on battery");
            return;
        }

        // Pause the service to save battery
        Intent serviceIntent = new Intent(context, MarabuntaWorkerService.class);
        serviceIntent.setAction(MarabuntaWorkerService.ACTION_PAUSE);

        try {
            context.startService(serviceIntent);
        } catch (Exception e) {
            Log.e(TAG, "Failed to pause service on power disconnect", e);
        }
    }
}
