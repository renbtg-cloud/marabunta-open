// Marabunta - Licensed under the MIT License.
package com.marabunta.worker;

import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.os.Build;
import android.util.Log;

/**
 * Broadcast receiver that starts the worker service on device boot.
 *
 * This ensures the Marabunta Worker automatically resumes computing
 * after the phone restarts, without requiring user intervention.
 *
 * Only starts if auto-start is enabled in settings.
 */
public class BootReceiver extends BroadcastReceiver {

    private static final String TAG = "BootReceiver";

    @Override
    public void onReceive(Context context, Intent intent) {
        String action = intent.getAction();

        if (Intent.ACTION_BOOT_COMPLETED.equals(action) ||
            "android.intent.action.QUICKBOOT_POWERON".equals(action)) {

            Log.i(TAG, "Boot completed received");

            // Check if auto-start is enabled
            MarabuntaWorkerApp app = (MarabuntaWorkerApp) context.getApplicationContext();
            if (!app.isAutoStartEnabled()) {
                Log.i(TAG, "Auto-start disabled, not starting service");
                return;
            }

            // Start the worker service
            startWorkerService(context);
        }
    }

    /**
     * Start the worker service as a foreground service.
     */
    private void startWorkerService(Context context) {
        Log.i(TAG, "Starting worker service on boot");

        Intent serviceIntent = new Intent(context, MarabuntaWorkerService.class);
        serviceIntent.setAction(MarabuntaWorkerService.ACTION_START);

        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                // Android 8.0+ requires startForegroundService for background starts
                context.startForegroundService(serviceIntent);
            } else {
                context.startService(serviceIntent);
            }
            Log.i(TAG, "Worker service start requested");
        } catch (Exception e) {
            Log.e(TAG, "Failed to start worker service on boot", e);
        }
    }
}
