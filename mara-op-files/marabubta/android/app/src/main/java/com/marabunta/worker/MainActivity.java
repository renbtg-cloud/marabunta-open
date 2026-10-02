// Marabunta - Licensed under the MIT License.
package com.marabunta.worker;

import android.Manifest;
import android.app.Activity;
import android.app.ActivityManager;
import android.app.AlertDialog;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.content.pm.PackageManager;
import android.net.Uri;
import android.os.Build;
import android.os.Bundle;
import android.os.Handler;
import android.os.IBinder;
import android.os.Looper;
import android.os.PowerManager;
import android.provider.Settings;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.Switch;
import android.widget.TextView;

import androidx.core.app.ActivityCompat;
import androidx.core.content.ContextCompat;

/**
 * Main activity showing worker status and controls.
 *
 * Features:
 * - Service status and statistics
 * - Start/Stop controls
 * - Settings for battery and resource limits
 * - Battery optimization exemption request
 */
public class MainActivity extends Activity implements MarabuntaWorkerService.StatusListener {

    private static final String TAG = "MainActivity";
    private static final int PERMISSION_REQUEST_CODE = 100;

    // UI Elements
    private TextView statusText;
    private TextView statsText;
    private TextView powerText;
    private TextView detailsText;
    private Button startStopButton;
    private Switch autoStartSwitch;
    private Switch batteryModeSwitch;

    // Service binding
    private MarabuntaWorkerService workerService;
    private boolean serviceBound = false;

    // Update handler
    private final Handler updateHandler = new Handler(Looper.getMainLooper());
    private final Runnable updateRunnable = this::updateUI;

    // Monitors
    private PowerMonitor powerMonitor;
    private IdleDetector idleDetector;

    private final ServiceConnection serviceConnection = new ServiceConnection() {
        @Override
        public void onServiceConnected(ComponentName name, IBinder service) {
            MarabuntaWorkerService.LocalBinder binder = (MarabuntaWorkerService.LocalBinder) service;
            workerService = binder.getService();
            serviceBound = true;
            workerService.setStatusListener(MainActivity.this);
            updateUI();
            Log.i(TAG, "Service connected");
        }

        @Override
        public void onServiceDisconnected(ComponentName name) {
            workerService = null;
            serviceBound = false;
            Log.i(TAG, "Service disconnected");
        }
    };

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        // Find views
        statusText = findViewById(R.id.status_text);
        statsText = findViewById(R.id.stats_text);
        powerText = findViewById(R.id.power_text);
        detailsText = findViewById(R.id.details_text);
        startStopButton = findViewById(R.id.start_stop_button);
        autoStartSwitch = findViewById(R.id.auto_start_switch);
        batteryModeSwitch = findViewById(R.id.battery_mode_switch);

        // Initialize monitors
        powerMonitor = new PowerMonitor(this);
        idleDetector = new IdleDetector(this);

        // Setup click listeners
        startStopButton.setOnClickListener(v -> toggleService());

        // Setup switches
        MarabuntaWorkerApp app = MarabuntaWorkerApp.getInstance();
        autoStartSwitch.setChecked(app.isAutoStartEnabled());
        autoStartSwitch.setOnCheckedChangeListener((buttonView, isChecked) -> {
            app.setAutoStartEnabled(isChecked);
        });

        batteryModeSwitch.setChecked(app.canComputeOnBattery());
        batteryModeSwitch.setOnCheckedChangeListener((buttonView, isChecked) -> {
            app.setComputeOnBattery(isChecked);
        });

        // Check permissions
        checkAndRequestPermissions();

        // Request battery optimization exemption
        checkBatteryOptimization();
    }

    @Override
    protected void onStart() {
        super.onStart();

        // Bind to service if running
        if (isServiceRunning()) {
            Intent intent = new Intent(this, MarabuntaWorkerService.class);
            bindService(intent, serviceConnection, Context.BIND_AUTO_CREATE);
        }

        // Start UI updates
        updateHandler.post(updateRunnable);
    }

    @Override
    protected void onStop() {
        super.onStop();

        // Unbind from service
        if (serviceBound) {
            if (workerService != null) {
                workerService.setStatusListener(null);
            }
            unbindService(serviceConnection);
            serviceBound = false;
        }

        // Stop UI updates
        updateHandler.removeCallbacks(updateRunnable);
    }

    @Override
    protected void onDestroy() {
        super.onDestroy();

        if (powerMonitor != null) {
            powerMonitor.unregister();
        }
        if (idleDetector != null) {
            idleDetector.unregister();
        }
    }

    /**
     * Check and request necessary permissions.
     */
    private void checkAndRequestPermissions() {
        // Check notification permission (Android 13+)
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            if (ContextCompat.checkSelfPermission(this, Manifest.permission.POST_NOTIFICATIONS)
                    != PackageManager.PERMISSION_GRANTED) {
                ActivityCompat.requestPermissions(this,
                    new String[]{Manifest.permission.POST_NOTIFICATIONS},
                    PERMISSION_REQUEST_CODE);
            }
        }
    }

    @Override
    public void onRequestPermissionsResult(int requestCode, String[] permissions, int[] grantResults) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);

        if (requestCode == PERMISSION_REQUEST_CODE) {
            if (grantResults.length > 0 && grantResults[0] != PackageManager.PERMISSION_GRANTED) {
                // Permission denied - show explanation
                new AlertDialog.Builder(this)
                    .setTitle("Notification Permission")
                    .setMessage("Notifications are required to run as a foreground service. " +
                               "Without this permission, the worker may be killed by the system.")
                    .setPositiveButton("OK", null)
                    .show();
            }
        }
    }

    /**
     * Check if battery optimization exemption is needed.
     */
    private void checkBatteryOptimization() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            PowerManager pm = (PowerManager) getSystemService(Context.POWER_SERVICE);
            String packageName = getPackageName();

            if (!pm.isIgnoringBatteryOptimizations(packageName)) {
                // Show dialog explaining why we need this
                new AlertDialog.Builder(this)
                    .setTitle("Battery Optimization")
                    .setMessage("For the Marabunta Worker to run reliably in the background, " +
                               "please disable battery optimization for this app.\n\n" +
                               "This allows the worker to continue computing even when " +
                               "the phone is in sleep mode.")
                    .setPositiveButton("Allow", (dialog, which) -> {
                        requestBatteryOptimizationExemption();
                    })
                    .setNegativeButton("Later", null)
                    .show();
            }
        }
    }

    /**
     * Request battery optimization exemption.
     */
    private void requestBatteryOptimizationExemption() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            Intent intent = new Intent();
            intent.setAction(Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS);
            intent.setData(Uri.parse("package:" + getPackageName()));
            startActivity(intent);
        }
    }

    /**
     * Toggle the worker service on/off.
     */
    private void toggleService() {
        if (isServiceRunning()) {
            // Stop service
            Intent intent = new Intent(this, MarabuntaWorkerService.class);
            intent.setAction(MarabuntaWorkerService.ACTION_STOP);
            startService(intent);

            if (serviceBound) {
                unbindService(serviceConnection);
                serviceBound = false;
            }

            startStopButton.setText("Start Contributing");
            statusText.setText("Status: Stopped");
        } else {
            // Start service
            Intent intent = new Intent(this, MarabuntaWorkerService.class);
            intent.setAction(MarabuntaWorkerService.ACTION_START);

            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                startForegroundService(intent);
            } else {
                startService(intent);
            }

            bindService(intent, serviceConnection, Context.BIND_AUTO_CREATE);

            startStopButton.setText("Stop Contributing");
            statusText.setText("Status: Starting...");
        }
    }

    /**
     * Check if the worker service is running.
     */
    private boolean isServiceRunning() {
        ActivityManager manager = (ActivityManager) getSystemService(Context.ACTIVITY_SERVICE);
        for (ActivityManager.RunningServiceInfo service : manager.getRunningServices(Integer.MAX_VALUE)) {
            if (MarabuntaWorkerService.class.getName().equals(service.service.getClassName())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Update UI with current status.
     */
    private void updateUI() {
        // Power status
        String powerStatus = powerMonitor.getStatusSummary();
        powerText.setText("Power: " + powerStatus);

        // Idle status
        String idleStatus = idleDetector.getStatusSummary();

        // Service status
        if (serviceBound && workerService != null) {
            startStopButton.setText("Stop Contributing");

            String status = workerService.getCurrentStatus();
            statusText.setText("Status: " + status);

            long tasks = workerService.getTasksCompleted();
            long tokens = workerService.getTokensEarned();
            statsText.setText(String.format("Tasks: %d | Tokens: %d", tasks, tokens));

            long sessionMs = workerService.getSessionDurationMs();
            String duration = formatDuration(sessionMs);
            detailsText.setText(String.format("Session: %s | Device: %s", duration, idleStatus));
        } else if (isServiceRunning()) {
            startStopButton.setText("Stop Contributing");
            statusText.setText("Status: Running (not bound)");
            detailsText.setText("Device: " + idleStatus);
        } else {
            startStopButton.setText("Start Contributing");
            statusText.setText("Status: Stopped");
            detailsText.setText("Device: " + idleStatus);

            // Show total lifetime stats when stopped
            MarabuntaWorkerApp app = MarabuntaWorkerApp.getInstance();
            long totalTasks = app.getTotalTasksCompleted();
            long totalTokens = app.getTotalTokensEarned();
            statsText.setText(String.format("Lifetime - Tasks: %d | Tokens: %d", totalTasks, totalTokens));
        }

        // Schedule next update
        updateHandler.postDelayed(updateRunnable, 1000);
    }

    /**
     * Format duration in human-readable form.
     */
    private String formatDuration(long ms) {
        long seconds = ms / 1000;
        long minutes = seconds / 60;
        long hours = minutes / 60;

        if (hours > 0) {
            return String.format("%dh %02dm", hours, minutes % 60);
        } else if (minutes > 0) {
            return String.format("%dm %02ds", minutes, seconds % 60);
        } else {
            return String.format("%ds", seconds);
        }
    }

    // ==================== StatusListener Implementation ====================

    @Override
    public void onStatusChanged(String status, long tasks, long tokens, boolean computing) {
        runOnUiThread(() -> {
            statusText.setText("Status: " + status);
            statsText.setText(String.format("Tasks: %d | Tokens: %d", tasks, tokens));
        });
    }
}
