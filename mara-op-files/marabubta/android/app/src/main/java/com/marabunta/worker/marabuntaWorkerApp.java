// Marabunta - Licensed under the MIT License.
package com.marabunta.worker;

import android.app.Application;
import android.content.SharedPreferences;
import android.util.Log;

/**
 * Application class for Marabunta Worker.
 *
 * Handles global initialization and configuration.
 */
public class MarabuntaWorkerApp extends Application {

    private static final String TAG = "MarabuntaWorkerApp";
    private static final String PREFS_NAME = "marabunta_worker_prefs";

    // Singleton instance
    private static MarabuntaWorkerApp instance;

    // Configuration
    private SharedPreferences prefs;
    private String coordinatorUrl;
    private String orgKey;
    private String nodeId;

    // Settings
    private boolean autoStartEnabled;
    private boolean computeOnBattery;
    private int maxCpuPercent;
    private int maxMemoryMb;

    @Override
    public void onCreate() {
        super.onCreate();
        instance = this;

        Log.i(TAG, "Marabunta Worker App initializing...");

        // Load preferences
        prefs = getSharedPreferences(PREFS_NAME, MODE_PRIVATE);
        loadConfiguration();

        // Generate or load node ID
        nodeId = prefs.getString("node_id", null);
        if (nodeId == null) {
            nodeId = generateNodeId();
            prefs.edit().putString("node_id", nodeId).apply();
        }

        Log.i(TAG, "Marabunta Worker App initialized. Node ID: " + nodeId);
    }

    public static MarabuntaWorkerApp getInstance() {
        return instance;
    }

    private void loadConfiguration() {
        coordinatorUrl = prefs.getString("coordinator_url", "https://marabunta.example.com/api");
        orgKey = prefs.getString("org_key", "");
        autoStartEnabled = prefs.getBoolean("auto_start", true);
        computeOnBattery = prefs.getBoolean("compute_on_battery", false);
        maxCpuPercent = prefs.getInt("max_cpu_percent", 80);
        maxMemoryMb = prefs.getInt("max_memory_mb", 256);
    }

    private String generateNodeId() {
        // Generate a unique node ID based on device info and random component
        String deviceInfo = android.os.Build.MANUFACTURER + "_" +
                           android.os.Build.MODEL + "_" +
                           System.currentTimeMillis();
        return "android_" + Math.abs(deviceInfo.hashCode()) + "_" +
               Long.toHexString(System.nanoTime());
    }

    // Getters
    public String getCoordinatorUrl() {
        return coordinatorUrl;
    }

    public String getOrgKey() {
        return orgKey;
    }

    public String getNodeId() {
        return nodeId;
    }

    public boolean isAutoStartEnabled() {
        return autoStartEnabled;
    }

    public boolean canComputeOnBattery() {
        return computeOnBattery;
    }

    public int getMaxCpuPercent() {
        return maxCpuPercent;
    }

    public int getMaxMemoryMb() {
        return maxMemoryMb;
    }

    // Setters with persistence
    public void setCoordinatorUrl(String url) {
        this.coordinatorUrl = url;
        prefs.edit().putString("coordinator_url", url).apply();
    }

    public void setOrgKey(String key) {
        this.orgKey = key;
        prefs.edit().putString("org_key", key).apply();
    }

    public void setAutoStartEnabled(boolean enabled) {
        this.autoStartEnabled = enabled;
        prefs.edit().putBoolean("auto_start", enabled).apply();
    }

    public void setComputeOnBattery(boolean enabled) {
        this.computeOnBattery = enabled;
        prefs.edit().putBoolean("compute_on_battery", enabled).apply();
    }

    public void setMaxCpuPercent(int percent) {
        this.maxCpuPercent = Math.max(10, Math.min(100, percent));
        prefs.edit().putInt("max_cpu_percent", this.maxCpuPercent).apply();
    }

    public void setMaxMemoryMb(int mb) {
        this.maxMemoryMb = Math.max(64, Math.min(1024, mb));
        prefs.edit().putInt("max_memory_mb", this.maxMemoryMb).apply();
    }

    // Statistics persistence
    public void saveStats(long tasksCompleted, long tokensEarned) {
        prefs.edit()
            .putLong("total_tasks_completed", tasksCompleted)
            .putLong("total_tokens_earned", tokensEarned)
            .apply();
    }

    public long getTotalTasksCompleted() {
        return prefs.getLong("total_tasks_completed", 0);
    }

    public long getTotalTokensEarned() {
        return prefs.getLong("total_tokens_earned", 0);
    }
}
