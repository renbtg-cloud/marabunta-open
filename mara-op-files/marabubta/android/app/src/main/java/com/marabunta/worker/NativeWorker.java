// Marabunta - Licensed under the MIT License.
package com.marabunta.worker;

import android.util.Log;

/**
 * JNI bridge to the native Rust Marabunta Worker.
 *
 * This class provides the interface to the native Rust library that handles:
 * - Communication with the Marabunta coordinator via BootstrapClient
 * - Task execution via TaskExecutor (MonteCarlo, ParameterSweep, Shell, Python)
 * - Resource management and throttling via MemoryPressureMonitor
 * - Network and thermal state awareness
 *
 * The native library must be compiled for each target architecture:
 * - arm64-v8a (64-bit ARM)
 * - armeabi-v7a (32-bit ARM)
 * - x86 (32-bit x86, for emulators)
 * - x86_64 (64-bit x86, for emulators)
 */
public class NativeWorker {

    private static final String TAG = "NativeWorker";
    private static final String LIBRARY_NAME = "marabunta_worker";

    private static boolean libraryLoaded = false;
    private static String loadError = null;

    // Singleton instance (used by NetworkMonitor / ThermalMonitor callbacks)
    private static NativeWorker instance;

    // Load native library
    static {
        try {
            System.loadLibrary(LIBRARY_NAME);
            libraryLoaded = true;
            Log.i(TAG, "Native library loaded successfully");
        } catch (UnsatisfiedLinkError e) {
            loadError = e.getMessage();
            Log.e(TAG, "Failed to load native library: " + loadError);
        }
    }

    /**
     * Check if the native library was loaded successfully.
     */
    public static boolean isLibraryLoaded() {
        return libraryLoaded;
    }

    /**
     * Get the error message if library loading failed.
     */
    public static String getLoadError() {
        return loadError;
    }

    /**
     * Get the singleton NativeWorker instance.
     * Returns null if not yet created.
     */
    public static NativeWorker getInstance() {
        return instance;
    }

    /**
     * Constructor. Sets the singleton instance.
     */
    public NativeWorker() {
        instance = this;
    }

    // ==================== Native Methods ====================

    /**
     * Initialize the native worker with coordinator connection info and
     * Android app directories.
     *
     * @param coordinatorUrl URL of the Marabunta coordinator / bootstrap server
     * @param orgKey Organization key for authentication
     * @param dataDir App files directory from Context.getFilesDir()
     * @param cacheDir App cache directory from Context.getCacheDir()
     * @throws RuntimeException if initialization fails
     */
    public native void initialize(String coordinatorUrl, String orgKey,
                                  String dataDir, String cacheDir);

    /**
     * Shutdown the native worker gracefully.
     *
     * This will:
     * - Complete any in-progress task
     * - Close coordinator connection
     * - Shut down the Tokio runtime
     * - Release all resources
     */
    public native void shutdown();

    /**
     * Fetch and execute the next available task.
     *
     * This is a blocking call that:
     * 1. Checks preconditions (connected, not paused, network OK, thermal OK)
     * 2. Requests a task from the coordinator
     * 3. Executes the task via TaskExecutor
     * 4. Reports results to coordinator
     *
     * @return TaskResult with execution details, or null if no task available
     */
    public native TaskResult executeNextTask();

    /**
     * Get current worker statistics.
     *
     * @return WorkerStats with counters and connection status
     */
    public native WorkerStats getStats();

    /**
     * Set maximum CPU usage percentage.
     *
     * @param percent Maximum CPU percentage (1-100)
     */
    public native void setMaxCpuPercent(int percent);

    /**
     * Set maximum memory usage in MB.
     *
     * @param mb Maximum memory in megabytes (64+)
     */
    public native void setMaxMemoryMb(int mb);

    /**
     * Get the unique node ID assigned to this worker.
     *
     * @return Node ID string
     */
    public native String getNodeId();

    /**
     * Check if connected to coordinator.
     *
     * @return true if connected
     */
    public native boolean isConnected();

    /**
     * Reconnect to coordinator if disconnected.
     *
     * @return true if reconnection successful
     */
    public native boolean reconnect();

    /**
     * Inform the native layer of the current network connectivity state.
     *
     * Called by NetworkMonitor when connectivity changes.
     *
     * @param available true if network is available
     */
    public native void setNetworkState(boolean available);

    /**
     * Inform the native layer of the current thermal status.
     *
     * Called by ThermalMonitor when thermal state changes.
     *
     * @param status ThermalMonitor.ThermalStatus ordinal
     *               (0=NOMINAL, 1=LIGHT, 2=MODERATE, 3=SEVERE, ...)
     */
    public native void setThermalState(int status);

    /**
     * Pause task execution. Already-running tasks are not cancelled;
     * only new task fetching is suppressed.
     */
    public native void pauseCompute();

    /**
     * Resume task execution after a pause.
     */
    public native void resumeCompute();

    /**
     * Set the path to the Toybox tools directory.
     *
     * This directory is prepended to PATH when executing shell tasks,
     * providing coreutils (ls, grep, cat, awk, etc.) on Android.
     *
     * @param path Absolute path from ToyboxManager.getToolsDir()
     */
    public native void setToolsDir(String path);

    /**
     * Configure Python interpreter path, PYTHONHOME, and venv directory.
     *
     * Pass empty strings to disable Python support (Python tasks will be
     * declined and the coordinator will route them elsewhere).
     *
     * @param pythonPath Absolute path to python3 binary, or ""
     * @param pythonHome PYTHONHOME for bundled Python, or ""
     * @param venvDir    Directory for virtual environments, or ""
     */
    public native void setPythonConfig(String pythonPath, String pythonHome, String venvDir);

    /**
     * Get worker capabilities as a pipe-delimited string.
     *
     * Format: "shell=1|python=0|monte_carlo=1|parameter_sweep=1|wasm=0"
     *
     * @return capabilities string, or null if not initialized
     */
    public native String getCapabilities();

    // ==================== Data Classes ====================

    /**
     * Result of a task execution.
     */
    public static class TaskResult {
        /** Unique task identifier */
        public String taskId;

        /** Whether the task completed successfully */
        public boolean success;

        /** Error message if task failed */
        public String errorMessage;

        /** Tokens earned for this task */
        public long tokensEarned;

        /** Execution duration in milliseconds */
        public long durationMs;

        /** Memory used in bytes */
        public long memoryUsedBytes;

        /** Output size in bytes */
        public long outputSizeBytes;

        @Override
        public String toString() {
            return "TaskResult{" +
                    "taskId='" + taskId + '\'' +
                    ", success=" + success +
                    ", tokensEarned=" + tokensEarned +
                    ", durationMs=" + durationMs +
                    '}';
        }
    }

    /**
     * Worker statistics.
     */
    public static class WorkerStats {
        /** Total tasks completed successfully */
        public long tasksCompleted;

        /** Total tasks failed */
        public long tasksFailed;

        /** Total tokens earned */
        public long tokensEarned;

        /** Total compute time in milliseconds */
        public long totalComputeTimeMs;

        /** Whether connected to coordinator */
        public boolean connected;

        /** Current coordinator URL */
        public String coordinatorUrl;

        /** Time since last heartbeat in milliseconds */
        public long timeSinceHeartbeatMs;

        /** Number of WASM modules cached */
        public int cachedModules;

        /** Current CPU usage percentage */
        public int cpuUsagePercent;

        /** Current memory usage in MB */
        public int memoryUsageMb;

        @Override
        public String toString() {
            return "WorkerStats{" +
                    "tasksCompleted=" + tasksCompleted +
                    ", tasksFailed=" + tasksFailed +
                    ", tokensEarned=" + tokensEarned +
                    ", connected=" + connected +
                    '}';
        }
    }

    // ==================== Mock Implementation ====================
    // These methods provide a mock implementation when native library is not available
    // This allows the app to be tested without the native library

    private boolean mockInitialized = false;
    private long mockTasksCompleted = 0;
    private long mockTokensEarned = 0;

    /**
     * Mock initialize for testing without native library.
     */
    public void initializeMock(String coordinatorUrl, String orgKey) {
        if (!libraryLoaded) {
            Log.i(TAG, "Using mock implementation");
            mockInitialized = true;
        } else {
            initialize(coordinatorUrl, orgKey,
                       "/data/data/com.marabunta.worker/files",
                       "/data/data/com.marabunta.worker/cache");
        }
    }

    /**
     * Mock execute for testing without native library.
     */
    public TaskResult executeNextTaskMock() {
        if (!libraryLoaded) {
            // Simulate task execution
            try {
                Thread.sleep(1000 + (long)(Math.random() * 2000));
            } catch (InterruptedException e) {
                return null;
            }

            TaskResult result = new TaskResult();
            result.taskId = "mock_" + System.currentTimeMillis();
            result.success = Math.random() > 0.1;  // 90% success rate
            result.tokensEarned = result.success ? (long)(Math.random() * 100) : 0;
            result.durationMs = 1000 + (long)(Math.random() * 2000);

            if (result.success) {
                mockTasksCompleted++;
                mockTokensEarned += result.tokensEarned;
            }

            return result;
        } else {
            return executeNextTask();
        }
    }

    /**
     * Mock stats for testing without native library.
     */
    public WorkerStats getStatsMock() {
        if (!libraryLoaded) {
            WorkerStats stats = new WorkerStats();
            stats.tasksCompleted = mockTasksCompleted;
            stats.tasksFailed = 0;
            stats.tokensEarned = mockTokensEarned;
            stats.connected = mockInitialized;
            stats.coordinatorUrl = "mock://localhost";
            return stats;
        } else {
            return getStats();
        }
    }
}
