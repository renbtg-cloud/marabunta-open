// Marabunta - Licensed under the MIT License.
package com.marabunta.worker;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Context;
import android.content.Intent;
import android.os.Binder;
import android.os.Build;
import android.os.Handler;
import android.os.IBinder;
import android.os.Looper;
import android.os.PowerManager;
import android.util.Log;

import androidx.core.app.NotificationCompat;

/**
 * Foreground service that runs the Marabunta Worker.
 *
 * Foreground services can't be killed by Android's memory management,
 * but require a persistent notification. This service:
 *
 * - Runs continuously in the background
 * - Holds a partial wake lock to keep CPU running
 * - Only computes when phone is idle and charging
 * - Updates notification with current status
 * - Survives app being closed
 */
public class MarabuntaWorkerService extends Service {

    private static final String TAG = "MarabuntaWorkerService";
    private static final int NOTIFICATION_ID = 1;
    private static final String CHANNEL_ID = "marabunta_worker_channel";
    private static final String CHANNEL_NAME = "Marabunta Worker Status";

    // Actions for controlling the service
    public static final String ACTION_START = "com.marabunta.worker.START";
    public static final String ACTION_STOP = "com.marabunta.worker.STOP";
    public static final String ACTION_PAUSE = "com.marabunta.worker.PAUSE";
    public static final String ACTION_RESUME = "com.marabunta.worker.RESUME";

    // Wake lock tag
    private static final String WAKE_LOCK_TAG = "Marabunta::WorkerWakeLock";

    // Components
    private PowerManager.WakeLock wakeLock;
    private NativeWorker nativeWorker;
    private PowerMonitor powerMonitor;
    private IdleDetector idleDetector;
    private NetworkMonitor networkMonitor;
    private ThermalMonitor thermalMonitor;
    private ToyboxManager toyboxManager;
    private PythonManager pythonManager;
    private NotificationHelper notificationHelper;
    private Handler mainHandler;

    // State
    private volatile boolean isRunning = false;
    private volatile boolean isPaused = false;
    private volatile boolean isComputing = false;
    private Thread workerThread;

    // Statistics
    private long tasksCompleted = 0;
    private long tasksFailed = 0;
    private long tokensEarned = 0;
    private long sessionStartTime = 0;
    private String currentStatus = "Initializing...";

    // Binder for activity communication
    private final IBinder binder = new LocalBinder();

    public class LocalBinder extends Binder {
        public MarabuntaWorkerService getService() {
            return MarabuntaWorkerService.this;
        }
    }

    // Status listener for UI updates
    public interface StatusListener {
        void onStatusChanged(String status, long tasks, long tokens, boolean computing);
    }

    private StatusListener statusListener;

    @Override
    public void onCreate() {
        super.onCreate();
        Log.i(TAG, "MarabuntaWorkerService onCreate");

        mainHandler = new Handler(Looper.getMainLooper());

        // Create notification channel (required for Android 8+)
        createNotificationChannel();

        // Initialize notification helper
        notificationHelper = new NotificationHelper(this, CHANNEL_ID);

        // Start as foreground service immediately to prevent ANR
        startForeground(NOTIFICATION_ID, createNotification("Initializing..."));

        // Acquire partial wake lock to keep CPU running
        acquireWakeLock();

        // Initialize monitoring components
        powerMonitor = new PowerMonitor(this);
        idleDetector = new IdleDetector(this);
        networkMonitor = new NetworkMonitor(this);
        thermalMonitor = new ThermalMonitor(this);

        // Initialize shell & Python support
        toyboxManager = new ToyboxManager(this);
        pythonManager = new PythonManager(this);

        // Extract Toybox tools (first run: extract + symlink, subsequent: fast verify)
        if (toyboxManager.setup()) {
            Log.i(TAG, "Toybox ready: " + toyboxManager.getAvailableCommands().length +
                  " commands in " + toyboxManager.getToolsDir());
        } else {
            Log.w(TAG, "Toybox not available — shell tasks will use system commands only");
        }

        // Detect Python availability
        if (pythonManager.setup()) {
            Log.i(TAG, "Python ready: " + pythonManager.getStatusSummary());
        } else {
            Log.i(TAG, "Python not available — Python tasks will be declined");
        }

        // Initialize native worker
        initializeNativeWorker();

        // Start network and thermal monitoring (after native worker init so
        // callbacks can forward state to the native layer)
        networkMonitor.start();
        thermalMonitor.start();

        // Load persisted stats
        MarabuntaWorkerApp app = MarabuntaWorkerApp.getInstance();
        tasksCompleted = app.getTotalTasksCompleted();
        tokensEarned = app.getTotalTokensEarned();

        sessionStartTime = System.currentTimeMillis();
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        Log.i(TAG, "onStartCommand: " + (intent != null ? intent.getAction() : "null"));

        String action = intent != null ? intent.getAction() : null;

        if (ACTION_STOP.equals(action)) {
            stopWorker();
            stopForeground(true);
            stopSelf();
            return START_NOT_STICKY;
        } else if (ACTION_PAUSE.equals(action)) {
            pauseWorker();
        } else if (ACTION_RESUME.equals(action)) {
            resumeWorker();
        } else {
            // Start or restart worker
            startWorker();
        }

        // Return STICKY so Android restarts service if killed
        return START_STICKY;
    }

    @Override
    public IBinder onBind(Intent intent) {
        return binder;
    }

    @Override
    public void onDestroy() {
        Log.i(TAG, "MarabuntaWorkerService onDestroy");

        stopWorker();

        // Release wake lock
        if (wakeLock != null && wakeLock.isHeld()) {
            wakeLock.release();
        }

        // Cleanup native resources
        if (nativeWorker != null) {
            try {
                nativeWorker.shutdown();
            } catch (Exception e) {
                Log.e(TAG, "Error shutting down native worker", e);
            }
        }

        // Unregister monitors
        if (powerMonitor != null) {
            powerMonitor.unregister();
        }
        if (networkMonitor != null) {
            networkMonitor.stop();
        }
        if (thermalMonitor != null) {
            thermalMonitor.stop();
        }

        // Save statistics
        MarabuntaWorkerApp.getInstance().saveStats(tasksCompleted, tokensEarned);

        super.onDestroy();
    }

    private void createNotificationChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            NotificationChannel channel = new NotificationChannel(
                CHANNEL_ID,
                CHANNEL_NAME,
                NotificationManager.IMPORTANCE_LOW  // Low importance = no sound
            );
            channel.setDescription("Shows Marabunta Worker status and statistics");
            channel.setShowBadge(false);

            NotificationManager notificationManager =
                getSystemService(NotificationManager.class);
            notificationManager.createNotificationChannel(channel);
        }
    }

    private Notification createNotification(String status) {
        // Intent to open main activity
        Intent mainIntent = new Intent(this, MainActivity.class);
        mainIntent.setFlags(Intent.FLAG_ACTIVITY_SINGLE_TOP);
        PendingIntent pendingIntent = PendingIntent.getActivity(
            this, 0, mainIntent,
            PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE
        );

        // Pause/Resume action
        Intent actionIntent = new Intent(this, MarabuntaWorkerService.class);
        actionIntent.setAction(isPaused ? ACTION_RESUME : ACTION_PAUSE);
        PendingIntent actionPendingIntent = PendingIntent.getService(
            this, 1, actionIntent,
            PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE
        );

        // Stop action
        Intent stopIntent = new Intent(this, MarabuntaWorkerService.class);
        stopIntent.setAction(ACTION_STOP);
        PendingIntent stopPendingIntent = PendingIntent.getService(
            this, 2, stopIntent,
            PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE
        );

        String contentText = String.format("Tasks: %d | Tokens: %d", tasksCompleted, tokensEarned);

        NotificationCompat.Builder builder = new NotificationCompat.Builder(this, CHANNEL_ID)
            .setContentTitle("Marabunta Worker - " + status)
            .setContentText(contentText)
            .setSmallIcon(R.drawable.ic_marabunta)
            .setContentIntent(pendingIntent)
            .setOngoing(true)
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .setCategory(NotificationCompat.CATEGORY_SERVICE)
            .addAction(
                isPaused ? android.R.drawable.ic_media_play : android.R.drawable.ic_media_pause,
                isPaused ? "Resume" : "Pause",
                actionPendingIntent
            )
            .addAction(
                android.R.drawable.ic_delete,
                "Stop",
                stopPendingIntent
            );

        return builder.build();
    }

    private void updateNotification(String status) {
        this.currentStatus = status;
        NotificationManager notificationManager =
            (NotificationManager) getSystemService(Context.NOTIFICATION_SERVICE);
        notificationManager.notify(NOTIFICATION_ID, createNotification(status));

        // Notify listener on main thread
        if (statusListener != null) {
            mainHandler.post(() -> {
                if (statusListener != null) {
                    statusListener.onStatusChanged(status, tasksCompleted, tokensEarned, isComputing);
                }
            });
        }
    }

    private void acquireWakeLock() {
        PowerManager powerManager = (PowerManager) getSystemService(Context.POWER_SERVICE);
        wakeLock = powerManager.newWakeLock(
            PowerManager.PARTIAL_WAKE_LOCK,
            WAKE_LOCK_TAG
        );
        wakeLock.acquire();
        Log.i(TAG, "Wake lock acquired");
    }

    private void initializeNativeWorker() {
        try {
            nativeWorker = new NativeWorker();
            MarabuntaWorkerApp app = MarabuntaWorkerApp.getInstance();
            nativeWorker.initialize(
                app.getCoordinatorUrl(),
                app.getOrgKey(),
                getFilesDir().getAbsolutePath(),
                getCacheDir().getAbsolutePath()
            );
            nativeWorker.setMaxCpuPercent(app.getMaxCpuPercent());
            nativeWorker.setMaxMemoryMb(app.getMaxMemoryMb());

            // Configure shell tools path (Toybox)
            if (toyboxManager != null && toyboxManager.isReady()) {
                nativeWorker.setToolsDir(toyboxManager.getToolsDir());
            }

            // Configure Python interpreter
            if (pythonManager != null && pythonManager.isPythonAvailable()) {
                nativeWorker.setPythonConfig(
                    pythonManager.getPythonPath(),
                    pythonManager.getPythonHome() != null ? pythonManager.getPythonHome() : "",
                    pythonManager.getVenvDir()
                );
            } else {
                // Explicitly disable Python on the native side
                nativeWorker.setPythonConfig("", "", "");
            }

            Log.i(TAG, "Native worker initialized (capabilities: " +
                  nativeWorker.getCapabilities() + ")");
        } catch (UnsatisfiedLinkError e) {
            Log.e(TAG, "Failed to load native library", e);
            updateNotification("Error: Native library not found");
        } catch (Exception e) {
            Log.e(TAG, "Failed to initialize native worker", e);
            updateNotification("Error: " + e.getMessage());
        }
    }

    private void startWorker() {
        if (isRunning) {
            Log.w(TAG, "Worker already running");
            return;
        }

        isRunning = true;
        isPaused = false;

        workerThread = new Thread(this::workerLoop, "MarabuntaWorkerThread");
        workerThread.start();

        Log.i(TAG, "Worker started");
        updateNotification("Running");
    }

    private void stopWorker() {
        isRunning = false;

        if (workerThread != null) {
            workerThread.interrupt();
            try {
                workerThread.join(5000);
            } catch (InterruptedException e) {
                Log.w(TAG, "Interrupted waiting for worker thread");
            }
            workerThread = null;
        }

        Log.i(TAG, "Worker stopped");
    }

    private void pauseWorker() {
        isPaused = true;
        if (nativeWorker != null) {
            nativeWorker.pauseCompute();
        }
        updateNotification("Paused");
        Log.i(TAG, "Worker paused");
    }

    private void resumeWorker() {
        isPaused = false;
        if (nativeWorker != null) {
            nativeWorker.resumeCompute();
        }
        updateNotification("Running");
        Log.i(TAG, "Worker resumed");
    }

    /**
     * Main worker loop.
     *
     * Continuously checks conditions and executes tasks when appropriate.
     */
    private void workerLoop() {
        Log.i(TAG, "Worker loop started");

        while (isRunning) {
            try {
                // Check if we should compute
                boolean shouldCompute = canCompute();

                if (shouldCompute) {
                    isComputing = true;
                    updateNotification("Computing...");

                    // Execute next task via native code
                    NativeWorker.TaskResult result = nativeWorker.executeNextTask();

                    if (result != null) {
                        if (result.success) {
                            tasksCompleted++;
                            tokensEarned += result.tokensEarned;
                            Log.i(TAG, "Task completed: " + result.taskId +
                                  ", tokens: " + result.tokensEarned);
                        } else {
                            tasksFailed++;
                            Log.w(TAG, "Task failed: " + result.taskId);
                        }
                        updateNotification("Active");
                    } else {
                        // No task available, wait before checking again
                        updateNotification("Waiting for tasks...");
                        Thread.sleep(10000);  // 10 seconds
                    }

                    isComputing = false;
                } else {
                    // Not ready to compute, wait and check again
                    isComputing = false;

                    String waitReason = getWaitReason();
                    updateNotification(waitReason);

                    Thread.sleep(5000);  // 5 seconds
                }

                // Periodic stats save
                if (tasksCompleted % 10 == 0 && tasksCompleted > 0) {
                    MarabuntaWorkerApp.getInstance().saveStats(tasksCompleted, tokensEarned);
                }

            } catch (InterruptedException e) {
                Log.i(TAG, "Worker thread interrupted");
                break;
            } catch (Exception e) {
                Log.e(TAG, "Error in worker loop", e);
                updateNotification("Error: " + e.getMessage());

                try {
                    Thread.sleep(30000);  // Wait 30 seconds before retrying
                } catch (InterruptedException ie) {
                    break;
                }
            }
        }

        isComputing = false;
        Log.i(TAG, "Worker loop ended");
    }

    /**
     * Check if conditions allow computing.
     */
    private boolean canCompute() {
        // User paused
        if (isPaused) {
            return false;
        }

        // Native worker not ready
        if (nativeWorker == null) {
            return false;
        }

        // Must have network connectivity
        if (networkMonitor != null && !networkMonitor.canUseNetwork()) {
            return false;
        }

        // Must not be thermally throttled
        if (thermalMonitor != null && !thermalMonitor.canCompute()) {
            return false;
        }

        // Must be plugged in (or battery mode enabled with sufficient charge)
        MarabuntaWorkerApp app = MarabuntaWorkerApp.getInstance();
        if (!powerMonitor.canCompute(app.canComputeOnBattery())) {
            return false;
        }

        // Must be idle (screen off or locked)
        if (!idleDetector.isIdle()) {
            return false;
        }

        return true;
    }

    /**
     * Get human-readable reason for waiting.
     */
    private String getWaitReason() {
        if (isPaused) {
            return "Paused";
        }

        if (networkMonitor != null && !networkMonitor.canUseNetwork()) {
            return "No network";
        }

        if (thermalMonitor != null && !thermalMonitor.canCompute()) {
            return "Thermal throttle";
        }

        if (!powerMonitor.isPluggedIn()) {
            int battery = powerMonitor.getBatteryPercent();
            return "Waiting for charger (" + battery + "%)";
        }

        if (!idleDetector.isIdle()) {
            return "Waiting for idle...";
        }

        return "Ready";
    }

    // Public methods for activity binding

    public void setStatusListener(StatusListener listener) {
        this.statusListener = listener;
        // Immediately notify with current status
        if (listener != null) {
            listener.onStatusChanged(currentStatus, tasksCompleted, tokensEarned, isComputing);
        }
    }

    public boolean isWorkerRunning() {
        return isRunning;
    }

    public boolean isWorkerPaused() {
        return isPaused;
    }

    public boolean isWorkerComputing() {
        return isComputing;
    }

    public String getCurrentStatus() {
        return currentStatus;
    }

    public long getTasksCompleted() {
        return tasksCompleted;
    }

    public long getTokensEarned() {
        return tokensEarned;
    }

    public long getSessionDurationMs() {
        return System.currentTimeMillis() - sessionStartTime;
    }

    public NativeWorker.WorkerStats getNativeStats() {
        if (nativeWorker != null) {
            return nativeWorker.getStats();
        }
        return null;
    }
}
