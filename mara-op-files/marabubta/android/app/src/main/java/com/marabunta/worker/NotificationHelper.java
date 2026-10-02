// Marabunta - Licensed under the MIT License.
package com.marabunta.worker;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.content.Context;
import android.content.Intent;
import android.graphics.Color;
import android.os.Build;

import androidx.core.app.NotificationCompat;

/**
 * Helper class for creating and managing notifications.
 *
 * Handles the complexity of notification channels (Android 8.0+)
 * and provides convenient methods for updating the foreground notification.
 */
public class NotificationHelper {

    private static final String TAG = "NotificationHelper";

    // Notification IDs
    public static final int NOTIFICATION_ID_SERVICE = 1;
    public static final int NOTIFICATION_ID_TASK_COMPLETE = 2;
    public static final int NOTIFICATION_ID_ERROR = 3;

    // Channel IDs
    public static final String CHANNEL_SERVICE = "marabunta_worker_service";
    public static final String CHANNEL_UPDATES = "marabunta_worker_updates";
    public static final String CHANNEL_ERRORS = "marabunta_worker_errors";

    private final Context context;
    private final NotificationManager notificationManager;
    private final String defaultChannelId;

    public NotificationHelper(Context context, String defaultChannelId) {
        this.context = context;
        this.defaultChannelId = defaultChannelId;
        this.notificationManager =
            (NotificationManager) context.getSystemService(Context.NOTIFICATION_SERVICE);

        // Create notification channels for Android 8.0+
        createNotificationChannels();
    }

    /**
     * Create all notification channels.
     * Channels are only created once - subsequent calls are no-ops.
     */
    private void createNotificationChannels() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) {
            return;  // Channels not needed before Android 8.0
        }

        // Service channel - Low importance (no sound, always visible)
        NotificationChannel serviceChannel = new NotificationChannel(
            CHANNEL_SERVICE,
            "Worker Status",
            NotificationManager.IMPORTANCE_LOW
        );
        serviceChannel.setDescription("Shows current Marabunta Worker status");
        serviceChannel.setShowBadge(false);
        serviceChannel.enableLights(false);
        serviceChannel.enableVibration(false);
        serviceChannel.setLockscreenVisibility(Notification.VISIBILITY_PUBLIC);

        // Updates channel - Default importance (can show sound)
        NotificationChannel updatesChannel = new NotificationChannel(
            CHANNEL_UPDATES,
            "Task Updates",
            NotificationManager.IMPORTANCE_DEFAULT
        );
        updatesChannel.setDescription("Notifications about completed tasks and earnings");
        updatesChannel.setShowBadge(true);
        updatesChannel.enableLights(true);
        updatesChannel.setLightColor(Color.GREEN);

        // Errors channel - High importance
        NotificationChannel errorsChannel = new NotificationChannel(
            CHANNEL_ERRORS,
            "Errors",
            NotificationManager.IMPORTANCE_HIGH
        );
        errorsChannel.setDescription("Important errors and issues");
        errorsChannel.setShowBadge(true);
        errorsChannel.enableLights(true);
        errorsChannel.setLightColor(Color.RED);

        // Register channels
        notificationManager.createNotificationChannel(serviceChannel);
        notificationManager.createNotificationChannel(updatesChannel);
        notificationManager.createNotificationChannel(errorsChannel);
    }

    /**
     * Create a foreground service notification.
     */
    public Notification createServiceNotification(
            String title,
            String content,
            boolean isPaused) {

        // Main tap intent - opens activity
        Intent mainIntent = new Intent(context, MainActivity.class);
        mainIntent.setFlags(Intent.FLAG_ACTIVITY_SINGLE_TOP);
        PendingIntent mainPendingIntent = PendingIntent.getActivity(
            context, 0, mainIntent,
            PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE
        );

        // Pause/Resume action
        Intent pauseIntent = new Intent(context, MarabuntaWorkerService.class);
        pauseIntent.setAction(isPaused ?
            MarabuntaWorkerService.ACTION_RESUME : MarabuntaWorkerService.ACTION_PAUSE);
        PendingIntent pausePendingIntent = PendingIntent.getService(
            context, 1, pauseIntent,
            PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE
        );

        // Stop action
        Intent stopIntent = new Intent(context, MarabuntaWorkerService.class);
        stopIntent.setAction(MarabuntaWorkerService.ACTION_STOP);
        PendingIntent stopPendingIntent = PendingIntent.getService(
            context, 2, stopIntent,
            PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE
        );

        NotificationCompat.Builder builder = new NotificationCompat.Builder(context, CHANNEL_SERVICE)
            .setContentTitle(title)
            .setContentText(content)
            .setSmallIcon(R.drawable.ic_marabunta)
            .setContentIntent(mainPendingIntent)
            .setOngoing(true)
            .setOnlyAlertOnce(true)
            .setShowWhen(false)
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .setCategory(NotificationCompat.CATEGORY_SERVICE)
            .setVisibility(NotificationCompat.VISIBILITY_PUBLIC)
            .addAction(
                isPaused ? android.R.drawable.ic_media_play : android.R.drawable.ic_media_pause,
                isPaused ? "Resume" : "Pause",
                pausePendingIntent
            )
            .addAction(
                android.R.drawable.ic_delete,
                "Stop",
                stopPendingIntent
            );

        return builder.build();
    }

    /**
     * Create a progress notification for task execution.
     */
    public Notification createProgressNotification(
            String title,
            String content,
            int progress,
            int max) {

        Intent mainIntent = new Intent(context, MainActivity.class);
        PendingIntent mainPendingIntent = PendingIntent.getActivity(
            context, 0, mainIntent,
            PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE
        );

        NotificationCompat.Builder builder = new NotificationCompat.Builder(context, CHANNEL_SERVICE)
            .setContentTitle(title)
            .setContentText(content)
            .setSmallIcon(R.drawable.ic_marabunta)
            .setContentIntent(mainPendingIntent)
            .setOngoing(true)
            .setOnlyAlertOnce(true)
            .setProgress(max, progress, progress == 0)
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .setCategory(NotificationCompat.CATEGORY_PROGRESS);

        return builder.build();
    }

    /**
     * Show a task completion notification.
     */
    public void showTaskCompleteNotification(long tasksCompleted, long tokensEarned) {
        String title = "Tasks Completed!";
        String content = String.format("Completed %d tasks, earned %d tokens", tasksCompleted, tokensEarned);

        Intent mainIntent = new Intent(context, MainActivity.class);
        PendingIntent mainPendingIntent = PendingIntent.getActivity(
            context, 0, mainIntent,
            PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE
        );

        Notification notification = new NotificationCompat.Builder(context, CHANNEL_UPDATES)
            .setContentTitle(title)
            .setContentText(content)
            .setSmallIcon(R.drawable.ic_marabunta)
            .setContentIntent(mainPendingIntent)
            .setAutoCancel(true)
            .setPriority(NotificationCompat.PRIORITY_DEFAULT)
            .setCategory(NotificationCompat.CATEGORY_STATUS)
            .build();

        notificationManager.notify(NOTIFICATION_ID_TASK_COMPLETE, notification);
    }

    /**
     * Show an error notification.
     */
    public void showErrorNotification(String error) {
        String title = "Marabunta Worker Error";

        Intent mainIntent = new Intent(context, MainActivity.class);
        PendingIntent mainPendingIntent = PendingIntent.getActivity(
            context, 0, mainIntent,
            PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE
        );

        Notification notification = new NotificationCompat.Builder(context, CHANNEL_ERRORS)
            .setContentTitle(title)
            .setContentText(error)
            .setSmallIcon(R.drawable.ic_marabunta)
            .setContentIntent(mainPendingIntent)
            .setAutoCancel(true)
            .setPriority(NotificationCompat.PRIORITY_HIGH)
            .setCategory(NotificationCompat.CATEGORY_ERROR)
            .build();

        notificationManager.notify(NOTIFICATION_ID_ERROR, notification);
    }

    /**
     * Update the service notification.
     */
    public void updateServiceNotification(String title, String content, boolean isPaused) {
        Notification notification = createServiceNotification(title, content, isPaused);
        notificationManager.notify(NOTIFICATION_ID_SERVICE, notification);
    }

    /**
     * Cancel a notification by ID.
     */
    public void cancel(int notificationId) {
        notificationManager.cancel(notificationId);
    }

    /**
     * Cancel all notifications.
     */
    public void cancelAll() {
        notificationManager.cancelAll();
    }
}
