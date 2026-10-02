// Marabunta - Licensed under the MIT License.
package com.marabunta.worker;

import android.content.Context;
import android.net.ConnectivityManager;
import android.net.LinkProperties;
import android.net.Network;
import android.net.NetworkCapabilities;
import android.net.NetworkRequest;
import android.os.Build;
import android.util.Log;

/**
 * Monitors network connectivity changes and notifies the native worker.
 * Uses ConnectivityManager.NetworkCallback for real-time state updates.
 *
 * Network awareness rules:
 * - ALWAYS require connectivity before attempting task fetch
 * - PREFER WiFi for large data transfers (task payloads > 1 MB)
 * - AVOID metered connections for large uploads/downloads
 * - NOTIFY native layer of connectivity changes immediately
 *
 * This ensures we don't waste cellular data or attempt tasks without connectivity.
 */
public class NetworkMonitor {

    private static final String TAG = "NetworkMonitor";

    private final Context context;
    private final ConnectivityManager connectivityManager;

    // Current state
    private volatile boolean isConnected = false;
    private volatile boolean isWifi = false;
    private volatile boolean isCellular = false;
    private volatile boolean isMetered = false;

    // Network callback for API 21+
    private ConnectivityManager.NetworkCallback networkCallback;

    private boolean callbackRegistered = false;

    public NetworkMonitor(Context context) {
        this.context = context;
        this.connectivityManager =
                (ConnectivityManager) context.getSystemService(Context.CONNECTIVITY_SERVICE);

        // Get initial state
        updateState();
    }

    /**
     * Start monitoring network changes.
     */
    public void start() {
        if (callbackRegistered) {
            return;
        }

        networkCallback = new ConnectivityManager.NetworkCallback() {
            @Override
            public void onAvailable(Network network) {
                Log.d(TAG, "Network available");
                updateState();
                notifyNativeLayer();
            }

            @Override
            public void onLost(Network network) {
                Log.d(TAG, "Network lost");
                isConnected = false;
                isWifi = false;
                isCellular = false;
                isMetered = false;
                notifyNativeLayer();
            }

            @Override
            public void onCapabilitiesChanged(Network network,
                                              NetworkCapabilities capabilities) {
                boolean wasConnected = isConnected;
                updateStateFromCapabilities(capabilities);

                if (wasConnected != isConnected) {
                    notifyNativeLayer();
                }

                Log.d(TAG, "Network capabilities changed: connected=" + isConnected +
                      ", wifi=" + isWifi +
                      ", cellular=" + isCellular +
                      ", metered=" + isMetered);
            }

            @Override
            public void onLinkPropertiesChanged(Network network,
                                                LinkProperties linkProperties) {
                Log.d(TAG, "Link properties changed");
            }
        };

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N) {
            // API 24+: registerDefaultNetworkCallback
            connectivityManager.registerDefaultNetworkCallback(networkCallback);
        } else {
            // API 21-23: registerNetworkCallback with a broad request
            NetworkRequest request = new NetworkRequest.Builder()
                    .addCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)
                    .build();
            connectivityManager.registerNetworkCallback(request, networkCallback);
        }

        callbackRegistered = true;
        Log.i(TAG, "Network callback registered");
    }

    /**
     * Stop monitoring network changes.
     */
    public void stop() {
        if (callbackRegistered && networkCallback != null) {
            try {
                connectivityManager.unregisterNetworkCallback(networkCallback);
                callbackRegistered = false;
                Log.i(TAG, "Network callback unregistered");
            } catch (Exception e) {
                Log.w(TAG, "Error unregistering network callback", e);
            }
        }
    }

    /**
     * Update state from the current active network.
     */
    private void updateState() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            Network activeNetwork = connectivityManager.getActiveNetwork();
            if (activeNetwork == null) {
                isConnected = false;
                isWifi = false;
                isCellular = false;
                isMetered = false;
                return;
            }

            NetworkCapabilities capabilities =
                    connectivityManager.getNetworkCapabilities(activeNetwork);
            if (capabilities != null) {
                updateStateFromCapabilities(capabilities);
            } else {
                isConnected = false;
                isWifi = false;
                isCellular = false;
                isMetered = false;
            }
        } else {
            // API 21-22 fallback using deprecated APIs
            android.net.NetworkInfo networkInfo = connectivityManager.getActiveNetworkInfo();
            if (networkInfo != null && networkInfo.isConnected()) {
                isConnected = true;
                isWifi = networkInfo.getType() == ConnectivityManager.TYPE_WIFI;
                isCellular = networkInfo.getType() == ConnectivityManager.TYPE_MOBILE;
                isMetered = connectivityManager.isActiveNetworkMetered();
            } else {
                isConnected = false;
                isWifi = false;
                isCellular = false;
                isMetered = false;
            }
        }

        Log.d(TAG, "Network state updated: connected=" + isConnected +
              ", wifi=" + isWifi +
              ", cellular=" + isCellular +
              ", metered=" + isMetered);
    }

    /**
     * Update state from network capabilities.
     */
    private void updateStateFromCapabilities(NetworkCapabilities capabilities) {
        isConnected = capabilities.hasCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET) &&
                      capabilities.hasCapability(NetworkCapabilities.NET_CAPABILITY_VALIDATED);
        isWifi = capabilities.hasTransport(NetworkCapabilities.TRANSPORT_WIFI);
        isCellular = capabilities.hasTransport(NetworkCapabilities.TRANSPORT_CELLULAR);
        isMetered = !capabilities.hasCapability(NetworkCapabilities.NET_CAPABILITY_NOT_METERED);
    }

    /**
     * Notify the native worker of network state changes.
     */
    private void notifyNativeLayer() {
        try {
            if (NativeWorker.isLibraryLoaded()) {
                NativeWorker.getInstance().setNetworkState(isConnected);
            }
        } catch (Exception e) {
            Log.w(TAG, "Failed to notify native layer of network change", e);
        }
    }

    // ==================== Public API ====================

    /**
     * Check if network is available for task operations.
     *
     * @return true if connected to any network
     */
    public boolean canUseNetwork() {
        return isConnected;
    }

    /**
     * Check if the current connection is unmetered (suitable for large transfers).
     *
     * @return true if on WiFi or otherwise unmetered connection
     */
    public boolean isUnmetered() {
        return isConnected && !isMetered;
    }

    /**
     * Get the current network type as a human-readable string.
     *
     * @return "wifi", "cellular", or "none"
     */
    public String getNetworkType() {
        if (!isConnected) {
            return "none";
        }
        if (isWifi) {
            return "wifi";
        }
        if (isCellular) {
            return "cellular";
        }
        return "none";
    }

    /**
     * Check if currently connected to any network.
     */
    public boolean isConnected() {
        return isConnected;
    }

    /**
     * Check if currently connected via WiFi.
     */
    public boolean isWifi() {
        return isWifi;
    }

    /**
     * Check if currently connected via cellular.
     */
    public boolean isCellular() {
        return isCellular;
    }

    /**
     * Check if the current network is metered.
     */
    public boolean isMetered() {
        return isMetered;
    }

    /**
     * Get a summary string of the current network state.
     */
    public String getStatusSummary() {
        if (!isConnected) {
            return "Disconnected";
        }

        StringBuilder sb = new StringBuilder();
        sb.append(isWifi ? "WiFi" : isCellular ? "Cellular" : "Connected");
        if (isMetered) {
            sb.append(" (Metered)");
        }
        return sb.toString();
    }
}
