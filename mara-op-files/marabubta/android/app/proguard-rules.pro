# Marabunta Worker ProGuard Rules
#
# CRITICAL: These rules ensure that JNI, Android framework callbacks,
# and reflection-based access all survive R8/ProGuard minification.

# ======================================================================
# JNI: Keep ALL native methods across all classes
# ======================================================================
-keepclasseswithmembernames class * {
    native <methods>;
}

# ======================================================================
# NativeWorker: The Rust JNI bridge and its inner classes
# ======================================================================
-keep class com.marabunta.worker.NativeWorker { *; }
-keep class com.marabunta.worker.NativeWorker$TaskResult { *; }
-keep class com.marabunta.worker.NativeWorker$WorkerStats { *; }

# ======================================================================
# Service + Application: Accessed from native code and Android framework
# ======================================================================
-keep class com.marabunta.worker.MarabuntaWorkerService { *; }
-keep class com.marabunta.worker.MarabuntaWorkerService$LocalBinder { *; }
-keep class com.marabunta.worker.MarabuntaWorkerService$StatusListener { *; }
-keep class com.marabunta.worker.MarabuntaWorkerApp { *; }
-keep class com.marabunta.worker.MainActivity { *; }

# ======================================================================
# Broadcast receivers: Instantiated by Android framework via manifest
# ======================================================================
-keep class com.marabunta.worker.BootReceiver { *; }
-keep class com.marabunta.worker.PowerStateReceiver { *; }

# ======================================================================
# Monitor classes: Callbacks from Android system APIs
# ======================================================================
-keep class com.marabunta.worker.PowerMonitor { *; }
-keep class com.marabunta.worker.IdleDetector { *; }
-keep class com.marabunta.worker.NetworkMonitor { *; }
-keep class com.marabunta.worker.NetworkMonitor$* { *; }
-keep class com.marabunta.worker.ThermalMonitor { *; }
-keep class com.marabunta.worker.ThermalMonitor$ThermalStatus { *; }

# ======================================================================
# Tool managers: Use SharedPreferences keys, asset paths, reflection
# ======================================================================
-keep class com.marabunta.worker.ToyboxManager { *; }
-keep class com.marabunta.worker.PythonManager { *; }
-keep class com.marabunta.worker.NotificationHelper { *; }

# ======================================================================
# Android API compatibility: suppress warnings for multi-API-level code
# ======================================================================
-dontwarn android.os.PowerManager$OnThermalStatusChangedListener
-dontwarn android.net.ConnectivityManager$NetworkCallback
-dontwarn android.system.Os

# ======================================================================
# Optimization: keep source file names for stack traces
# ======================================================================
-keepattributes SourceFile,LineNumberTable
-renamesourcefileattribute SourceFile

# ======================================================================
# Serialization: keep enum values (used in ThermalMonitor.ThermalStatus)
# ======================================================================
-keepclassmembers enum * {
    public static **[] values();
    public static ** valueOf(java.lang.String);
}
