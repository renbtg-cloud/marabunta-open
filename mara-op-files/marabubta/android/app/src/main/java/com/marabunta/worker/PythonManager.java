// Marabunta - Licensed under the MIT License.
package com.marabunta.worker;

import android.content.Context;
import android.content.SharedPreferences;
import android.util.Log;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages Python interpreter availability and configuration for Android.
 *
 * Python on Android is non-trivial. There are several strategies:
 *
 * <ol>
 *   <li><b>Bundled CPython</b> — Cross-compiled Python 3.x static binary in
 *       assets, extracted on first run (~30 MB). Most reliable.</li>
 *   <li><b>System Python</b> — Some rooted devices or Termux installs may
 *       have python3 in PATH. Rare on stock Android.</li>
 *   <li><b>None</b> — Python tasks are reported as "unsupported on this worker"
 *       and the coordinator routes them elsewhere. This is the safe default.</li>
 * </ol>
 *
 * <h3>Assets layout (if bundling Python):</h3>
 * <pre>
 * assets/
 *   python/
 *     arm64-v8a/
 *       python3                  # static binary
 *       lib/python3.11/          # standard library (zip or dir)
 *     armeabi-v7a/
 *       ...
 *     version.txt               # e.g., "3.11.8"
 * </pre>
 *
 * <h3>Capability reporting:</h3>
 * The native layer queries {@link #isPythonAvailable()} before accepting
 * Python tasks. If false, the coordinator is informed via capabilities
 * that this worker cannot run Python payloads.
 */
public class PythonManager {

    private static final String TAG = "PythonManager";

    /** SharedPreferences file for Python state */
    private static final String PREFS_NAME = "python_prefs";
    private static final String KEY_PYTHON_PATH = "python_path";
    private static final String KEY_PYTHON_VERSION = "python_version";
    private static final String KEY_PYTHON_SOURCE = "python_source";  // "bundled", "system", "none"

    /** Subdirectory inside app data dir for Python */
    private static final String PYTHON_DIR_NAME = "python";

    /** Asset base path */
    private static final String ASSET_BASE = "python";
    private static final String ASSET_VERSION = ASSET_BASE + "/version.txt";

    /** Timeout for python --version check */
    private static final int VERSION_CHECK_TIMEOUT_SECONDS = 5;

    private final Context context;
    private final File pythonDir;
    private final File venvDir;
    private final SharedPreferences prefs;
    private final AtomicBoolean available = new AtomicBoolean(false);

    private String pythonPath;
    private String pythonVersion;
    private String pythonSource;  // "bundled", "system", "none"

    public PythonManager(Context context) {
        this.context = context.getApplicationContext();
        this.pythonDir = new File(context.getFilesDir(), PYTHON_DIR_NAME);
        this.venvDir = new File(context.getCacheDir(), "marabunta-venvs");
        this.prefs = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE);
    }

    // =====================================================================
    // Public API
    // =====================================================================

    /**
     * Detect and configure Python support.
     *
     * Strategy (in order of preference):
     * <ol>
     *   <li>Check for bundled Python in assets</li>
     *   <li>Check for system python3 in PATH</li>
     *   <li>Mark as unavailable</li>
     * </ol>
     *
     * @return true if Python is available
     */
    public boolean setup() {
        Log.i(TAG, "Detecting Python availability...");

        // 1. Try bundled Python
        if (setupBundled()) {
            return true;
        }

        // 2. Try system Python
        if (detectSystemPython()) {
            return true;
        }

        // 3. Not available
        pythonPath = null;
        pythonVersion = null;
        pythonSource = "none";
        available.set(false);

        prefs.edit()
            .putString(KEY_PYTHON_PATH, "")
            .putString(KEY_PYTHON_VERSION, "")
            .putString(KEY_PYTHON_SOURCE, "none")
            .apply();

        Log.i(TAG, "Python not available. Python tasks will be declined.");
        return false;
    }

    /**
     * @return true if a working Python interpreter is available
     */
    public boolean isPythonAvailable() {
        return available.get();
    }

    /**
     * @return absolute path to the Python interpreter, or null
     */
    public String getPythonPath() {
        return pythonPath;
    }

    /**
     * @return Python version string (e.g., "3.11.8"), or null
     */
    public String getPythonVersion() {
        return pythonVersion;
    }

    /**
     * @return source of Python: "bundled", "system", or "none"
     */
    public String getPythonSource() {
        return pythonSource;
    }

    /**
     * @return absolute path to the virtual environments directory
     */
    public String getVenvDir() {
        return venvDir.getAbsolutePath();
    }

    /**
     * @return absolute path to the Python home directory (for PYTHONHOME), or null
     */
    public String getPythonHome() {
        if ("bundled".equals(pythonSource)) {
            return pythonDir.getAbsolutePath();
        }
        return null;
    }

    /**
     * Clean up old virtual environments older than the given number of hours.
     *
     * @param maxAgeHours max age in hours
     * @return number of venvs removed
     */
    public int cleanupVenvs(int maxAgeHours) {
        if (!venvDir.exists()) return 0;

        long cutoffMs = System.currentTimeMillis() - (maxAgeHours * 3600_000L);
        int removed = 0;

        File[] children = venvDir.listFiles();
        if (children == null) return 0;

        for (File child : children) {
            if (child.isDirectory() && child.lastModified() < cutoffMs) {
                deleteRecursive(child);
                removed++;
                Log.d(TAG, "Removed stale venv: " + child.getName());
            }
        }

        return removed;
    }

    /**
     * Get a human-readable summary of Python status.
     */
    public String getStatusSummary() {
        if (!available.get()) {
            return "Python: not available";
        }
        return String.format("Python %s (%s) at %s", pythonVersion, pythonSource, pythonPath);
    }

    // =====================================================================
    // Bundled Python
    // =====================================================================

    private boolean setupBundled() {
        // Check if we have a bundled Python in assets
        String bundledVersion;
        try {
            bundledVersion = readAssetString(ASSET_VERSION).trim();
        } catch (IOException e) {
            Log.d(TAG, "No bundled Python in assets");
            return false;
        }

        String abi = detectAbi();
        String assetPath = ASSET_BASE + "/" + abi + "/python3";

        // Check if already extracted at correct version
        String installedVersion = prefs.getString(KEY_PYTHON_VERSION, "");
        String installedSource = prefs.getString(KEY_PYTHON_SOURCE, "");
        File pythonBin = new File(pythonDir, "python3");

        if (bundledVersion.equals(installedVersion)
                && "bundled".equals(installedSource)
                && pythonBin.exists()
                && pythonBin.canExecute()) {
            pythonPath = pythonBin.getAbsolutePath();
            pythonVersion = bundledVersion;
            pythonSource = "bundled";
            available.set(true);
            Log.i(TAG, "Bundled Python " + bundledVersion + " already installed");
            return true;
        }

        // Extract
        Log.i(TAG, "Extracting bundled Python " + bundledVersion + " for " + abi);

        if (pythonDir.exists()) {
            deleteRecursive(pythonDir);
        }
        if (!pythonDir.mkdirs()) {
            Log.e(TAG, "Failed to create Python directory: " + pythonDir);
            return false;
        }

        try {
            copyAssetToFile(assetPath, pythonBin);
        } catch (IOException e) {
            Log.e(TAG, "Failed to extract Python binary", e);
            return false;
        }

        if (!pythonBin.setExecutable(true, false)) {
            Log.e(TAG, "Failed to set executable on Python binary");
            return false;
        }

        // Extract standard library if present
        try {
            extractPythonStdlib(abi);
        } catch (IOException e) {
            Log.w(TAG, "Failed to extract Python stdlib (Python may still work for simple scripts)", e);
        }

        // Verify it actually works
        if (!verifyPythonWorks(pythonBin.getAbsolutePath())) {
            Log.e(TAG, "Extracted Python binary does not execute correctly");
            deleteRecursive(pythonDir);
            return false;
        }

        pythonPath = pythonBin.getAbsolutePath();
        pythonVersion = bundledVersion;
        pythonSource = "bundled";
        available.set(true);

        prefs.edit()
            .putString(KEY_PYTHON_PATH, pythonPath)
            .putString(KEY_PYTHON_VERSION, pythonVersion)
            .putString(KEY_PYTHON_SOURCE, "bundled")
            .apply();

        Log.i(TAG, "Bundled Python " + bundledVersion + " installed successfully");
        return true;
    }

    /**
     * Extract the Python standard library from assets.
     */
    private void extractPythonStdlib(String abi) throws IOException {
        // Try to find lib directory in assets
        String libBase = ASSET_BASE + "/" + abi + "/lib";
        try {
            String[] files = context.getAssets().list(libBase);
            if (files == null || files.length == 0) {
                return;
            }
            File libDir = new File(pythonDir, "lib");
            libDir.mkdirs();
            // Recursively extract
            extractAssetDir(libBase, libDir);
        } catch (IOException e) {
            // lib directory doesn't exist in assets — that's OK
            Log.d(TAG, "No Python stdlib found in assets/" + libBase);
        }
    }

    private void extractAssetDir(String assetDir, File outDir) throws IOException {
        String[] files = context.getAssets().list(assetDir);
        if (files == null) return;

        for (String file : files) {
            String assetPath = assetDir + "/" + file;
            File outFile = new File(outDir, file);

            String[] children = context.getAssets().list(assetPath);
            if (children != null && children.length > 0) {
                // It's a directory
                outFile.mkdirs();
                extractAssetDir(assetPath, outFile);
            } else {
                // It's a file
                copyAssetToFile(assetPath, outFile);
            }
        }
    }

    // =====================================================================
    // System Python detection
    // =====================================================================

    /**
     * Check common paths for a working python3 binary.
     */
    private boolean detectSystemPython() {
        String[] candidates = {
            "/usr/bin/python3",
            "/usr/local/bin/python3",
            "/data/data/com.termux/files/usr/bin/python3",
            "/system/bin/python3",
        };

        // Also try bare "python3" via PATH
        String pathPython = findInPath("python3");
        if (pathPython != null) {
            if (verifyPythonWorks(pathPython)) {
                setPythonFound(pathPython, "system");
                return true;
            }
        }

        for (String candidate : candidates) {
            File bin = new File(candidate);
            if (bin.exists() && bin.canExecute()) {
                if (verifyPythonWorks(candidate)) {
                    setPythonFound(candidate, "system");
                    return true;
                }
            }
        }

        return false;
    }

    private void setPythonFound(String path, String source) {
        pythonPath = path;
        pythonSource = source;
        pythonVersion = queryPythonVersion(path);
        available.set(true);

        prefs.edit()
            .putString(KEY_PYTHON_PATH, pythonPath)
            .putString(KEY_PYTHON_VERSION, pythonVersion != null ? pythonVersion : "unknown")
            .putString(KEY_PYTHON_SOURCE, source)
            .apply();

        Log.i(TAG, "Found " + source + " Python " + pythonVersion + " at " + path);
    }

    /**
     * Execute `python3 --version` and check for valid output.
     */
    private boolean verifyPythonWorks(String pythonBin) {
        try {
            ProcessBuilder pb = new ProcessBuilder(pythonBin, "-c", "print('marabunta_ok')");
            pb.redirectErrorStream(true);
            Process proc = pb.start();

            boolean finished = proc.waitFor(VERSION_CHECK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!finished) {
                proc.destroyForcibly();
                return false;
            }

            BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            String line = reader.readLine();
            return "marabunta_ok".equals(line != null ? line.trim() : "");
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Query the version string from a Python binary.
     */
    private String queryPythonVersion(String pythonBin) {
        try {
            ProcessBuilder pb = new ProcessBuilder(pythonBin, "--version");
            pb.redirectErrorStream(true);
            Process proc = pb.start();

            boolean finished = proc.waitFor(VERSION_CHECK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!finished) {
                proc.destroyForcibly();
                return null;
            }

            BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            String line = reader.readLine();
            if (line != null && line.startsWith("Python ")) {
                return line.substring(7).trim();
            }
            return line;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Find an executable name in PATH.
     */
    private String findInPath(String name) {
        String pathEnv = System.getenv("PATH");
        if (pathEnv == null) return null;

        for (String dir : pathEnv.split(":")) {
            File candidate = new File(dir, name);
            if (candidate.exists() && candidate.canExecute()) {
                return candidate.getAbsolutePath();
            }
        }
        return null;
    }

    // =====================================================================
    // Helpers
    // =====================================================================

    private String readAssetString(String path) throws IOException {
        try (InputStream is = context.getAssets().open(path)) {
            byte[] buf = new byte[is.available()];
            int read = is.read(buf);
            return new String(buf, 0, read, "UTF-8");
        }
    }

    private void copyAssetToFile(String assetPath, File outFile) throws IOException {
        File tmpFile = new File(outFile.getParentFile(), outFile.getName() + ".tmp");

        try (InputStream in = context.getAssets().open(assetPath);
             OutputStream out = new FileOutputStream(tmpFile)) {
            byte[] buf = new byte[8192];
            int len;
            while ((len = in.read(buf)) > 0) {
                out.write(buf, 0, len);
            }
            out.flush();
            ((FileOutputStream) out).getFD().sync();
        }

        if (!tmpFile.renameTo(outFile)) {
            try (InputStream in = new java.io.FileInputStream(tmpFile);
                 OutputStream out = new FileOutputStream(outFile)) {
                byte[] buf = new byte[8192];
                int len;
                while ((len = in.read(buf)) > 0) {
                    out.write(buf, 0, len);
                }
            }
            tmpFile.delete();
        }
    }

    private static String detectAbi() {
        String[] bundledAbis = {"arm64-v8a", "armeabi-v7a", "x86_64", "x86"};
        String[] deviceAbis = android.os.Build.SUPPORTED_ABIS;

        for (String deviceAbi : deviceAbis) {
            for (String bundledAbi : bundledAbis) {
                if (deviceAbi.equals(bundledAbi)) {
                    return bundledAbi;
                }
            }
        }
        return "arm64-v8a";
    }

    private static void deleteRecursive(File fileOrDir) {
        if (fileOrDir.isDirectory()) {
            File[] children = fileOrDir.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteRecursive(child);
                }
            }
        }
        fileOrDir.delete();
    }
}
