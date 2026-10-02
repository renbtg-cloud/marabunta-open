// Marabunta - Licensed under the MIT License.
package com.marabunta.worker;

import android.content.Context;
import android.content.SharedPreferences;
import android.os.Build;
import android.util.Log;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages the bundled Toybox binary for Android shell command support.
 *
 * Toybox (https://landley.net/toybox/) is a BSD-licensed (0BSD) implementation
 * of ~200 POSIX commands in a single static binary (~800KB). Android's
 * /system/bin/sh (mksh) is functional, but GNU coreutils (ls, grep, cat, awk,
 * sed, etc.) are NOT present. Toybox provides them.
 *
 * <h3>Extraction flow:</h3>
 * <ol>
 *   <li>Detect device ABI (arm64-v8a, armeabi-v7a, x86_64, x86)</li>
 *   <li>Copy the matching binary from assets/{abi}/toybox to data dir</li>
 *   <li>Verify SHA-256 integrity hash</li>
 *   <li>Set executable permissions</li>
 *   <li>Create multi-call symlinks for each command</li>
 *   <li>Record the version in SharedPreferences so we skip on next boot</li>
 * </ol>
 *
 * <h3>Assets layout expected:</h3>
 * <pre>
 * assets/
 *   toybox/
 *     arm64-v8a/toybox
 *     armeabi-v7a/toybox
 *     x86_64/toybox
 *     x86/toybox
 *     sha256sums.txt          # one line per ABI: "hex_hash  abi/toybox"
 *     version.txt             # e.g., "0.8.11"
 *     commands.txt            # newline-separated list of supported commands
 * </pre>
 */
public class ToyboxManager {

    private static final String TAG = "ToyboxManager";

    /** SharedPreferences file for Toybox state */
    private static final String PREFS_NAME = "toybox_prefs";
    private static final String KEY_INSTALLED_VERSION = "installed_version";
    private static final String KEY_INSTALLED_ABI = "installed_abi";
    private static final String KEY_INSTALLED_HASH = "installed_hash";

    /** Subdirectory inside app data dir where tools live */
    private static final String TOOLS_DIR_NAME = "tools";

    /** Name of the main Toybox binary */
    private static final String TOYBOX_BIN = "toybox";

    /** Asset paths */
    private static final String ASSET_BASE = "toybox";
    private static final String ASSET_VERSION = ASSET_BASE + "/version.txt";
    private static final String ASSET_SHA256 = ASSET_BASE + "/sha256sums.txt";
    private static final String ASSET_COMMANDS = ASSET_BASE + "/commands.txt";

    private final Context context;
    private final File toolsDir;
    private final SharedPreferences prefs;
    private final AtomicBoolean ready = new AtomicBoolean(false);

    // Detected state
    private String detectedAbi;
    private String bundledVersion;
    private String[] availableCommands;

    public ToyboxManager(Context context) {
        this.context = context.getApplicationContext();
        this.toolsDir = new File(context.getFilesDir(), TOOLS_DIR_NAME);
        this.prefs = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE);
        this.detectedAbi = detectAbi();
    }

    // =====================================================================
    // Public API
    // =====================================================================

    /**
     * Set up Toybox: extract if needed, verify integrity, create symlinks.
     *
     * This is safe to call on every app start — it no-ops if the correct
     * version is already installed.
     *
     * @return true if Toybox is ready for use, false on failure
     */
    public boolean setup() {
        try {
            bundledVersion = readAssetString(ASSET_VERSION).trim();
        } catch (IOException e) {
            Log.w(TAG, "No bundled Toybox found in assets (version.txt missing). " +
                       "Shell tasks will use system-provided commands only.");
            // Not an error — Toybox just isn't bundled in this build.
            // Shell tasks can still use /system/bin/sh (mksh) and whatever
            // Android provides in /system/bin.
            ready.set(false);
            return false;
        }

        String installedVersion = prefs.getString(KEY_INSTALLED_VERSION, "");
        String installedAbi = prefs.getString(KEY_INSTALLED_ABI, "");

        // Fast path: already installed and correct
        if (bundledVersion.equals(installedVersion)
                && detectedAbi.equals(installedAbi)
                && verifyInstallation()) {
            Log.i(TAG, "Toybox " + bundledVersion + " already installed for " + detectedAbi);
            ready.set(true);
            loadCommandsList();
            return true;
        }

        Log.i(TAG, "Installing Toybox " + bundledVersion + " for " + detectedAbi);

        // Full install
        if (!extractAndInstall()) {
            Log.e(TAG, "Failed to install Toybox");
            return false;
        }

        // Record state
        prefs.edit()
            .putString(KEY_INSTALLED_VERSION, bundledVersion)
            .putString(KEY_INSTALLED_ABI, detectedAbi)
            .apply();

        loadCommandsList();
        ready.set(true);
        Log.i(TAG, "Toybox " + bundledVersion + " installed successfully (" +
                   (availableCommands != null ? availableCommands.length : 0) + " commands)");
        return true;
    }

    /**
     * @return true if Toybox is extracted and ready
     */
    public boolean isReady() {
        return ready.get();
    }

    /**
     * @return absolute path to the tools directory (for native PATH setup)
     */
    public String getToolsDir() {
        return toolsDir.getAbsolutePath();
    }

    /**
     * @return the Toybox version string, or null if not bundled
     */
    public String getVersion() {
        return bundledVersion;
    }

    /**
     * @return the detected device ABI string
     */
    public String getDetectedAbi() {
        return detectedAbi;
    }

    /**
     * @return array of available command names (ls, cat, grep, ...), or empty
     */
    public String[] getAvailableCommands() {
        return availableCommands != null ? availableCommands : new String[0];
    }

    /**
     * Check if a specific command is available via Toybox.
     */
    public boolean hasCommand(String command) {
        if (availableCommands == null) return false;
        for (String cmd : availableCommands) {
            if (cmd.equals(command)) return true;
        }
        return false;
    }

    /**
     * Remove the installed Toybox (for debugging / cleanup).
     */
    public void uninstall() {
        deleteRecursive(toolsDir);
        prefs.edit().clear().apply();
        ready.set(false);
        Log.i(TAG, "Toybox uninstalled");
    }

    // =====================================================================
    // Installation
    // =====================================================================

    private boolean extractAndInstall() {
        // 1. Clean previous installation
        if (toolsDir.exists()) {
            deleteRecursive(toolsDir);
        }
        if (!toolsDir.mkdirs()) {
            Log.e(TAG, "Failed to create tools directory: " + toolsDir);
            return false;
        }

        // 2. Copy binary from assets
        String assetPath = ASSET_BASE + "/" + detectedAbi + "/" + TOYBOX_BIN;
        File toyboxFile = new File(toolsDir, TOYBOX_BIN);

        try {
            copyAssetToFile(assetPath, toyboxFile);
        } catch (IOException e) {
            Log.e(TAG, "Failed to extract Toybox binary from assets/" + assetPath, e);
            return false;
        }

        // 3. Verify SHA-256 integrity
        if (!verifyIntegrity(toyboxFile)) {
            Log.e(TAG, "SHA-256 integrity check FAILED — binary may be corrupted");
            toyboxFile.delete();
            return false;
        }

        // 4. Set executable permission
        if (!toyboxFile.setExecutable(true, false)) {
            Log.e(TAG, "Failed to set executable permission on " + toyboxFile);
            return false;
        }

        // 5. Create multi-call symlinks
        createSymlinks(toyboxFile);

        // 6. Record installed hash
        String hash = sha256Hex(toyboxFile);
        if (hash != null) {
            prefs.edit().putString(KEY_INSTALLED_HASH, hash).apply();
        }

        return true;
    }

    /**
     * Create symlinks for each Toybox command.
     *
     * Toybox is a "multi-call binary" — when invoked as "ls" it behaves like ls,
     * when invoked as "grep" it behaves like grep, etc. We create symlinks so
     * that `PATH` resolution finds the right command name.
     *
     * On Android < 21 (API < L) symlink() is not available via File API,
     * so we fall back to shell-based symlink creation.
     */
    private void createSymlinks(File toyboxBin) {
        String[] commands = loadCommandsFromAssets();
        if (commands == null || commands.length == 0) {
            Log.w(TAG, "No commands list found — skipping symlink creation. " +
                       "Toybox will work but commands must be invoked as 'toybox <cmd>'.");
            return;
        }

        int created = 0;
        int failed = 0;

        for (String cmd : commands) {
            cmd = cmd.trim();
            if (cmd.isEmpty() || cmd.startsWith("#")) continue;
            // Don't create a symlink for "toybox" itself
            if (cmd.equals(TOYBOX_BIN)) continue;

            File link = new File(toolsDir, cmd);
            if (link.exists()) continue;

            try {
                // Use Os.symlink on API 21+ (our minSdk)
                android.system.Os.symlink(toyboxBin.getAbsolutePath(), link.getAbsolutePath());
                created++;
            } catch (Exception e) {
                // Fallback: try shell-based symlink
                try {
                    Runtime.getRuntime().exec(new String[]{
                        "ln", "-s", toyboxBin.getAbsolutePath(), link.getAbsolutePath()
                    }).waitFor();
                    created++;
                } catch (Exception e2) {
                    Log.w(TAG, "Failed to create symlink for " + cmd + ": " + e2.getMessage());
                    failed++;
                }
            }
        }

        Log.i(TAG, "Created " + created + " symlinks" +
              (failed > 0 ? " (" + failed + " failed)" : ""));
    }

    // =====================================================================
    // Integrity verification
    // =====================================================================

    /**
     * Verify the binary's SHA-256 against the bundled sha256sums.txt.
     */
    private boolean verifyIntegrity(File binaryFile) {
        String expectedHash = getExpectedHash();
        if (expectedHash == null) {
            Log.w(TAG, "No SHA-256 hash found in assets — skipping integrity check");
            return true; // Allow but warn
        }

        String actualHash = sha256Hex(binaryFile);
        if (actualHash == null) {
            Log.e(TAG, "Failed to compute SHA-256 of extracted binary");
            return false;
        }

        boolean match = expectedHash.equalsIgnoreCase(actualHash);
        if (!match) {
            Log.e(TAG, "SHA-256 mismatch! Expected: " + expectedHash +
                       ", got: " + actualHash);
        }
        return match;
    }

    /**
     * Verify the current installation is intact (binary exists, is executable,
     * hash matches what we recorded at install time).
     */
    private boolean verifyInstallation() {
        File toyboxFile = new File(toolsDir, TOYBOX_BIN);
        if (!toyboxFile.exists()) return false;
        if (!toyboxFile.canExecute()) return false;

        // Verify recorded hash
        String recordedHash = prefs.getString(KEY_INSTALLED_HASH, "");
        if (recordedHash.isEmpty()) return false;

        String currentHash = sha256Hex(toyboxFile);
        return recordedHash.equalsIgnoreCase(currentHash);
    }

    /**
     * Read the expected SHA-256 hash for the current ABI from sha256sums.txt.
     */
    private String getExpectedHash() {
        try {
            String sums = readAssetString(ASSET_SHA256);
            String targetSuffix = detectedAbi + "/" + TOYBOX_BIN;
            for (String line : sums.split("\n")) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;
                // Format: "abcdef1234567890  arm64-v8a/toybox"
                String[] parts = line.split("\\s+", 2);
                if (parts.length == 2 && parts[1].trim().equals(targetSuffix)) {
                    return parts[0].trim();
                }
            }
        } catch (IOException e) {
            Log.w(TAG, "Could not read sha256sums.txt");
        }
        return null;
    }

    // =====================================================================
    // ABI detection
    // =====================================================================

    /**
     * Detect the best ABI for this device.
     *
     * Uses Build.SUPPORTED_ABIS (API 21+, which is our minSdk) to pick
     * the first supported ABI that we bundle.
     */
    private static String detectAbi() {
        // Ordered by preference: 64-bit first, then 32-bit
        String[] bundledAbis = {"arm64-v8a", "armeabi-v7a", "x86_64", "x86"};
        String[] deviceAbis = Build.SUPPORTED_ABIS;

        for (String deviceAbi : deviceAbis) {
            for (String bundledAbi : bundledAbis) {
                if (deviceAbi.equals(bundledAbi)) {
                    Log.d(TAG, "Selected ABI: " + bundledAbi);
                    return bundledAbi;
                }
            }
        }

        // Fallback: this shouldn't happen on any real device
        Log.w(TAG, "No matching ABI found! Device ABIs: " +
              String.join(", ", deviceAbis));
        return "arm64-v8a";
    }

    // =====================================================================
    // Asset helpers
    // =====================================================================

    private String readAssetString(String path) throws IOException {
        try (InputStream is = context.getAssets().open(path)) {
            byte[] buf = new byte[is.available()];
            int read = is.read(buf);
            return new String(buf, 0, read, "UTF-8");
        }
    }

    private void copyAssetToFile(String assetPath, File outFile) throws IOException {
        // Write to a temp file first, then atomically rename. This avoids
        // a half-written binary if the process is killed mid-extraction.
        File tmpFile = new File(outFile.getParentFile(), outFile.getName() + ".tmp");

        try (InputStream in = context.getAssets().open(assetPath);
             OutputStream out = new FileOutputStream(tmpFile)) {
            byte[] buf = new byte[8192];
            int len;
            while ((len = in.read(buf)) > 0) {
                out.write(buf, 0, len);
            }
            out.flush();
            // Force sync to disk
            ((FileOutputStream) out).getFD().sync();
        }

        // Atomic rename
        if (!tmpFile.renameTo(outFile)) {
            // renameTo can fail across filesystems; fallback to copy+delete
            copyFile(tmpFile, outFile);
            tmpFile.delete();
        }
    }

    private void copyFile(File src, File dst) throws IOException {
        try (InputStream in = new java.io.FileInputStream(src);
             OutputStream out = new FileOutputStream(dst)) {
            byte[] buf = new byte[8192];
            int len;
            while ((len = in.read(buf)) > 0) {
                out.write(buf, 0, len);
            }
        }
    }

    private String[] loadCommandsFromAssets() {
        try {
            String commands = readAssetString(ASSET_COMMANDS);
            return commands.split("\n");
        } catch (IOException e) {
            Log.w(TAG, "commands.txt not found in assets");
            return null;
        }
    }

    private void loadCommandsList() {
        String[] cmds = loadCommandsFromAssets();
        if (cmds != null) {
            // Filter blanks and comments
            java.util.List<String> filtered = new java.util.ArrayList<>();
            for (String cmd : cmds) {
                cmd = cmd.trim();
                if (!cmd.isEmpty() && !cmd.startsWith("#")) {
                    filtered.add(cmd);
                }
            }
            availableCommands = filtered.toArray(new String[0]);
        }
    }

    // =====================================================================
    // Crypto helpers
    // =====================================================================

    /**
     * Compute the SHA-256 hex digest of a file.
     */
    private static String sha256Hex(File file) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (java.io.FileInputStream fis = new java.io.FileInputStream(file)) {
                byte[] buf = new byte[8192];
                int len;
                while ((len = fis.read(buf)) > 0) {
                    digest.update(buf, 0, len);
                }
            }
            byte[] hashBytes = digest.digest();
            StringBuilder sb = new StringBuilder(hashBytes.length * 2);
            for (byte b : hashBytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException | IOException e) {
            Log.e(TAG, "SHA-256 computation failed", e);
            return null;
        }
    }

    // =====================================================================
    // Filesystem helpers
    // =====================================================================

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
