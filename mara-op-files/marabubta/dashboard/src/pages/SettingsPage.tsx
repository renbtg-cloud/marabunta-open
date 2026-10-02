// Marabunta - Licensed under the MIT License.
// marabunta-compute/dashboard/src/pages/SettingsPage.tsx
import React from 'react';

export function SettingsPage() {
  const { keystore, lock, generateAndDownloadKey } = useKeystore();

  return (
    <>
      <h1>Settings</h1>
      <h2>Security & Keys</h2>
      <div className="card">
        <h3>Client-Side Keystore</h3>
        {keystore.privateKey ? (
          <div>
            <p style={{ color: '#22c55e' }}>✅ Keystore is <strong>unlocked</strong> for this session.</p>
            <button className="btn btn-secondary" onClick={lock}>Lock Keystore</button>
          </div>
        ) : (
          <div>
            <p style={{ color: 'var(--text-secondary)' }}>
              Don't have a key? Generate one now.
              <strong>
                You will be prompted to download a .pem file. Keep it safe!
              </strong>
            </p>
            <button className="btn btn-primary" onClick={generateAndDownloadKey}>Generate & Download New Key</button>
          </div>
        )}
      </div>
    </>
  );
}
