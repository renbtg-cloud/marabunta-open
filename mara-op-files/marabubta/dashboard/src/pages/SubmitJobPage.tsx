// Marabunta - Licensed under the MIT License.
// marabunta-compute/dashboard/src/pages/SubmitJobPage.tsx
import React, { useState, useEffect } from 'react';
import { UnlockKeystoreModal } from '../components/UnlockKeystoreModal';
import { CheckResult } from '../services/apiTypes';

const MOCK_MODULE_ID = 'mock-image-processing-v1';

export function SubmitJobPage() {
  const [files, setFiles] = useState<File[]>([]);
  const [isKeystoreModalOpen, setIsKeystoreModalOpen] = useState(false);
  const { keystore } = useKeystore();

  const [manifest, setManifest] = useState<any>(null);
  const [verification, setVerification] = useState<CheckResult[] | null>(null);
  const [isVerifying, setIsVerifying] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (isBlind) {
      verifySoftwareEnvironment();
    } else {
      setManifest(null);
      setVerification(null);
      setError(null);
    }
  }, [isBlind]);

  const verifySoftwareEnvironment = async () => {
    setIsVerifying(true);
    setError(null);
    try {
      setManifest(manifestData);
      setVerification(verificationResults);
    } catch (err: any) {
      setError(err.message || 'Failed to verify manifest.');
      setVerification([{ step: 'Fetch Manifest', passed: false, detail: err.message }]);
    } finally {
      setIsVerifying(false);
    }
  };

  const handleToggleBlind = (e: React.ChangeEvent<HTMLInputElement>) => {
    setIsBlind(e.target.checked);
    if (e.target.checked && !keystore.privateKey) {
      setIsKeystoreModalOpen(true);
    }
  };

  const handleSubmit = () => {
    if (isBlind) {
      if (!verification || !verification.every(v => v.passed)) {
        return;
      }
      alert(`Submitting BLIND job. Files will be encrypted with a session key, which is then encrypted using the verified environment's public key.`);
    } else {
      alert(`Submitting REGULAR job.`);
    }
    // Reset form
    setFiles([]);
  };


  return (
    <>
      <h1>Submit Job</h1>
      <div className="card">
        <label>
        </label>
        <p style={{ fontSize: '0.9rem', color: 'var(--text-secondary)' }}>
          Encrypts data client-side for processing in a cryptographically verified software environment.
        </p>

          <div className="verification-box">
            {isVerifying && <p>Verifying...</p>}
            {error && <p style={{color: '#ef4444'}}>Error: {error}</p>}
            {verification && (
              <div>
                {verification.map((result, i) => (
                  <p key={i} style={{ color: result.passed ? '#22c55e' : '#ef4444', margin: '0.2rem 0' }}>
                    {result.passed ? '✅' : '❌'} {result.step}: {result.detail}
                  </p>
                ))}
              </div>
            )}
          </div>
        <hr style={{ margin: '1.5rem 0', borderColor: 'var(--border-color)'}} />

        <input type="file" multiple onChange={e => setFiles(Array.from(e.target.files || []))} />
        <button className="btn btn-primary" style={{ marginTop: '1rem' }} onClick={handleSubmit} disabled={isSubmitDisabled}>
        </button>
      </div>

      <UnlockKeystoreModal
        isOpen={isKeystoreModalOpen}
        onClose={() => setIsKeystoreModalOpen(false)}
      />
      <style>{`
        .verification-box {
          border: 1px solid var(--border-color);
          border-radius: 6px;
          padding: 1rem;
          margin-top: 1rem;
          background-color: var(--app-bg);
        }
      `}</style>
    </>
  );
}
