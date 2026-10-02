// Marabunta - Licensed under the MIT License.
// marabunta-compute/dashboard/src/components/UnlockKeystoreModal.tsx
import React, { useRef, useState } from 'react';
import '../styles/Modal.css';

export function UnlockKeystoreModal({ isOpen, onClose, onUnlock }: { isOpen: boolean, onClose: () => void, onUnlock: () => void }) {
  const { unlock } = useKeystore();
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [error, setError] = useState('');

  if (!isOpen) {
    return null;
  }

  const handleFileSelect = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (file) {
      const reader = new FileReader();
      reader.onload = (e) => {
        const content = e.target?.result as string;
        if (unlock(content)) {
          onUnlock();
          onClose();
        } else {
          setError('Failed to unlock keystore. The provided file is not a valid key.');
        }
      };
      reader.readAsText(file);
    }
  };

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" onClick={e => e.stopPropagation()}>
        <h2>Unlock Session Keystore</h2>
        <p className="text-secondary">This key will only be held in memory for this session.</p>
        {error && <p className="error-text">{error}</p>}
        <input
          type="file"
          ref={fileInputRef}
          accept=".pem"
          onChange={handleFileSelect}
          style={{ display: 'none' }}
        />
        <div className="modal-actions">
          <button className="btn btn-secondary" onClick={onClose}>Cancel</button>
          <button className="btn btn-primary" onClick={() => fileInputRef.current?.click()}>
            Upload Key File
          </button>
        </div>
      </div>
    </div>
  );
}
