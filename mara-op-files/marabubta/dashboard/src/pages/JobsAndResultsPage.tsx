// Marabunta - Licensed under the MIT License.
import React, { useState } from 'react';
import { UnlockKeystoreModal } from '../components/UnlockKeystoreModal';
import { User } from '../hooks/useAuth';
import '../styles/Jobs.css';

const mockJobs = [
];

export function JobsAndResultsPage({ user }: { user: User }) {
  const { keystore } = useKeystore();
  const [isKeystoreModalOpen, setIsKeystoreModalOpen] = useState(false);
  const [isAuditModalOpen, setIsAuditModalOpen] = useState(false);
  const [auditResults, setAuditResults] = useState<CheckResult[] | null>(null);
  const [selectedJob, setSelectedJob] = useState<any>(null);

  const handleDownload = (job: any, file: any) => {
    if (file.is_encrypted) {
      if (!keystore.privateKey) {
        setSelectedJob(job);
        setIsKeystoreModalOpen(true);
        return;
      }
      alert(`Decrypting ${file.name} client-side...`);
      downloadFile(file.name, file.content.replace('encrypted(', '').replace(')', ''));
    } else {
      downloadFile(file.name, file.content);
    }
  };

  const handleVerifyAudit = async (job: any) => {
    setSelectedJob(job);
    setIsAuditModalOpen(true);
    setAuditResults(null); // Clear previous results
    setAuditResults(results);
  };

  const downloadFile = (filename: string, content: string) => {
    const blob = new Blob([content], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <>
      <h1>{user.role.includes('Customer') ? 'My' : 'All'} Jobs & Results</h1>
      <div className="card">
        <table className="jobs-table">
          <thead>
            <tr>
              <th>Job ID</th>
              <th>Name</th>
              <th>Status</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {mockJobs.map(job => (
              <tr key={job.id}>
                <td>{job.name}</td>
                <td><span className={`status-badge status-${job.status.toLowerCase()}`}>{job.status}</span></td>
                <td>
                  {job.status === 'Completed' && job.results.map(file => (
                    <button key={file.name} className="btn btn-secondary" onClick={() => handleDownload(job, file)}>
                      Download {file.name}
                    </button>
                  ))}
                    <button className="btn btn-primary" onClick={() => handleVerifyAudit(job)} style={{ marginLeft: '0.5rem' }}>
                      Verify Audit
                    </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <UnlockKeystoreModal
        isOpen={isKeystoreModalOpen}
        onClose={() => setIsKeystoreModalOpen(false)}
        onUnlock={() => alert('Keystore unlocked! Please click Download again.')}
      />

      {selectedJob && (
        <div className="modal-backdrop" style={{ display: isAuditModalOpen ? 'flex' : 'none' }} onClick={() => setIsAuditModalOpen(false)}>
          <div className="modal" onClick={e => e.stopPropagation()}>
            <h2>Audit Verification for Job {selectedJob.id}</h2>
            {!auditResults ? <p>Verifying audit trail...</p> : (
              <div>
                {auditResults.every(r => r.passed) ?
                  <p style={{ color: '#22c55e' }}>✅ Audit Trail Verified: Execution integrity confirmed.</p> :
                  <p style={{ color: '#ef4444' }}>❌ Audit Tampered! Do not trust the results.</p>
                }
                <ul>
                  {auditResults.map((step, i) => (
                    <li key={i} style={{ color: step.passed ? 'inherit' : '#ef4444' }}>
                      {step.passed ? '✅' : '❌'} {step.step}: {step.detail}
                    </li>
                  ))}
                </ul>
              </div>
            )}
            <div className="modal-actions">
              <button className="btn btn-secondary" onClick={() => setIsAuditModalOpen(false)}>Close</button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
