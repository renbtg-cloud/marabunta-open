// Marabunta - Licensed under the MIT License.
// marabunta-compute/dashboard/src/pages/UserManagementPage.tsx
import React from 'react';
import { UserRole } from '../hooks/useAuth';

const mockUsers = [
  { id: 'user-01', name: 'John Doe', role: 'Customer', teamId: 'team-a' },
  { id: 'user-02', name: 'Jane Smith', role: 'Team Lead', teamId: 'team-a' },
  { id: 'user-03', name: 'Peter Jones', role: 'Operator', teamId: 'internal' },
  { id: 'user-04', name: 'Susan Bell', role: 'Security Auditor', teamId: 'internal' },
];

export function UserManagementPage() {

  const handleInvite = () => {
    const email = prompt("Enter email for new user:");
    if (email) {
      alert(`(Mock) Invitation sent to ${email}. In a real app, this would trigger a backend process to send a magic link.`);
    }
  };

  const handleDelete = (userId: string, userName: string) => {
    if (window.confirm(`Are you sure you want to delete user ${userName} (${userId})?`)) {
      alert(`(Mock) User ${userName} deleted.`);
    }
  };

  return (
    <>
      <h1>User Management</h1>
      <div className="card">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <p>Manage users and their roles across the platform.</p>
          <button className="btn btn-primary" onClick={handleInvite}>Invite New User</button>
        </div>
        <table className="jobs-table" style={{ marginTop: '1rem' }}>
          <thead>
            <tr>
              <th>User ID</th>
              <th>Name</th>
              <th>Role</th>
              <th>Team</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {mockUsers.map(user => (
              <tr key={user.id}>
                <td><code>{user.id}</code></td>
                <td>{user.name}</td>
                <td>{user.role}</td>
                <td>{user.teamId}</td>
                <td>
                  <button className="btn btn-secondary" style={{ marginRight: '0.5rem' }}>Edit</button>
                  <button className="btn btn-secondary" onClick={() => handleDelete(user.id, user.name)}>Delete</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
