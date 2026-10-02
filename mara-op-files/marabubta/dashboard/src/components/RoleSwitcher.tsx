// Marabunta - Licensed under the MIT License.
// marabunta-compute/dashboard/src/components/RoleSwitcher.tsx
import React from 'react';
import { useAuth, UserRole } from '../hooks/useAuth';

export function RoleSwitcher() {
  const { user, changeRole, availableRoles } = useAuth();

  return (
    <div style={{
      position: 'fixed',
      bottom: '5px',
      right: '5px',
      backgroundColor: 'rgba(0,0,0,0.8)',
      color: '#0f0',
      padding: '4px 8px',
      zIndex: 9999,
      fontSize: '11px',
      border: '1px solid #0f0',
      fontFamily: 'monospace',
    }}>
      <label htmlFor="role-switcher" style={{ marginRight: '8px' }}>
        ROLE:
      </label>
      <select
        id="role-switcher"
        value={user.role}
        onChange={(e) => changeRole(e.target.value as UserRole)}
        style={{ backgroundColor: '#111', color: '#0f0', border: '1px solid #0f0', verticalAlign: 'middle' }}
      >
        {availableRoles.map(role => (
          <option key={role} value={role}>{role}</option>
        ))}
      </select>
    </div>
  );
}
