// Marabunta - Licensed under the MIT License.
// marabunta-compute/dashboard/src/hooks/useAuth.ts
import { useState, useEffect } from 'react';

export type UserRole = 'Customer' | 'Team Lead' | 'Operator' | 'Security Auditor' | 'Administrator';

export interface User {
  id: string;
  name: string;
  email: string;
  role: UserRole;
}

const mockUsers: Record<UserRole, User> = {
  // Simple, safe role
  Customer: { id: 'usr_cst_01', name: 'j.doe', role: 'Customer' },
  // Analytical, data-dense role
  'Team Lead': { id: 'usr_tl_01', name: 'j.smith', role: 'Team Lead' },
  // Mission-critical operator role
  Operator: { id: 'usr_ops_01', name: 'ops-primary', role: 'Operator' },
  'Security Auditor': { id: 'usr_aud_01', name: 'auditor-sec', role: 'Security Auditor' },
  Administrator: { id: 'usr_adm_01', name: 'admin-sys', role: 'Administrator' },
};

const DEV_ROLE_KEY = 'enso_dev_user_role';

export function useAuth() {
  const [user, setUser] = useState<User>(mockUsers.Operator); // Default to the Operator role

  useEffect(() => {
    const savedRole = localStorage.getItem(DEV_ROLE_KEY) as UserRole;
    if (savedRole && mockUsers[savedRole]) {
      setUser(mockUsers[savedRole]);
    }
  }, []);

  const changeRole = (role: UserRole) => {
    localStorage.setItem(DEV_ROLE_KEY, role);
    window.location.reload();
  };

  return { user, changeRole, availableRoles: Object.keys(mockUsers) as UserRole[] };
}
