// Marabunta - Licensed under the MIT License.
// marabunta-compute/dashboard/src/app/Sidebar.tsx
import React from 'react';
import { User } from '../hooks/useAuth';

interface NavLink { path: string; label: string; }

// Navigation is now tailored to the mission-critical mindset
const navConfig: Record<User['role'], NavLink[]> = {
  Customer: [
    { path: '/app/submit', label: 'Submit Job' },
    { path: '/app/jobs', label: 'Jobs & Results' },
    { path: '/app/settings', label: 'Settings' },
  ],
  'Team Lead': [
    { path: '/app/dashboard', label: 'Team Dashboard' },
    { path: '/app/jobs', label: 'Team Jobs' },
    { path: '/app/team', label: 'Team Management' },
    { path: '/app/settings', label: 'Settings' },
  ],
  Operator: [
    { path: '/app/status', label: 'System Status' },
    { path: '/app/alerts', label: 'Active Alerts' },
    { path: '/app/nodes', label: 'Node Control' },
    { path: '/app/jobs', label: 'Global Job Queue' },
    { path: '/app/audit', label: 'Audit Trail' },
  ],
  'Security Auditor': [
      { path: '/app/audit', label: 'Audit Trail Explorer' },
      { path: '/app/compliance', label: 'Compliance Posture' },
  ],
  Administrator: [
    { path: '/app/status', label: 'System Status' },
    { path: '/app/alerts', label: 'Active Alerts' },
    { path: '/app/nodes', label: 'Node Control' },
    { path: '/app/config', label: 'System Configuration' },
    { path: '/app/users', label: 'User Management' },
  ],
};

export function Sidebar({ user }: { user: User }) {
  const links = navConfig[user.role] || [];
  const currentPath = window.location.pathname;

  return (
    <nav className="sidebar">
      <div className="sidebar-header">
        <h1>MARABUNTA // MC</h1>
        <span className="sidebar-role">{user.role}</span>
      </div>
      <ul className="sidebar-nav">
        {links.map(link => (
          <li key={link.path} className={currentPath === link.path ? 'active' : ''}>
            <a href={link.path}>{link.label}</a>
          </li>
        ))}
      </ul>
    </nav>
  );
}
