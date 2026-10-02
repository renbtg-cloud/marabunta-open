// Marabunta - Licensed under the MIT License.
// marabunta-compute/dashboard/src/components/StatusIcon.tsx
import React from 'react';

type StatusLevel = 'ok' | 'warning' | 'critical' | 'info';

interface StatusIconProps {
  status: StatusLevel;
  size?: number;
}

export function StatusIcon({ status, size = 16 }: StatusIconProps) {
  const iconPath = {
    ok: 'M 8, 8 m -7, 0 a 7,7 0 1,0 14,0 a 7,7 0 1,0 -14,0', // Circle
    warning: 'M 8,1 L 15,14 H 1 Z', // Triangle
    critical: 'M 1,1 H 15 V 15 H 1 Z', // Square
    info: 'M 8,8 m -7,0 a 7,7 0 1,0 14,0 a 7,7 0 1,0 -14,0 M 8,4 v1 M 8,7 v5', // Circle with 'i'
  };

  const statusClass = `status-icon status-icon-${status}`;

  return (
    <svg 
      className={statusClass}
      width={size} 
      height={size} 
      viewBox="0 0 16 16" 
      fill="currentColor" 
      xmlns="http://www.w3.org/2000/svg"
      style={{ verticalAlign: 'middle', marginRight: '0.5em' }}
    >
      <path d={iconPath[status]} />
    </svg>
  );
}
