// Marabunta - Licensed under the MIT License.
// ── NodeTooltip ──
// drei Html overlay that displays detailed node information on hover.
// Only one tooltip renders at a time. Content is memoized to avoid
// re-renders when the 3D scene updates but the hovered node hasn't changed.

import { useMemo } from 'react';
import { Html } from '@react-three/drei';
import * as THREE from 'three';
import {
  type SwarmNode,
  type NodeRole,
  ROLE_COLORS,
  formatUptime,
} from './types';

interface NodeTooltipProps {
  node: SwarmNode | null;
  position: THREE.Vector3 | null;
}

// ── Stat bar sub-component ──

interface StatBarProps {
  label: string;
  value: number;
  max: number;
  unit?: string;
}

function StatBar({ label, value, max, unit = '' }: StatBarProps) {
  const pct = max > 0 ? Math.min((value / max) * 100, 100) : 0;
  const barColor = pct < 60 ? '#34d399' : pct < 85 ? '#fbbf24' : '#fb7185';

  return (
    <div style={{ marginBottom: 4 }}>
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          fontSize: 10,
          fontFamily: "'IBM Plex Mono', monospace",
          color: '#94a3b8',
          marginBottom: 2,
        }}
      >
        <span>{label}</span>
        <span>
          {value.toFixed(1)}{unit} / {max.toFixed(1)}{unit}
        </span>
      </div>
      <div
        style={{
          height: 4,
          borderRadius: 2,
          background: '#1e293b',
          overflow: 'hidden',
        }}
      >
        <div
          style={{
            height: '100%',
            width: `${pct}%`,
            borderRadius: 2,
            background: barColor,
            transition: 'width 0.3s ease',
          }}
        />
      </div>
    </div>
  );
}

// ── Role badge ──

function RoleBadge({ role }: { role: NodeRole }) {
  const color = ROLE_COLORS[role];
  return (
    <span
      style={{
        display: 'inline-block',
        fontSize: 9,
        fontFamily: "'IBM Plex Mono', monospace",
        fontWeight: 600,
        letterSpacing: '0.05em',
        padding: '1px 6px',
        borderRadius: 3,
        color,
        border: `1px solid ${color}60`,
        background: `${color}15`,
        marginRight: 3,
      }}
    >
      {role}
    </span>
  );
}

// ── Load color helper ──

function loadColor(pct: number): string {
  if (pct < 60) return '#34d399';
  if (pct < 85) return '#fbbf24';
  return '#fb7185';
}

// ── Main component ──

export function NodeTooltip({ node, position }: NodeTooltipProps) {
  const content = useMemo(() => {
    if (!node) return null;

    return (
      <div
        style={{
          background: '#111827ee',
          backdropFilter: 'blur(12px)',
          border: '1px solid #1e293b',
          borderRadius: 8,
          padding: '10px 14px',
          width: 220,
          pointerEvents: 'none',
          fontFamily: "'DM Sans', sans-serif",
          color: '#e8ecf4',
        }}
      >
        {/* Header: ID + role badges */}
        <div style={{ marginBottom: 8 }}>
          <div
            style={{
              fontFamily: "'IBM Plex Mono', monospace",
              fontSize: 12,
              fontWeight: 600,
              color: '#22d3ee',
              marginBottom: 4,
              letterSpacing: '0.03em',
            }}
          >
            {node.id.slice(0, 8)}...
          </div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 2 }}>
            {node.roles.map((r) => (
              <RoleBadge key={r} role={r} />
            ))}
          </div>
        </div>

        {/* Resource bars */}
        <StatBar label="CPU" value={node.cpuUsage} max={node.cpuCores * 100} />
        <StatBar label="RAM" value={node.ramUsedGb} max={node.ramGb} unit="GB" />
        <StatBar label="Disk" value={node.diskUsedGb} max={node.diskGb} unit="GB" />

        {/* Footer stats */}
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            marginTop: 8,
            paddingTop: 6,
            borderTop: '1px solid #1e293b',
            fontSize: 10,
            fontFamily: "'IBM Plex Mono', monospace",
            color: '#64748b',
          }}
        >
          <span>
            Load:{' '}
            <span style={{ color: loadColor(node.loadPercent) }}>
              {node.loadPercent}%
            </span>
          </span>
          <span>Up: {formatUptime(node.uptimeSeconds)}</span>
        </div>
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            marginTop: 3,
            fontSize: 10,
            fontFamily: "'IBM Plex Mono', monospace",
            color: '#64748b',
          }}
        >
          <span>Peers: {node.gossipPeerCount}</span>
          <span>Rep: {node.reputationScore.toFixed(2)}</span>
        </div>
      </div>
    );
  }, [node]);

  if (!node || !position) return null;

  return (
    <Html
      position={position}
      distanceFactor={10}
      center
      style={{ pointerEvents: 'none' }}
      zIndexRange={[100, 0]}
    >
      {content}
    </Html>
  );
}
