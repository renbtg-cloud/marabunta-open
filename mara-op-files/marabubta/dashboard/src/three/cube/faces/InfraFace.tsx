// Marabunta - Licensed under the MIT License.
// ── InfraFace ──
// Left face (-X): Shows infrastructure details -- node ID (truncated to
// 8 chars), node tier, replication factor, replication health percentage,
// and a bar chart of replica status using small colored rectangles.

import { FONT_MONO } from '../types';
import type { FaceProps, InfraFaceData } from '../types';

// ── Status Color Map ──

const REPLICA_STATUS_COLORS: Record<string, string> = {
  healthy: '#34d399',
  syncing: '#fbbf24',
  failed:  '#fb7185',
};

// ── Replica Bar Sub-component ──

function ReplicaBar({
  status,
  x,
  size,
}: {
  status: 'healthy' | 'syncing' | 'failed';
  x: number;
  size: number;
}) {
  const color = REPLICA_STATUS_COLORS[status] ?? '#64748b';
  // Bar height proportional to status: healthy=full, syncing=half, failed=quarter
  const heightScale = status === 'healthy' ? 1.0 : status === 'syncing' ? 0.6 : 0.25;
  const barHeight = size * 0.12 * heightScale;

  return (
    <group position={[x, -size * 0.2, 0.002]}>
      {/* Bar background */}
      <mesh position={[0, 0, -0.001]}>
        <planeGeometry args={[size * 0.08, size * 0.12]} />
        <meshStandardMaterial color="#1e293b" transparent opacity={0.6} />
      </mesh>
      {/* Bar fill */}
      <mesh position={[0, -(size * 0.12 - barHeight) / 2, 0]}>
        <planeGeometry args={[size * 0.08, barHeight]} />
        <meshStandardMaterial
          color={color}
          emissive={color}
          emissiveIntensity={0.2}
        />
      </mesh>
    </group>
  );
}

// ── Component ──

export function InfraFace({ data, size }: FaceProps<InfraFaceData>) {
  const healthColor =
    data.replicaHealth > 0.9 ? '#34d399' :
    data.replicaHealth > 0.6 ? '#fbbf24' : '#fb7185';

  // Center the replica bars
  const barSpacing = size * 0.15;
  const totalWidth = (data.replicas.length - 1) * barSpacing;
  const startX = -totalWidth / 2;

  return (
    <group>
      {/* Node ID */}
      <troikaText
        text={`NODE ${data.nodeId.slice(0, 8)}`}
        fontSize={size * 0.055}
        font={FONT_MONO}
        color="#60a5fa"
        anchorX="center"
        anchorY="middle"
        position={[0, size * 0.3, 0.002]}
        sdfGlyphSize={64}
      />
      {/* Tier badge */}
      <troikaText
        text={data.nodeTier}
        fontSize={size * 0.04}
        font={FONT_MONO}
        color="#64748b"
        anchorX="center"
        anchorY="middle"
        position={[0, size * 0.18, 0.002]}
        sdfGlyphSize={64}
      />
      {/* Region */}
      <troikaText
        text={data.region}
        fontSize={size * 0.032}
        font={FONT_MONO}
        color="#475569"
        anchorX="center"
        anchorY="middle"
        position={[0, size * 0.1, 0.002]}
        sdfGlyphSize={64}
      />
      {/* Replication health */}
      <troikaText
        text={`${Math.round(data.replicaHealth * 100)}% healthy`}
        fontSize={size * 0.045}
        font={FONT_MONO}
        color={healthColor}
        anchorX="center"
        anchorY="middle"
        position={[0, size * 0.02, 0.002]}
        sdfGlyphSize={64}
      />
      {/* Replication factor */}
      <troikaText
        text={`RF ${data.replicationFactor}`}
        fontSize={size * 0.032}
        font={FONT_MONO}
        color="#475569"
        anchorX="center"
        anchorY="middle"
        position={[0, -size * 0.06, 0.002]}
        sdfGlyphSize={64}
      />
      {/* Replica bar chart */}
      {data.replicas.map((r, i) => (
        <ReplicaBar
          key={i}
          status={r.status}
          x={startX + i * barSpacing}
          size={size}
        />
      ))}
    </group>
  );
}
