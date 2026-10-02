// Marabunta - Licensed under the MIT License.
// ── AuditFace ──
// Right face (+X): Renders a compact audit trail. Top section shows total
// event count and witness count. Below, the 4 most recent events are listed
// with timestamp (relative), actor, and action. This face is decomposable.

import { FONT_MONO } from '../types';
import type { FaceProps, AuditFaceData } from '../types';

// ── Relative Time Formatter ──

function relativeTime(timestamp: string): string {
  const now = Date.now();
  const then = new Date(timestamp).getTime();
  const diffMs = now - then;
  if (isNaN(diffMs) || diffMs < 0) return timestamp;

  const seconds = Math.floor(diffMs / 1000);
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

// ── Audit Row Sub-component ──

function AuditRow({
  event,
  y,
  size,
}: {
  event: { timestamp: string; actor: string; action: string; verified: boolean };
  y: number;
  size: number;
}) {
  return (
    <group position={[0, y, 0.002]}>
      {/* Timestamp */}
      <troikaText
        text={relativeTime(event.timestamp)}
        fontSize={size * 0.03}
        font={FONT_MONO}
        color="#475569"
        anchorX="left"
        anchorY="top"
        position={[-size * 0.42, size * 0.04, 0]}
        sdfGlyphSize={64}
      />
      {/* Actor + Action */}
      <troikaText
        text={`${event.actor}: ${event.action}`}
        fontSize={size * 0.035}
        font={FONT_MONO}
        color="#e8ecf4"
        anchorX="left"
        anchorY="top"
        position={[-size * 0.42, -size * 0.01, 0]}
        maxWidth={size * 0.75}
        sdfGlyphSize={64}
      />
      {/* Verification indicator */}
      <mesh position={[size * 0.42, 0, 0]}>
        <circleGeometry args={[size * 0.012, 16]} />
        <meshStandardMaterial
          color={event.verified ? '#34d399' : '#fb7185'}
          emissive={event.verified ? '#34d399' : '#fb7185'}
          emissiveIntensity={0.3}
        />
      </mesh>
    </group>
  );
}

// ── Component ──

export function AuditFace({ data, size }: FaceProps<AuditFaceData>) {
  const headerText =
    `${data.totalEvents} events \u00b7 ${data.witnessCount} witnesses`;

  return (
    <group>
      {/* Header with counts */}
      <troikaText
        text={headerText}
        fontSize={size * 0.04}
        font={FONT_MONO}
        color="#34d399"
        anchorX="center"
        anchorY="top"
        position={[0, size * 0.38, 0.002]}
        sdfGlyphSize={64}
      />
      {/* Separator line */}
      <mesh position={[0, size * 0.28, 0.002]}>
        <planeGeometry args={[size * 0.8, size * 0.003]} />
        <meshStandardMaterial color="#1e293b" />
      </mesh>
      {/* Recent events */}
      {data.recentEvents.slice(0, 4).map((evt, i) => (
        <AuditRow
          key={i}
          event={evt}
          y={size * 0.2 - i * size * 0.14}
          size={size}
        />
      ))}
    </group>
  );
}
