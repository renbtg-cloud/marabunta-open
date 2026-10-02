// Marabunta - Licensed under the MIT License.
// ── StateFace ──
// Front face (+Z): Displays the current workflow state as a large centered
// label with color-coded glow. Below the state name, available transitions
// are rendered as small labeled items. State progress shown as "N / M".

import { FONT_MONO } from '../types';
import type { FaceProps, StateFaceData } from '../types';

// ── Transition Button Sub-component ──

function TransitionButton({
  label,
  position,
  size,
}: {
  label: string;
  position: [number, number, number];
  size: number;
}) {
  return (
    <group position={position}>
      {/* Background rectangle for the transition */}
      <mesh>
        <planeGeometry args={[size * 0.7, size * 0.055]} />
        <meshStandardMaterial
          color="#1e293b"
          transparent
          opacity={0.8}
        />
      </mesh>
      {/* Transition label */}
      <troikaText
        text={label}
        fontSize={size * 0.035}
        font={FONT_MONO}
        color="#94a3b8"
        anchorX="center"
        anchorY="middle"
        position={[0, 0, 0.001]}
        maxWidth={size * 0.65}
        sdfGlyphSize={64}
      />
    </group>
  );
}

// ── Component ──

export function StateFace({ data, size }: FaceProps<StateFaceData>) {
  return (
    <group>
      {/* Current state -- large centered text */}
      <troikaText
        text={data.currentState}
        fontSize={size * 0.1}
        font={FONT_MONO}
        color={data.stateColor}
        anchorX="center"
        anchorY="middle"
        position={[0, size * 0.1, 0.002]}
        outlineWidth={0.002}
        sdfGlyphSize={64}
        maxWidth={size * 0.9}
      />
      {/* State progress: "3 / 7" */}
      <troikaText
        text={`${data.stateIndex} / ${data.totalStates}`}
        fontSize={size * 0.04}
        font={FONT_MONO}
        color="#64748b"
        anchorX="center"
        anchorY="middle"
        position={[0, -size * 0.05, 0.002]}
        sdfGlyphSize={64}
      />
      {/* Entered-at timestamp */}
      <troikaText
        text={`since ${data.enteredAt}`}
        fontSize={size * 0.03}
        font={FONT_MONO}
        color="#475569"
        anchorX="center"
        anchorY="middle"
        position={[0, -size * 0.1, 0.002]}
        maxWidth={size * 0.9}
        sdfGlyphSize={64}
      />
      {/* Available transitions */}
      {data.transitions.map((t, i) => (
        <TransitionButton
          key={i}
          label={`\u2192 ${t.targetState}${t.requiredRole ? ` (${t.requiredRole})` : ''}`}
          position={[0, -size * 0.18 - i * size * 0.08, 0.002]}
          size={size}
        />
      ))}
    </group>
  );
}
