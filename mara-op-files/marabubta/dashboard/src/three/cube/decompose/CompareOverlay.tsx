// Marabunta - Licensed under the MIT License.
// ── CompareOverlay ──
// Renders a translucent wireframe "ghost cube" next to the selected cube
// to visualize comparison against the population average. Green if below
// average, red if above. Size difference encodes magnitude of deviation.
// The ghost appears with a 200ms fade-in, 2 world units to the right.

import React, { useState, useMemo, useRef } from 'react';
import * as THREE from 'three';
import { useFrame } from '@react-three/fiber';
import { Text } from 'troika-three-text';
import { extend } from '@react-three/fiber';
import type { Object3DNode } from '@react-three/fiber';
import type { CompareData } from './DecompositionTree';
import { FONT_MONO } from '../types';

extend({ TroikaText: Text });

declare module '@react-three/fiber' {
  interface ThreeElements {
    troikaText: Object3DNode<Text, typeof Text>;
  }
}

// ── Utility ──

function formatValue(v: number): string {
  if (Math.abs(v) >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
  if (Math.abs(v) >= 1_000) return `${(v / 1_000).toFixed(1)}K`;
  return v.toFixed(1);
}

// ── Props ──

interface CompareOverlayProps {
  data: CompareData;
  worldPos: THREE.Vector3;
  /** Whether the overlay should be visible */
  visible: boolean;
  /** Called to dismiss the overlay */
  onDismiss?: () => void;
}

// ── Component ──

export function CompareOverlay({
  data,
  worldPos,
  visible,
  onDismiss,
}: CompareOverlayProps) {
  const groupRef = useRef<THREE.Group>(null);
  const [opacity, setOpacity] = useState(0);

  // Fade-in / fade-out animation
  useFrame((_, delta) => {
    if (!groupRef.current) return;
    const target = visible ? 0.25 : 0;
    const speed = 1 / 0.2; // 200ms fade
    const next = THREE.MathUtils.lerp(opacity, target, Math.min(1, delta * speed * 5));
    setOpacity(next);

    // Hide group when fully faded out
    groupRef.current.visible = next > 0.005;
  });

  const ratio = data.currentValue / (data.populationAvg || 1);
  const isAbove = ratio > 1;
  const ghostScale = 1 / ratio;
  const color = isAbove ? '#fb7185' : '#34d399';
  const deviationPct = ((ratio - 1) * 100).toFixed(1);

  // Ghost position: 2 world units to the right of the cube
  const ghostPos = useMemo(
    () => [worldPos.x + 2.0, worldPos.y, worldPos.z] as [number, number, number],
    [worldPos],
  );

  return (
    <group ref={groupRef} position={ghostPos}>
      {/* Ghost wireframe cube */}
      <mesh scale={[ghostScale, ghostScale, ghostScale]}>
        <boxGeometry args={[1, 1, 1]} />
        <meshStandardMaterial
          color={color}
          transparent
          opacity={opacity}
          wireframe
        />
      </mesh>

      {/* Solid fill at very low opacity for depth */}
      <mesh scale={[ghostScale, ghostScale, ghostScale]}>
        <boxGeometry args={[1, 1, 1]} />
        <meshStandardMaterial
          color={color}
          transparent
          opacity={opacity * 0.3}
          side={THREE.DoubleSide}
        />
      </mesh>

      {/* Label: population average value */}
      <troikaText
        text={`AVG: ${formatValue(data.populationAvg)}`}
        font={FONT_MONO}
        fontSize={0.12}
        color={color}
        anchorX="center"
        anchorY="bottom"
        position={[0, 0.8, 0]}
        outlineWidth={0.002}
        sdfGlyphSize={48}
      />

      {/* Deviation percentage */}
      <troikaText
        text={`${isAbove ? '+' : ''}${deviationPct}%`}
        font={FONT_MONO}
        fontSize={0.1}
        color={color}
        anchorX="center"
        anchorY="top"
        position={[0, 0.75, 0]}
        sdfGlyphSize={48}
      />

      {/* Sample size */}
      <troikaText
        text={`n=${data.sampleSize}`}
        font={FONT_MONO}
        fontSize={0.07}
        color="#64748b"
        anchorX="center"
        anchorY="top"
        position={[0, -0.7, 0]}
        sdfGlyphSize={48}
      />

      {/* Connecting dashed line between real cube and ghost */}
      <mesh position={[-1, 0, 0]} rotation={[0, 0, Math.PI / 2]}>
        <cylinderGeometry args={[0.005, 0.005, 2, 4]} />
        <meshBasicMaterial color="#334155" transparent opacity={opacity * 2} />
      </mesh>
    </group>
  );
}
