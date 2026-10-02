// Marabunta - Licensed under the MIT License.
// ── TrendChart ──
// Renders a sparkline directly on the cube face, replacing the static value
// with a time-series chart. X-axis is time, Y-axis is metric value. The
// sparkline is rendered as a Line geometry from @react-three/drei. Clicking
// any point on the sparkline loads the historical state at that timestamp.

import React, { useMemo, useState, useRef } from 'react';
import * as THREE from 'three';
import { useFrame } from '@react-three/fiber';
import { Line } from '@react-three/drei';
import { Text } from 'troika-three-text';
import { extend } from '@react-three/fiber';
import type { Object3DNode } from '@react-three/fiber';
import type { TrendPoint } from './DecompositionTree';
import { FONT_MONO } from '../types';

extend({ TroikaText: Text });

declare module '@react-three/fiber' {
  interface ThreeElements {
    troikaText: Object3DNode<Text, typeof Text>;
  }
}

// ── Utility ──

function formatTimestamp(ts: number): string {
  const d = new Date(ts);
  return `${d.getMonth() + 1}/${d.getDate()} ${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
}

function formatShortValue(v: number): string {
  if (Math.abs(v) >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
  if (Math.abs(v) >= 1_000) return `${(v / 1_000).toFixed(1)}K`;
  return v.toFixed(0);
}

// ── Props ──

interface TrendChartProps {
  /** Time-series data points */
  points: TrendPoint[];
  /** Size of the cube face */
  faceSize: number;
  /** Offset from face surface */
  zOffset?: number;
  /** Called when user clicks a data point to load historical state */
  onPointClick?: (timestamp: number) => void;
  /** Whether the chart is visible */
  visible?: boolean;
}

// ── Component ──

export function TrendChart({
  points,
  faceSize,
  zOffset = 0.02,
  onPointClick,
  visible = true,
}: TrendChartProps) {
  const [hoveredIndex, setHoveredIndex] = useState<number | null>(null);
  const groupRef = useRef<THREE.Group>(null);

  // Compute normalized line points
  const { linePoints, minVal, maxVal, range } = useMemo(() => {
    if (!points.length)
      return { linePoints: [] as [number, number, number][], minVal: 0, maxVal: 0, range: 1 };

    const vals = points.map((p) => p.value);
    const min = Math.min(...vals);
    const max = Math.max(...vals);
    const r = max - min || 1;

    const pts = points.map(
      (p, i) =>
        [
          (i / (points.length - 1) - 0.5) * faceSize * 0.9,
          ((p.value - min) / r - 0.5) * faceSize * 0.7,
          zOffset,
        ] as [number, number, number],
    );

    return { linePoints: pts, minVal: min, maxVal: max, range: r };
  }, [points, faceSize, zOffset]);

  // Fade animation
  useFrame(() => {
    if (!groupRef.current) return;
    groupRef.current.visible = visible;
  });

  if (!points.length || !linePoints.length) return null;

  return (
    <group ref={groupRef}>
      {/* Main sparkline */}
      <Line
        points={linePoints}
        color="#22d3ee"
        lineWidth={2}
      />

      {/* Clickable data points */}
      {linePoints.map((pos, i) => (
        <mesh
          key={i}
          position={pos}
          onClick={(e) => {
            e.stopPropagation();
            onPointClick?.(points[i].timestamp);
          }}
          onPointerOver={() => setHoveredIndex(i)}
          onPointerOut={() => setHoveredIndex(null)}
        >
          <sphereGeometry args={[hoveredIndex === i ? 0.04 : 0.02, 8, 8]} />
          <meshBasicMaterial
            color={hoveredIndex === i ? '#ffffff' : '#22d3ee'}
          />
        </mesh>
      ))}

      {/* Hover tooltip */}
      {hoveredIndex !== null && linePoints[hoveredIndex] && (
        <group position={linePoints[hoveredIndex]}>
          {/* Background panel */}
          <mesh position={[0, 0.12, 0.01]}>
            <planeGeometry args={[0.4, 0.12]} />
            <meshBasicMaterial
              color="#111827"
              transparent
              opacity={0.9}
            />
          </mesh>
          <troikaText
            text={`${formatShortValue(points[hoveredIndex].value)}`}
            font={FONT_MONO}
            fontSize={faceSize * 0.04}
            color="#e8ecf4"
            anchorX="center"
            anchorY="bottom"
            position={[0, 0.1, 0.02]}
            sdfGlyphSize={48}
          />
          <troikaText
            text={formatTimestamp(points[hoveredIndex].timestamp)}
            font={FONT_MONO}
            fontSize={faceSize * 0.03}
            color="#64748b"
            anchorX="center"
            anchorY="top"
            position={[0, 0.09, 0.02]}
            sdfGlyphSize={48}
          />
        </group>
      )}

      {/* Y-axis labels: min and max */}
      <troikaText
        text={formatShortValue(maxVal)}
        font={FONT_MONO}
        fontSize={faceSize * 0.035}
        color="#475569"
        anchorX="right"
        anchorY="middle"
        position={[-faceSize * 0.47, faceSize * 0.35, zOffset]}
        sdfGlyphSize={48}
      />
      <troikaText
        text={formatShortValue(minVal)}
        font={FONT_MONO}
        fontSize={faceSize * 0.035}
        color="#475569"
        anchorX="right"
        anchorY="middle"
        position={[-faceSize * 0.47, -faceSize * 0.35, zOffset]}
        sdfGlyphSize={48}
      />

      {/* X-axis baseline */}
      <mesh position={[0, -faceSize * 0.35 - 0.01, zOffset - 0.005]}>
        <boxGeometry args={[faceSize * 0.92, 0.005, 0.001]} />
        <meshBasicMaterial color="#1e293b" />
      </mesh>
    </group>
  );
}
