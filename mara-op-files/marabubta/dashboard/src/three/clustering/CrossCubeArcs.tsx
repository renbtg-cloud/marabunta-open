// Marabunta - Licensed under the MIT License.
// ── Cross-Cube Arcs ──
// Luminous arcs between cubes that have causal, replication, data-feed,
// or dependency relationships. Arcs use CubicBezierCurve3 geometry with
// animated dash-offset pulses. Arcs re-route when cubes cluster or
// collapse into mega-cubes.
// W6C / Spec S21

import React, { useMemo, useRef } from 'react';
import * as THREE from 'three';
import { useFrame } from '@react-three/fiber';
import { Line2, LineGeometry, LineMaterial } from 'three-stdlib';

// ── Arc Types ──

export type ArcRelationship = 'trigger' | 'replication' | 'data-feed' | 'dependency';

export interface ArcDefinition {
  id: string;
  sourceId: string;
  targetId: string;
  relationship: ArcRelationship;
  /** Events per second (for trigger / data-feed thickness). */
  frequency: number;
  /** Bytes per second (for replication thickness). */
  volume: number;
}

// ── Color palette per relationship type ──

const ARC_COLORS: Record<ArcRelationship, string> = {
  trigger: '#22d3ee',
  replication: '#34d399',
  'data-feed': '#fbbf24',
  dependency: '#a78bfa',
};

// ── Thickness rules ──

function mapFrequencyToThickness(
  relationship: ArcRelationship,
  frequency: number,
  volume: number,
): number {
  switch (relationship) {
    case 'trigger':
      return Math.min(0.1, 0.02 + frequency * 0.005);
    case 'replication': {
      const mbPerSec = volume / (1024 * 1024);
      return Math.min(0.12, 0.03 + mbPerSec * 0.001);
    }
    case 'data-feed':
      return Math.min(0.08, 0.02 + frequency * 0.003);
    case 'dependency':
      return 0.015;
  }
}

// ── Arc curve geometry ──

function computeArcCurve(
  source: THREE.Vector3,
  target: THREE.Vector3,
): THREE.CubicBezierCurve3 {
  const distance = source.distanceTo(target);
  const liftHeight = distance * 0.35;
  const direction = target.clone().sub(source);

  const cp1 = source.clone().add(direction.clone().multiplyScalar(0.25));
  cp1.y += liftHeight;

  const cp2 = source.clone().add(direction.clone().multiplyScalar(0.75));
  cp2.y += liftHeight;

  return new THREE.CubicBezierCurve3(source, cp1, cp2, target);
}

// ── Animated Arc Line ──

interface ArcLineProps {
  points: THREE.Vector3[];
  thickness: number;
  color: string;
  animated: boolean;
  speed?: number;
}

function ArcLine({ points, thickness, color, animated, speed = 2.0 }: ArcLineProps) {
  const lineRef = useRef<Line2>(null);
  const materialRef = useRef<LineMaterial>(null);

  const geometry = useMemo(() => {
    const geo = new LineGeometry();
    geo.setPositions(points.flatMap((p) => [p.x, p.y, p.z]));
    return geo;
  }, [points]);

  const material = useMemo(() => {
    const mat = new LineMaterial({
      color: new THREE.Color(color).getHex(),
      linewidth: thickness,
      dashed: true,
      dashSize: 0.3,
      gapSize: 0.15,
      transparent: true,
      opacity: 0.8,
    });
    mat.resolution.set(window.innerWidth, window.innerHeight);
    return mat;
  }, [color, thickness]);

  useFrame((_, delta) => {
    if (animated && material) {
      material.dashOffset -= delta * speed;
    }
  });

  return <primitive object={new Line2(geometry, material)} ref={lineRef} />;
}

// ── Resolve effective positions ──

/**
 * Resolve the effective endpoint for an arc, considering clustering state.
 * If the cube is inside a collapsed cluster, the arc terminates at the
 * mega-cube centroid. If in a non-collapsed cluster, terminates at the
 * cluster boundary nearest to the source.
 */
function resolveArcEndpoint(
  cubeId: string,
  cubePositions: Map<string, THREE.Vector3>,
  clusterMap: Map<string, { centroid: THREE.Vector3; collapsed: boolean; memberIds: Set<string> }>,
): THREE.Vector3 | null {
  // Check if cube is in a collapsed cluster
  for (const cluster of clusterMap.values()) {
    if (cluster.memberIds.has(cubeId)) {
      if (cluster.collapsed) {
        return cluster.centroid.clone();
      }
    }
  }
  // Return raw cube position
  return cubePositions.get(cubeId)?.clone() ?? null;
}

// ── Props ──

export interface CrossCubeArcsProps {
  arcs: ArcDefinition[];
  cubePositions: Map<string, THREE.Vector3>;
  /** Map of clusterId -> cluster info for arc re-routing. Optional. */
  clusterMap?: Map<string, {
    centroid: THREE.Vector3;
    collapsed: boolean;
    memberIds: Set<string>;
  }>;
}

// ── Component ──

export function CrossCubeArcs({
  arcs,
  cubePositions,
  clusterMap = new Map(),
}: CrossCubeArcsProps) {
  return (
    <group>
      {arcs.map((arc) => {
        const source = resolveArcEndpoint(arc.sourceId, cubePositions, clusterMap);
        const target = resolveArcEndpoint(arc.targetId, cubePositions, clusterMap);
        if (!source || !target) return null;

        // Skip internal arcs within same collapsed cluster (rendered as boundary glow instead)
        if (source.equals(target)) return null;

        const curve = computeArcCurve(source, target);
        const points = curve.getPoints(64);
        const thickness = mapFrequencyToThickness(arc.relationship, arc.frequency, arc.volume);
        const color = ARC_COLORS[arc.relationship];
        // Higher-frequency arcs get faster pulse
        const speed = Math.min(6, 1 + arc.frequency * 0.05);

        return (
          <ArcLine
            key={arc.id}
            points={points}
            thickness={thickness}
            color={color}
            animated
            speed={speed}
          />
        );
      })}
    </group>
  );
}
