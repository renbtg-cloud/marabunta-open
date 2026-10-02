// Marabunta - Licensed under the MIT License.
// ── NodeLift ──
// Lifts nodes off the Z=0 plane according to their role weight during unfold.
// AGGREGATOR nodes rise highest (40 units), EPHEMERAL barely lift (8 units).
// Includes optional micro-stagger: nodes offset their start by a small amount
// derived from distance to the graph centroid, creating a ripple effect.
// (W5D / Spec S21)

import { useRef, type ReactNode } from 'react';
import { useFrame } from '@react-three/fiber';
import * as THREE from 'three';
import { easeOutCubic, clamp } from './easing';

// ── Constants ──

/** Maximum Z height in world units (AGGREGATOR at full weight) */
const MAX_Z_HEIGHT = 40;

/** Maximum stagger offset (normalized progress units, ~50ms at 2s duration) */
const MAX_STAGGER = 0.025;

/** Role weight mapping: determines how high each role lifts */
const ROLE_WEIGHTS: Record<string, number> = {
  AGGREGATOR: 1.0,
  GATEWAY:    0.8,
  WITNESS:    0.7,
  RELAY:      0.6,
  STORAGE:    0.5,
  EPHEMERAL:  0.2,
};

// ── Props ──

interface NodeLiftProps {
  /** Node role from swarm types */
  role: string;
  /** 0-1 progress from timeline.nodeLift */
  progress: number;
  /** Original 2D position [x, y] on the Z=0 plane */
  position2D: [number, number];
  /** Stagger offset based on distance from graph centroid (0 to 1) */
  staggerFactor?: number;
  /** Child elements (NodeSphere, RoleCorona, etc.) */
  children: ReactNode;
}

// ── Component ──

export function NodeLift({
  role,
  progress,
  position2D,
  staggerFactor = 0,
  children,
}: NodeLiftProps) {
  const groupRef = useRef<THREE.Group>(null);
  const weight = ROLE_WEIGHTS[role] ?? 0.5;
  const targetZ = weight * MAX_Z_HEIGHT;

  // Micro-stagger: offset the effective progress by a small amount
  // so nodes closer to the edge of the graph start lifting slightly later.
  const staggerOffset = staggerFactor * MAX_STAGGER;

  useFrame(() => {
    if (!groupRef.current) return;

    const effectiveProgress = clamp(progress - staggerOffset, 0, 1);
    const easedProgress = easeOutCubic(effectiveProgress);

    groupRef.current.position.set(
      position2D[0],
      position2D[1],
      targetZ * easedProgress,
    );
  });

  return <group ref={groupRef}>{children}</group>;
}

// ── Utility ──

/**
 * Get the role weight for a given role name.
 * Useful for external components that need to know a node's target Z height.
 */
export function getRoleWeight(role: string): number {
  return ROLE_WEIGHTS[role] ?? 0.5;
}

/**
 * Compute the final Z position for a node with a given role.
 */
export function getTargetZ(role: string): number {
  return (ROLE_WEIGHTS[role] ?? 0.5) * MAX_Z_HEIGHT;
}
