// Marabunta - Licensed under the MIT License.
// ── Fly-To-Cluster Animation ──
// Animates cubes along quadratic Bezier arcs from their current position
// to a cluster's centroid. Staggered timing creates a cascade effect.
// W6C / Spec S21

import React, { useRef, useMemo, useCallback } from 'react';
import * as THREE from 'three';
import { useFrame } from '@react-three/fiber';

// ── Easing Functions ──

/** Decelerating cubic easing. */
export function easeOutCubic(t: number): number {
  return 1 - Math.pow(1 - t, 3);
}

/** Accelerating quadratic easing (for collapse shrink phase). */
export function easeInQuad(t: number): number {
  return t * t;
}

/** Symmetric cubic easing (for layout transitions). */
export function easeInOutCubic(t: number): number {
  return t < 0.5
    ? 4 * t * t * t
    : 1 - Math.pow(-2 * t + 2, 3) / 2;
}

/** Overshoot easing (for mega-cube pop-in). */
export function easeOutBack(t: number): number {
  const c1 = 1.70158;
  const c3 = c1 + 1;
  return 1 + c3 * Math.pow(t - 1, 3) + c1 * Math.pow(t - 1, 2);
}

export type EasingFn = (t: number) => number;

// ── Props ──

export interface FlyToClusterProps {
  cubeId: string;
  startPosition: THREE.Vector3;
  targetPosition: THREE.Vector3;
  duration?: number;
  delay?: number;
  easing?: EasingFn;
  onComplete: () => void;
  children?: React.ReactNode;
}

// ── Component ──

export function FlyToCluster({
  cubeId,
  startPosition,
  targetPosition,
  duration = 1200,
  delay = 0,
  easing = easeOutCubic,
  onComplete,
  children,
}: FlyToClusterProps) {
  const groupRef = useRef<THREE.Group>(null);
  const elapsed = useRef(0);
  const completed = useRef(false);

  // Compute arc midpoint: average XZ, elevated Y proportional to distance
  const midpoint = useMemo(() => {
    const mid = startPosition.clone().lerp(targetPosition, 0.5);
    const dist = startPosition.distanceTo(targetPosition);
    mid.y += dist * 0.4;
    return mid;
  }, [startPosition, targetPosition]);

  useFrame((_, delta) => {
    if (!groupRef.current || completed.current) return;

    elapsed.current += delta * 1000;
    const t = Math.min(1, Math.max(0, (elapsed.current - delay) / duration));
    if (t <= 0) return;

    const easedT = easing(t);

    // Quadratic Bezier: B(t) = (1-t)^2*P0 + 2(1-t)t*P1 + t^2*P2
    const oneMinusT = 1 - easedT;
    const pos = new THREE.Vector3()
      .addScaledVector(startPosition, oneMinusT * oneMinusT)
      .addScaledVector(midpoint, 2 * oneMinusT * easedT)
      .addScaledVector(targetPosition, easedT * easedT);

    groupRef.current.position.copy(pos);

    if (t >= 1 && !completed.current) {
      completed.current = true;
      onComplete();
    }
  });

  /** Reset animation state for reverse / re-trigger. */
  const reset = useCallback(() => {
    elapsed.current = 0;
    completed.current = false;
  }, []);

  return (
    <group ref={groupRef} position={startPosition.toArray() as [number, number, number]}>
      {children}
    </group>
  );
}

// ── Stagger Utility ──

/**
 * Compute stagger delays for a set of cube IDs based on distance to centroid.
 * Closer cubes get shorter delays. Returns a Map of cubeId -> delay in ms.
 *
 * @param cubePositions Map of cube ID to current position
 * @param centroid      Target centroid
 * @param baseDelay     Per-cube stagger increment (default 30ms)
 */
export function computeStaggerDelays(
  cubePositions: Map<string, THREE.Vector3>,
  centroid: THREE.Vector3,
  baseDelay = 30,
): Map<string, number> {
  // Sort by distance to centroid (ascending) so nearest cubes fly first
  const entries = [...cubePositions.entries()].map(([id, pos]) => ({
    id,
    distance: pos.distanceTo(centroid),
  }));
  entries.sort((a, b) => a.distance - b.distance);

  const delays = new Map<string, number>();
  entries.forEach((entry, index) => {
    delays.set(entry.id, index * baseDelay);
  });
  return delays;
}

/**
 * Compute flight duration scaled by distance.
 * Minimum 800ms, maximum 1500ms. Further cubes take longer.
 */
export function computeFlightDuration(
  startPosition: THREE.Vector3,
  targetPosition: THREE.Vector3,
  minDuration = 800,
  maxDuration = 1500,
): number {
  const dist = startPosition.distanceTo(targetPosition);
  // Normalise against a reference distance of 30 world units
  const t = Math.min(1, dist / 30);
  return minDuration + t * (maxDuration - minDuration);
}
