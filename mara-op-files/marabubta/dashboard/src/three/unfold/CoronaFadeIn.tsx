// Marabunta - Licensed under the MIT License.
// ── CoronaFadeIn ──
// Materializes role coronas (glowing halo rings) and database geometry during
// unfold. Corona opacity ramps 0 -> 0.6 with a spring overshoot on scale
// (peaks at 1.15 then settles to 1.0). Database cylinder geometry scales
// from 0 to 1 for STORAGE-role nodes. (W5D / Spec S21)

import { useRef } from 'react';
import { useFrame } from '@react-three/fiber';
import * as THREE from 'three';
import { easeOutCubic } from './easing';

// ── Constants ──

/** Maximum corona opacity (never fully opaque -- glowing, not solid) */
const MAX_CORONA_OPACITY = 0.6;

/** Corona ring inner radius */
const CORONA_INNER_RADIUS = 1.2;

/** Corona ring outer radius */
const CORONA_OUTER_RADIUS = 1.8;

/** Corona ring segment count (smooth circle at any zoom) */
const CORONA_SEGMENTS = 32;

/** Spring overshoot peak factor */
const OVERSHOOT_PEAK = 1.15;

/** Database cylinder dimensions */
const DB_RADIUS = 0.8;
const DB_HEIGHT = 0.3;
const DB_SEGMENTS = 16;

// ── Props ──

interface CoronaFadeInProps {
  /** 0-1 from timeline.corona */
  coronaProgress: number;
  /** 0-1 from timeline.dbGeometry */
  dbProgress: number;
  /** Role color hex string (e.g. '#22d3ee') */
  roleColor: string;
  /** Whether this node has database geometry (STORAGE role) */
  hasDbGeometry: boolean;
}

// ── Spring Overshoot Function ──

/**
 * Simple spring overshoot: scale goes to `overshoot` peak then settles to 1.0.
 * t in [0, 0.7]: ramp up to overshoot
 * t in [0.7, 1.0]: settle from overshoot back to 1.0
 */
function springOvershoot(t: number, overshoot: number): number {
  if (t <= 0) return 0;
  if (t >= 1) return 1;
  if (t < 0.7) {
    return (t / 0.7) * overshoot;
  }
  return overshoot + ((t - 0.7) / 0.3) * (1 - overshoot);
}

// ── Component ──

export function CoronaFadeIn({
  coronaProgress,
  dbProgress,
  roleColor,
  hasDbGeometry,
}: CoronaFadeInProps) {
  const coronaRef = useRef<THREE.Mesh>(null);
  const dbRef = useRef<THREE.Group>(null);

  useFrame(() => {
    // Corona: animate opacity and scale with spring overshoot
    if (coronaRef.current) {
      const mat = coronaRef.current.material as THREE.MeshBasicMaterial;
      mat.opacity = coronaProgress * MAX_CORONA_OPACITY;

      const scale = springOvershoot(coronaProgress, OVERSHOOT_PEAK);
      coronaRef.current.scale.setScalar(scale);
      coronaRef.current.visible = coronaProgress > 0.01;
    }

    // Database geometry: scale from 0 to 1
    if (dbRef.current && hasDbGeometry) {
      const s = easeOutCubic(dbProgress);
      dbRef.current.scale.setScalar(s);
      dbRef.current.visible = dbProgress > 0.01;
    }
  });

  return (
    <>
      {/* Role corona: semi-transparent ring around the node */}
      <mesh ref={coronaRef} visible={false}>
        <ringGeometry args={[CORONA_INNER_RADIUS, CORONA_OUTER_RADIUS, CORONA_SEGMENTS]} />
        <meshBasicMaterial
          color={roleColor}
          transparent
          opacity={0}
          side={THREE.DoubleSide}
          depthWrite={false}
          blending={THREE.AdditiveBlending}
        />
      </mesh>

      {/* Database geometry: gold cylinder for STORAGE-role nodes */}
      {hasDbGeometry && (
        <group ref={dbRef} scale={0} visible={false}>
          <mesh position={[0, -0.8, 0]}>
            <cylinderGeometry args={[DB_RADIUS, DB_RADIUS, DB_HEIGHT, DB_SEGMENTS]} />
            <meshStandardMaterial
              color="#fbbf24"
              metalness={0.3}
              roughness={0.6}
              emissive="#fbbf24"
              emissiveIntensity={0.1}
            />
          </mesh>
        </group>
      )}
    </>
  );
}
