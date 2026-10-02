// Marabunta - Licensed under the MIT License.
// ── EdgeCurve ──
// Transitions edges from flat straight lines (Z=0 plane) to 3D arcs as nodes
// lift during unfold. At t=0.6s (particles progress > 0), gossip particles
// begin spawning and traveling along the arcs. Uses pre-allocated buffers
// to avoid GC during animation. (W5D / Spec S21)

import { useRef, useMemo } from 'react';
import { useFrame } from '@react-three/fiber';
import * as THREE from 'three';
import { easeOutCubic } from './easing';

// ── Constants ──

/** Number of segments in the arc curve */
const ARC_SEGMENTS = 32;

/** Perpendicular offset as a fraction of edge length for arc curvature */
const ARC_HEIGHT_FACTOR = 0.3;

/** Maximum concurrent gossip particles per edge */
const MAX_PARTICLES_PER_EDGE = 3;

/** Particle size in world units */
const PARTICLE_SIZE = 0.15;

// ── Pre-allocated reusable vectors (avoid per-frame allocation) ──

const _mid = new THREE.Vector3();
const _dir = new THREE.Vector3();
const _up = new THREE.Vector3(0, 1, 0);
const _perp = new THREE.Vector3();

// ── Props ──

interface EdgeCurveProps {
  /** Source node 3D position (already lifted) */
  sourcePos: THREE.Vector3;
  /** Target node 3D position (already lifted) */
  targetPos: THREE.Vector3;
  /** 0-1 from timeline.edgeCurve */
  curveProgress: number;
  /** 0-1 from timeline.particles */
  particleProgress: number;
  /** Edge color (defaults to theme cyan) */
  color?: string;
}

// ── Component ──

export function EdgeCurve({
  sourcePos,
  targetPos,
  curveProgress,
  particleProgress,
  color = '#22d3ee',
}: EdgeCurveProps) {
  const lineRef = useRef<THREE.Line>(null);
  const pointsRef = useRef<THREE.Points>(null);

  // Pre-allocate positions buffer for the arc line
  const linePositions = useMemo(
    () => new Float32Array((ARC_SEGMENTS + 1) * 3),
    [],
  );

  // Pre-allocate particle positions and state
  const particleState = useMemo(() => {
    const positions = new Float32Array(MAX_PARTICLES_PER_EDGE * 3);
    const tValues = new Float32Array(MAX_PARTICLES_PER_EDGE);
    const durations = new Float32Array(MAX_PARTICLES_PER_EDGE);

    // Initialize with staggered start positions and random durations
    for (let i = 0; i < MAX_PARTICLES_PER_EDGE; i++) {
      tValues[i] = i / MAX_PARTICLES_PER_EDGE;
      durations[i] = 1.5 + Math.random() * 1.0; // 1.5-2.5 seconds
    }

    return { positions, tValues, durations };
  }, []);

  // Create buffer geometries once
  const lineGeometry = useMemo(() => {
    const geo = new THREE.BufferGeometry();
    geo.setAttribute(
      'position',
      new THREE.BufferAttribute(linePositions, 3),
    );
    return geo;
  }, [linePositions]);

  const particleGeometry = useMemo(() => {
    const geo = new THREE.BufferGeometry();
    geo.setAttribute(
      'position',
      new THREE.BufferAttribute(particleState.positions, 3),
    );
    return geo;
  }, [particleState.positions]);

  useFrame((_, delta) => {
    if (!lineRef.current) return;

    const easedCurve = easeOutCubic(curveProgress);

    // Compute midpoint between source and target
    _mid.lerpVectors(sourcePos, targetPos, 0.5);

    // Compute perpendicular offset direction for the arc
    _dir.subVectors(targetPos, sourcePos).normalize();
    _perp.crossVectors(_dir, _up).normalize();

    // If direction is parallel to up, use a different reference
    if (_perp.lengthSq() < 0.001) {
      _perp.set(1, 0, 0);
    }

    // Scale arc height by edge length and animation progress
    const edgeLen = sourcePos.distanceTo(targetPos);
    const arcHeight = edgeLen * ARC_HEIGHT_FACTOR * easedCurve;
    _mid.addScaledVector(_perp, arcHeight);

    // Also lift the midpoint control point upward for more natural arcs
    _mid.y += arcHeight * 0.5;

    // Build the quadratic bezier curve
    const curve = new THREE.QuadraticBezierCurve3(
      sourcePos.clone(),
      _mid.clone(),
      targetPos.clone(),
    );
    const points = curve.getPoints(ARC_SEGMENTS);

    // Update line geometry in place (no allocation)
    for (let i = 0; i <= ARC_SEGMENTS; i++) {
      linePositions[i * 3] = points[i].x;
      linePositions[i * 3 + 1] = points[i].y;
      linePositions[i * 3 + 2] = points[i].z;
    }
    lineGeometry.attributes.position.needsUpdate = true;
    lineGeometry.computeBoundingSphere();

    // Advance gossip particles along the curve
    if (particleProgress > 0 && pointsRef.current) {
      const { positions, tValues, durations } = particleState;

      for (let i = 0; i < MAX_PARTICLES_PER_EDGE; i++) {
        tValues[i] += delta / durations[i];
        if (tValues[i] > 1.0) tValues[i] -= 1.0;

        // Sample the curve at the particle's t value
        const point = curve.getPoint(tValues[i]);
        positions[i * 3] = point.x;
        positions[i * 3 + 1] = point.y;
        positions[i * 3 + 2] = point.z;
      }

      particleGeometry.attributes.position.needsUpdate = true;
    }
  });

  return (
    <>
      {/* Arc line */}
      <line ref={lineRef} geometry={lineGeometry}>
        <lineBasicMaterial
          color={color}
          opacity={0.6}
          transparent
          depthWrite={false}
          blending={THREE.AdditiveBlending}
        />
      </line>

      {/* Gossip particles traveling along the arc */}
      {particleProgress > 0 && (
        <points ref={pointsRef} geometry={particleGeometry}>
          <pointsMaterial
            color={color}
            size={PARTICLE_SIZE}
            sizeAttenuation
            transparent
            opacity={easeOutCubic(particleProgress) * 0.9}
            depthWrite={false}
            blending={THREE.AdditiveBlending}
          />
        </points>
      )}
    </>
  );
}
