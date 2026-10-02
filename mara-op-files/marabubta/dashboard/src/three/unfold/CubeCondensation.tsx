// Marabunta - Licensed under the MIT License.
// ── CubeCondensation ──
// Condenses workflow instance markers into InspectionCube objects during unfold.
// Four phases: Gather (markers fly to cube center), Form (cube scales in),
// Populate (face content fades in), Connect (cross-cube dependency arcs).
// (W5D / Spec S21)

import { useRef, useMemo } from 'react';
import { useFrame } from '@react-three/fiber';
import * as THREE from 'three';
import { easeOutCubic } from './easing';

// ── Constants ──

/** Number of marker particles per workflow instance */
const MARKERS_PER_WORKFLOW = 6;

/** Cross-cube arc color (violet, differentiated from gossip edges) */
const CROSS_ARC_COLOR = '#a78bfa';

/** Cross-cube arc max opacity (semi-transparent to avoid clutter) */
const CROSS_ARC_MAX_OPACITY = 0.5;

/** Number of segments in cross-cube arc curves */
const CROSS_ARC_SEGMENTS = 24;

/** Marker particle size */
const MARKER_SIZE = 0.12;

// ── Pre-allocated vectors ──

const _cubeTarget = new THREE.Vector3();
const _arcMid = new THREE.Vector3();

// ── Props ──

interface CubeCondensationProps {
  /** 0-1 from timeline.cubeScale -- cube geometry scale */
  scaleProgress: number;
  /** 0-1 from timeline.cubeFaces -- face content opacity */
  faceProgress: number;
  /** 0-1 from timeline.crossArcs -- cross-cube arc opacity */
  arcProgress: number;
  /** Target 3D position for this cube */
  targetPosition: [number, number, number];
  /** Initial 2D marker positions (scattered in the flat view) */
  markerPositions?: [number, number][];
  /** Connected cube positions for cross-arcs */
  connections?: Array<{ id: string; position: [number, number, number] }>;
  /** Children: the actual InspectionCube component */
  children?: React.ReactNode;
}

// ── Component ──

export function CubeCondensation({
  scaleProgress,
  faceProgress,
  arcProgress,
  targetPosition,
  markerPositions,
  connections = [],
  children,
}: CubeCondensationProps) {
  const groupRef = useRef<THREE.Group>(null);
  const markersRef = useRef<THREE.Points>(null);

  const cubeScale = easeOutCubic(scaleProgress);
  const faceOpacity = easeOutCubic(faceProgress);

  // Pre-allocate marker particle positions
  const markerState = useMemo(() => {
    const count = markerPositions?.length ?? MARKERS_PER_WORKFLOW;
    const positions = new Float32Array(count * 3);

    // Initialize markers at their 2D scatter positions
    for (let i = 0; i < count; i++) {
      if (markerPositions && i < markerPositions.length) {
        positions[i * 3] = markerPositions[i][0];
        positions[i * 3 + 1] = markerPositions[i][1];
        positions[i * 3 + 2] = 0;
      } else {
        // Random scatter positions around the target
        const angle = (i / count) * Math.PI * 2;
        const radius = 2 + Math.random() * 3;
        positions[i * 3] = targetPosition[0] + Math.cos(angle) * radius;
        positions[i * 3 + 1] = targetPosition[1] + Math.sin(angle) * radius;
        positions[i * 3 + 2] = 0;
      }
    }

    const geo = new THREE.BufferGeometry();
    geo.setAttribute('position', new THREE.BufferAttribute(positions, 3));
    return { positions, geometry: geo, count };
  }, [markerPositions, targetPosition]);

  // Pre-allocate cross-arc line buffers
  const arcBuffers = useMemo(() => {
    return connections.map(() => {
      const positions = new Float32Array((CROSS_ARC_SEGMENTS + 1) * 3);
      const geo = new THREE.BufferGeometry();
      geo.setAttribute('position', new THREE.BufferAttribute(positions, 3));
      return { positions, geometry: geo };
    });
  }, [connections]);

  useFrame(() => {
    if (!groupRef.current) return;

    // Set the cube group position and scale
    _cubeTarget.set(targetPosition[0], targetPosition[1], targetPosition[2]);
    groupRef.current.position.copy(_cubeTarget);
    groupRef.current.scale.setScalar(Math.max(cubeScale, 0.001));

    // Animate marker particles converging toward cube center (Gather phase)
    // scaleProgress < 0.5: markers are still visible and moving inward
    // scaleProgress >= 0.5: markers have merged into the cube
    if (markersRef.current && scaleProgress < 1) {
      const { positions, count } = markerState;
      const gatherT = easeOutCubic(Math.min(scaleProgress * 2, 1)); // double speed

      for (let i = 0; i < count; i++) {
        const startX = markerPositions?.[i]?.[0] ?? positions[i * 3];
        const startY = markerPositions?.[i]?.[1] ?? positions[i * 3 + 1];

        positions[i * 3] = startX + (targetPosition[0] - startX) * gatherT;
        positions[i * 3 + 1] = startY + (targetPosition[1] - startY) * gatherT;
        positions[i * 3 + 2] = targetPosition[2] * gatherT;
      }

      markerState.geometry.attributes.position.needsUpdate = true;
    }

    // Update cross-cube arc geometry
    if (arcProgress > 0) {
      connections.forEach((conn, idx) => {
        if (idx >= arcBuffers.length) return;
        const buf = arcBuffers[idx];

        const src = _cubeTarget;
        const tgt = new THREE.Vector3(
          conn.position[0],
          conn.position[1],
          conn.position[2],
        );

        // Midpoint with upward arc
        _arcMid.lerpVectors(src, tgt, 0.5);
        const dist = src.distanceTo(tgt);
        _arcMid.y += dist * 0.25 * easeOutCubic(arcProgress);

        const curve = new THREE.QuadraticBezierCurve3(
          src.clone(),
          _arcMid.clone(),
          tgt,
        );
        const points = curve.getPoints(CROSS_ARC_SEGMENTS);

        for (let i = 0; i <= CROSS_ARC_SEGMENTS; i++) {
          buf.positions[i * 3] = points[i].x;
          buf.positions[i * 3 + 1] = points[i].y;
          buf.positions[i * 3 + 2] = points[i].z;
        }

        buf.geometry.attributes.position.needsUpdate = true;
        buf.geometry.computeBoundingSphere();
      });
    }
  });

  // Determine whether markers are still visible (before cube fully forms)
  const showMarkers = scaleProgress > 0 && scaleProgress < 1;

  return (
    <>
      {/* Converging marker particles (Gather phase) */}
      {showMarkers && (
        <points ref={markersRef} geometry={markerState.geometry}>
          <pointsMaterial
            color="#a78bfa"
            size={MARKER_SIZE}
            sizeAttenuation
            transparent
            opacity={1 - easeOutCubic(scaleProgress)}
            depthWrite={false}
            blending={THREE.AdditiveBlending}
          />
        </points>
      )}

      {/* Cube container: scales in during Form phase */}
      <group ref={groupRef}>
        {cubeScale > 0.01 && (
          <group>
            {/* Children should be InspectionCube with faceOpacity support */}
            {children}

            {/* Face content overlay opacity indicator (edge glow ramp) */}
            {faceOpacity > 0 && faceOpacity < 1 && (
              <mesh>
                <boxGeometry args={[1.02, 1.02, 1.02]} />
                <meshBasicMaterial
                  color="#22d3ee"
                  transparent
                  opacity={(1 - faceOpacity) * 0.1}
                  wireframe
                  depthWrite={false}
                />
              </mesh>
            )}
          </group>
        )}
      </group>

      {/* Cross-cube connection arcs (Connect phase) */}
      {arcProgress > 0 &&
        connections.map((conn, idx) => {
          if (idx >= arcBuffers.length) return null;
          return (
            <line key={conn.id} geometry={arcBuffers[idx].geometry}>
              <lineDashedMaterial
                color={CROSS_ARC_COLOR}
                transparent
                opacity={easeOutCubic(arcProgress) * CROSS_ARC_MAX_OPACITY}
                dashSize={0.3}
                gapSize={0.15}
                depthWrite={false}
                blending={THREE.AdditiveBlending}
              />
            </line>
          );
        })}
    </>
  );
}
