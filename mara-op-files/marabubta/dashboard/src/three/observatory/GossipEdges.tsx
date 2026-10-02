// Marabunta - Licensed under the MIT License.
// ── GossipEdges ──
// Renders gossip communication channels as thin lines with flowing particles.
// Lines encode traffic volume via brightness; particles encode message type via color.
// All edges merged into a single LineSegments draw call; particles as a single Points object.

import { useRef, useMemo } from 'react';
import { useFrame } from '@react-three/fiber';
import * as THREE from 'three';
import {
  type GossipEdge,
  type LayoutEngine,
  type MessageType,
  MESSAGE_PARTICLE_COLORS,
} from './types';

interface GossipEdgesProps {
  edges: GossipEdge[];
  layout: LayoutEngine;
}

// ── Particle state per flowing dot ──

interface Particle {
  edgeIndex: number;
  t: number;           // 0..1 progress along edge
  duration: number;    // seconds to traverse the full edge
  color: THREE.Color;
}

/** Maximum particles per edge, scaled by traffic volume. */
function particlesPerEdge(trafficVolume: number): number {
  if (trafficVolume <= 10) return 1;
  if (trafficVolume <= 50) return 2;
  if (trafficVolume <= 80) return 3;
  return 4;
}

/** Pick the dominant message type color for a given edge. */
function dominantMessageColor(types: MessageType[]): THREE.Color {
  if (types.length === 0) return new THREE.Color(MESSAGE_PARTICLE_COLORS.health);
  // First type is treated as dominant.
  return new THREE.Color(MESSAGE_PARTICLE_COLORS[types[0]]);
}

export function GossipEdges({ edges, layout }: GossipEdgesProps) {
  const linesRef = useRef<THREE.LineSegments>(null!);
  const pointsRef = useRef<THREE.Points>(null!);

  // ── Build merged line geometry ──
  const lineGeometry = useMemo(() => {
    const positions: number[] = [];
    const colors: number[] = [];

    edges.forEach((edge) => {
      const src = layout.getPosition(edge.sourceId);
      const tgt = layout.getPosition(edge.targetId);

      positions.push(src.x, src.y, src.z, tgt.x, tgt.y, tgt.z);

      const brightness = 0.1 + (edge.trafficVolume / 100) * 0.9;
      colors.push(brightness, brightness, brightness, brightness, brightness, brightness);
    });

    const geo = new THREE.BufferGeometry();
    geo.setAttribute('position', new THREE.Float32BufferAttribute(positions, 3));
    geo.setAttribute('color', new THREE.Float32BufferAttribute(colors, 3));
    return geo;
  }, [edges, layout]);

  // ── Initialize particle pool ──
  const particles = useMemo<Particle[]>(() => {
    const pool: Particle[] = [];

    edges.forEach((edge, edgeIdx) => {
      const count = particlesPerEdge(edge.trafficVolume);
      const color = dominantMessageColor(edge.messageTypes);

      for (let p = 0; p < count; p++) {
        pool.push({
          edgeIndex: edgeIdx,
          t: p / count, // stagger starting positions
          duration: 1.5 + Math.random() * 1.0, // 1.5-2.5 seconds
          color: color.clone(),
        });
      }
    });

    return pool;
  }, [edges]);

  // ── Build points geometry for particles ──
  const { pointsGeometry, pointPositions, pointColors } = useMemo(() => {
    const pos = new Float32Array(particles.length * 3);
    const col = new Float32Array(particles.length * 3);

    particles.forEach((p, i) => {
      p.color.toArray(col, i * 3);
    });

    const geo = new THREE.BufferGeometry();
    geo.setAttribute('position', new THREE.BufferAttribute(pos, 3));
    geo.setAttribute('color', new THREE.BufferAttribute(col, 3));

    return { pointsGeometry: geo, pointPositions: pos, pointColors: col };
  }, [particles]);

  // ── Per-frame: update line positions and advance particles ──
  const lastTime = useRef(0);

  useFrame(({ clock }) => {
    // Update edge line positions in case layout changes.
    if (linesRef.current) {
      const posAttr = linesRef.current.geometry.getAttribute('position');
      const colAttr = linesRef.current.geometry.getAttribute('color');

      let vi = 0;
      let ci = 0;
      edges.forEach((edge) => {
        const src = layout.getPosition(edge.sourceId);
        const tgt = layout.getPosition(edge.targetId);

        (posAttr.array as Float32Array)[vi++] = src.x;
        (posAttr.array as Float32Array)[vi++] = src.y;
        (posAttr.array as Float32Array)[vi++] = src.z;
        (posAttr.array as Float32Array)[vi++] = tgt.x;
        (posAttr.array as Float32Array)[vi++] = tgt.y;
        (posAttr.array as Float32Array)[vi++] = tgt.z;

        const brightness = 0.1 + (edge.trafficVolume / 100) * 0.9;
        (colAttr.array as Float32Array)[ci++] = brightness;
        (colAttr.array as Float32Array)[ci++] = brightness;
        (colAttr.array as Float32Array)[ci++] = brightness;
        (colAttr.array as Float32Array)[ci++] = brightness;
        (colAttr.array as Float32Array)[ci++] = brightness;
        (colAttr.array as Float32Array)[ci++] = brightness;
      });

      posAttr.needsUpdate = true;
      colAttr.needsUpdate = true;
    }

    // Advance particles along their edges.
    const deltaTime = clock.elapsedTime - lastTime.current;
    lastTime.current = clock.elapsedTime;

    const srcVec = new THREE.Vector3();
    const tgtVec = new THREE.Vector3();

    particles.forEach((p, i) => {
      p.t += deltaTime / p.duration;
      if (p.t > 1.0) p.t -= 1.0;

      const edge = edges[p.edgeIndex];
      if (!edge) return;

      const src = layout.getPosition(edge.sourceId);
      const tgt = layout.getPosition(edge.targetId);
      srcVec.copy(src);
      tgtVec.copy(tgt);

      // Lerp position along the edge.
      const x = srcVec.x + (tgtVec.x - srcVec.x) * p.t;
      const y = srcVec.y + (tgtVec.y - srcVec.y) * p.t;
      const z = srcVec.z + (tgtVec.z - srcVec.z) * p.t;

      pointPositions[i * 3] = x;
      pointPositions[i * 3 + 1] = y;
      pointPositions[i * 3 + 2] = z;
    });

    if (pointsRef.current) {
      pointsRef.current.geometry.attributes.position.needsUpdate = true;
    }
  });

  if (edges.length === 0) return null;

  return (
    <group>
      {/* Edge lines */}
      <lineSegments ref={linesRef} geometry={lineGeometry} renderOrder={1}>
        <lineBasicMaterial
          vertexColors
          transparent
          opacity={0.5}
          depthWrite={false}
          blending={THREE.AdditiveBlending}
        />
      </lineSegments>

      {/* Flowing particles */}
      <points ref={pointsRef} geometry={pointsGeometry} renderOrder={2}>
        <pointsMaterial
          vertexColors
          size={3}
          sizeAttenuation
          transparent
          opacity={0.9}
          depthWrite={false}
          blending={THREE.AdditiveBlending}
        />
      </points>
    </group>
  );
}
