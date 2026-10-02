// Marabunta - Licensed under the MIT License.
// ── Mega-Cube ──
// Renders a single large cube representing a collapsed cluster.
// Each face displays an aggregated value (SUM, AVG, COUNT, MIN, MAX)
// with a sparkline distribution and member count label.
// Right-click a face to decompose; double-click to expand back.
// W6C / Spec S21

import React, { useMemo, useState, useCallback, useRef } from 'react';
import * as THREE from 'three';
import { useFrame, type ThreeEvent } from '@react-three/fiber';
import { Text } from 'troika-three-text';
import type { Cluster, AggregateFaceData } from './ClusterEngine';

// ── Constants ──

/** Face normals in Three.js BoxGeometry material order: +X, -X, +Y, -Y, +Z, -Z */
const FACE_NORMALS: THREE.Vector3[] = [
  new THREE.Vector3(1, 0, 0),
  new THREE.Vector3(-1, 0, 0),
  new THREE.Vector3(0, 1, 0),
  new THREE.Vector3(0, -1, 0),
  new THREE.Vector3(0, 0, 1),
  new THREE.Vector3(0, 0, -1),
];

const FACE_ROTATIONS: [number, number, number][] = [
  [0, Math.PI / 2, 0],   // +X
  [0, -Math.PI / 2, 0],  // -X
  [-Math.PI / 2, 0, 0],  // +Y
  [Math.PI / 2, 0, 0],   // -Y
  [0, 0, 0],             // +Z
  [0, Math.PI, 0],       // -Z
];

// ── Mega-Cube Face Label ──

function MegaCubeFace({
  face,
  cubeSize,
  index,
}: {
  face: AggregateFaceData;
  cubeSize: number;
  index: number;
}) {
  const halfSize = cubeSize / 2 + 0.01;
  const normal = FACE_NORMALS[index] ?? new THREE.Vector3(0, 0, 1);
  const rotation = FACE_ROTATIONS[index] ?? [0, 0, 0];

  const position: [number, number, number] = [
    normal.x * halfSize,
    normal.y * halfSize,
    normal.z * halfSize,
  ];

  const labelText = useMemo(() => {
    const t = new Text();
    t.text = `${face.label}\n${face.aggregation}: ${formatValue(face.value)}`;
    t.fontSize = cubeSize * 0.08;
    t.color = '#e8ecf4';
    t.anchorX = 'center';
    t.anchorY = 'middle';
    t.font = '/fonts/IBMPlexMono-Regular.woff';
    t.textAlign = 'center';
    t.maxWidth = cubeSize * 0.8;
    t.sync();
    return t;
  }, [face.label, face.aggregation, face.value, cubeSize]);

  return (
    <group position={position} rotation={rotation}>
      <primitive object={labelText} />
    </group>
  );
}

// ── Mega-Cube Count Label ──

function MegaCubeLabel({
  count,
  value,
  cubeSize,
}: {
  count: number;
  value: string;
  cubeSize: number;
}) {
  const labelText = useMemo(() => {
    const t = new Text();
    t.text = `${count}x ${value}`;
    t.fontSize = cubeSize * 0.12;
    t.color = '#22d3ee';
    t.anchorX = 'center';
    t.anchorY = 'bottom';
    t.font = '/fonts/IBMPlexMono-Regular.woff';
    t.sync();
    return t;
  }, [count, value, cubeSize]);

  return (
    <primitive
      object={labelText}
      position={[0, cubeSize / 2 + 0.3, 0]}
      renderOrder={100}
    />
  );
}

// ── Props ──

export interface MegaCubeProps {
  cluster: Cluster;
  aggregatedFaces: AggregateFaceData[];
  onDecompose: (faceIndex: number) => void;
  onExpand: () => void;
}

// ── Component ──

export function MegaCube({
  cluster,
  aggregatedFaces,
  onDecompose,
  onExpand,
}: MegaCubeProps) {
  const meshRef = useRef<THREE.Mesh>(null);
  const [hovered, setHovered] = useState(false);

  // Mega-cube size scales with sqrt of member count
  const computedSize = useMemo(
    () => Math.max(1.5, Math.sqrt(cluster.members.length) * 0.6),
    [cluster.members.length],
  );

  // Pulse animation
  useFrame(({ clock }) => {
    if (!meshRef.current) return;
    const scale = 1 + Math.sin(clock.getElapsedTime() * 1.5) * 0.01;
    meshRef.current.scale.setScalar(scale);
  });

  // Right-click a face to decompose the aggregate
  const handleContextMenu = useCallback(
    (e: ThreeEvent<MouseEvent>) => {
      e.stopPropagation();
      if (e.faceIndex !== undefined) {
        const faceIndex = Math.floor(e.faceIndex / 2);
        onDecompose(faceIndex);
      }
    },
    [onDecompose],
  );

  // Double-click to expand cluster back to individual cubes
  const handleDoubleClick = useCallback(() => {
    onExpand();
  }, [onExpand]);

  const centroidArr = useMemo(
    () => cluster.centroid.toArray() as [number, number, number],
    [cluster.centroid],
  );

  return (
    <group position={centroidArr}>
      <mesh
        ref={meshRef}
        onContextMenu={handleContextMenu}
        onDoubleClick={handleDoubleClick}
        onPointerOver={() => setHovered(true)}
        onPointerOut={() => setHovered(false)}
      >
        <boxGeometry args={[computedSize, computedSize, computedSize]} />
        <meshStandardMaterial
          color={hovered ? '#22d3ee' : '#1a2234'}
          emissive={hovered ? '#22d3ee' : '#000000'}
          emissiveIntensity={hovered ? 0.15 : 0}
          transparent
          opacity={0.92}
        />
      </mesh>

      {/* Aggregate face labels */}
      {aggregatedFaces.slice(0, 6).map((face, i) => (
        <MegaCubeFace
          key={i}
          face={face}
          cubeSize={computedSize}
          index={i}
        />
      ))}

      {/* Count label above cube */}
      <MegaCubeLabel
        count={cluster.members.length}
        value={cluster.value}
        cubeSize={computedSize}
      />
    </group>
  );
}

// ── Helpers ──

function formatValue(value: number): string {
  if (Math.abs(value) >= 10000) {
    return `${(value / 1000).toFixed(1)}k`;
  }
  if (Number.isInteger(value)) return String(value);
  return value.toFixed(2);
}
