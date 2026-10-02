// Marabunta - Licensed under the MIT License.
// ── DatabaseGeometry ──
// Renders Tier 1/2 database instances as distinct 3D shapes:
//   SQL (PostgreSQL)    -> cylinder    (#60a5fa blue)
//   Document (MongoDB)  -> hexagon     (#34d399 emerald)
//   Cache (Redis)       -> cube        (#fb7185 rose)
//   Streaming (Kafka)   -> ribbon      (#fbbf24 amber)
// Thick pipe connections link databases to leveraging nodes.

import { useRef, useMemo } from 'react';
import { useFrame } from '@react-three/fiber';
import * as THREE from 'three';
import {
  type DatabaseObject,
  type DatabaseType,
  type LayoutEngine,
  DATABASE_COLORS,
} from './types';

interface DatabaseGeometryProps {
  databases: DatabaseObject[];
  layout: LayoutEngine;
}

// ── Geometry builders ──

function createHexagonShape(radius: number): THREE.Shape {
  const shape = new THREE.Shape();
  for (let i = 0; i < 6; i++) {
    const angle = (Math.PI / 3) * i - Math.PI / 6;
    const x = radius * Math.cos(angle);
    const y = radius * Math.sin(angle);
    if (i === 0) shape.moveTo(x, y);
    else shape.lineTo(x, y);
  }
  shape.closePath();
  return shape;
}

function createRibbonShape(): THREE.Shape {
  const shape = new THREE.Shape();
  shape.moveTo(-0.5, -0.05);
  shape.bezierCurveTo(-0.25, 0.15, 0.0, -0.05, 0.25, 0.15);
  shape.lineTo(0.5, 0.05);
  shape.lineTo(0.5, -0.05);
  shape.bezierCurveTo(0.25, -0.15, 0.0, 0.05, -0.25, -0.15);
  shape.closePath();
  return shape;
}

// Pre-built geometries (shared across all instances of the same type).
const SQL_GEOMETRY = new THREE.CylinderGeometry(0.4, 0.4, 0.6, 32);
const CACHE_GEOMETRY = new THREE.BoxGeometry(0.5, 0.5, 0.5);
const DOCUMENT_GEOMETRY = new THREE.ExtrudeGeometry(
  createHexagonShape(0.35),
  { depth: 0.5, bevelEnabled: false },
);
const STREAMING_GEOMETRY = new THREE.ExtrudeGeometry(createRibbonShape(), {
  depth: 0.08,
  bevelEnabled: true,
  bevelThickness: 0.01,
  bevelSize: 0.01,
});

function getGeometryForType(type: DatabaseType): THREE.BufferGeometry {
  switch (type) {
    case 'sql':
      return SQL_GEOMETRY;
    case 'document':
      return DOCUMENT_GEOMETRY;
    case 'cache':
      return CACHE_GEOMETRY;
    case 'streaming':
      return STREAMING_GEOMETRY;
  }
}

// ── DatabasePipe: curved tube connecting a database to a node ──

interface PipeProps {
  dbPosition: THREE.Vector3;
  nodePosition: THREE.Vector3;
  color: string;
}

function DatabasePipe({ dbPosition, nodePosition, color }: PipeProps) {
  const meshRef = useRef<THREE.Mesh>(null!);

  const tubeGeo = useMemo(() => {
    const mid = new THREE.Vector3().lerpVectors(dbPosition, nodePosition, 0.5);
    mid.y += 0.5;
    const curve = new THREE.QuadraticBezierCurve3(dbPosition, mid, nodePosition);
    return new THREE.TubeGeometry(curve, 20, 0.04, 8, false);
  }, [dbPosition, nodePosition]);

  // Gentle pulse to indicate active data flow.
  useFrame(({ clock }) => {
    if (!meshRef.current) return;
    const mat = meshRef.current.material as THREE.MeshStandardMaterial;
    mat.emissiveIntensity = 0.2 + Math.sin(clock.elapsedTime * 1.5) * 0.1;
  });

  return (
    <mesh ref={meshRef} geometry={tubeGeo}>
      <meshStandardMaterial
        color={color}
        emissive={color}
        emissiveIntensity={0.3}
        transparent
        opacity={0.6}
        depthWrite={false}
      />
    </mesh>
  );
}

// ── Single database object with its pipe connections ──

interface DatabaseInstanceProps {
  db: DatabaseObject;
  layout: LayoutEngine;
}

function DatabaseInstance({ db, layout }: DatabaseInstanceProps) {
  const meshRef = useRef<THREE.Mesh>(null!);
  const color = DATABASE_COLORS[db.type];
  const geometry = getGeometryForType(db.type);

  // Place database below the node plane, offset from its primary node.
  const position = useMemo(() => {
    if (db.position) return db.position;
    if (db.nodeIds.length === 0) return new THREE.Vector3(0, -1.5, 0);

    const primaryPos = layout.getPosition(db.nodeIds[0]);
    return new THREE.Vector3(
      primaryPos.x + 1.2,
      primaryPos.y - 1.5,
      primaryPos.z + 0.8,
    );
  }, [db, layout]);

  // Slow rotation for visual interest.
  useFrame(({ clock }) => {
    if (!meshRef.current) return;
    meshRef.current.rotation.y = clock.elapsedTime * 0.2;
  });

  return (
    <group>
      {/* Database shape */}
      <mesh ref={meshRef} geometry={geometry} position={position}>
        <meshStandardMaterial
          color={color}
          emissive={color}
          emissiveIntensity={0.25}
          roughness={0.3}
          metalness={0.2}
          transparent
          opacity={0.85}
        />
      </mesh>

      {/* Pipes to leveraging nodes */}
      {db.nodeIds.map((nodeId) => {
        const nodePos = layout.getPosition(nodeId);
        return (
          <DatabasePipe
            key={`${db.id}-${nodeId}`}
            dbPosition={position}
            nodePosition={nodePos}
            color={color}
          />
        );
      })}
    </group>
  );
}

// ── Root component ──

export function DatabaseGeometry({ databases, layout }: DatabaseGeometryProps) {
  if (databases.length === 0) return null;

  return (
    <group>
      {databases.map((db) => (
        <DatabaseInstance key={db.id} db={db} layout={layout} />
      ))}
    </group>
  );
}
