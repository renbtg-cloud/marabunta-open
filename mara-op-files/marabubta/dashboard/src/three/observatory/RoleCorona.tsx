// Marabunta - Licensed under the MIT License.
// ── RoleCorona ──
// Concentric torus rings around multi-role nodes. Each ring represents a
// secondary role (the primary role colors the sphere itself). Rings rotate
// in alternating directions with increasing tilt for visual distinction.

import { useRef } from 'react';
import { useFrame } from '@react-three/fiber';
import * as THREE from 'three';
import { type NodeRole, ROLE_COLORS } from './types';

// ── Sub-component: single corona ring ──

interface CoronaRingProps {
  role: NodeRole;
  ringRadius: number;
  tubeRadius: number;
  rotationDirection: 1 | -1;
  tiltIndex: number;
}

function CoronaRing({
  role,
  ringRadius,
  tubeRadius,
  rotationDirection,
  tiltIndex,
}: CoronaRingProps) {
  const ref = useRef<THREE.Mesh>(null!);
  const color = ROLE_COLORS[role];

  // Each ring gets a slightly different tilt so they don't overlap.
  const baseTilt = Math.PI / 2 + tiltIndex * 0.15;

  useFrame(({ clock }) => {
    if (!ref.current) return;
    ref.current.rotation.z = clock.elapsedTime * 0.3 * rotationDirection;
  });

  return (
    <mesh ref={ref} rotation={[baseTilt, 0, 0]}>
      <torusGeometry args={[ringRadius, tubeRadius, 16, 64]} />
      <meshStandardMaterial
        color={color}
        emissive={color}
        emissiveIntensity={0.4}
        transparent
        opacity={0.7}
        depthWrite={false}
      />
    </mesh>
  );
}

// ── Main component ──

interface RoleCoronaProps {
  nodeId: string;
  roles: NodeRole[];
  position: THREE.Vector3;
  baseRadius: number;
}

export function RoleCorona({ roles, position, baseRadius }: RoleCoronaProps) {
  // Skip the primary role (index 0) since it colors the sphere.
  const secondaryRoles = roles.slice(1);

  if (secondaryRoles.length === 0) return null;

  return (
    <group position={position}>
      {secondaryRoles.map((role, i) => {
        const ringRadius = baseRadius + 0.15 + i * 0.12;
        const tubeRadius = 0.02;
        return (
          <CoronaRing
            key={role}
            role={role}
            ringRadius={ringRadius}
            tubeRadius={tubeRadius}
            rotationDirection={i % 2 === 0 ? 1 : -1}
            tiltIndex={i}
          />
        );
      })}
    </group>
  );
}
