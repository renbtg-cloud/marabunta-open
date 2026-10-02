// Marabunta - Licensed under the MIT License.
// ── NodeSphere ──
// Individual node visual: role color, capacity-based size, load pulse, uptime glow.
// Not rendered directly in production (NodeCloud uses InstancedMesh), but defines
// the per-node visual logic and can be used for isolated previews / debugging.

import { useRef } from 'react';
import { useFrame } from '@react-three/fiber';
import * as THREE from 'three';
import {
  type SwarmNode,
  type NodeRole,
  ROLE_COLORS,
  computeRadius,
} from './types';

interface NodeSphereProps {
  node: SwarmNode;
  position: THREE.Vector3;
  highlighted?: boolean;
}

/** Compute pulse scale multiplier for the current frame. */
export function computePulse(node: SwarmNode, elapsedTime: number): number {
  if (node.chaosState?.is_active) {
    // Chaotic jitter pulse
    return 1.0 + (Math.random() - 0.5) * 0.2;
  }

  const loadFraction = node.loadPercent / 100;
  let freq = 0.5 + loadFraction * 2.5;
  let amp = loadFraction * 0.08;

  if (node.isTraining) {
    // Training nodes pulse rapidly and deeply (DiLoCo heartbeat)
    freq *= 4.0;
    amp += 0.15;
  }

  return 1.0 + Math.sin(elapsedTime * freq * Math.PI * 2) * amp;
}

/** Compute emissive glow intensity from uptime. */
export function computeGlow(node: SwarmNode): number {
  if (node.chaosState?.is_active) return 1.5; // High intensity red alert

  const uptimeHours = Math.min(node.uptimeSeconds / 3600, 720);
  let baseGlow = Math.log1p(uptimeHours) / Math.log1p(720);

  if (node.isPgWireActive) {
    // Legacy integration activity adds significant brightness
    baseGlow += 0.5;
  }

  return baseGlow;
}

export function NodeSphere({ node, position, highlighted = false }: NodeSphereProps) {
  const meshRef = useRef<THREE.Mesh>(null!);
  const radius = computeRadius(node);
  const roleColor = ROLE_COLORS[node.primaryRole];
  const glowIntensity = computeGlow(node);

  useFrame(({ clock }) => {
    if (!meshRef.current) return;
    const pulse = computePulse(node, clock.elapsedTime);
    meshRef.current.scale.setScalar(radius * pulse);
  });

  return (
    <mesh ref={meshRef} position={position}>
      <sphereGeometry args={[1, 24, 24]} />
      <meshStandardMaterial
        color={roleColor}
        emissive={roleColor}
        emissiveIntensity={highlighted ? 0.9 : glowIntensity * 0.6}
        roughness={0.4}
        metalness={0.1}
      />
    </mesh>
  );
}

/** Look up role color, exported for use by NodeCloud and other components. */
export function getRoleColor(role: NodeRole): string {
  return ROLE_COLORS[role];
}

export { computeRadius };
