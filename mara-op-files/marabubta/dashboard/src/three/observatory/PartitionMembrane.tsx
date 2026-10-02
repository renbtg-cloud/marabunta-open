// Marabunta - Licensed under the MIT License.
// ── PartitionMembrane ──
// Visualizes network partition as a red translucent membrane at the boundary
// between disconnected node clusters. Pulses with increasing urgency over time.
// Dissolves with a green flash when the partition heals.

import { useRef, useMemo, useState, useEffect } from 'react';
import { useFrame } from '@react-three/fiber';
import * as THREE from 'three';
import { type PartitionInfo, type LayoutEngine } from './types';

interface PartitionMembraneProps {
  partition: PartitionInfo;
  layout: LayoutEngine;
  healed?: boolean;
  onDissolveComplete?: () => void;
}

// ── Helpers ──

function computeCentroid(positions: THREE.Vector3[]): THREE.Vector3 {
  const centroid = new THREE.Vector3();
  positions.forEach((p) => centroid.add(p));
  centroid.divideScalar(positions.length || 1);
  return centroid;
}

function computeSpread(positions: THREE.Vector3[]): number {
  if (positions.length < 2) return 5;
  const centroid = computeCentroid(positions);
  let maxDist = 0;
  positions.forEach((p) => {
    const d = p.distanceTo(centroid);
    if (d > maxDist) maxDist = d;
  });
  return maxDist;
}

interface MembraneGeom {
  position: THREE.Vector3;
  quaternion: THREE.Quaternion;
  width: number;
  height: number;
}

function computeMembranePlane(
  groupA: THREE.Vector3[],
  groupB: THREE.Vector3[],
): MembraneGeom {
  const centerA = computeCentroid(groupA);
  const centerB = computeCentroid(groupB);
  const midpoint = new THREE.Vector3().lerpVectors(centerA, centerB, 0.5);

  // Orient plane perpendicular to the line between centroids.
  const direction = new THREE.Vector3().subVectors(centerB, centerA).normalize();
  const quat = new THREE.Quaternion().setFromUnitVectors(
    new THREE.Vector3(0, 0, 1),
    direction,
  );

  const spread = Math.max(computeSpread(groupA), computeSpread(groupB));

  return {
    position: midpoint,
    quaternion: quat,
    width: Math.max(spread * 1.5, 4),
    height: Math.max(spread * 1.2, 3),
  };
}

// ── Main component ──

export function PartitionMembrane({
  partition,
  layout,
  healed = false,
  onDissolveComplete,
}: PartitionMembraneProps) {
  const meshRef = useRef<THREE.Mesh>(null!);
  const [dissolving, setDissolving] = useState(false);
  const dissolveStart = useRef(0);
  const flashRef = useRef<THREE.PointLight>(null!);

  // Compute the membrane plane from the two largest components.
  const membrane = useMemo<MembraneGeom | null>(() => {
    const { components } = partition;
    if (components.length < 2) return null;

    // Get positions for the two largest components.
    const sorted = [...components].sort((a, b) => b.length - a.length);
    const groupA = sorted[0].map((id) => layout.getPosition(id));
    const groupB = sorted[1].map((id) => layout.getPosition(id));

    return computeMembranePlane(groupA, groupB);
  }, [partition, layout]);

  // Trigger dissolve when healed.
  useEffect(() => {
    if (healed && !dissolving) {
      setDissolving(true);
      dissolveStart.current = performance.now();
    }
  }, [healed, dissolving]);

  // Time since detection for escalation pulse.
  const detectedAt = useRef(partition.detectedAt);

  useFrame(({ clock }) => {
    if (!meshRef.current) return;
    const mat = meshRef.current.material as THREE.MeshStandardMaterial;

    if (dissolving) {
      // Dissolve over 2 seconds.
      const elapsed = (performance.now() - dissolveStart.current) / 1000;
      const progress = Math.min(elapsed / 2.0, 1.0);
      mat.opacity = (0.15 + 0.08) * (1 - progress);
      mat.color.set('#34d399'); // Green tint during heal.
      mat.emissive.set('#34d399');

      // Green flash light during dissolve.
      if (flashRef.current) {
        flashRef.current.intensity = 3 * (1 - progress);
        flashRef.current.color.set('#34d399');
      }

      if (progress >= 1.0 && onDissolveComplete) {
        onDissolveComplete();
      }
    } else {
      // Pulsing red membrane.
      // Escalation: pulse frequency increases after 60 seconds.
      const age = (Date.now() - detectedAt.current) / 1000;
      const pulseFreq = age > 60 ? 4 : 2;

      const opacity = 0.15 + Math.sin(clock.elapsedTime * pulseFreq) * 0.08;
      mat.opacity = Math.max(0.05, opacity);
      mat.color.set('#fb7185');
      mat.emissive.set('#fb7185');
    }
  });

  if (!membrane) return null;

  return (
    <group>
      <mesh
        ref={meshRef}
        position={membrane.position}
        quaternion={membrane.quaternion}
      >
        <planeGeometry args={[membrane.width, membrane.height, 32, 32]} />
        <meshStandardMaterial
          color="#fb7185"
          emissive="#fb7185"
          emissiveIntensity={0.3}
          transparent
          opacity={0.15}
          side={THREE.DoubleSide}
          depthWrite={false}
        />
      </mesh>

      {/* Flash light for heal animation */}
      {dissolving && (
        <pointLight
          ref={flashRef}
          position={membrane.position}
          color="#34d399"
          intensity={3}
          distance={15}
          decay={2}
        />
      )}
    </group>
  );
}
