// Marabunta - Licensed under the MIT License.
// ── DeadNodeGhost ──
// Visualizes node death with a 3-phase animation:
//   Phase 1: Red flash burst (PointLight, 0.3s)
//   Phase 2: Fading ghost sphere (opacity 1->0, sinking, 5s)
//   Phase 3: Arc rerouting (endpoints animate from dead to new target, 2s)
// Self-destructs after the fade completes.

import { useRef, useState, useEffect, useCallback } from 'react';
import { useFrame } from '@react-three/fiber';
import * as THREE from 'three';

interface RerouteTarget {
  fromPos: THREE.Vector3;
  oldTarget: THREE.Vector3;
  newTarget: THREE.Vector3;
}

interface DeadNodeGhostProps {
  nodeId: string;
  position: THREE.Vector3;
  originalColor: string;
  radius: number;
  rerouteTargets: RerouteTarget[];
  onFadeComplete: (nodeId: string) => void;
}

// ── Sub-component: animated rerouting arc ──

interface ReroutingArcProps {
  fromPos: THREE.Vector3;
  oldTarget: THREE.Vector3;
  newTarget: THREE.Vector3;
  duration: number;
}

function ReroutingArc({ fromPos, oldTarget, newTarget, duration }: ReroutingArcProps) {
  const meshRef = useRef<THREE.Mesh>(null!);
  const startTime = useRef(performance.now());
  const geoRef = useRef<THREE.TubeGeometry | null>(null);

  useFrame(() => {
    if (!meshRef.current) return;

    const t = Math.min((performance.now() - startTime.current) / (duration * 1000), 1.0);
    const eased = 1 - Math.pow(1 - t, 3); // ease-out cubic

    const currentTarget = new THREE.Vector3().lerpVectors(oldTarget, newTarget, eased);

    // Rebuild tube geometry with the interpolated endpoint.
    const mid = new THREE.Vector3().lerpVectors(fromPos, currentTarget, 0.5);
    const dist = fromPos.distanceTo(currentTarget);
    mid.y += dist * 0.3;

    const curve = new THREE.QuadraticBezierCurve3(fromPos, mid, currentTarget);

    if (geoRef.current) {
      geoRef.current.dispose();
    }
    geoRef.current = new THREE.TubeGeometry(curve, 24, 0.03, 8, false);
    meshRef.current.geometry = geoRef.current;

    // Fade the arc as the reroute completes.
    const mat = meshRef.current.material as THREE.MeshStandardMaterial;
    mat.opacity = 0.7 * (1 - t * 0.5);
  });

  // Cleanup geometry on unmount.
  useEffect(() => {
    return () => {
      if (geoRef.current) geoRef.current.dispose();
    };
  }, []);

  // Initial geometry.
  const initialGeo = (() => {
    const mid = new THREE.Vector3().lerpVectors(fromPos, oldTarget, 0.5);
    const dist = fromPos.distanceTo(oldTarget);
    mid.y += dist * 0.3;
    const curve = new THREE.QuadraticBezierCurve3(fromPos, mid, oldTarget);
    return new THREE.TubeGeometry(curve, 24, 0.03, 8, false);
  })();

  return (
    <mesh ref={meshRef} geometry={initialGeo}>
      <meshStandardMaterial
        color="#22d3ee"
        emissive="#22d3ee"
        emissiveIntensity={0.5}
        transparent
        opacity={0.7}
        depthWrite={false}
      />
    </mesh>
  );
}

// ── Main ghost component ──

export function DeadNodeGhost({
  nodeId,
  position,
  originalColor,
  radius,
  rerouteTargets,
  onFadeComplete,
}: DeadNodeGhostProps) {
  const ghostRef = useRef<THREE.Mesh>(null!);
  const groupRef = useRef<THREE.Group>(null!);
  const birthTime = useRef(performance.now());

  // Phase 1: red flash burst.
  const [flashIntensity, setFlashIntensity] = useState(5.0);

  useEffect(() => {
    setFlashIntensity(5.0);
    const timer = setTimeout(() => setFlashIntensity(0), 300);
    return () => clearTimeout(timer);
  }, []);

  // Track whether we already fired the completion callback.
  const completedRef = useRef(false);

  const handleComplete = useCallback(() => {
    if (completedRef.current) return;
    completedRef.current = true;
    onFadeComplete(nodeId);
  }, [nodeId, onFadeComplete]);

  // Phase 2: fading ghost sphere with sinking effect.
  useFrame(() => {
    if (!ghostRef.current) return;

    const elapsed = (performance.now() - birthTime.current) / 1000;
    const fadeProgress = Math.min(elapsed / 5.0, 1.0);

    const mat = ghostRef.current.material as THREE.MeshStandardMaterial;
    mat.opacity = 1.0 - fadeProgress;
    mat.emissiveIntensity = (1.0 - fadeProgress) * 0.5;

    // Sink downward.
    ghostRef.current.position.y -= 0.002;

    // Self-destruct after fade completes.
    if (fadeProgress >= 1.0) {
      handleComplete();
    }
  });

  return (
    <group ref={groupRef} position={position}>
      {/* Red flash light */}
      <pointLight
        color="#fb7185"
        intensity={flashIntensity}
        distance={8}
        decay={2}
      />

      {/* Ghost sphere */}
      <mesh ref={ghostRef}>
        <sphereGeometry args={[radius, 24, 24]} />
        <meshStandardMaterial
          color={originalColor}
          emissive={originalColor}
          emissiveIntensity={0.5}
          transparent
          opacity={1.0}
          depthWrite={false}
        />
      </mesh>

      {/* Phase 3: rerouting arcs */}
      {rerouteTargets.map((rt, i) => (
        <ReroutingArc
          key={i}
          fromPos={rt.fromPos}
          oldTarget={rt.oldTarget}
          newTarget={rt.newTarget}
          duration={2.0}
        />
      ))}
    </group>
  );
}
