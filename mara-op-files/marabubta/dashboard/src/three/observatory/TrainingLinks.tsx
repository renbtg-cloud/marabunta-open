// Marabunta - Licensed under the MIT License.
import { useMemo, useRef } from 'react';
import { useFrame } from '@react-three/fiber';
import * as THREE from 'three';
import { type LayoutEngine } from './types';

interface TrainingLink {
  sourceId: string;
  targetId: string;
}

interface TrainingLinksProps {
  links: TrainingLink[];
  layout: LayoutEngine;
}

export function TrainingLinks({ links, layout }: TrainingLinksProps) {
  const lineRef = useRef<THREE.LineSegments>(null!);

  const points = useMemo(() => {
    const pts: THREE.Vector3[] = [];
    links.forEach((link) => {
      const start = layout.getPosition(link.sourceId);
      const end = layout.getPosition(link.targetId);
      pts.push(start, end);
    });
    return pts;
  }, [links, layout]);

  const geometry = useMemo(() => {
    return new THREE.BufferGeometry().setFromPoints(points);
  }, [points]);

  useFrame(({ clock }) => {
    if (!lineRef.current) return;
    const material = lineRef.current.material as THREE.LineBasicMaterial;
    // Rapidly cycle opacity to simulate high-frequency gradient updates
    material.opacity = 0.4 + Math.sin(clock.elapsedTime * 20) * 0.3;
  });

  if (links.length === 0) return null;

  return (
    <lineSegments ref={lineRef} geometry={geometry}>
      <lineBasicMaterial
        color="#fb7185" // Ominous training red/pink
        transparent
        opacity={0.6}
        linewidth={2}
        depthWrite={false}
      />
    </lineSegments>
  );
}
