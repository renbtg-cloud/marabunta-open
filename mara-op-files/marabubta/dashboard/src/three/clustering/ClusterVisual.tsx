// Marabunta - Licensed under the MIT License.
// ── Cluster Visual ──
// Renders a translucent boundary around grouped cubes with wireframe edges,
// a floating billboard label, hover highlight, and click-to-collapse handler.
// W6C / Spec S21

import React, { useMemo, useState, useCallback } from 'react';
import * as THREE from 'three';
import { Text } from 'troika-three-text';
import type { Cluster } from './ClusterEngine';

// ── Troika text primitive for R3F ──

function ClusterLabel({
  text,
  position,
  color,
}: {
  text: string;
  position: [number, number, number];
  color: string;
}) {
  const textMesh = useMemo(() => {
    const t = new Text();
    t.text = text;
    t.fontSize = 0.5;
    t.color = color;
    t.anchorX = 'center';
    t.anchorY = 'bottom';
    t.font = '/fonts/IBMPlexMono-Regular.woff';
    t.sync();
    return t;
  }, [text, color]);

  return (
    <primitive
      object={textMesh}
      position={position}
      // Billboard: always face camera (handled by lookAt in parent or drei Billboard)
      renderOrder={100}
    />
  );
}

// ── Props ──

export interface ClusterVisualProps {
  cluster: Cluster;
  color: string;
  onCollapse: () => void;
  highlighted?: boolean;
}

// ── Component ──

export function ClusterVisual({
  cluster,
  color,
  onCollapse,
  highlighted = false,
}: ClusterVisualProps) {
  const [hovered, setHovered] = useState(false);

  const size = useMemo(() => {
    return cluster.boundingBox.getSize(new THREE.Vector3());
  }, [cluster.boundingBox]);

  const center = useMemo(() => {
    return cluster.boundingBox.getCenter(new THREE.Vector3());
  }, [cluster.boundingBox]);

  const edgesGeo = useMemo(() => {
    return new THREE.EdgesGeometry(
      new THREE.BoxGeometry(size.x, size.y, size.z),
    );
  }, [size]);

  const isActive = highlighted || hovered;

  const handleClick = useCallback(
    (e: THREE.Event) => {
      (e as { stopPropagation?: () => void }).stopPropagation?.();
      onCollapse();
    },
    [onCollapse],
  );

  return (
    <group position={center.toArray() as [number, number, number]}>
      {/* Translucent fill */}
      <mesh
        onClick={handleClick}
        onPointerOver={() => setHovered(true)}
        onPointerOut={() => setHovered(false)}
        renderOrder={90}
      >
        <boxGeometry args={[size.x, size.y, size.z]} />
        <meshStandardMaterial
          color={color}
          transparent
          opacity={isActive ? 0.12 : 0.06}
          depthWrite={false}
        />
      </mesh>

      {/* Wireframe border */}
      <lineSegments renderOrder={91}>
        <primitive object={edgesGeo} attach="geometry" />
        <lineBasicMaterial
          color={color}
          transparent
          opacity={isActive ? 0.6 : 0.4}
        />
      </lineSegments>

      {/* Cluster label floating above */}
      <ClusterLabel
        text={`${cluster.value} (${cluster.members.length})`}
        position={[0, size.y / 2 + 0.5, 0]}
        color={color}
      />
    </group>
  );
}
