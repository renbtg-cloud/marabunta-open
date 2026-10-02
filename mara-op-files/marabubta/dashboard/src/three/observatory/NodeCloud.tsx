// Marabunta - Licensed under the MIT License.
// ── NodeCloud ──
// InstancedMesh-based batched renderer for up to 1000 swarm nodes.
// Single draw call for all node spheres with per-instance matrix, color,
// glow, and pulse attributes. Supports raycasting for hover/click.

import { useRef, useMemo, useCallback, useEffect } from 'react';
import { useFrame, type ThreeEvent } from '@react-three/fiber';
import * as THREE from 'three';
import {
  type SwarmNode,
  type LayoutEngine,
  ROLE_COLORS,
  computeRadius,
} from './types';
import { computePulse, computeGlow } from './NodeSphere';

interface NodeCloudProps {
  nodes: SwarmNode[];
  layout: LayoutEngine;
  onNodeHover: (id: string | null) => void;
  onNodeClick: (id: string) => void;
  hoveredNodeId?: string | null;
}

/** Headroom multiplier for InstancedMesh buffer allocation. */
const BUFFER_HEADROOM = 1.2;

export function NodeCloud({
  nodes,
  layout,
  onNodeHover,
  onNodeClick,
  hoveredNodeId,
}: NodeCloudProps) {
  const meshRef = useRef<THREE.InstancedMesh>(null!);

  const tempMatrix = useMemo(() => new THREE.Matrix4(), []);
  const tempColor = useMemo(() => new THREE.Color(), []);
  const tempScale = useMemo(() => new THREE.Vector3(), []);
  const tempPos = useMemo(() => new THREE.Vector3(), []);

  // Allocate instance count with headroom to avoid frequent reallocations.
  const instanceCount = useMemo(
    () => Math.ceil(nodes.length * BUFFER_HEADROOM),
    [nodes.length],
  );

  // Color buffer shared across frames; rebuilt when node count changes.
  const colorArray = useMemo(
    () => new Float32Array(instanceCount * 3),
    [instanceCount],
  );

  // Glow buffer: updated every poll cycle (not every frame).
  const glowBuffer = useMemo(
    () => new Float32Array(instanceCount),
    [instanceCount],
  );

  // Rebuild glow values when node data changes.
  useEffect(() => {
    nodes.forEach((node, i) => {
      glowBuffer[i] = computeGlow(node);
    });
  }, [nodes, glowBuffer]);

  // Per-frame: update matrices, colors, and pulse.
  useFrame(({ clock }) => {
    const mesh = meshRef.current;
    if (!mesh) return;

    nodes.forEach((node, i) => {
      const pos = layout.getPosition(node.id);
      const radius = computeRadius(node);
      const pulse = computePulse(node, clock.elapsedTime);
      const scale = radius * pulse;

      const isHovered = node.id === hoveredNodeId;
      const highlightBoost = isHovered ? 1.3 : 1.0;

      tempScale.setScalar(scale * highlightBoost);
      tempPos.copy(pos);
      tempMatrix.identity();
      tempMatrix.scale(tempScale);
      tempMatrix.setPosition(tempPos.x, tempPos.y, tempPos.z);

      mesh.setMatrixAt(i, tempMatrix);

      let finalColor = ROLE_COLORS[node.primaryRole] || '#64748b';
      
      // Visual Overrides for extreme states
      if (node.chaosState?.is_active) {
        finalColor = '#ef4444'; // Chaos Alert Red
      } else if (node.isPgWireActive) {
        finalColor = '#22d3ee'; // PgWire Active Cyan
      }

      tempColor.set(finalColor);
      if (isHovered) {
        tempColor.lerp(new THREE.Color('#ffffff'), 0.2);
      }
      tempColor.toArray(colorArray, i * 3);
    });

    // Hide unused instances by scaling them to zero.
    for (let i = nodes.length; i < instanceCount; i++) {
      tempMatrix.makeScale(0, 0, 0);
      tempMatrix.setPosition(0, -9999, 0);
      mesh.setMatrixAt(i, tempMatrix);
    }

    mesh.instanceMatrix.needsUpdate = true;

    if (mesh.instanceColor) {
      (mesh.instanceColor as THREE.InstancedBufferAttribute).set(colorArray);
      mesh.instanceColor.needsUpdate = true;
    }

    mesh.count = nodes.length;
  });

  // Initialize instanceColor on mount.
  useEffect(() => {
    const mesh = meshRef.current;
    if (!mesh) return;

    const colors = new Float32Array(instanceCount * 3);
    nodes.forEach((node, i) => {
      const c = new THREE.Color(ROLE_COLORS[node.primaryRole]);
      c.toArray(colors, i * 3);
    });

    mesh.instanceColor = new THREE.InstancedBufferAttribute(colors, 3);
  }, [instanceCount, nodes]);

  // Raycasting handlers.
  const handlePointerOver = useCallback(
    (e: ThreeEvent<PointerEvent>) => {
      e.stopPropagation();
      if (e.instanceId !== undefined && e.instanceId < nodes.length) {
        onNodeHover(nodes[e.instanceId].id);
      }
    },
    [nodes, onNodeHover],
  );

  const handlePointerOut = useCallback(() => {
    onNodeHover(null);
  }, [onNodeHover]);

  const handleClick = useCallback(
    (e: ThreeEvent<MouseEvent>) => {
      e.stopPropagation();
      if (e.instanceId !== undefined && e.instanceId < nodes.length) {
        onNodeClick(nodes[e.instanceId].id);
      }
    },
    [nodes, onNodeClick],
  );

  return (
    <instancedMesh
      ref={meshRef}
      args={[undefined, undefined, instanceCount]}
      onPointerOver={handlePointerOver}
      onPointerOut={handlePointerOut}
      onClick={handleClick}
      frustumCulled={false}
    >
      <sphereGeometry args={[1, 24, 24]} />
      <meshStandardMaterial
        vertexColors
        roughness={0.4}
        metalness={0.1}
        toneMapped={false}
      />
    </instancedMesh>
  );
}
