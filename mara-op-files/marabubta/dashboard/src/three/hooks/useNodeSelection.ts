// Marabunta - Licensed under the MIT License.
// dashboard/src/three/hooks/useNodeSelection.ts
// Raycasting + click selection hook for instanced node meshes.
// Uses three-mesh-bvh for O(log n) intersection testing.

import { useCallback, useEffect, useRef } from 'react';
import { useThree } from '@react-three/fiber';
import { useSwarmData } from '../SwarmDataContext';
import * as THREE from 'three';
import {
  computeBoundsTree,
  disposeBoundsTree,
  acceleratedRaycast,
} from 'three-mesh-bvh';

// ──────── BVH Prototype Patching ────────
// Patch Three.js prototypes once for BVH-accelerated raycasting.
// This is idempotent — safe to call multiple times.

// eslint-disable-next-line @typescript-eslint/no-explicit-any
(THREE.BufferGeometry.prototype as any).computeBoundsTree = computeBoundsTree;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
(THREE.BufferGeometry.prototype as any).disposeBoundsTree = disposeBoundsTree;
THREE.Mesh.prototype.raycast = acceleratedRaycast;

/**
 * Hook for raycasting-based node selection on an InstancedMesh.
 * Maps instanceId from the intersection to the corresponding nodeId
 * and dispatches SELECT_NODE actions.
 *
 * @param meshRef Ref to the InstancedMesh containing node spheres
 * @param nodeIds Ordered array of node IDs matching instance indices
 * @param onSelect Optional callback fired on selection change
 */
export function useNodeSelection(
  meshRef: React.RefObject<THREE.InstancedMesh | null>,
  nodeIds: string[],
  onSelect?: (nodeId: string | null) => void,
) {
  const { camera, gl } = useThree();
  const { dispatch } = useSwarmData();
  const raycasterRef = useRef(new THREE.Raycaster());
  const pointerRef = useRef(new THREE.Vector2());

  const handleClick = useCallback((event: MouseEvent) => {
    const mesh = meshRef.current;
    if (!mesh) return;

    const rect = gl.domElement.getBoundingClientRect();
    pointerRef.current.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
    pointerRef.current.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;

    raycasterRef.current.setFromCamera(pointerRef.current, camera);
    const intersects = raycasterRef.current.intersectObject(mesh, false);

    if (intersects.length > 0 && intersects[0].instanceId !== undefined) {
      const nodeId = nodeIds[intersects[0].instanceId];
      if (nodeId) {
        dispatch({ type: 'SELECT_NODE', payload: nodeId });
        onSelect?.(nodeId);
      }
    } else {
      // Clicked empty space — deselect
      dispatch({ type: 'SELECT_NODE', payload: null });
      onSelect?.(null);
    }
  }, [camera, gl, meshRef, nodeIds, dispatch, onSelect]);

  // Attach click listener to the WebGL canvas
  useEffect(() => {
    const canvas = gl.domElement;
    canvas.addEventListener('click', handleClick);
    return () => canvas.removeEventListener('click', handleClick);
  }, [gl, handleClick]);

  // Build BVH when the instanced mesh geometry is available
  useEffect(() => {
    const mesh = meshRef.current;
    if (mesh?.geometry) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (mesh.geometry as any).computeBoundsTree();
      return () => {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (mesh.geometry as any).disposeBoundsTree();
      };
    }
  }, [meshRef]);
}
