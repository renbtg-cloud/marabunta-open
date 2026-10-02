// Marabunta - Licensed under the MIT License.
// ── CameraAnimation ──
// Animates the camera from a top-down 2D view to an orbital 3D perspective.
// The camera pulls back, widens FOV, and shifts its look-at target upward
// to reveal the Z-axis that nodes rise along. (W5D / Spec S21)

import { useRef } from 'react';
import { useFrame, useThree } from '@react-three/fiber';
import * as THREE from 'three';
import { easeOutCubic, lerp } from './easing';

// ── Props ──

interface CameraAnimationProps {
  /** 0-1 progress from timeline.camera */
  progress: number;
}

// ── Camera Configuration ──

const CAMERA_2D = {
  position: [0, 0, 80] as const,
  fov: 45,
  lookAt: [0, 0, 0] as const,
};

const CAMERA_3D = {
  position: [0, 50, 120] as const,
  fov: 60,
  lookAt: [0, 15, 0] as const,
};

// ── Pre-allocated vector to avoid per-frame GC ──

const _lookAtTarget = new THREE.Vector3();

// ── Component ──

/**
 * Pure side-effect component: modifies the camera each frame based on progress.
 * Renders nothing to the scene graph.
 */
export function CameraAnimation({ progress }: CameraAnimationProps) {
  const { camera } = useThree();
  const prevProgress = useRef(-1);

  useFrame(() => {
    // Skip update if progress hasn't changed (static states)
    if (progress === prevProgress.current) return;
    prevProgress.current = progress;

    if (!(camera instanceof THREE.PerspectiveCamera)) return;

    const t = easeOutCubic(progress);

    // Interpolate position
    camera.position.x = lerp(CAMERA_2D.position[0], CAMERA_3D.position[0], t);
    camera.position.y = lerp(CAMERA_2D.position[1], CAMERA_3D.position[1], t);
    camera.position.z = lerp(CAMERA_2D.position[2], CAMERA_3D.position[2], t);

    // Interpolate FOV -- must call updateProjectionMatrix after changing fov
    camera.fov = lerp(CAMERA_2D.fov, CAMERA_3D.fov, t);
    camera.updateProjectionMatrix();

    // Interpolate look-at target
    _lookAtTarget.set(
      lerp(CAMERA_2D.lookAt[0], CAMERA_3D.lookAt[0], t),
      lerp(CAMERA_2D.lookAt[1], CAMERA_3D.lookAt[1], t),
      lerp(CAMERA_2D.lookAt[2], CAMERA_3D.lookAt[2], t),
    );
    camera.lookAt(_lookAtTarget);
  });

  return null;
}
