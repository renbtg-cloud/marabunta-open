// Marabunta - Licensed under the MIT License.
// ── CubeRotation ──
// Drag-to-rotate handler with snap-to-90deg and free rotation modes.
// Uses useFrame for damped spring animation and exposes getActiveFace().

import { useRef, useCallback } from 'react';
import { useFrame, useThree } from '@react-three/fiber';
import * as THREE from 'three';
import type { FacePosition } from './types';

// ── Spring Constants ──

const SPRING_STIFFNESS = 300;
const SPRING_DAMPING = 25;
const SNAP_ANGLE = Math.PI / 2; // 90 degrees

// ── Props ──

interface CubeRotationProps {
  target: React.RefObject<THREE.Group | null>;
  snapMode?: boolean;
  onActiveFaceChange?: (face: FacePosition) => void;
  children: React.ReactNode;
}

// ── Active Face Detection ──

const FACE_NORMALS: Record<FacePosition, THREE.Vector3> = {
  front:  new THREE.Vector3( 0,  0,  1),
  back:   new THREE.Vector3( 0,  0, -1),
  top:    new THREE.Vector3( 0,  1,  0),
  bottom: new THREE.Vector3( 0, -1,  0),
  right:  new THREE.Vector3( 1,  0,  0),
  left:   new THREE.Vector3(-1,  0,  0),
};

export function getActiveFace(
  cubeRotation: THREE.Euler,
  cameraPosition: THREE.Vector3,
): FacePosition {
  const rotMatrix = new THREE.Matrix4().makeRotationFromEuler(cubeRotation);
  let bestFace: FacePosition = 'front';
  let bestDot = -Infinity;

  const camDir = cameraPosition.clone().normalize();

  for (const [face, normal] of Object.entries(FACE_NORMALS)) {
    const transformed = normal.clone().applyMatrix4(rotMatrix);
    const dot = transformed.dot(camDir);
    if (dot > bestDot) {
      bestDot = dot;
      bestFace = face as FacePosition;
    }
  }

  return bestFace;
}

// ── Snap Utility ──

function snapToAngle(angle: number): number {
  return Math.round(angle / SNAP_ANGLE) * SNAP_ANGLE;
}

// ── Component ──

export function CubeRotation({
  target,
  snapMode = true,
  onActiveFaceChange,
  children,
}: CubeRotationProps) {
  const { camera } = useThree();
  const isDragging = useRef(false);
  const startPointer = useRef({ x: 0, y: 0 });
  const startRotation = useRef(new THREE.Euler());
  const velocity = useRef({ x: 0, y: 0 });
  const targetRotation = useRef(new THREE.Euler());
  const lastActiveFace = useRef<FacePosition>('front');

  const onPointerDown = useCallback((e: THREE.Event) => {
    (e as any).stopPropagation?.();
    isDragging.current = true;
    const pointerEvent = (e as any).nativeEvent ?? e;
    startPointer.current = {
      x: pointerEvent.clientX ?? 0,
      y: pointerEvent.clientY ?? 0,
    };
    if (target.current) {
      startRotation.current.copy(target.current.rotation);
    }
  }, [target]);

  const onPointerMove = useCallback((e: THREE.Event) => {
    if (!isDragging.current || !target.current) return;
    const pointerEvent = (e as any).nativeEvent ?? e;
    const clientX = pointerEvent.clientX ?? 0;
    const clientY = pointerEvent.clientY ?? 0;
    const dx = (clientX - startPointer.current.x) * 0.01;
    const dy = (clientY - startPointer.current.y) * 0.01;
    target.current.rotation.y = startRotation.current.y + dx;
    target.current.rotation.x = startRotation.current.x + dy;
    velocity.current = { x: dx * 0.1, y: dy * 0.1 };
  }, [target]);

  const onPointerUp = useCallback(() => {
    isDragging.current = false;
    if (!target.current || !snapMode) return;
    // Snap to nearest 90-degree increment
    targetRotation.current.set(
      snapToAngle(target.current.rotation.x),
      snapToAngle(target.current.rotation.y),
      0,
    );
  }, [target, snapMode]);

  // Damped spring animation toward snap target + active face detection
  useFrame((_, delta) => {
    if (!target.current) return;

    if (isDragging.current) return;

    if (!snapMode) {
      // Free mode: apply momentum decay
      target.current.rotation.y += velocity.current.x;
      target.current.rotation.x += velocity.current.y;
      velocity.current.x *= 0.95;
      velocity.current.y *= 0.95;
    } else {
      // Spring toward target
      const t = target.current.rotation;
      const tr = targetRotation.current;
      const forceX = (tr.x - t.x) * SPRING_STIFFNESS * delta;
      const forceY = (tr.y - t.y) * SPRING_STIFFNESS * delta;
      velocity.current.x += forceY - velocity.current.x * SPRING_DAMPING * delta;
      velocity.current.y += forceX - velocity.current.y * SPRING_DAMPING * delta;
      t.y += velocity.current.x * delta;
      t.x += velocity.current.y * delta;
    }

    // Detect active face and notify if changed
    if (onActiveFaceChange && target.current) {
      const active = getActiveFace(target.current.rotation, camera.position);
      if (active !== lastActiveFace.current) {
        lastActiveFace.current = active;
        onActiveFaceChange(active);
      }
    }
  });

  return (
    <group
      onPointerDown={onPointerDown as any}
      onPointerMove={onPointerMove as any}
      onPointerUp={onPointerUp as any}
    >
      {children}
    </group>
  );
}
