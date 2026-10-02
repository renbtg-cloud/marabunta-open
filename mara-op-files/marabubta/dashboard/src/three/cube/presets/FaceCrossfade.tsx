// Marabunta - Licensed under the MIT License.
// ── Face Crossfade ──
// Manages the 0.5-second crossfade animation between face contents
// when switching dimension presets (W6A / Spec S21).
//
// Uses a dual-material technique: each face has a "current" and "incoming"
// material layer. During crossfade, incoming opacity ramps 0->1 while
// current ramps 1->0 using ease-out cubic easing. After completion,
// references are swapped so incoming becomes the new current.

import { useRef, useCallback } from 'react';
import { useFrame } from '@react-three/fiber';
import * as THREE from 'three';
import type { FaceSlot, FaceDimension } from './types';

/** Duration of the crossfade animation in seconds */
export const CROSSFADE_DURATION = 0.5;

/** Z-offset to prevent Z-fighting between dual material layers */
const Z_OFFSET = 0.001;

/** Internal state tracked per face during crossfade */
interface FaceCrossfadeState {
  isActive: boolean;
  elapsed: number;
  fromDimension: FaceDimension | null;
  toDimension: FaceDimension | null;
}

/** Return type for the useFaceCrossfade hook */
export interface FaceCrossfadeHandle {
  /** Start a crossfade transition from one dimension to another */
  startCrossfade: (from: FaceDimension, to: FaceDimension) => void;
  /** Ref to the current (outgoing) material */
  currentMatRef: React.MutableRefObject<THREE.MeshStandardMaterial | null>;
  /** Ref to the incoming (new) material */
  incomingMatRef: React.MutableRefObject<THREE.MeshStandardMaterial | null>;
  /** Whether this face is currently mid-crossfade */
  isActive: () => boolean;
  /** The Z-offset for the incoming material layer */
  zOffset: number;
}

/**
 * Manages crossfade animation for a single cube face.
 * One instance per face, coordinated by PresetManager.
 *
 * @param _face - The face slot this crossfade manages (for debugging)
 */
export function useFaceCrossfade(_face: FaceSlot): FaceCrossfadeHandle {
  const stateRef = useRef<FaceCrossfadeState>({
    isActive: false,
    elapsed: 0,
    fromDimension: null,
    toDimension: null,
  });

  const currentMatRef = useRef<THREE.MeshStandardMaterial>(null);
  const incomingMatRef = useRef<THREE.MeshStandardMaterial>(null);

  const startCrossfade = useCallback(
    (from: FaceDimension, to: FaceDimension) => {
      stateRef.current = {
        isActive: true,
        elapsed: 0,
        fromDimension: from,
        toDimension: to,
      };
      if (incomingMatRef.current) {
        incomingMatRef.current.opacity = 0;
        incomingMatRef.current.transparent = true;
        incomingMatRef.current.needsUpdate = true;
      }
    },
    [],
  );

  // Per-frame animation update driven by R3F render loop
  useFrame((_, delta) => {
    const state = stateRef.current;
    if (!state.isActive) return;

    state.elapsed += delta;
    const t = Math.min(state.elapsed / CROSSFADE_DURATION, 1.0);

    // Ease-out cubic: f(t) = 1 - (1 - t)^3
    // Starts quickly (most change in first 200ms), decelerates gently.
    const eased = 1 - Math.pow(1 - t, 3);

    if (currentMatRef.current) {
      currentMatRef.current.opacity = 1 - eased;
    }
    if (incomingMatRef.current) {
      incomingMatRef.current.opacity = eased;
    }

    // Transition complete
    if (t >= 1.0) {
      state.isActive = false;
      if (currentMatRef.current) {
        currentMatRef.current.opacity = 0;
      }
      if (incomingMatRef.current) {
        incomingMatRef.current.opacity = 1;
        incomingMatRef.current.transparent = false;
      }
      // Swap references: incoming becomes current for next crossfade
      const tmp = currentMatRef.current;
      currentMatRef.current = incomingMatRef.current;
      incomingMatRef.current = tmp;
    }
  });

  const isActive = useCallback(() => stateRef.current.isActive, []);

  return {
    startCrossfade,
    currentMatRef,
    incomingMatRef,
    isActive,
    zOffset: Z_OFFSET,
  };
}
