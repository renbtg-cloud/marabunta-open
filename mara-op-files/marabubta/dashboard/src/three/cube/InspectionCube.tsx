// Marabunta - Licensed under the MIT License.
// ── InspectionCube ──
// Core component: BoxGeometry(1,1,1) with 6 MeshStandardMaterial instances,
// wrapped in a CubeRotation handler with CubeFace overlays on all 6 positions.
// W6A: Preset-driven face content with crossfade material support.

import { useRef, useMemo, useState, useCallback, useContext, createContext } from 'react';
import * as THREE from 'three';
import { CubeFace } from './CubeFace';
import { CubeRotation } from './CubeRotation';
import type {
  CubeData,
  FaceMapping,
  FacePosition,
  RotationMode,
  CubePreset,
} from './types';
import type { FaceSlot, FaceDimension } from './presets/types';
import { FACE_SLOTS } from './presets/types';
import { usePresetContext } from './presets/PresetManager';
import type { FaceCrossfadeHandle } from './presets/FaceCrossfade';

// ── Default Face Mapping ──

const DEFAULT_FACE_MAPPING: FaceMapping = {
  front:  'state',
  top:    'actors',
  right:  'audit',
  back:   'context',
  left:   'infra',
  bottom: 'timers',
};

// Map renderer keys from presets to face type keys
const RENDERER_TO_FACE_TYPE: Record<string, string> = {
  'state-transitions': 'state',
  'actor-roster':      'actors',
  'audit-log':         'audit',
  'context-data':      'context',
  'infra-metrics':     'infra',
  'timer-display':     'timers',
};

// Material index map for BoxGeometry face ordering (Three.js convention):
// +X=0, -X=1, +Y=2, -Y=3, +Z=4, -Z=5
const MAT_INDEX_MAP: Record<FaceSlot, number> = {
  right: 0, left: 1, top: 2, bottom: 3, front: 4, back: 5,
};

// ── Props ──

interface InspectionCubeProps {
  data: CubeData;
  faceMapping?: FaceMapping;
  preset?: CubePreset;
  size?: number;
  position?: [number, number, number];
  rotationMode?: RotationMode;
  onFaceClick?: (face: FacePosition) => void;
  onFaceRightClick?: (face: FacePosition) => void;
  onActiveFaceChange?: (face: FacePosition) => void;
}

// ── Preset-Aware Wrapper ──
// Uses usePresetContext() which throws if not inside PresetManager.
// The outer InspectionCube catches this and falls back to standalone mode.

function PresetAwareInspectionCube({
  data,
  size,
  position,
  rotationMode,
  onFaceClick,
  onFaceRightClick,
  onActiveFaceChange,
}: Omit<InspectionCubeProps, 'faceMapping' | 'preset'>) {
  const groupRef = useRef<THREE.Group>(null);
  const [, setActiveFace] = useState<FacePosition>('front');

  const { getActiveFaces, getCrossfade, state } = usePresetContext();

  // Derive face mapping from active preset dimensions
  const resolvedMapping = useMemo(() => {
    const faces = getActiveFaces();
    const mapping: Record<string, string> = {};
    for (const slot of FACE_SLOTS) {
      const dim = faces[slot];
      mapping[slot] = RENDERER_TO_FACE_TYPE[dim.renderer] ?? dim.renderer;
    }
    return mapping as unknown as FaceMapping;
  }, [getActiveFaces]);

  // Create 6 materials — current layer (one per face)
  const materials = useMemo(() => {
    return Array.from({ length: 6 }, () =>
      new THREE.MeshStandardMaterial({
        color: 0x1a2234,
        metalness: 0.1,
        roughness: 0.8,
        transparent: true,
        opacity: 0.92,
      }),
    );
  }, []);

  // Create 6 incoming materials — for crossfade overlay layer
  const incomingMaterials = useMemo(() => {
    return Array.from({ length: 6 }, () =>
      new THREE.MeshStandardMaterial({
        color: 0x1a2234,
        metalness: 0.1,
        roughness: 0.8,
        transparent: true,
        opacity: 0,
      }),
    );
  }, []);

  // Wire crossfade refs to material instances
  useMemo(() => {
    for (const slot of FACE_SLOTS) {
      const handle = getCrossfade(slot);
      const idx = MAT_INDEX_MAP[slot];
      handle.currentMatRef.current = materials[idx];
      handle.incomingMatRef.current = incomingMaterials[idx];
    }
  }, [getCrossfade, materials, incomingMaterials]);

  const handleActiveFaceChange = useCallback(
    (face: FacePosition) => {
      setActiveFace(face);
      onActiveFaceChange?.(face);
    },
    [onActiveFaceChange],
  );

  return (
    <group ref={groupRef} position={position}>
      <CubeRotation
        target={groupRef}
        snapMode={rotationMode === 'snap'}
        onActiveFaceChange={handleActiveFaceChange}
      >
        {/* Core cube mesh with 6 current materials */}
        <mesh material={materials}>
          <boxGeometry args={[size!, size!, size!]} />
        </mesh>

        {/* Crossfade incoming overlay — slightly larger to prevent Z-fighting */}
        <mesh material={incomingMaterials} renderOrder={1}>
          <boxGeometry args={[size! + 0.002, size! + 0.002, size! + 0.002]} />
        </mesh>

        {/* Edge highlight wireframe */}
        <lineSegments>
          <edgesGeometry args={[new THREE.BoxGeometry(size!, size!, size!)]} />
          <lineBasicMaterial color="#334155" transparent opacity={0.4} />
        </lineSegments>

        {/* Render 6 face overlays */}
        {(Object.entries(resolvedMapping) as [FacePosition, string][]).map(
          ([pos, type]) => (
            <CubeFace
              key={pos}
              facePosition={pos}
              faceType={type as any}
              data={data}
              size={size!}
              onClick={onFaceClick}
              onRightClick={onFaceRightClick}
            />
          ),
        )}
      </CubeRotation>
    </group>
  );
}

// ── Standalone (no presets) ──

function StandaloneInspectionCube({
  data,
  faceMapping,
  preset,
  size = 1,
  position = [0, 0, 0],
  rotationMode = 'snap',
  onFaceClick,
  onFaceRightClick,
  onActiveFaceChange,
}: InspectionCubeProps) {
  const groupRef = useRef<THREE.Group>(null);
  const [, setActiveFace] = useState<FacePosition>('front');

  const resolvedMapping = useMemo(() => {
    if (preset) return preset.mapping;
    if (faceMapping) return faceMapping;
    return DEFAULT_FACE_MAPPING;
  }, [preset, faceMapping]);

  const materials = useMemo(() => {
    return Array.from({ length: 6 }, () =>
      new THREE.MeshStandardMaterial({
        color: 0x1a2234,
        metalness: 0.1,
        roughness: 0.8,
        transparent: true,
        opacity: 0.92,
      }),
    );
  }, []);

  const handleActiveFaceChange = useCallback(
    (face: FacePosition) => {
      setActiveFace(face);
      onActiveFaceChange?.(face);
    },
    [onActiveFaceChange],
  );

  return (
    <group ref={groupRef} position={position}>
      <CubeRotation
        target={groupRef}
        snapMode={rotationMode === 'snap'}
        onActiveFaceChange={handleActiveFaceChange}
      >
        <mesh material={materials}>
          <boxGeometry args={[size, size, size]} />
        </mesh>
        <lineSegments>
          <edgesGeometry args={[new THREE.BoxGeometry(size, size, size)]} />
          <lineBasicMaterial color="#334155" transparent opacity={0.4} />
        </lineSegments>
        {(Object.entries(resolvedMapping) as [FacePosition, string][]).map(
          ([pos, type]) => (
            <CubeFace
              key={pos}
              facePosition={pos}
              faceType={type as any}
              data={data}
              size={size}
              onClick={onFaceClick}
              onRightClick={onFaceRightClick}
            />
          ),
        )}
      </CubeRotation>
    </group>
  );
}

// ── Error Boundary for Preset Context ──

import { Component, type ErrorInfo, type ReactNode } from 'react';

interface FallbackState { hasError: boolean }

class PresetErrorBoundary extends Component<
  { fallback: ReactNode; children: ReactNode },
  FallbackState
> {
  state: FallbackState = { hasError: false };

  static getDerivedStateFromError(): FallbackState {
    return { hasError: true };
  }

  componentDidCatch(error: Error, _info: ErrorInfo) {
    // Expected error when not wrapped in PresetManager — silently degrade
    if (error.message.includes('usePresetContext must be inside PresetManager')) {
      return;
    }
    console.warn('InspectionCube preset error:', error);
  }

  render() {
    if (this.state.hasError) return this.props.fallback;
    return this.props.children;
  }
}

// ── Public API ──

export function InspectionCube(props: InspectionCubeProps) {
  const {
    size = 1,
    position = [0, 0, 0],
    rotationMode = 'snap',
    ...rest
  } = props;

  return (
    <PresetErrorBoundary
      fallback={<StandaloneInspectionCube {...props} />}
    >
      <PresetAwareInspectionCube
        {...rest}
        size={size}
        position={position}
        rotationMode={rotationMode}
      />
    </PresetErrorBoundary>
  );
}
