// Marabunta - Licensed under the MIT License.
// ── ChildCubeSpawner ──
// Handles the visual explosion when a face is decomposed. Receives a
// DecomposeResult (SUM), DistributionResult (AVG), or CountResult (COUNT)
// and spawns child cubes / histogram bars / grid items from the parent
// face position. Uses useFrame for smooth 600ms easeOutBack animation
// with 40ms stagger per child. Supports three COUNT layout strategies:
// grid (<=50), cloud (51-1000), instanced (>1000).

import React, { useMemo, useRef, useState, useCallback } from 'react';
import * as THREE from 'three';
import { useFrame } from '@react-three/fiber';
import { Text } from 'troika-three-text';
import { extend } from '@react-three/fiber';
import type { Object3DNode } from '@react-three/fiber';
import type {
  DecomposedItem,
  DecomposeResult,
  DistributionResult,
  CountResult,
} from './DecompositionTree';
import { FONT_MONO } from '../types';

extend({ TroikaText: Text });

declare module '@react-three/fiber' {
  interface ThreeElements {
    troikaText: Object3DNode<Text, typeof Text>;
  }
}

// ── Easing Functions ──

function easeOutBack(t: number): number {
  const c1 = 1.70158;
  const c3 = c1 + 1;
  return 1 + c3 * Math.pow(t - 1, 3) + c1 * Math.pow(t - 1, 2);
}

function easeInBack(t: number): number {
  const c1 = 1.70158;
  const c3 = c1 + 1;
  return c3 * t * t * t - c1 * t * t;
}

// ── Layout Computation ──

export type LayoutStrategy = 'grid' | 'cloud' | 'instanced';

export function selectLayout(count: number): LayoutStrategy {
  if (count <= 50) return 'grid';
  if (count <= 1000) return 'cloud';
  return 'instanced';
}

export interface SpawnedChild {
  item: DecomposedItem;
  targetPosition: THREE.Vector3;
  targetScale: number;
  delay: number;
}

/** Compute child cube target positions for SUM decomposition (radial layout) */
export function computeChildLayout(
  items: DecomposedItem[],
  parentPos: THREE.Vector3,
  faceNormal: THREE.Vector3,
): SpawnedChild[] {
  if (items.length === 0) return [];

  const maxPct = Math.max(...items.map((i) => i.percentage));
  const spread = 2.5;

  return items.map((item, i) => {
    const angle = (i / items.length) * Math.PI * 2;
    const radius = spread * (1 - (item.percentage / maxPct) * 0.3);
    const offset = new THREE.Vector3(
      Math.cos(angle) * radius,
      Math.sin(angle) * radius * 0.6,
      faceNormal.z * 1.5 + faceNormal.x * 1.5 + faceNormal.y * 1.5,
    );

    // Cube-root scaling so visual volume matches data ratio
    const scale = Math.max(0.15, Math.cbrt(item.percentage / maxPct) * 0.8);

    return {
      item,
      targetPosition: parentPos.clone().add(offset),
      targetScale: scale,
      delay: i * 40,
    };
  });
}

/** Grid layout for small COUNT decompositions (<=50) */
export function gridLayout(
  count: number,
  origin: THREE.Vector3,
): THREE.Vector3[] {
  const cols = Math.ceil(Math.sqrt(count));
  const rows = Math.ceil(count / cols);
  const spacing = 0.3;
  const positions: THREE.Vector3[] = [];

  for (let i = 0; i < count; i++) {
    const col = i % cols;
    const row = Math.floor(i / cols);
    positions.push(
      new THREE.Vector3(
        origin.x + (col - cols / 2) * spacing,
        origin.y + (row - rows / 2) * spacing,
        origin.z + 1.0,
      ),
    );
  }
  return positions;
}

/** Spherical cloud layout for medium COUNT (51-1000) using golden angle */
export function cloudLayout(
  count: number,
  origin: THREE.Vector3,
): THREE.Vector3[] {
  const positions: THREE.Vector3[] = [];
  const goldenAngle = Math.PI * (3 - Math.sqrt(5));

  for (let i = 0; i < count; i++) {
    const t = i / count;
    const inclination = Math.acos(1 - 2 * t);
    const azimuth = goldenAngle * i;
    const radius = 2.0 + (i % 7) * 0.04; // slight deterministic jitter
    positions.push(
      new THREE.Vector3(
        origin.x + radius * Math.sin(inclination) * Math.cos(azimuth),
        origin.y + radius * Math.sin(inclination) * Math.sin(azimuth),
        origin.z + radius * Math.cos(inclination),
      ),
    );
  }
  return positions;
}

// ── AnimatedChildCube ──

interface AnimatedChildCubeProps {
  item: DecomposedItem;
  from: THREE.Vector3;
  to: THREE.Vector3;
  targetScale: number;
  delay: number;
  duration: number;
  onRightClick?: (item: DecomposedItem, event: THREE.Event) => void;
  onClick?: (item: DecomposedItem) => void;
}

function AnimatedChildCube({
  item,
  from,
  to,
  targetScale,
  delay,
  duration,
  onRightClick,
  onClick,
}: AnimatedChildCubeProps) {
  const meshRef = useRef<THREE.Mesh>(null);
  const [elapsed, setElapsed] = useState(0);
  const [hovered, setHovered] = useState(false);

  useFrame((_, delta) => {
    if (!meshRef.current) return;
    const next = elapsed + delta * 1000;
    setElapsed(next);

    const active = Math.max(0, next - delay);
    const progress = Math.min(1, active / duration);
    const eased = easeOutBack(progress);

    // Interpolate position
    meshRef.current.position.lerpVectors(from, to, eased);

    // Interpolate scale
    const s = eased * targetScale;
    meshRef.current.scale.set(s, s, s);
  });

  const color = item.color ?? '#22d3ee';

  return (
    <mesh
      ref={meshRef}
      position={[from.x, from.y, from.z]}
      scale={[0, 0, 0]}
      onClick={(e) => {
        e.stopPropagation();
        onClick?.(item);
      }}
      onContextMenu={(e) => {
        e.stopPropagation();
        (e as any).nativeEvent?.preventDefault?.();
        onRightClick?.(item, e as any);
      }}
      onPointerOver={() => setHovered(true)}
      onPointerOut={() => setHovered(false)}
    >
      <boxGeometry args={[1, 1, 1]} />
      <meshStandardMaterial
        color={color}
        transparent
        opacity={0.85}
        emissive={color}
        emissiveIntensity={hovered ? 0.4 : 0.1}
        metalness={0.1}
        roughness={0.7}
      />
      {/* Label text above the cube */}
      <troikaText
        text={item.label}
        font={FONT_MONO}
        fontSize={0.1}
        color="#e8ecf4"
        anchorX="center"
        anchorY="bottom"
        position={[0, 0.6, 0]}
        outlineWidth={0.002}
        sdfGlyphSize={48}
      />
      {/* Value text below label */}
      <troikaText
        text={String(item.value)}
        font={FONT_MONO}
        fontSize={0.08}
        color="#94a3b8"
        anchorX="center"
        anchorY="top"
        position={[0, -0.6, 0]}
        sdfGlyphSize={48}
      />
    </mesh>
  );
}

// ── Histogram Face (AVG decomposition) ──

interface HistogramFaceProps {
  dist: DistributionResult;
  faceSize: number;
}

export function HistogramFace({ dist, faceSize }: HistogramFaceProps) {
  const maxCount = Math.max(...dist.bins.map((b) => b.count));
  const barWidth = faceSize / dist.bins.length;
  const range = dist.max - dist.min || 1;

  // Median line position (normalized 0..1 within range)
  const medianNorm = (dist.median - dist.min) / range;
  const medianX = (medianNorm - 0.5) * faceSize * 0.9;

  // StdDev band
  const stdLow = ((dist.mean - dist.stdDev - dist.min) / range - 0.5) * faceSize * 0.9;
  const stdHigh = ((dist.mean + dist.stdDev - dist.min) / range - 0.5) * faceSize * 0.9;

  return (
    <group>
      {/* Histogram bars */}
      {dist.bins.map((bin, i) => {
        const height = (bin.count / maxCount) * faceSize * 0.9;
        const isOutlier = dist.outlierIndices.includes(i);
        return (
          <mesh
            key={i}
            position={[
              (i - dist.bins.length / 2) * barWidth + barWidth / 2,
              height / 2 - faceSize / 2,
              0.01,
            ]}
          >
            <boxGeometry args={[barWidth * 0.85, height, 0.02]} />
            <meshStandardMaterial
              color={isOutlier ? '#fb7185' : '#22d3ee'}
              transparent
              opacity={0.85}
            />
          </mesh>
        );
      })}

      {/* Median line */}
      <mesh position={[medianX, 0, 0.02]}>
        <boxGeometry args={[0.02, faceSize * 0.95, 0.005]} />
        <meshBasicMaterial color="#ffffff" />
      </mesh>

      {/* Standard deviation band */}
      <mesh position={[(stdLow + stdHigh) / 2, 0, 0.005]}>
        <boxGeometry args={[stdHigh - stdLow, faceSize * 0.95, 0.003]} />
        <meshStandardMaterial
          color="#a78bfa"
          transparent
          opacity={0.15}
        />
      </mesh>

      {/* Statistical annotations */}
      <troikaText
        text={`\u03BC=${dist.mean.toFixed(1)}`}
        font={FONT_MONO}
        fontSize={faceSize * 0.05}
        color="#94a3b8"
        anchorX="left"
        anchorY="bottom"
        position={[-faceSize * 0.45, faceSize * 0.45, 0.03]}
        sdfGlyphSize={48}
      />
      <troikaText
        text={`Med=${dist.median.toFixed(1)}`}
        font={FONT_MONO}
        fontSize={faceSize * 0.05}
        color="#ffffff"
        anchorX="left"
        anchorY="bottom"
        position={[-faceSize * 0.45, faceSize * 0.38, 0.03]}
        sdfGlyphSize={48}
      />
      <troikaText
        text={`\u03C3=${dist.stdDev.toFixed(1)}`}
        font={FONT_MONO}
        fontSize={faceSize * 0.05}
        color="#a78bfa"
        anchorX="left"
        anchorY="bottom"
        position={[-faceSize * 0.45, faceSize * 0.31, 0.03]}
        sdfGlyphSize={48}
      />
    </group>
  );
}

// ── Instanced Cube Cloud (COUNT > 1000) ──

interface InstancedCubeCloudProps {
  positions: THREE.Vector3[];
  color?: string;
}

function InstancedCubeCloud({
  positions,
  color = '#22d3ee',
}: InstancedCubeCloudProps) {
  const meshRef = useRef<THREE.InstancedMesh>(null);
  const tempMatrix = useMemo(() => new THREE.Matrix4(), []);
  const cubeScale = 0.1;

  useMemo(() => {
    if (!meshRef.current) return;
    positions.forEach((pos, i) => {
      tempMatrix.makeTranslation(pos.x, pos.y, pos.z);
      tempMatrix.scale(new THREE.Vector3(cubeScale, cubeScale, cubeScale));
      meshRef.current!.setMatrixAt(i, tempMatrix);
    });
    meshRef.current.instanceMatrix.needsUpdate = true;
  }, [positions, tempMatrix]);

  return (
    <instancedMesh
      ref={meshRef}
      args={[undefined, undefined, positions.length]}
    >
      <boxGeometry args={[1, 1, 1]} />
      <meshStandardMaterial
        color={color}
        transparent
        opacity={0.7}
        emissive={color}
        emissiveIntensity={0.1}
      />
    </instancedMesh>
  );
}

// ── Main ChildCubeSpawner ──

interface ChildCubeSpawnerProps {
  /** SUM decomposition result */
  result?: DecomposeResult;
  /** AVG distribution result */
  distribution?: DistributionResult;
  /** COUNT result */
  countResult?: CountResult;
  /** Parent cube world position */
  parentPos: THREE.Vector3;
  /** Normal direction of the face that was decomposed */
  faceNormal: THREE.Vector3;
  /** Size of parent face (for histogram) */
  faceSize?: number;
  /** Called when a child cube is right-clicked */
  onChildRightClick?: (item: DecomposedItem, event: THREE.Event) => void;
  /** Called when a child cube is clicked */
  onChildClick?: (item: DecomposedItem) => void;
  /** Whether to render as collapsing (reverse animation) */
  collapsing?: boolean;
}

export function ChildCubeSpawner({
  result,
  distribution,
  countResult,
  parentPos,
  faceNormal,
  faceSize = 1,
  onChildRightClick,
  onChildClick,
  collapsing = false,
}: ChildCubeSpawnerProps) {
  // SUM decomposition: radial child cubes
  if (result) {
    const children = computeChildLayout(result.items, parentPos, faceNormal);
    return (
      <group>
        {children.map((child) => (
          <AnimatedChildCube
            key={child.item.id}
            item={child.item}
            from={collapsing ? child.targetPosition : parentPos}
            to={collapsing ? parentPos : child.targetPosition}
            targetScale={child.targetScale}
            delay={child.delay}
            duration={collapsing ? 400 : 600}
            onRightClick={onChildRightClick}
            onClick={onChildClick}
          />
        ))}
      </group>
    );
  }

  // AVG decomposition: histogram on face
  if (distribution) {
    return (
      <group position={[parentPos.x, parentPos.y, parentPos.z + faceNormal.z * 0.5]}>
        <HistogramFace dist={distribution} faceSize={faceSize} />
      </group>
    );
  }

  // COUNT decomposition: grid / cloud / instanced
  if (countResult) {
    const count = countResult.totalCount;
    const strategy = selectLayout(count);

    if (strategy === 'instanced') {
      const positions = cloudLayout(count, parentPos);
      return <InstancedCubeCloud positions={positions} />;
    }

    const positions =
      strategy === 'grid'
        ? gridLayout(count, parentPos)
        : cloudLayout(count, parentPos);

    // Build DecomposedItem array for individual cubes
    const items = countResult.items.slice(0, positions.length);

    return (
      <group>
        {items.map((item, i) => (
          <AnimatedChildCube
            key={item.id}
            item={item}
            from={parentPos}
            to={positions[i]}
            targetScale={strategy === 'grid' ? 0.12 : 0.08}
            delay={i * 20}
            duration={600}
            onRightClick={onChildRightClick}
            onClick={onChildClick}
          />
        ))}
      </group>
    );
  }

  return null;
}
