// Marabunta - Licensed under the MIT License.
// ── GroupByController ──
// Re-aggregation by a different dimension. When "Group by..." is selected,
// cubes fly through the scene, clustering by the new grouping, with
// connecting lines dissolving and reforming over 1.2s. Also handles
// the "Ungroup" operation with a safety threshold at >1000 cubes.

import React, { useState, useCallback, useRef, useMemo, useEffect } from 'react';
import * as THREE from 'three';
import { useFrame } from '@react-three/fiber';
import { Text } from 'troika-three-text';
import { extend } from '@react-three/fiber';
import type { Object3DNode } from '@react-three/fiber';
import { createPortal } from 'react-dom';
import type { GroupByCluster } from './DecompositionTree';
import { FONT_MONO } from '../types';

extend({ TroikaText: Text });

declare module '@react-three/fiber' {
  interface ThreeElements {
    troikaText: Object3DNode<Text, typeof Text>;
  }
}

// ── Easing ──

function easeInOutCubic(t: number): number {
  return t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2;
}

// ── Types ──

export interface CubeRef {
  id: string;
  position: THREE.Vector3;
  startPosition: THREE.Vector3;
  meshRef?: React.RefObject<THREE.Mesh>;
}

export interface GroupByState {
  dimension: string;
  clusters: Map<string, string[]>;
  clusterPositions: Map<string, THREE.Vector3>;
  animating: boolean;
  progress: number;
}

// ── Cluster Position Calculator ──

/** Lay out clusters in a ring around the origin */
function computeClusterPositions(
  clusters: GroupByCluster[],
): Map<string, THREE.Vector3> {
  const positions = new Map<string, THREE.Vector3>();
  const radius = 6;

  clusters.forEach((cluster, i) => {
    const angle = (i / clusters.length) * Math.PI * 2;
    positions.set(
      cluster.label,
      new THREE.Vector3(
        Math.cos(angle) * radius,
        0,
        Math.sin(angle) * radius,
      ),
    );
  });

  return positions;
}

/** Offset within a cluster for a specific cube (small grid) */
function intraClusterOffset(
  cubeIndex: number,
  clusterSize: number,
): THREE.Vector3 {
  const cols = Math.ceil(Math.sqrt(clusterSize));
  const col = cubeIndex % cols;
  const row = Math.floor(cubeIndex / cols);
  const spacing = 0.5;
  return new THREE.Vector3(
    (col - cols / 2) * spacing,
    (row - Math.ceil(clusterSize / cols) / 2) * spacing,
    0,
  );
}

// ── Dimension Picker (HTML Overlay) ──

interface DimensionPickerProps {
  dimensions: string[];
  screenPos: { x: number; y: number };
  onSelect: (dimension: string) => void;
  onClose: () => void;
}

export function DimensionPicker({
  dimensions,
  screenPos,
  onSelect,
  onClose,
}: DimensionPickerProps) {
  const ref = useRef<HTMLDivElement>(null);

  // Close on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        onClose();
      }
    };
    document.addEventListener('mousedown', handler, true);
    return () => document.removeEventListener('mousedown', handler, true);
  }, [onClose]);

  // Close on Escape
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    document.addEventListener('keydown', handler);
    return () => document.removeEventListener('keydown', handler);
  }, [onClose]);

  return createPortal(
    <div
      ref={ref}
      role="menu"
      aria-label="Select grouping dimension"
      style={{
        position: 'fixed',
        left: screenPos.x + 220,
        top: screenPos.y,
        zIndex: 10001,
        background: '#111827',
        border: '1px solid #1e293b',
        borderRadius: '8px',
        padding: '4px',
        minWidth: '160px',
        boxShadow: '0 8px 32px rgba(0,0,0,0.5)',
        fontFamily: "'DM Sans', sans-serif",
      }}
    >
      <div
        style={{
          padding: '6px 12px',
          fontSize: '11px',
          color: '#64748b',
          fontFamily: "'IBM Plex Mono', monospace",
          textTransform: 'uppercase',
          letterSpacing: '0.1em',
        }}
      >
        Group by
      </div>
      {dimensions.map((dim) => (
        <button
          key={dim}
          role="menuitem"
          onClick={() => onSelect(dim)}
          style={{
            display: 'block',
            width: '100%',
            padding: '6px 12px',
            border: 'none',
            background: 'transparent',
            color: '#e8ecf4',
            cursor: 'pointer',
            fontFamily: "'DM Sans', sans-serif",
            fontSize: '13px',
            textAlign: 'left',
            borderRadius: '4px',
          }}
          onMouseEnter={(e) => {
            (e.currentTarget as HTMLButtonElement).style.background =
              'rgba(34,211,238,0.12)';
          }}
          onMouseLeave={(e) => {
            (e.currentTarget as HTMLButtonElement).style.background =
              'transparent';
          }}
        >
          {dim}
        </button>
      ))}
    </div>,
    document.body,
  );
}

// ── Ungroup Confirmation Dialog ──

interface UngroupConfirmProps {
  count: number;
  onConfirm: () => void;
  onLimit: () => void;
  onCancel: () => void;
}

export function UngroupConfirmDialog({
  count,
  onConfirm,
  onLimit,
  onCancel,
}: UngroupConfirmProps) {
  return createPortal(
    <div
      role="alertdialog"
      aria-label="Ungroup confirmation"
      style={{
        position: 'fixed',
        inset: 0,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        zIndex: 10002,
        background: 'rgba(0,0,0,0.6)',
      }}
    >
      <div
        style={{
          background: '#111827',
          border: '1px solid #fbbf24',
          borderRadius: '12px',
          padding: '24px',
          maxWidth: '400px',
          fontFamily: "'DM Sans', sans-serif",
          color: '#e8ecf4',
        }}
      >
        <h3 style={{ margin: '0 0 12px', fontSize: '16px', fontWeight: 600 }}>
          Ungroup Warning
        </h3>
        <p style={{ color: '#94a3b8', fontSize: '14px', lineHeight: 1.6 }}>
          Ungrouping will create ~{count.toLocaleString()} individual cubes.
          Performance may be affected.
        </p>
        <div
          style={{
            display: 'flex',
            gap: '8px',
            justifyContent: 'flex-end',
            marginTop: '16px',
          }}
        >
          <button
            onClick={onCancel}
            style={{
              padding: '6px 16px',
              border: '1px solid #334155',
              borderRadius: '6px',
              background: 'transparent',
              color: '#94a3b8',
              cursor: 'pointer',
              fontFamily: "'DM Sans', sans-serif",
              fontSize: '13px',
            }}
          >
            Cancel
          </button>
          <button
            onClick={onLimit}
            style={{
              padding: '6px 16px',
              border: '1px solid #fbbf24',
              borderRadius: '6px',
              background: 'rgba(251,191,36,0.1)',
              color: '#fbbf24',
              cursor: 'pointer',
              fontFamily: "'DM Sans', sans-serif",
              fontSize: '13px',
            }}
          >
            Limit to 500
          </button>
          <button
            onClick={onConfirm}
            style={{
              padding: '6px 16px',
              border: '1px solid #22d3ee',
              borderRadius: '6px',
              background: 'rgba(34,211,238,0.1)',
              color: '#22d3ee',
              cursor: 'pointer',
              fontFamily: "'DM Sans', sans-serif",
              fontSize: '13px',
            }}
          >
            Continue
          </button>
        </div>
      </div>
    </div>,
    document.body,
  );
}

// ── GroupBy Animation Controller ──

interface GroupByControllerProps {
  /** Cube refs that will be repositioned */
  cubes: CubeRef[];
  /** Cluster data from API */
  clusters: GroupByCluster[];
  /** Called when animation completes */
  onComplete: () => void;
  /** Animation duration in seconds */
  duration?: number;
}

export function GroupByController({
  cubes,
  clusters,
  onComplete,
  duration = 1.2,
}: GroupByControllerProps) {
  const progressRef = useRef(0);
  const completedRef = useRef(false);

  // Build cluster membership and positions
  const { clusterMap, clusterPositions } = useMemo(() => {
    const cmap = new Map<string, string[]>();
    clusters.forEach((c) => {
      cmap.set(c.label, c.cubeIds);
    });
    const cpos = computeClusterPositions(clusters);
    return { clusterMap: cmap, clusterPositions: cpos };
  }, [clusters]);

  // Find which cluster a cube belongs to
  const findClusterLabel = useCallback(
    (cubeId: string): string | null => {
      for (const [label, ids] of clusterMap) {
        if (ids.includes(cubeId)) return label;
      }
      return null;
    },
    [clusterMap],
  );

  useFrame((_, delta) => {
    if (completedRef.current) return;

    progressRef.current = Math.min(1, progressRef.current + delta / duration);
    const t = easeInOutCubic(progressRef.current);

    cubes.forEach((cube) => {
      const clusterLabel = findClusterLabel(cube.id);
      if (!clusterLabel) return;

      const clusterPos = clusterPositions.get(clusterLabel);
      if (!clusterPos) return;

      // Determine intra-cluster offset
      const ids = clusterMap.get(clusterLabel) ?? [];
      const idx = ids.indexOf(cube.id);
      const offset = intraClusterOffset(idx, ids.length);
      const target = clusterPos.clone().add(offset);

      // Lerp from start to target
      cube.position.lerpVectors(cube.startPosition, target, t);
    });

    if (progressRef.current >= 1 && !completedRef.current) {
      completedRef.current = true;
      onComplete();
    }
  });

  // Render cluster labels at each cluster position
  return (
    <group>
      {clusters.map((cluster) => {
        const pos = clusterPositions.get(cluster.label);
        if (!pos) return null;
        return (
          <troikaText
            key={cluster.label}
            text={cluster.label}
            font={FONT_MONO}
            fontSize={0.2}
            color="#64748b"
            anchorX="center"
            anchorY="bottom"
            position={[pos.x, pos.y + 2, pos.z]}
            outlineWidth={0.003}
            sdfGlyphSize={48}
          />
        );
      })}
    </group>
  );
}
