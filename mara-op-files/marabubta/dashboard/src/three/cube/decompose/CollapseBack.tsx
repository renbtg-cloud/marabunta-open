// Marabunta - Licensed under the MIT License.
// ── CollapseBack ──
// Animates the reverse of decomposition: child cubes scale from their
// current size down to scale(0,0,0) while simultaneously moving back
// toward the parent face position. Animation takes 400ms with easeInBack.
// Also renders the breadcrumb trail as an HTML overlay for navigation.

import React, { useRef, useState, useCallback, useEffect } from 'react';
import { createPortal } from 'react-dom';
import * as THREE from 'three';
import { useFrame } from '@react-three/fiber';
import type {
  DecompositionTree,
  TreeNode,
  DecomposeOperation,
} from './DecompositionTree';
import { getNodeLabel } from './DecompositionTree';

// ── Easing ──

function easeInBack(t: number): number {
  const c1 = 1.70158;
  const c3 = c1 + 1;
  return c3 * t * t * t - c1 * t * t;
}

// ── Collapse Animation Controller ──

interface CollapseTarget {
  meshRef: React.RefObject<THREE.Object3D>;
  startPosition: THREE.Vector3;
  startScale: number;
}

interface CollapseAnimationProps {
  /** Targets to animate back to parent */
  targets: CollapseTarget[];
  /** Parent position to converge toward */
  parentPos: THREE.Vector3;
  /** Duration in ms */
  duration?: number;
  /** Called when animation completes */
  onComplete: () => void;
}

export function CollapseAnimation({
  targets,
  parentPos,
  duration = 400,
  onComplete,
}: CollapseAnimationProps) {
  const elapsedRef = useRef(0);
  const completedRef = useRef(false);

  useFrame((_, delta) => {
    if (completedRef.current) return;

    elapsedRef.current += delta * 1000;
    const progress = Math.min(1, elapsedRef.current / duration);
    const eased = easeInBack(progress);

    targets.forEach((target) => {
      if (!target.meshRef.current) return;

      // Move toward parent position
      target.meshRef.current.position.lerpVectors(
        target.startPosition,
        parentPos,
        eased,
      );

      // Scale down to zero
      const s = target.startScale * (1 - eased);
      target.meshRef.current.scale.set(s, s, s);
    });

    if (progress >= 1 && !completedRef.current) {
      completedRef.current = true;
      onComplete();
    }
  });

  return null; // Pure animation controller
}

// ── Breadcrumb Trail (HTML Overlay) ──

interface BreadcrumbTrailProps {
  tree: DecompositionTree;
  /** Called when a breadcrumb is clicked; collapses everything below that level */
  onCollapseToNode: (nodeId: string) => void;
}

export function BreadcrumbTrail({
  tree,
  onCollapseToNode,
}: BreadcrumbTrailProps) {
  if (tree.breadcrumbs.length <= 1) return null; // Don't show for root only

  return createPortal(
    <div
      role="navigation"
      aria-label="Decomposition breadcrumb trail"
      style={{
        position: 'fixed',
        top: '56px',
        left: '50%',
        transform: 'translateX(-50%)',
        zIndex: 9997,
        display: 'flex',
        alignItems: 'center',
        gap: '4px',
        padding: '6px 12px',
        background: 'rgba(17, 24, 39, 0.92)',
        border: '1px solid #1e293b',
        borderRadius: '8px',
        fontFamily: "'DM Sans', sans-serif",
        fontSize: '12px',
        backdropFilter: 'blur(8px)',
        maxWidth: '90vw',
        overflow: 'auto',
        whiteSpace: 'nowrap',
      }}
    >
      {tree.breadcrumbs.map((node, i) => (
        <React.Fragment key={node.id}>
          {i > 0 && (
            <span
              style={{
                color: '#334155',
                margin: '0 2px',
                fontSize: '10px',
              }}
            >
              {'\u2192'}
            </span>
          )}
          <button
            onClick={() => onCollapseToNode(node.id)}
            style={{
              padding: '3px 8px',
              border:
                node.id === tree.activeNodeId
                  ? '1px solid #22d3ee'
                  : '1px solid transparent',
              borderRadius: '4px',
              background:
                node.id === tree.activeNodeId
                  ? 'rgba(34,211,238,0.1)'
                  : 'transparent',
              color:
                node.id === tree.activeNodeId ? '#22d3ee' : '#94a3b8',
              cursor: 'pointer',
              fontFamily: "'DM Sans', sans-serif",
              fontSize: '12px',
              display: 'inline-flex',
              alignItems: 'center',
              gap: '4px',
              transition: 'all 0.15s',
            }}
            onMouseEnter={(e) => {
              if (node.id !== tree.activeNodeId) {
                (e.currentTarget as HTMLButtonElement).style.background =
                  'rgba(148,163,184,0.1)';
              }
            }}
            onMouseLeave={(e) => {
              if (node.id !== tree.activeNodeId) {
                (e.currentTarget as HTMLButtonElement).style.background =
                  'transparent';
              }
            }}
          >
            {node.operation !== 'ROOT' && (
              <span
                style={{
                  fontSize: '9px',
                  fontFamily: "'IBM Plex Mono', monospace",
                  color: '#64748b',
                  padding: '1px 4px',
                  background: '#1a2234',
                  borderRadius: '3px',
                }}
              >
                {node.operation}
              </span>
            )}
            <span>{getNodeLabel(node)}</span>
          </button>
        </React.Fragment>
      ))}

      {/* Depth indicator */}
      <span
        style={{
          marginLeft: '8px',
          fontSize: '10px',
          color: '#475569',
          fontFamily: "'IBM Plex Mono', monospace",
        }}
      >
        depth:{tree.maxDepth}
      </span>
    </div>,
    document.body,
  );
}

// ── Collapse Back Button (3D HUD element) ──
// A small "collapse" button that appears near the decomposed children
// to quickly collapse back to the parent level.

interface CollapseBackButtonProps {
  /** World position to place the button */
  position: [number, number, number];
  /** Called when the button is clicked */
  onCollapse: () => void;
  visible?: boolean;
}

export function CollapseBackButton({
  position,
  onCollapse,
  visible = true,
}: CollapseBackButtonProps) {
  const [hovered, setHovered] = useState(false);

  if (!visible) return null;

  return (
    <group position={position}>
      <mesh
        onClick={(e) => {
          e.stopPropagation();
          onCollapse();
        }}
        onPointerOver={() => setHovered(true)}
        onPointerOut={() => setHovered(false)}
      >
        <circleGeometry args={[0.15, 16]} />
        <meshBasicMaterial
          color={hovered ? '#22d3ee' : '#334155'}
          transparent
          opacity={hovered ? 0.9 : 0.6}
        />
      </mesh>
      {/* Arrow icon pointing "back" (left) */}
      <mesh position={[-0.02, 0, 0.001]} rotation={[0, 0, Math.PI / 2]}>
        <coneGeometry args={[0.05, 0.08, 3]} />
        <meshBasicMaterial color="#e8ecf4" />
      </mesh>
    </group>
  );
}
