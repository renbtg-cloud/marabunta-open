// Marabunta - Licensed under the MIT License.
// ── ContextMenu ──
// Right-click context menu for cube face decomposition. Intercepts
// onContextMenu events from R3F mesh elements, converts the 3D
// intersection point into screen coordinates, and renders a floating
// HTML overlay via React portal with all 10 operations, applicability
// rules, keyboard accelerators, and viewport-clamped positioning.

import React, {
  useState,
  useCallback,
  useEffect,
  useRef,
  useMemo,
} from 'react';
import { createPortal } from 'react-dom';
import * as THREE from 'three';
import type { ThreeEvent } from '@react-three/fiber';
import type { AggregateType } from './DecompositionTree';
import { MATERIAL_INDEX_MAP } from '../types';
import type { FacePosition } from '../types';

// ── Operation Definitions ──

export type OperationId =
  | 'decompose_sum'
  | 'decompose_avg'
  | 'decompose_count'
  | 'group_by'
  | 'ungroup'
  | 'compare_avg'
  | 'trend'
  | 'filter'
  | 'pin'
  | 'export';

export interface MenuOperation {
  id: OperationId;
  label: string;
  accelerator: string;
  /** Key code (single lowercase char) for keyboard shortcut */
  hotkey: string;
  /** Icon character / emoji / short glyph */
  icon: string;
  /** Check whether this operation applies for the given aggregate type */
  isApplicable: (aggregateType: AggregateType) => boolean;
  /** Category for visual grouping */
  category: 'decompose' | 'arrange' | 'analyze' | 'action';
}

export const OPERATIONS: MenuOperation[] = [
  {
    id: 'decompose_sum',
    label: 'Decompose SUM',
    accelerator: 'S',
    hotkey: 's',
    icon: '\u2211', // Sigma
    isApplicable: (t) => t === 'SUM',
    category: 'decompose',
  },
  {
    id: 'decompose_avg',
    label: 'Decompose AVG',
    accelerator: 'A',
    hotkey: 'a',
    icon: '\u03BC', // Mu
    isApplicable: (t) => t === 'AVG',
    category: 'decompose',
  },
  {
    id: 'decompose_count',
    label: 'Decompose COUNT',
    accelerator: 'C',
    hotkey: 'c',
    icon: '#',
    isApplicable: (t) => t === 'COUNT',
    category: 'decompose',
  },
  {
    id: 'group_by',
    label: 'Group by\u2026',
    accelerator: 'G',
    hotkey: 'g',
    icon: '\u25A6', // Box grid
    isApplicable: () => true,
    category: 'arrange',
  },
  {
    id: 'ungroup',
    label: 'Ungroup',
    accelerator: 'U',
    hotkey: 'u',
    icon: '\u25A1', // Empty box
    isApplicable: () => true,
    category: 'arrange',
  },
  {
    id: 'compare_avg',
    label: 'Compare to AVG',
    accelerator: 'V',
    hotkey: 'v',
    icon: '\u2194', // Left-right arrow
    isApplicable: (t) => t !== 'RAW',
    category: 'analyze',
  },
  {
    id: 'trend',
    label: 'Trend over time',
    accelerator: 'T',
    hotkey: 't',
    icon: '\u2197', // NE arrow
    isApplicable: () => true,
    category: 'analyze',
  },
  {
    id: 'filter',
    label: 'Filter swarm by this',
    accelerator: 'F',
    hotkey: 'f',
    icon: '\u29D6', // Hourglass / filter
    isApplicable: () => true,
    category: 'action',
  },
  {
    id: 'pin',
    label: 'Pin to dashboard',
    accelerator: 'P',
    hotkey: 'p',
    icon: '\u{1F4CC}', // Pin (pushpin)
    isApplicable: () => true,
    category: 'action',
  },
  {
    id: 'export',
    label: 'Export\u2026',
    accelerator: 'E',
    hotkey: 'e',
    icon: '\u2B07', // Down arrow
    isApplicable: () => true,
    category: 'action',
  },
];

// ── Context Menu State ──

export interface ContextMenuState {
  visible: boolean;
  screenX: number;
  screenY: number;
  faceIndex: number;
  facePosition: FacePosition | null;
  aggregateType: AggregateType;
  cubeId: string;
  parentValue: number;
  worldPosition: THREE.Vector3;
}

function initialState(cubeId: string): ContextMenuState {
  return {
    visible: false,
    screenX: 0,
    screenY: 0,
    faceIndex: 0,
    facePosition: null,
    aggregateType: 'RAW',
    cubeId,
    parentValue: 0,
    worldPosition: new THREE.Vector3(),
  };
}

// ── Hook: useContextMenu ──

export interface UseContextMenuOptions {
  cubeId: string;
  getAggregateType?: (facePosition: FacePosition) => AggregateType;
  getFaceValue?: (facePosition: FacePosition) => number;
}

export function useContextMenu({
  cubeId,
  getAggregateType,
  getFaceValue,
}: UseContextMenuOptions) {
  const [menu, setMenu] = useState<ContextMenuState>(() => initialState(cubeId));

  const onContextMenu = useCallback(
    (e: ThreeEvent<PointerEvent>) => {
      e.nativeEvent.preventDefault();
      e.stopPropagation();

      // Determine which face was right-clicked
      const materialIdx = e.face
        ? (e.faceIndex !== undefined ? Math.floor(e.faceIndex / 2) : 0)
        : 0;
      const facePosition: FacePosition =
        MATERIAL_INDEX_MAP[materialIdx] ?? 'front';
      const aggType = getAggregateType?.(facePosition) ?? 'RAW';
      const value = getFaceValue?.(facePosition) ?? 0;

      setMenu({
        visible: true,
        screenX: e.nativeEvent.clientX,
        screenY: e.nativeEvent.clientY,
        faceIndex: materialIdx,
        facePosition,
        aggregateType: aggType,
        cubeId,
        parentValue: value,
        worldPosition: e.point.clone(),
      });
    },
    [cubeId, getAggregateType, getFaceValue],
  );

  const close = useCallback(() => {
    setMenu((prev) => ({ ...prev, visible: false }));
  }, []);

  return { menu, setMenu, onContextMenu, close };
}

// ── Menu Item Component ──

interface MenuItemProps {
  op: MenuOperation;
  disabled: boolean;
  focused: boolean;
  onClick: () => void;
}

const MenuItem = React.memo(function MenuItem({
  op,
  disabled,
  focused,
  onClick,
}: MenuItemProps) {
  return (
    <button
      role="menuitem"
      className={`dcm-item${disabled ? ' dcm-disabled' : ''}${focused ? ' dcm-focused' : ''}`}
      disabled={disabled}
      onClick={onClick}
      aria-disabled={disabled}
      tabIndex={disabled ? -1 : 0}
      style={{
        display: 'flex',
        alignItems: 'center',
        gap: '8px',
        width: '100%',
        padding: '6px 12px',
        border: 'none',
        background: focused ? 'rgba(34,211,238,0.12)' : 'transparent',
        color: disabled ? '#475569' : '#e8ecf4',
        cursor: disabled ? 'not-allowed' : 'pointer',
        fontFamily: "'DM Sans', sans-serif",
        fontSize: '13px',
        textAlign: 'left',
        borderRadius: '4px',
        transition: 'background 0.1s',
      }}
      onMouseEnter={(e) => {
        if (!disabled) {
          (e.currentTarget as HTMLButtonElement).style.background =
            'rgba(34,211,238,0.12)';
        }
      }}
      onMouseLeave={(e) => {
        if (!focused) {
          (e.currentTarget as HTMLButtonElement).style.background =
            'transparent';
        }
      }}
    >
      <span style={{ width: '18px', textAlign: 'center', flexShrink: 0 }}>
        {op.icon}
      </span>
      <span style={{ flex: 1 }}>{op.label}</span>
      <kbd
        style={{
          fontFamily: "'IBM Plex Mono', monospace",
          fontSize: '11px',
          padding: '1px 5px',
          borderRadius: '3px',
          border: `1px solid ${disabled ? '#1e293b' : '#334155'}`,
          color: disabled ? '#334155' : '#64748b',
          background: disabled ? 'transparent' : '#0d1321',
        }}
      >
        {op.accelerator}
      </kbd>
    </button>
  );
});

// ── Menu Overlay (HTML Portal) ──

interface ContextMenuOverlayProps {
  state: ContextMenuState;
  onAction: (opId: OperationId, state: ContextMenuState) => void;
  onClose: () => void;
}

export function ContextMenuOverlay({
  state,
  onAction,
  onClose,
}: ContextMenuOverlayProps) {
  const ref = useRef<HTMLDivElement>(null);
  const [focusIndex, setFocusIndex] = useState(-1);

  // Applicable operations for current aggregate type
  const applicability = useMemo(
    () =>
      OPERATIONS.map((op) => ({
        op,
        applicable: op.isApplicable(state.aggregateType),
      })),
    [state.aggregateType],
  );

  // Viewport clamping
  useEffect(() => {
    if (!ref.current || !state.visible) return;
    const rect = ref.current.getBoundingClientRect();
    const vw = window.innerWidth;
    const vh = window.innerHeight;

    if (state.screenX + rect.width > vw - 8) {
      ref.current.style.left = `${state.screenX - rect.width}px`;
    }
    if (state.screenY + rect.height > vh - 8) {
      ref.current.style.top = `${state.screenY - rect.height}px`;
    }
  }, [state]);

  // Close on outside click
  useEffect(() => {
    if (!state.visible) return;
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        onClose();
      }
    };
    document.addEventListener('mousedown', handler, true);
    return () => document.removeEventListener('mousedown', handler, true);
  }, [state.visible, onClose]);

  // Keyboard navigation
  useEffect(() => {
    if (!state.visible) return;

    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        onClose();
        return;
      }

      // Arrow key navigation
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        setFocusIndex((prev) => {
          let next = prev + 1;
          while (next < OPERATIONS.length && !applicability[next].applicable) {
            next++;
          }
          return next < OPERATIONS.length ? next : prev;
        });
        return;
      }
      if (e.key === 'ArrowUp') {
        e.preventDefault();
        setFocusIndex((prev) => {
          let next = prev - 1;
          while (next >= 0 && !applicability[next].applicable) {
            next--;
          }
          return next >= 0 ? next : prev;
        });
        return;
      }
      if (e.key === 'Enter' && focusIndex >= 0) {
        e.preventDefault();
        const item = applicability[focusIndex];
        if (item.applicable) {
          onAction(item.op.id, state);
          onClose();
        }
        return;
      }

      // Keyboard accelerators
      const key = e.key.toLowerCase();
      const match = OPERATIONS.find((op) => op.hotkey === key);
      if (match && match.isApplicable(state.aggregateType)) {
        e.preventDefault();
        onAction(match.id, state);
        onClose();
      }
    };

    document.addEventListener('keydown', handler);
    return () => document.removeEventListener('keydown', handler);
  }, [state, focusIndex, applicability, onAction, onClose]);

  // Reset focus when menu opens
  useEffect(() => {
    if (state.visible) setFocusIndex(-1);
  }, [state.visible]);

  if (!state.visible) return null;

  // Group operations by category for visual separators
  let lastCategory = '';

  return createPortal(
    <div
      ref={ref}
      role="menu"
      aria-label="Cube decomposition operations"
      style={{
        position: 'fixed',
        left: state.screenX,
        top: state.screenY,
        zIndex: 10000,
        background: '#111827',
        border: '1px solid #1e293b',
        borderRadius: '8px',
        padding: '4px',
        minWidth: '220px',
        boxShadow: '0 8px 32px rgba(0,0,0,0.5)',
        fontFamily: "'DM Sans', sans-serif",
      }}
    >
      {applicability.map(({ op, applicable }, i) => {
        const showSeparator = lastCategory !== '' && op.category !== lastCategory;
        lastCategory = op.category;
        return (
          <React.Fragment key={op.id}>
            {showSeparator && (
              <div
                style={{
                  height: '1px',
                  background: '#1e293b',
                  margin: '4px 8px',
                }}
              />
            )}
            <MenuItem
              op={op}
              disabled={!applicable}
              focused={i === focusIndex}
              onClick={() => {
                if (applicable) {
                  onAction(op.id, state);
                  onClose();
                }
              }}
            />
          </React.Fragment>
        );
      })}
    </div>,
    document.body,
  );
}
