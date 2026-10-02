// Marabunta - Licensed under the MIT License.
// ── PinPanel ──
// Detaches a decomposed view from the 3D scene and renders it as a floating
// panel anchored to the viewport. The panel is an HTML overlay with its own
// miniature 3D viewport (a secondary R3F Canvas) showing the decomposed
// cubes. Panels are draggable, resizable, and minimizable.

import React, { useState, useCallback, useRef, useEffect } from 'react';
import { createPortal } from 'react-dom';
import type { DecompositionTree, TreeNode } from './DecompositionTree';
import { getNodeLabel } from './DecompositionTree';

// ── Types ──

export interface PinnedView {
  id: string;
  title: string;
  tree: DecompositionTree;
  position: { x: number; y: number };
  size: { width: number; height: number };
  minimized: boolean;
  createdAt: number;
}

// ── Hook: usePinManager ──

export function usePinManager() {
  const [pins, setPins] = useState<PinnedView[]>([]);

  const addPin = useCallback(
    (title: string, tree: DecompositionTree) => {
      const pin: PinnedView = {
        id: `pin-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
        title,
        tree,
        position: { x: 50 + pins.length * 30, y: 100 + pins.length * 20 },
        size: { width: 400, height: 300 },
        minimized: false,
        createdAt: Date.now(),
      };
      setPins((prev) => [...prev, pin]);
      return pin.id;
    },
    [pins.length],
  );

  const removePin = useCallback((id: string) => {
    setPins((prev) => prev.filter((p) => p.id !== id));
  }, []);

  const toggleMinimize = useCallback((id: string) => {
    setPins((prev) =>
      prev.map((p) =>
        p.id === id ? { ...p, minimized: !p.minimized } : p,
      ),
    );
  }, []);

  const updatePosition = useCallback(
    (id: string, pos: { x: number; y: number }) => {
      setPins((prev) =>
        prev.map((p) => (p.id === id ? { ...p, position: pos } : p)),
      );
    },
    [],
  );

  const updateSize = useCallback(
    (id: string, size: { width: number; height: number }) => {
      setPins((prev) =>
        prev.map((p) => (p.id === id ? { ...p, size } : p)),
      );
    },
    [],
  );

  return { pins, addPin, removePin, toggleMinimize, updatePosition, updateSize };
}

// ── Tree Summary Renderer ──
// Renders a compact text summary of the decomposition tree for the panel
// body (used when a secondary R3F Canvas is not available or the panel
// is kept lightweight).

function TreeSummary({ tree }: { tree: DecompositionTree }) {
  const renderNode = (node: TreeNode, depth: number): React.ReactNode => {
    const indent = depth * 16;
    const label = getNodeLabel(node);
    const opLabel =
      node.operation !== 'ROOT' ? (
        <span
          style={{
            fontSize: '10px',
            color: '#64748b',
            fontFamily: "'IBM Plex Mono', monospace",
            marginRight: '6px',
          }}
        >
          [{node.operation}]
        </span>
      ) : null;

    return (
      <React.Fragment key={node.id}>
        <div
          style={{
            paddingLeft: `${indent}px`,
            padding: `3px 8px 3px ${indent + 8}px`,
            fontSize: '12px',
            color: node.id === tree.activeNodeId ? '#22d3ee' : '#94a3b8',
            fontFamily: "'DM Sans', sans-serif",
            borderLeft:
              depth > 0 ? '1px solid #1e293b' : 'none',
            marginLeft: depth > 0 ? `${(depth - 1) * 16 + 8}px` : 0,
          }}
        >
          {opLabel}
          <span style={{ fontWeight: node.children.length > 0 ? 600 : 400 }}>
            {label}
          </span>
        </div>
        {node.children.map((child) => renderNode(child, depth + 1))}
      </React.Fragment>
    );
  };

  return (
    <div
      style={{
        overflow: 'auto',
        flex: 1,
        padding: '8px 0',
      }}
    >
      {renderNode(tree.root, 0)}
    </div>
  );
}

// ── Draggable Panel ──

interface PinPanelProps {
  view: PinnedView;
  onClose: () => void;
  onMinimize: () => void;
  onPositionChange: (pos: { x: number; y: number }) => void;
}

export function PinPanel({
  view,
  onClose,
  onMinimize,
  onPositionChange,
}: PinPanelProps) {
  const panelRef = useRef<HTMLDivElement>(null);
  const dragRef = useRef<{
    dragging: boolean;
    startX: number;
    startY: number;
    offsetX: number;
    offsetY: number;
  }>({ dragging: false, startX: 0, startY: 0, offsetX: 0, offsetY: 0 });

  // Drag handlers
  const onMouseDown = useCallback(
    (e: React.MouseEvent) => {
      dragRef.current = {
        dragging: true,
        startX: e.clientX,
        startY: e.clientY,
        offsetX: view.position.x,
        offsetY: view.position.y,
      };
    },
    [view.position],
  );

  useEffect(() => {
    const onMouseMove = (e: MouseEvent) => {
      if (!dragRef.current.dragging) return;
      const dx = e.clientX - dragRef.current.startX;
      const dy = e.clientY - dragRef.current.startY;
      onPositionChange({
        x: dragRef.current.offsetX + dx,
        y: dragRef.current.offsetY + dy,
      });
    };

    const onMouseUp = () => {
      dragRef.current.dragging = false;
    };

    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
    return () => {
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('mouseup', onMouseUp);
    };
  }, [onPositionChange]);

  return createPortal(
    <div
      ref={panelRef}
      style={{
        position: 'fixed',
        left: view.position.x,
        top: view.position.y,
        width: view.size.width,
        height: view.minimized ? 36 : view.size.height,
        zIndex: 9998,
        background: '#111827',
        border: '1px solid #1e293b',
        borderRadius: '8px',
        overflow: 'hidden',
        display: 'flex',
        flexDirection: 'column',
        boxShadow: '0 8px 32px rgba(0,0,0,0.5)',
        fontFamily: "'DM Sans', sans-serif",
        transition: 'height 0.2s ease',
      }}
    >
      {/* Header — draggable */}
      <div
        onMouseDown={onMouseDown}
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          padding: '6px 12px',
          background: '#0d1321',
          borderBottom: view.minimized ? 'none' : '1px solid #1e293b',
          cursor: 'grab',
          userSelect: 'none',
          flexShrink: 0,
        }}
      >
        <span
          style={{
            fontSize: '12px',
            fontWeight: 600,
            color: '#e8ecf4',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
            flex: 1,
          }}
        >
          <span style={{ color: '#22d3ee', marginRight: '6px' }}>
            {'\u{1F4CC}'}
          </span>
          {view.title}
        </span>
        <div style={{ display: 'flex', gap: '4px', flexShrink: 0 }}>
          <button
            onClick={onMinimize}
            title="Minimize"
            style={{
              width: '20px',
              height: '20px',
              border: '1px solid #334155',
              borderRadius: '4px',
              background: 'transparent',
              color: '#64748b',
              cursor: 'pointer',
              fontSize: '12px',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              lineHeight: 1,
            }}
          >
            _
          </button>
          <button
            onClick={onClose}
            title="Close"
            style={{
              width: '20px',
              height: '20px',
              border: '1px solid #334155',
              borderRadius: '4px',
              background: 'transparent',
              color: '#fb7185',
              cursor: 'pointer',
              fontSize: '12px',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              lineHeight: 1,
            }}
          >
            x
          </button>
        </div>
      </div>

      {/* Body — tree summary view */}
      {!view.minimized && <TreeSummary tree={view.tree} />}
    </div>,
    document.body,
  );
}

// ── Pin Panel Container (renders all pinned views) ──

interface PinPanelContainerProps {
  pins: PinnedView[];
  onClose: (id: string) => void;
  onMinimize: (id: string) => void;
  onPositionChange: (id: string, pos: { x: number; y: number }) => void;
}

export function PinPanelContainer({
  pins,
  onClose,
  onMinimize,
  onPositionChange,
}: PinPanelContainerProps) {
  if (pins.length === 0) return null;

  return (
    <>
      {pins.map((pin) => (
        <PinPanel
          key={pin.id}
          view={pin}
          onClose={() => onClose(pin.id)}
          onMinimize={() => onMinimize(pin.id)}
          onPositionChange={(pos) => onPositionChange(pin.id, pos)}
        />
      ))}
    </>
  );
}
