// Marabunta - Licensed under the MIT License.
// ── Custom Preset Editor ──
// Drag-and-drop editor for user-defined face mappings.
// When the Custom preset is active, the MetricSidebar slides open
// and face slots become drop targets. (W6A / Spec S21)

import React, { useState, useCallback } from 'react';
import type { FaceSlot, CustomPreset, FaceDimension, MetricDragPayload } from './types';
import { FACE_SLOTS } from './types';
import { MetricSidebar, resolveRenderer, resolveAccent } from './MetricSidebar';

// ── Props ──

interface CustomPresetEditorProps {
  preset: CustomPreset;
  onSave: (preset: CustomPreset) => void;
  onCancel: () => void;
}

// ── Inline Styles ──

const editorStyle: React.CSSProperties = {
  display: 'flex',
  flexDirection: 'column',
  height: '100%',
  fontFamily: "'DM Sans', sans-serif",
  color: '#e8ecf4',
};

const headerStyle: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  gap: '8px',
  padding: '10px 12px',
  borderBottom: '1px solid #1e293b',
  background: '#0a0e17e8',
};

const nameInputStyle: React.CSSProperties = {
  flex: 1,
  padding: '6px 10px',
  border: '1px solid #1e293b',
  borderRadius: '6px',
  background: '#1a2234',
  color: '#e8ecf4',
  fontFamily: "'DM Sans', sans-serif",
  fontSize: '0.88rem',
  outline: 'none',
};

const actionBtnStyle: React.CSSProperties = {
  padding: '6px 14px',
  border: '1px solid #1e293b',
  borderRadius: '6px',
  background: '#1a2234',
  color: '#94a3b8',
  cursor: 'pointer',
  fontFamily: "'DM Sans', sans-serif",
  fontSize: '0.82rem',
  fontWeight: 600,
  transition: 'all 0.15s',
};

const saveBtnStyle: React.CSSProperties = {
  ...actionBtnStyle,
  borderColor: '#22d3ee40',
  background: '#22d3ee15',
  color: '#22d3ee',
};

const bodyStyle: React.CSSProperties = {
  display: 'flex',
  flex: 1,
  overflow: 'hidden',
};

const faceGridStyle: React.CSSProperties = {
  display: 'grid',
  gridTemplateColumns: 'repeat(3, 1fr)',
  gridTemplateRows: 'repeat(2, 1fr)',
  gap: '8px',
  padding: '12px',
  flex: 1,
};

const dropTargetBase: React.CSSProperties = {
  display: 'flex',
  flexDirection: 'column',
  alignItems: 'center',
  justifyContent: 'center',
  gap: '6px',
  padding: '12px',
  border: '2px dashed #1e293b',
  borderRadius: '10px',
  background: '#0d1321',
  transition: 'all 0.2s',
  minHeight: '90px',
};

const dropTargetHovered: React.CSSProperties = {
  borderColor: '#22d3ee',
  background: '#22d3ee08',
  boxShadow: '0 0 12px #22d3ee20',
};

const faceLabelStyle: React.CSSProperties = {
  fontFamily: "'IBM Plex Mono', monospace",
  fontSize: '0.7rem',
  fontWeight: 600,
  letterSpacing: '0.1em',
  textTransform: 'uppercase' as const,
  color: '#64748b',
};

const metricNameStyle: React.CSSProperties = {
  fontSize: '0.82rem',
  fontWeight: 600,
  color: '#e8ecf4',
  textAlign: 'center',
};

const clearBtnStyle: React.CSSProperties = {
  padding: '2px 8px',
  border: '1px solid #1e293b',
  borderRadius: '4px',
  background: 'transparent',
  color: '#fb7185',
  cursor: 'pointer',
  fontFamily: "'IBM Plex Mono', monospace",
  fontSize: '0.7rem',
  transition: 'all 0.15s',
};

const emptyHintStyle: React.CSSProperties = {
  fontSize: '0.75rem',
  color: '#64748b',
  fontStyle: 'italic',
  textAlign: 'center',
};

// ── FaceDropTarget sub-component ──

interface FaceDropTargetProps {
  face: FaceSlot;
  dimension: FaceDimension | null;
  isHovered: boolean;
  onDrop: (payload: MetricDragPayload) => void;
  onDragEnter: () => void;
  onDragLeave: () => void;
  onClear: () => void;
}

function FaceDropTarget({
  face,
  dimension,
  isHovered,
  onDrop,
  onDragEnter,
  onDragLeave,
  onClear,
}: FaceDropTargetProps) {
  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = 'copy';
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    const raw = e.dataTransfer.getData('application/json');
    if (!raw) return;
    try {
      const payload: MetricDragPayload = JSON.parse(raw);
      onDrop(payload);
    } catch {
      /* ignore malformed payload */
    }
  };

  const style: React.CSSProperties = {
    ...dropTargetBase,
    ...(isHovered ? dropTargetHovered : {}),
    ...(dimension
      ? { borderStyle: 'solid', borderColor: dimension.accent + '60' }
      : {}),
  };

  return (
    <div
      className="face-drop-target"
      style={style}
      onDragOver={handleDragOver}
      onDragEnter={onDragEnter}
      onDragLeave={onDragLeave}
      onDrop={handleDrop}
      role="region"
      aria-label={`${face} face slot`}
    >
      <div style={faceLabelStyle}>{face.toUpperCase()}</div>
      {dimension ? (
        <>
          <span style={metricNameStyle}>{dimension.label}</span>
          <button
            style={clearBtnStyle}
            onClick={onClear}
            aria-label={`Clear ${face} face`}
          >
            x
          </button>
        </>
      ) : (
        <div style={emptyHintStyle}>Drop Metric Here</div>
      )}
    </div>
  );
}

// ── Main Component ──

export function CustomPresetEditor({
  preset,
  onSave,
  onCancel,
}: CustomPresetEditorProps) {
  const [faces, setFaces] = useState<
    Record<FaceSlot, FaceDimension | null>
  >({ ...preset.faces });
  const [name, setName] = useState(preset.name);
  const [hoveredFace, setHoveredFace] = useState<FaceSlot | null>(null);

  const handleDrop = useCallback(
    (face: FaceSlot, payload: MetricDragPayload) => {
      setFaces(prev => ({
        ...prev,
        [face]: {
          metricId:    payload.metricId,
          label:       payload.label,
          description: '',
          renderer:    resolveRenderer(payload.metricId),
          accent:      resolveAccent(payload.sourceCategory),
        } satisfies FaceDimension,
      }));
      setHoveredFace(null);
    },
    [],
  );

  const handleClearFace = useCallback(
    (face: FaceSlot) => {
      setFaces(prev => ({ ...prev, [face]: null }));
    },
    [],
  );

  const handleSave = useCallback(() => {
    onSave({
      ...preset,
      name,
      faces,
      updatedAt: Date.now(),
    });
  }, [preset, name, faces, onSave]);

  return (
    <div className="custom-preset-editor" style={editorStyle}>
      <div style={headerStyle}>
        <input
          style={nameInputStyle}
          value={name}
          onChange={e => setName(e.target.value)}
          placeholder="Custom preset name..."
          aria-label="Custom preset name"
        />
        <button style={saveBtnStyle} onClick={handleSave}>
          Save
        </button>
        <button style={actionBtnStyle} onClick={onCancel}>
          Cancel
        </button>
      </div>

      <div style={bodyStyle}>
        <div style={faceGridStyle}>
          {FACE_SLOTS.map(slot => (
            <FaceDropTarget
              key={slot}
              face={slot}
              dimension={faces[slot]}
              isHovered={hoveredFace === slot}
              onDrop={payload => handleDrop(slot, payload)}
              onDragEnter={() => setHoveredFace(slot)}
              onDragLeave={() => setHoveredFace(null)}
              onClear={() => handleClearFace(slot)}
            />
          ))}
        </div>

        <MetricSidebar />
      </div>
    </div>
  );
}
