// Marabunta - Licensed under the MIT License.
// ── Preset Toolbar ──
// Horizontal bar of 7 preset buttons rendered at the top of the 3D viewport.
// Each button shows icon, label, and keyboard shortcut hint.
// Active preset has a cyan underline; all buttons pulse during crossfade.
// (W6A / Spec S21)

import React from 'react';
import { usePresetContext } from './PresetManager';
import { PRESETS, PRESET_ORDER } from './PresetDefinitions';
import type { PresetName } from './types';

// ── Inline styles (dark-glass aesthetic matching the dashboard theme) ──

const toolbarStyle: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  gap: '4px',
  padding: '6px 12px',
  background: '#0a0e17e8',
  backdropFilter: 'blur(20px)',
  borderBottom: '1px solid #1e293b',
  fontFamily: "'DM Sans', sans-serif",
  userSelect: 'none',
};

const btnBase: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  gap: '6px',
  height: '40px',
  padding: '8px 16px',
  border: 'none',
  borderBottom: '2px solid transparent',
  borderRadius: '6px 6px 0 0',
  background: 'transparent',
  color: '#94a3b8',
  cursor: 'pointer',
  fontFamily: "'DM Sans', sans-serif",
  fontSize: '0.82rem',
  fontWeight: 500,
  transition: 'all 0.2s ease',
  position: 'relative',
};

const btnActiveExtra: React.CSSProperties = {
  borderBottomColor: '#22d3ee',
  background: '#22d3ee10',
  color: '#e8ecf4',
};

const btnDisabledExtra: React.CSSProperties = {
  opacity: 0.6,
  cursor: 'not-allowed',
};

const iconStyle: React.CSSProperties = {
  fontFamily: "'IBM Plex Mono', monospace",
  fontSize: '0.75rem',
  letterSpacing: '0.05em',
};

const labelStyle: React.CSSProperties = {
  whiteSpace: 'nowrap',
};

const shortcutStyle: React.CSSProperties = {
  fontFamily: "'IBM Plex Mono', monospace",
  fontSize: '0.65rem',
  color: '#64748b',
  background: '#1a2234',
  padding: '1px 5px',
  borderRadius: '3px',
  border: '1px solid #1e293b',
  lineHeight: '1.4',
};

// ── Component ──

export function PresetToolbar() {
  const { state, switchPreset } = usePresetContext();

  return (
    <div
      className="preset-toolbar"
      style={toolbarStyle}
      role="toolbar"
      aria-label="Dimension presets"
      aria-busy={state.isTransitioning}
    >
      {PRESET_ORDER.map((name: PresetName) => {
        const def = PRESETS[name];
        const isActive = state.activePreset === name;
        const isTransitioning = state.isTransitioning;

        const combinedStyle: React.CSSProperties = {
          ...btnBase,
          ...(isActive ? btnActiveExtra : {}),
          ...(isTransitioning ? btnDisabledExtra : {}),
          ...(isTransitioning && isActive
            ? { animation: 'presetPulse 0.5s ease-in-out' }
            : {}),
        };

        return (
          <button
            key={name}
            style={combinedStyle}
            onClick={() => switchPreset(name)}
            disabled={isTransitioning}
            title={`${def.label} (${def.shortcut})`}
            aria-label={`${def.label} preset, shortcut ${def.shortcut}`}
            aria-pressed={isActive}
          >
            <span style={iconStyle}>{def.icon}</span>
            <span style={labelStyle}>{def.label}</span>
            <span style={shortcutStyle}>{def.shortcut}</span>
          </button>
        );
      })}
    </div>
  );
}
