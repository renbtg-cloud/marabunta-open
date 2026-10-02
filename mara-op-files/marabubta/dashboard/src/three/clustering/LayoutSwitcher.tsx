// Marabunta - Licensed under the MIT License.
// ── Layout Switcher ──
// Toolbar dropdown for switching between the six layout engines.
// Keyboard shortcuts 1-6 switch layouts when the scene is focused.
// Ctrl+G opens the grouping dimension picker. Escape clears clustering.
// W6C / Spec S21

import React, { useState, useCallback, useEffect } from 'react';
import type { LayoutKey } from './layouts/LayoutEngine';

// ── Engine Definitions ──

const ENGINES: { key: LayoutKey; label: string; icon: string }[] = [
  { key: 'force',        label: 'Force-Directed', icon: '\u26A1' },   // lightning
  { key: 'geographic',   label: 'Geographic',     icon: '\uD83C\uDF10' }, // globe
  { key: 'hierarchical', label: 'Hierarchical',   icon: '\uD83C\uDF33' }, // tree
  { key: 'spectral',     label: 'Spectral',       icon: '\u03BB' },   // lambda
  { key: 'grid',         label: 'Grid',           icon: '\u25A6' },   // grid
  { key: 'custom',       label: 'Custom',         icon: '\uD83D\uDCCC' }, // pin
];

// ── Props ──

export interface LayoutSwitcherProps {
  current: LayoutKey;
  onChange: (key: LayoutKey) => void;
  onGroupingRequest?: () => void;
  onClearClustering?: () => void;
  disabled?: boolean;
}

// ── Component ──

export function LayoutSwitcher({
  current,
  onChange,
  onGroupingRequest,
  onClearClustering,
  disabled = false,
}: LayoutSwitcherProps) {
  const [open, setOpen] = useState(false);

  const handleSelect = useCallback(
    (key: LayoutKey) => {
      onChange(key);
      setOpen(false);
    },
    [onChange],
  );

  // Keyboard shortcuts
  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      if (disabled) return;

      // Ignore when typing in an input
      const tag = (e.target as HTMLElement)?.tagName;
      if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return;

      // 1-6 switch layout
      const num = parseInt(e.key, 10);
      if (num >= 1 && num <= 6) {
        const engine = ENGINES[num - 1];
        if (engine) {
          e.preventDefault();
          onChange(engine.key);
        }
        return;
      }

      // Ctrl+G or Cmd+G opens grouping picker
      if ((e.ctrlKey || e.metaKey) && e.key === 'g') {
        e.preventDefault();
        onGroupingRequest?.();
        return;
      }

      // Escape clears clustering
      if (e.key === 'Escape') {
        onClearClustering?.();
        return;
      }
    }

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [disabled, onChange, onGroupingRequest, onClearClustering]);

  const currentEngine = ENGINES.find((e) => e.key === current);

  return (
    <div
      style={{
        position: 'relative',
        fontFamily: "'IBM Plex Mono', monospace",
        fontSize: '0.8rem',
        zIndex: 50,
      }}
    >
      <button
        onClick={() => setOpen(!open)}
        disabled={disabled}
        style={{
          background: '#1a2234',
          border: '1px solid #1e293b',
          borderRadius: '6px',
          color: '#e8ecf4',
          padding: '0.4rem 0.8rem',
          cursor: disabled ? 'not-allowed' : 'pointer',
          display: 'flex',
          alignItems: 'center',
          gap: '0.4rem',
          fontSize: '0.8rem',
          fontFamily: 'inherit',
          opacity: disabled ? 0.5 : 1,
        }}
      >
        <span>{currentEngine?.icon}</span>
        <span>{currentEngine?.label ?? 'Layout'}</span>
        <span style={{ color: '#64748b', marginLeft: '0.3rem' }}>
          {open ? '\u25B2' : '\u25BC'}
        </span>
      </button>

      {open && (
        <div
          style={{
            position: 'absolute',
            top: '100%',
            left: 0,
            marginTop: '4px',
            background: '#111827',
            border: '1px solid #1e293b',
            borderRadius: '8px',
            overflow: 'hidden',
            minWidth: '180px',
            boxShadow: '0 8px 24px rgba(0,0,0,0.4)',
          }}
        >
          {ENGINES.map((engine, idx) => (
            <button
              key={engine.key}
              onClick={() => handleSelect(engine.key)}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '0.5rem',
                width: '100%',
                padding: '0.5rem 0.8rem',
                background:
                  engine.key === current ? '#22d3ee15' : 'transparent',
                border: 'none',
                borderBottom:
                  idx < ENGINES.length - 1 ? '1px solid #1e293b' : 'none',
                color: engine.key === current ? '#22d3ee' : '#94a3b8',
                cursor: 'pointer',
                fontSize: '0.78rem',
                fontFamily: 'inherit',
                textAlign: 'left',
              }}
            >
              <span style={{ width: '1.4em', textAlign: 'center' }}>
                {engine.icon}
              </span>
              <span style={{ flex: 1 }}>{engine.label}</span>
              <span style={{ color: '#64748b', fontSize: '0.65rem' }}>
                {idx + 1}
              </span>
              {engine.key === current && (
                <span
                  style={{
                    fontSize: '0.6rem',
                    color: '#22d3ee',
                    background: '#22d3ee15',
                    padding: '0.1rem 0.4rem',
                    borderRadius: '3px',
                  }}
                >
                  active
                </span>
              )}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
