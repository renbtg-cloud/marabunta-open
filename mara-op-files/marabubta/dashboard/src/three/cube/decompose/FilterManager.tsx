// Marabunta - Licensed under the MIT License.
// ── FilterManager ──
// Applies global visual filter to the entire 3D scene. When a filter is
// active, non-matching cubes fade to 10% opacity and lose emissive glow.
// Matching cubes gain intensified glow. The filter persists until cleared
// via a "Clear Filter" HUD button rendered as an HTML overlay.

import React, { useEffect, useMemo, useCallback, useState } from 'react';
import { createPortal } from 'react-dom';
import * as THREE from 'three';
import type { FilterCriterion } from './DecompositionTree';

// ── Types ──

export interface FilterState {
  active: boolean;
  criterion: FilterCriterion | null;
  matchingCubeIds: Set<string>;
}

export function createEmptyFilter(): FilterState {
  return {
    active: false,
    criterion: null,
    matchingCubeIds: new Set(),
  };
}

// ── Cube Material Interface ──

export interface FilterableCube {
  userData: { cubeId: string };
  material: THREE.MeshStandardMaterial | THREE.Material;
}

// ── Hook: useFilterManager ──

export function useFilterManager() {
  const [filter, setFilter] = useState<FilterState>(createEmptyFilter);

  const applyFilter = useCallback(
    (criterion: FilterCriterion, matchingIds: string[]) => {
      setFilter({
        active: true,
        criterion,
        matchingCubeIds: new Set(matchingIds),
      });
    },
    [],
  );

  const clearFilter = useCallback(() => {
    setFilter(createEmptyFilter());
  }, []);

  return { filter, applyFilter, clearFilter };
}

// ── Filter Effect Applicator ──

interface FilterManagerProps {
  filter: FilterState;
  /** All cube meshes in the scene that should be affected */
  allCubes: FilterableCube[];
  /** Called when "Clear Filter" is clicked */
  onClear: () => void;
}

export function FilterManager({
  filter,
  allCubes,
  onClear,
}: FilterManagerProps) {
  // Apply opacity / emissive changes to cube materials
  useEffect(() => {
    allCubes.forEach((cube) => {
      const mat = cube.material;
      if (!(mat instanceof THREE.MeshStandardMaterial)) return;

      if (!filter.active) {
        // Restore defaults
        mat.opacity = 1.0;
        mat.transparent = false;
        mat.emissiveIntensity = 0.1;
        mat.needsUpdate = true;
        return;
      }

      const isMatch = filter.matchingCubeIds.has(cube.userData.cubeId);
      mat.transparent = true;

      if (isMatch) {
        mat.opacity = 1.0;
        mat.emissiveIntensity = 0.5; // glow
      } else {
        mat.opacity = 0.1; // fade
        mat.emissiveIntensity = 0.0;
      }
      mat.needsUpdate = true;
    });
  }, [filter, allCubes]);

  // Render the Clear Filter HUD button when filter is active
  if (!filter.active || !filter.criterion) return null;

  return <ClearFilterHUD criterion={filter.criterion} onClear={onClear} />;
}

// ── Clear Filter HUD Button ──

interface ClearFilterHUDProps {
  criterion: FilterCriterion;
  onClear: () => void;
}

function ClearFilterHUD({ criterion, onClear }: ClearFilterHUDProps) {
  return createPortal(
    <div
      style={{
        position: 'fixed',
        top: '80px',
        left: '50%',
        transform: 'translateX(-50%)',
        zIndex: 9999,
        display: 'flex',
        alignItems: 'center',
        gap: '12px',
        padding: '8px 16px',
        background: 'rgba(17, 24, 39, 0.95)',
        border: '1px solid #fbbf24',
        borderRadius: '8px',
        fontFamily: "'DM Sans', sans-serif",
        fontSize: '13px',
        color: '#e8ecf4',
        boxShadow: '0 4px 16px rgba(0,0,0,0.4)',
        backdropFilter: 'blur(8px)',
      }}
    >
      <span
        style={{
          display: 'inline-block',
          width: '8px',
          height: '8px',
          borderRadius: '50%',
          background: '#fbbf24',
          animation: 'pulse 2s ease-in-out infinite',
        }}
      />
      <span>
        Filter active:{' '}
        <span style={{ color: '#fbbf24', fontWeight: 600 }}>
          {criterion.sourceLabel}
        </span>
      </span>
      <span style={{ color: '#64748b' }}>
        ({criterion.field} {criterion.operator} {String(criterion.value)})
      </span>
      <button
        onClick={onClear}
        style={{
          padding: '4px 12px',
          border: '1px solid #334155',
          borderRadius: '4px',
          background: 'transparent',
          color: '#fb7185',
          cursor: 'pointer',
          fontFamily: "'DM Sans', sans-serif",
          fontSize: '12px',
        }}
        onMouseEnter={(e) => {
          (e.currentTarget as HTMLButtonElement).style.background =
            'rgba(251,113,133,0.1)';
        }}
        onMouseLeave={(e) => {
          (e.currentTarget as HTMLButtonElement).style.background =
            'transparent';
        }}
      >
        Clear Filter
      </button>
    </div>,
    document.body,
  );
}
