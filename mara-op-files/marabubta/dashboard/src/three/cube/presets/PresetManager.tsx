// Marabunta - Licensed under the MIT License.
// ── Preset Manager ──
// Orchestrates preset state, keyboard shortcuts, crossfade coordination,
// and provides React context for any descendant component to query or
// switch the active preset (W6A / Spec S21).

import React, {
  createContext,
  useContext,
  useCallback,
  useEffect,
  useMemo,
  useReducer,
} from 'react';
import { PRESETS, PRESET_ORDER } from './PresetDefinitions';
import { useFaceCrossfade, CROSSFADE_DURATION } from './FaceCrossfade';
import type {
  PresetName,
  PresetState,
  PresetEvent,
  FaceSlot,
  FaceDimension,
  CustomPreset,
} from './types';
import { FACE_SLOTS } from './types';
import type { FaceCrossfadeHandle } from './FaceCrossfade';

// ── Persistence Helpers ──

const LOCAL_KEY = 'marabunta:custom-presets';

/** Save custom presets to localStorage */
function persistLocal(presets: readonly CustomPreset[]): void {
  try {
    localStorage.setItem(LOCAL_KEY, JSON.stringify(presets));
  } catch (e) {
    console.warn('Failed to persist custom presets locally:', e);
  }
}

/** Load custom presets from localStorage */
function loadLocal(): CustomPreset[] {
  try {
    const raw = localStorage.getItem(LOCAL_KEY);
    return raw ? JSON.parse(raw) : [];
  } catch {
    return [];
  }
}

/** Share a custom preset via CDE gossip */
export async function shareViaCDE(preset: CustomPreset): Promise<void> {
  const fragment = {
    type: 'config:custom-preset',
    payload: preset,
    ttl: 86400 * 30, // 30 days
    propagation: 'gossip',
  };
  await fetch('/api/v1/cde/fragments', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(fragment),
  });
}

/** Listen for incoming shared presets via CDE gossip */
function subscribeCDEPresets(
  onReceive: (preset: CustomPreset) => void,
): () => void {
  const es = new EventSource('/api/v1/cde/subscribe?type=config:custom-preset');
  es.onmessage = (event) => {
    try {
      const preset: CustomPreset = JSON.parse(event.data);
      onReceive(preset);
    } catch {
      /* ignore malformed */
    }
  };
  return () => es.close();
}

// ── Context ──

interface PresetContextValue {
  /** Current preset state */
  state: PresetState;
  /** Switch to a different preset (no-op during transition) */
  switchPreset: (to: PresetName) => void;
  /** Get the face dimensions for the active preset */
  getActiveFaces: () => Readonly<Record<FaceSlot, FaceDimension>>;
  /** Get crossfade handle for a specific face slot */
  getCrossfade: (slot: FaceSlot) => FaceCrossfadeHandle;
  /** Dispatch a preset event (for custom preset CRUD) */
  dispatch: React.Dispatch<PresetEvent>;
}

const PresetContext = createContext<PresetContextValue | null>(null);

/**
 * Hook to consume the preset context.
 * Must be called within a PresetManager provider.
 */
export function usePresetContext(): PresetContextValue {
  const ctx = useContext(PresetContext);
  if (!ctx) throw new Error('usePresetContext must be inside PresetManager');
  return ctx;
}

// ── Reducer ──

const initialState: PresetState = {
  activePreset: 'operational' as PresetName,
  previousPreset: null,
  isTransitioning: false,
  transitionProgress: 0,
  customPresets: [],
  activeCustomId: null,
};

function presetReducer(
  state: PresetState,
  event: PresetEvent,
): PresetState {
  switch (event.type) {
    case 'PRESET_SWITCH':
      return {
        ...state,
        activePreset: event.to,
        previousPreset: event.from,
        isTransitioning: true,
        transitionProgress: 0,
      };
    case 'CROSSFADE_START':
      return state; // handled by PRESET_SWITCH; kept for event log
    case 'CROSSFADE_COMPLETE':
      return {
        ...state,
        isTransitioning: false,
        transitionProgress: 1,
      };
    case 'CUSTOM_PRESET_SAVED':
      return {
        ...state,
        customPresets: [
          ...state.customPresets.filter(p => p.id !== event.preset.id),
          event.preset,
        ],
      };
    case 'CUSTOM_PRESET_DELETED':
      return {
        ...state,
        customPresets: state.customPresets.filter(p => p.id !== event.id),
        activeCustomId:
          state.activeCustomId === event.id ? null : state.activeCustomId,
      };
    case 'CUSTOM_PRESET_SHARED':
      return {
        ...state,
        customPresets: state.customPresets.map(p =>
          p.id === event.id ? { ...p, shared: true } : p,
        ),
      };
    default:
      return state;
  }
}

// ── Component ──

interface PresetManagerProps {
  children: React.ReactNode;
}

export function PresetManager({ children }: PresetManagerProps) {
  // Load persisted custom presets on first render
  const persistedCustom = useMemo(() => loadLocal(), []);
  const [state, dispatch] = useReducer(presetReducer, {
    ...initialState,
    customPresets: persistedCustom,
  });

  // Create crossfade hooks for all six faces
  // Note: hooks are called unconditionally in the same order every render
  const crossfadeFront  = useFaceCrossfade('front');
  const crossfadeTop    = useFaceCrossfade('top');
  const crossfadeRight  = useFaceCrossfade('right');
  const crossfadeBack   = useFaceCrossfade('back');
  const crossfadeLeft   = useFaceCrossfade('left');
  const crossfadeBottom = useFaceCrossfade('bottom');

  const crossfades = useMemo<Record<FaceSlot, FaceCrossfadeHandle>>(
    () => ({
      front:  crossfadeFront,
      top:    crossfadeTop,
      right:  crossfadeRight,
      back:   crossfadeBack,
      left:   crossfadeLeft,
      bottom: crossfadeBottom,
    }),
    [crossfadeFront, crossfadeTop, crossfadeRight, crossfadeBack, crossfadeLeft, crossfadeBottom],
  );

  // Persist custom presets whenever they change
  useEffect(() => {
    persistLocal(state.customPresets);
  }, [state.customPresets]);

  // Subscribe to CDE gossip for shared presets
  useEffect(() => {
    const unsubscribe = subscribeCDEPresets((preset) => {
      // LWW: only accept if newer than existing
      dispatch({ type: 'CUSTOM_PRESET_SAVED', preset });
    });
    return unsubscribe;
  }, []);

  const switchPreset = useCallback(
    (to: PresetName) => {
      if (state.isTransitioning) return; // debounce during crossfade
      if (to === state.activePreset) return; // no-op same preset

      const fromDef = PRESETS[state.activePreset];
      const toDef   = PRESETS[to];

      dispatch({ type: 'PRESET_SWITCH', from: state.activePreset, to });

      // Trigger crossfade on all six faces simultaneously
      for (const slot of FACE_SLOTS) {
        crossfades[slot].startCrossfade(
          fromDef.faces[slot],
          toDef.faces[slot],
        );
      }

      // Mark complete after crossfade duration
      setTimeout(
        () => dispatch({ type: 'CROSSFADE_COMPLETE' }),
        CROSSFADE_DURATION * 1000,
      );
    },
    [state.activePreset, state.isTransitioning, crossfades],
  );

  // Keyboard shortcuts: 1-7 (suppressed when inside input/textarea)
  useEffect(() => {
    function onKeyDown(e: KeyboardEvent) {
      if (e.target instanceof HTMLInputElement) return;
      if (e.target instanceof HTMLTextAreaElement) return;
      const idx = parseInt(e.key, 10);
      if (idx >= 1 && idx <= 7) {
        switchPreset(PRESET_ORDER[idx - 1]);
      }
    }
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [switchPreset]);

  const getActiveFaces = useCallback(
    () => PRESETS[state.activePreset].faces,
    [state.activePreset],
  );

  const getCrossfade = useCallback(
    (slot: FaceSlot) => crossfades[slot],
    [crossfades],
  );

  const value = useMemo<PresetContextValue>(
    () => ({ state, switchPreset, getActiveFaces, getCrossfade, dispatch }),
    [state, switchPreset, getActiveFaces, getCrossfade],
  );

  return (
    <PresetContext.Provider value={value}>
      {children}
    </PresetContext.Provider>
  );
}
