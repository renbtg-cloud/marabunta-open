// Marabunta - Licensed under the MIT License.
// ── Presets — Barrel Export ──
// Re-exports all public API from the cube face presets subsystem (W6A / Spec S21)

// Types
export type {
  FaceSlot,
  FaceDimension,
  PresetDefinition,
  CustomPreset,
  PresetState,
  PresetEvent,
  MetricCategory,
  MetricEntry,
  MetricDragPayload,
} from './types';

export { PresetName, FACE_SLOTS } from './types';

// Preset definitions
export { PRESETS, PRESET_ORDER } from './PresetDefinitions';

// Crossfade animation hook
export { useFaceCrossfade, CROSSFADE_DURATION } from './FaceCrossfade';
export type { FaceCrossfadeHandle } from './FaceCrossfade';

// Manager & context
export { PresetManager, usePresetContext, shareViaCDE } from './PresetManager';

// UI components
export { PresetToolbar } from './PresetToolbar';
export { MetricSidebar, METRIC_CATALOG, resolveRenderer, resolveAccent } from './MetricSidebar';
export { CustomPresetEditor } from './CustomPresetEditor';
