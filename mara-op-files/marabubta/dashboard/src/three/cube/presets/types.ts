// Marabunta - Licensed under the MIT License.
// ── Preset Types ──
// Type definitions for the Cube Face Mapping & Dimension Presets system (W6A / Spec S21)

/** The six physical faces of an Inspection Cube */
export type FaceSlot =
  | 'front'
  | 'top'
  | 'right'
  | 'back'
  | 'left'
  | 'bottom';

/** All face slots as an ordered array for iteration */
export const FACE_SLOTS: readonly FaceSlot[] = [
  'front', 'top', 'right', 'back', 'left', 'bottom',
] as const;

/** Named dimension presets from the spec */
export enum PresetName {
  Operational = 'operational',
  Risk        = 'risk',
  Performance = 'performance',
  Financial   = 'financial',
  Audit       = 'audit',
  HedgeFund   = 'hedge-fund',
  Custom      = 'custom',
}

/** A single face dimension: what metric to render on one face */
export interface FaceDimension {
  /** Unique metric identifier (e.g., 'state-transitions') */
  readonly metricId: string;
  /** Human-readable label for the face */
  readonly label: string;
  /** Short description shown on hover */
  readonly description: string;
  /** Renderer component key (maps to a face renderer) */
  readonly renderer: string;
  /** Color accent for the face border glow */
  readonly accent: string;
  /** Optional unit of measurement */
  readonly unit?: string;
}

/** Complete preset definition: six face dimensions */
export interface PresetDefinition {
  readonly name: PresetName;
  readonly label: string;
  readonly description: string;
  readonly icon: string;
  readonly shortcut: string; // keyboard shortcut '1' through '7'
  readonly faces: Readonly<Record<FaceSlot, FaceDimension>>;
}

/** User-created custom preset */
export interface CustomPreset {
  readonly id: string;
  readonly name: string;
  readonly createdBy: string;
  readonly createdAt: number; // epoch ms
  readonly updatedAt: number;
  readonly faces: Record<FaceSlot, FaceDimension | null>;
  readonly shared: boolean; // sync via Tier 3 gossip
}

/** State shape for the preset manager */
export interface PresetState {
  readonly activePreset: PresetName;
  readonly previousPreset: PresetName | null;
  readonly isTransitioning: boolean;
  readonly transitionProgress: number; // 0.0 to 1.0
  readonly customPresets: readonly CustomPreset[];
  readonly activeCustomId: string | null;
}

/** Events dispatched during preset changes */
export type PresetEvent =
  | { type: 'PRESET_SWITCH'; from: PresetName; to: PresetName }
  | { type: 'CROSSFADE_START'; duration: number }
  | { type: 'CROSSFADE_COMPLETE' }
  | { type: 'CUSTOM_PRESET_SAVED'; preset: CustomPreset }
  | { type: 'CUSTOM_PRESET_DELETED'; id: string }
  | { type: 'CUSTOM_PRESET_SHARED'; id: string };

// === Metric Catalog Types ===

/** Metric catalog category */
export interface MetricCategory {
  readonly id: string;
  readonly label: string;
  readonly icon: string;
  readonly metrics: readonly MetricEntry[];
}

/** Single available metric for face assignment */
export interface MetricEntry {
  readonly metricId: string;
  readonly label: string;
  readonly description: string;
  readonly renderer: string;
  readonly defaultAccent: string;
  readonly category: string;
  readonly unit?: string;
}

/** Drag-and-drop payload when dragging a metric to a face */
export interface MetricDragPayload {
  readonly metricId: string;
  readonly sourceCategory: string;
  readonly label: string;
}
