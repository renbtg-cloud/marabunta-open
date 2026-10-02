// Marabunta - Licensed under the MIT License.
// ── Inspection Cube — Barrel Export ──
// Re-exports all public API from the cube subsystem (W5C / Spec S21)

// Core component
export { InspectionCube } from './InspectionCube';

// Sub-components
export { CubeFace } from './CubeFace';
export { CubeRotation, getActiveFace } from './CubeRotation';
export { FaceContent } from './FaceContent';

// Face renderers
export { StateFace } from './faces/StateFace';
export { ActorsFace } from './faces/ActorsFace';
export { AuditFace } from './faces/AuditFace';
export { ContextFace } from './faces/ContextFace';
export { InfraFace } from './faces/InfraFace';
export { TimersFace } from './faces/TimersFace';

// Data provider hook
export { useCubeData } from './CubeDataProvider';
export type { WorkflowInstance, SwarmNodeEntity, SourceEntity } from './CubeDataProvider';

// Types
export type {
  FacePosition,
  FaceType,
  FaceMapping,
  CubeFaceConfig,
  CubePreset,
  StateFaceData,
  ActorsFaceData,
  AuditFaceData,
  ContextFaceData,
  InfraFaceData,
  TimersFaceData,
  CubeData,
  RotationMode,
  CubeInteractionState,
  FaceProps,
  InspectionCubeProps,
  DecomposeAnimation,
} from './types';

// Constants & presets
export {
  OPERATIONAL_PRESET,
  FACE_POSITIONS,
  FACE_ROTATIONS,
  MATERIAL_INDEX_MAP,
  FONT_MONO,
  FONT_SANS,
  DECOMPOSE_DEFAULTS,
  truncate,
  formatDuration,
} from './types';

// ── W6A Dimension Presets ──

// Preset types
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
} from './presets';

export {
  PresetName,
  FACE_SLOTS,
  PRESETS,
  PRESET_ORDER,
  CROSSFADE_DURATION,
} from './presets';

export type { FaceCrossfadeHandle } from './presets';

// Preset components
export {
  PresetManager,
  usePresetContext,
  shareViaCDE,
  PresetToolbar,
  MetricSidebar,
  METRIC_CATALOG,
  resolveRenderer,
  resolveAccent,
  CustomPresetEditor,
  useFaceCrossfade,
} from './presets';

// ── W6B Right-Click Decomposition ──

// Decompose types
export type {
  DecomposeOperation,
  AggregateType,
  DecomposedItem,
  DecomposeResult,
  DistributionBin,
  DistributionResult,
  CountResult,
  TrendPoint,
  CompareData,
  FilterCriterion,
  GroupByCluster,
  TreeNode,
  DecompositionTree,
  DecomposeAnyResult,
  OperationId,
  MenuOperation,
  ContextMenuState,
  UseContextMenuOptions,
  LayoutStrategy,
  SpawnedChild,
  CubeRef,
  GroupByState,
  FilterState,
  FilterableCube,
  PinnedView,
  ExportFormat,
  ExportRow,
} from './decompose';

// Decompose components & utilities
export {
  DecompositionTreeManager,
  getNodeLabel,
  extractValue,
  DecomposeError,
  decomposeSUM,
  decomposeAVG,
  decomposeCOUNT,
  fetchPopulationAverage,
  fetchTrend,
  fetchGroupBy,
  fetchAvailableDimensions,
  detectAggregateType,
  decomposeByType,
  OPERATIONS,
  useContextMenu,
  ContextMenuOverlay,
  ChildCubeSpawner,
  HistogramFace,
  computeChildLayout,
  gridLayout,
  cloudLayout,
  selectLayout,
  GroupByController,
  DimensionPicker,
  UngroupConfirmDialog,
  CompareOverlay,
  TrendChart,
  FilterManager,
  useFilterManager,
  createEmptyFilter,
  PinPanel,
  PinPanelContainer,
  usePinManager,
  exportDecomposition,
  flattenTreeToRows,
  rowsToCSV,
  CollapseAnimation,
  BreadcrumbTrail,
  CollapseBackButton,
} from './decompose';
