// Marabunta - Licensed under the MIT License.
// ── Decompose — Barrel Export ──
// Right-click decomposition subsystem (W6B / Spec S21).
// Re-exports all public API from the decompose directory.

// DecompositionTree — state manager & types
export {
  DecompositionTreeManager,
  getNodeLabel,
  extractValue,
} from './DecompositionTree';
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
} from './DecompositionTree';

// DecomposeEngine — API client
export {
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
} from './DecomposeEngine';
export type { DecomposeAnyResult } from './DecomposeEngine';

// ContextMenu — right-click handler & overlay
export {
  OPERATIONS,
  useContextMenu,
  ContextMenuOverlay,
} from './ContextMenu';
export type {
  OperationId,
  MenuOperation,
  ContextMenuState,
  UseContextMenuOptions,
} from './ContextMenu';

// ChildCubeSpawner — visual decomposition
export {
  ChildCubeSpawner,
  HistogramFace,
  computeChildLayout,
  gridLayout,
  cloudLayout,
  selectLayout,
} from './ChildCubeSpawner';
export type {
  LayoutStrategy,
  SpawnedChild,
} from './ChildCubeSpawner';

// GroupByController — re-aggregation by dimension
export {
  GroupByController,
  DimensionPicker,
  UngroupConfirmDialog,
} from './GroupByController';
export type {
  CubeRef,
  GroupByState,
} from './GroupByController';

// CompareOverlay — ghost cube comparison
export { CompareOverlay } from './CompareOverlay';

// TrendChart — sparkline on face
export { TrendChart } from './TrendChart';

// FilterManager — global scene filter
export {
  FilterManager,
  useFilterManager,
  createEmptyFilter,
} from './FilterManager';
export type {
  FilterState,
  FilterableCube,
} from './FilterManager';

// PinPanel — floating decomposed view panels
export {
  PinPanel,
  PinPanelContainer,
  usePinManager,
} from './PinPanel';
export type { PinnedView } from './PinPanel';

// ExportManager — CSV / JSON / Parquet export
export {
  exportDecomposition,
  flattenTreeToRows,
  rowsToCSV,
} from './ExportManager';
export type {
  ExportFormat,
  ExportRow,
} from './ExportManager';

// CollapseBack — reverse animation & breadcrumbs
export {
  CollapseAnimation,
  BreadcrumbTrail,
  CollapseBackButton,
} from './CollapseBack';
