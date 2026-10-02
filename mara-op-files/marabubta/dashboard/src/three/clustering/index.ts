// Marabunta - Licensed under the MIT License.
// ── Clustering & Cross-Connections — Barrel Export ──
// W6C: Cube Clustering, Mega-Cubes, Cross-Cube Arcs, Layout Engines (Spec S21)
// Provides spatial aggregation and layout subsystem for the observatory.

// ── Cluster Engine ──
export { ClusterEngine } from './ClusterEngine';
export type {
  CubeEntry,
  FaceData,
  Cluster,
  AggregateFaceData,
  AggregationType,
} from './ClusterEngine';

// ── Cluster Visual ──
export { ClusterVisual } from './ClusterVisual';
export type { ClusterVisualProps } from './ClusterVisual';

// ── Mega-Cube ──
export { MegaCube } from './MegaCube';
export type { MegaCubeProps } from './MegaCube';

// ── Fly-To-Cluster Animation ──
export {
  FlyToCluster,
  computeStaggerDelays,
  computeFlightDuration,
  easeOutCubic,
  easeInQuad,
  easeInOutCubic,
  easeOutBack,
} from './FlyToCluster';
export type { FlyToClusterProps, EasingFn } from './FlyToCluster';

// ── Cross-Cube Arcs ──
export { CrossCubeArcs } from './CrossCubeArcs';
export type {
  ArcDefinition,
  ArcRelationship,
  CrossCubeArcsProps,
} from './CrossCubeArcs';

// ── Layout Switcher ──
export { LayoutSwitcher } from './LayoutSwitcher';
export type { LayoutSwitcherProps } from './LayoutSwitcher';

// ── Layout Engines ──
export type {
  LayoutEngine,
  LayoutNode,
  LayoutResult,
  LayoutKey,
} from './layouts/LayoutEngine';
export { buildEdges } from './layouts/LayoutEngine';

export { ForceLayout } from './layouts/ForceLayout';
export { GeographicLayout } from './layouts/GeographicLayout';
export { HierarchicalLayout } from './layouts/HierarchicalLayout';
export { SpectralLayout } from './layouts/SpectralLayout';
export { GridLayout } from './layouts/GridLayout';
export { CustomLayout } from './layouts/CustomLayout';
