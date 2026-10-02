// Marabunta - Licensed under the MIT License.
// dashboard/src/three/index.ts
// Barrel export for the 3D visualization module.
// Only public-facing components and utilities are exported here.
// Internal modules (SceneSetup, WebSocketFeed, workers, shaders, hooks)
// remain module-private.

/** Root R3F Canvas component — lazy-load this in SwarmOverview */
export { SwarmCanvas } from './SwarmCanvas';

/** WebGL support detection — call before showing the 3D toggle */
export { detectWebGLSupport } from './SwarmCanvas';

/** Role color map — shared with 2D dashboard for consistency */
export { ROLE_COLORS } from './constants';

/** Data context types — for type-safe integration with parent components */
export type { NodeData, EdgeData, SwarmState } from './SwarmDataContext';
