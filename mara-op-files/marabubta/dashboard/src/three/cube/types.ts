// Marabunta - Licensed under the MIT License.
// ── Inspection Cube Types ──
// Central type definitions for the Inspection Cube subsystem (W5C / Spec S21)

// === Geometry & Layout ===

export type FacePosition =
  | 'front'   // +Z
  | 'back'    // -Z
  | 'top'     // +Y
  | 'bottom'  // -Y
  | 'right'   // +X
  | 'left';   // -X

export type FaceType =
  | 'state'
  | 'actors'
  | 'audit'
  | 'context'
  | 'infra'
  | 'timers'
  | 'custom';

export type FaceMapping = Record<FacePosition, FaceType>;

export interface CubeFaceConfig {
  /** Which face position on the cube */
  position: FacePosition;
  /** What content type to render */
  type: FaceType;
  /** Display label shown at top of face */
  label: string;
  /** Accent color for this face (hex) */
  accentColor: string;
  /** Whether this face supports decomposition */
  decomposable: boolean;
  /** Custom renderer key (when type='custom') */
  customRenderer?: string;
}

export interface CubePreset {
  name: string;
  description: string;
  mapping: FaceMapping;
  faceConfigs: CubeFaceConfig[];
}

// === Face Data Types ===

export interface StateFaceData {
  currentState: string;
  stateColor: string;
  transitions: Array<{
    targetState: string;
    action: string;
    requiredRole?: string;
  }>;
  enteredAt: string;
  stateIndex: number;
  totalStates: number;
}

export interface ActorsFaceData {
  actors: Array<{
    name: string;
    role: string;
    permissions: string[];
    active: boolean;
  }>;
}

export interface AuditFaceData {
  totalEvents: number;
  witnessCount: number;
  recentEvents: Array<{
    timestamp: string;
    actor: string;
    action: string;
    verified: boolean;
  }>;
}

export interface ContextFaceData {
  entries: Array<{ key: string; value: string }>;
  totalKeys: number;
}

export interface InfraFaceData {
  nodeId: string;
  nodeTier: string;
  replicationFactor: number;
  replicaHealth: number;
  replicas: Array<{
    id: string;
    status: 'healthy' | 'syncing' | 'failed';
  }>;
  region: string;
}

export interface TimersFaceData {
  timers: Array<{
    label: string;
    deadline: string;
    totalDuration: number;
    remaining: number;
  }>;
}

// === Composite ===

export interface CubeData {
  state: StateFaceData;
  actors: ActorsFaceData;
  audit: AuditFaceData;
  context: ContextFaceData;
  infra: InfraFaceData;
  timers: TimersFaceData;
}

// === Interaction ===

export type RotationMode = 'snap' | 'free';

export interface CubeInteractionState {
  activeFace: FacePosition;
  rotationMode: RotationMode;
  isDragging: boolean;
  isDecomposed: boolean;
  decomposedFace: FacePosition | null;
}

// === Props ===

export interface FaceProps<T> {
  data: T;
  size: number;
  isActive?: boolean;
  onDecompose?: () => void;
}

export interface InspectionCubeProps {
  data: CubeData;
  faceMapping?: FaceMapping;
  preset?: CubePreset;
  size?: number;
  position?: [number, number, number];
  rotationMode?: RotationMode;
  onFaceClick?: (face: FacePosition) => void;
  onFaceRightClick?: (face: FacePosition) => void;
  onActiveFaceChange?: (face: FacePosition) => void;
}

// === Presets ===

export const OPERATIONAL_PRESET: CubePreset = {
  name: 'Operational',
  description: 'Standard workflow instance view',
  mapping: {
    front:  'state',
    top:    'actors',
    right:  'audit',
    back:   'context',
    left:   'infra',
    bottom: 'timers',
  },
  faceConfigs: [
    {
      position: 'front',   type: 'state',
      label: 'STATE',      accentColor: '#22d3ee',
      decomposable: true,
    },
    {
      position: 'top',     type: 'actors',
      label: 'ACTORS',     accentColor: '#a78bfa',
      decomposable: false,
    },
    {
      position: 'right',   type: 'audit',
      label: 'AUDIT',      accentColor: '#34d399',
      decomposable: true,
    },
    {
      position: 'back',    type: 'context',
      label: 'CONTEXT',    accentColor: '#fbbf24',
      decomposable: true,
    },
    {
      position: 'left',    type: 'infra',
      label: 'INFRA',      accentColor: '#60a5fa',
      decomposable: false,
    },
    {
      position: 'bottom',  type: 'timers',
      label: 'TIMERS',     accentColor: '#fb7185',
      decomposable: false,
    },
  ],
};

// === Face Position & Rotation Constants ===

/** Face offset positions (given half-size offset from cube center) */
export const FACE_POSITIONS: Record<FacePosition, (o: number) => [number, number, number]> = {
  front:  (o) => [0, 0,  o],
  back:   (o) => [0, 0, -o],
  top:    (o) => [0,  o, 0],
  bottom: (o) => [0, -o, 0],
  right:  (o) => [ o, 0, 0],
  left:   (o) => [-o, 0, 0],
};

/** Face rotations to orient the content plane correctly */
export const FACE_ROTATIONS: Record<FacePosition, [number, number, number]> = {
  front:  [0, 0, 0],
  back:   [0, Math.PI, 0],
  top:    [-Math.PI / 2, 0, 0],
  bottom: [ Math.PI / 2, 0, 0],
  right:  [0,  Math.PI / 2, 0],
  left:   [0, -Math.PI / 2, 0],
};

/** Material index to face position mapping (Three.js BoxGeometry order) */
export const MATERIAL_INDEX_MAP: Record<number, FacePosition> = {
  0: 'right',   // +X
  1: 'left',    // -X
  2: 'top',     // +Y
  3: 'bottom',  // -Y
  4: 'front',   // +Z
  5: 'back',    // -Z
};

// === Font Paths ===

export const FONT_MONO = '/fonts/IBMPlexMono-Regular.woff';
export const FONT_SANS = '/fonts/DMSans-Regular.woff';

// === Decomposition Animation Config ===

export interface DecomposeAnimation {
  expandDuration: number;
  collapseDuration: number;
  easing: string;
  inactiveFaceOpacity: number;
  cubeTranslateX: number;
  panelScale: [number, number, number];
}

export const DECOMPOSE_DEFAULTS: DecomposeAnimation = {
  expandDuration: 400,
  collapseDuration: 300,
  easing: 'easeOutCubic',
  inactiveFaceOpacity: 0.1,
  cubeTranslateX: -0.6,
  panelScale: [3, 2.5, 1],
};

// === Utility ===

export function truncate(str: string, maxLen: number): string {
  if (str.length <= maxLen) return str;
  return str.slice(0, maxLen - 1) + '\u2026';
}

export function formatDuration(seconds: number): string {
  if (seconds <= 0) return '00:00:00';
  const days = Math.floor(seconds / 86400);
  if (days > 0) {
    const hours = Math.floor((seconds % 86400) / 3600);
    return `${days}d ${hours}h`;
  }
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = Math.floor(seconds % 60);
  return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
}
