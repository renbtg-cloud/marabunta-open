// Marabunta - Licensed under the MIT License.
// dashboard/src/three/constants.ts
// Central configuration for all 3D visualization constants.
// Used by W5A (setup), W5B (observatory), W5C (inspection cube), W5D (unfold).

import * as THREE from 'three';

// ──────── Canvas ────────
/** R3F Canvas renderer configuration */
export const CANVAS_CONFIG = {
  /** Vertical field of view in degrees */
  FOV: 60,
  /** Near clipping plane distance */
  NEAR: 0.1,
  /** Far clipping plane distance */
  FAR: 500,
  /** Initial camera position [x, y, z] */
  INITIAL_POSITION: [0, 20, 60] as [number, number, number],
  /** Background clear color (matches marabunta-compute dark theme) */
  BACKGROUND_COLOR: '#0a0e17',
} as const;

// ──────── Scene ────────
/** Lighting and environment configuration */
export const SCENE_CONFIG = {
  /** Camera start position (same as CANVAS_CONFIG for consistency) */
  CAMERA_START: [0, 20, 60] as [number, number, number],
  /** Ambient light intensity — ensures no node is fully dark */
  AMBIENT_INTENSITY: 0.4,
  /** Ambient light color */
  AMBIENT_COLOR: '#e8ecf4',
  /** Key (directional) light position */
  KEY_LIGHT_POS: [10, 15, 10] as [number, number, number],
  /** Key light intensity */
  KEY_LIGHT_INTENSITY: 0.8,
  /** Key light color */
  KEY_LIGHT_COLOR: '#ffffff',
  /** Accent point light intensity — subtle cyan glow at origin */
  ACCENT_INTENSITY: 0.3,
  /** Accent point light color */
  ACCENT_COLOR: '#22d3ee',
  /** Fog color — matches background for seamless depth fade */
  FOG_COLOR: '#0a0e17',
} as const;

// ──────── Bloom ────────
/** Post-processing bloom configuration */
export const BLOOM_CONFIG = {
  /** Luminance threshold — pixels brighter than this get bloom */
  THRESHOLD: 0.6,
  /** Luminance smoothing — softer edge on bloom cutoff */
  SMOOTHING: 0.4,
  /** Bloom intensity multiplier */
  INTENSITY: 0.5,
} as const;

// ──────── Role Colors ────────
/** Each role maps to a distinct hue for instant visual identification */
export const ROLE_COLORS: Record<string, string> = {
  AGGREGATOR: '#22d3ee',  // Cyan   — decision makers float high
  WITNESS:    '#34d399',  // Green  — verification layer
  GATEWAY:    '#a78bfa',  // Violet — external-facing
  RELAY:      '#fbbf24',  // Amber  — message forwarding
  STORAGE:    '#60a5fa',  // Blue   — data persistence
  EPHEMERAL:  '#94a3b8',  // Grey   — temporary workers
  UNKNOWN:    '#64748b',  // Muted  — fallback
};

/** THREE.Color instances cached to avoid per-frame allocation */
export const ROLE_COLOR3: Record<string, THREE.Color> = Object.fromEntries(
  Object.entries(ROLE_COLORS).map(([role, hex]) => [role, new THREE.Color(hex)])
);

// ──────── Health Colors ────────
/** Node health state to color mapping */
export const HEALTH_COLORS: Record<string, string> = {
  alive:   '#34d399',  // Green
  suspect: '#fbbf24',  // Amber
  dead:    '#fb7185',  // Rose
};

// ──────── Node Sizing ────────
/** Node sphere radius parameters. Radius = BASE + (cpu + ram) / 2 * SCALE */
export const NODE_SIZE = {
  /** Base radius for a zero-load node */
  BASE_RADIUS: 0.5,
  /** Scale factor applied to (cpu + ram) / 2 */
  SCALE_FACTOR: 1.0,
  /** Minimum radius clamp */
  MIN_RADIUS: 0.3,
  /** Maximum radius clamp */
  MAX_RADIUS: 2.0,
} as const;

/**
 * Compute the visual radius of a node sphere based on CPU and RAM utilization.
 * @param cpu CPU utilization 0.0 - 1.0
 * @param ram RAM utilization 0.0 - 1.0
 * @returns Clamped radius in world units
 */
export function computeNodeRadius(cpu: number, ram: number): number {
  const raw = NODE_SIZE.BASE_RADIUS + ((cpu + ram) / 2) * NODE_SIZE.SCALE_FACTOR;
  return Math.max(NODE_SIZE.MIN_RADIUS, Math.min(NODE_SIZE.MAX_RADIUS, raw));
}

// ──────── Edge Styling ────────
/** Gossip edge visual parameters */
export const EDGE_STYLE = {
  /** Base opacity for low-weight edges */
  BASE_OPACITY: 0.15,
  /** Maximum opacity for high-weight edges */
  MAX_OPACITY: 0.6,
  /** Base line width in world units */
  BASE_WIDTH: 0.02,
  /** Maximum line width in world units */
  MAX_WIDTH: 0.08,
  /** Edge color (cyan to match theme) */
  COLOR: '#22d3ee',
} as const;

// ──────── Pulse Animation ────────
/** Health state to pulse frequency (Hz) mapping for the glow shader */
export const PULSE_RATES: Record<string, number> = {
  alive:   0.5,   // Slow, calm breathing
  suspect: 2.0,   // Rapid warning pulse
  dead:    0.0,   // No pulse (static dim)
};

// ──────── Instancing Limits ────────
/** GPU instanced mesh allocation parameters */
export const INSTANCE_LIMITS = {
  /** Initial node instance slots */
  MAX_NODES: 2048,
  /** Initial edge instance slots (MAX_NODES * 4) */
  MAX_EDGES: 8192,
  /** Capacity growth factor on overflow */
  GROWTH_FACTOR: 2,
} as const;

// ──────── Layout ────────
/** d3-force-3d simulation parameters for the Web Worker layout engine */
export const LAYOUT_CONFIG = {
  /** Charge (repulsion) strength — negative = repulsive */
  CHARGE_STRENGTH: -80,
  /** Maximum distance for charge computation — limits O(n^2) */
  CHARGE_DISTANCE_MAX: 100,
  /** Ideal link (edge) distance in world units */
  LINK_DISTANCE: 8,
  /** Link strength scale applied to edge weight */
  LINK_STRENGTH_SCALE: 0.3,
  /** Center force strength — gentle pull toward origin */
  CENTER_STRENGTH: 0.05,
  /** Minimum separation between node centers */
  COLLISION_RADIUS: 1.5,
  /** Collision detection iterations per tick */
  COLLISION_ITERATIONS: 2,
  /** Role-based Y-layer gravity strength */
  ROLE_Y_STRENGTH: 0.02,
  /** Alpha decay rate — controls how quickly the simulation cools */
  ALPHA_DECAY: 0.02,
  /** Velocity decay — friction coefficient */
  VELOCITY_DECAY: 0.3,
  /** Initial warmup ticks for fast convergence */
  WARMUP_TICKS: 100,
  /** Simulation ticks per animation frame request */
  TICKS_PER_FRAME: 3,
} as const;

// ──────── Multi-Role Blending ────────
/**
 * Compute a blended color for nodes with multiple roles.
 * Uses additive RGB blend divided by role count.
 * @param roles Array of role names (e.g. ['AGGREGATOR', 'WITNESS'])
 * @returns Blended THREE.Color (cloned, safe to mutate)
 */
export function blendRoleColors(roles: string[]): THREE.Color {
  if (roles.length === 0) return ROLE_COLOR3['UNKNOWN'].clone();
  if (roles.length === 1) return (ROLE_COLOR3[roles[0]] ?? ROLE_COLOR3['UNKNOWN']).clone();
  const blended = new THREE.Color(0, 0, 0);
  for (const role of roles) {
    blended.add(ROLE_COLOR3[role] ?? ROLE_COLOR3['UNKNOWN']);
  }
  blended.multiplyScalar(1 / roles.length);
  return blended;
}
