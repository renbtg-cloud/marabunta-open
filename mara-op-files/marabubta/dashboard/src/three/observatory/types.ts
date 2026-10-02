// Marabunta - Licensed under the MIT License.
// ── Observatory Types ──
// Shared types and constants for the Swarm Observatory (W5B / Spec S21)

import type * as THREE from 'three';

// ── Role System ──

export type NodeRole =
  | 'AGGREGATOR'
  | 'WITNESS'
  | 'GATEWAY'
  | 'RELAY'
  | 'STORAGE'
  | 'EPHEMERAL';

export const ROLE_COLORS: Record<NodeRole, string> = {
  AGGREGATOR: '#22d3ee',
  WITNESS: '#34d399',
  GATEWAY: '#a78bfa',
  RELAY: '#fbbf24',
  STORAGE: '#60a5fa',
  EPHEMERAL: '#6b7280',
};

/** Priority order: first match wins the sphere color. */
export const ROLE_PRIORITY: NodeRole[] = [
  'AGGREGATOR',
  'GATEWAY',
  'WITNESS',
  'STORAGE',
  'RELAY',
  'EPHEMERAL',
];

// ── Message / Particle Types ──

export type MessageType = 'health' | 'storage' | 'audit' | 'deploy';

export const MESSAGE_PARTICLE_COLORS: Record<MessageType, string> = {
  health: '#ffffff',
  storage: '#22d3ee',
  audit: '#a78bfa',
  deploy: '#fbbf24',
};

// ── Database Types ──

export type DatabaseType = 'sql' | 'document' | 'cache' | 'streaming';

export const DATABASE_COLORS: Record<DatabaseType, string> = {
  sql: '#60a5fa',
  document: '#34d399',
  cache: '#fb7185',
  streaming: '#fbbf24',
};

// ── Data Interfaces ──

export interface SwarmNode {
  id: string;
  roles: NodeRole[];
  primaryRole: NodeRole;
  cpuCores: number;
  ramGb: number;
  diskGb: number;
  cpuUsage: number;
  ramUsedGb: number;
  diskUsedGb: number;
  loadPercent: number;
  uptimeSeconds: number;
  gossipPeerCount: number;
  reputationScore: number;
  status: 'alive' | 'suspect' | 'dead';
  isTraining: boolean;
  isPgWireActive: boolean;
  chaosState: {
    is_active: boolean;
    current_death?: any;
  };
}

export interface GossipEdge {
  sourceId: string;
  targetId: string;
  trafficVolume: number;
  messageTypes: MessageType[];
}

export interface ReplicationLink {
  sourceId: string;
  targetId: string;
  replicationFactor: number;
  lagMs: number;
  healthy: boolean;
}

export interface DatabaseObject {
  id: string;
  type: DatabaseType;
  name: string;
  nodeIds: string[];
  position?: THREE.Vector3;
}

export interface DeadNodeInfo extends SwarmNode {
  deathTimestamp: number;
  rerouteTargets: Array<{
    fromPos: THREE.Vector3;
    oldTarget: THREE.Vector3;
    newTarget: THREE.Vector3;
  }>;
}

export interface PartitionInfo {
  components: string[][];
  boundaries: Array<{
    nodeA: string;
    nodeB: string;
  }>;
  detectedAt: number;
}

// ── Layout Engine Interface ──

export interface LayoutEngine {
  getPosition(nodeId: string): THREE.Vector3;
}

// ── Size Mapping ──

const MIN_RADIUS = 0.3;
const MAX_RADIUS = 1.5;
const CAPACITY_CEIL = 64; // sqrt(32 cores * 128GB) ~ 64

export function computeRadius(node: SwarmNode): number {
  const capacityScore = Math.sqrt(node.cpuCores * node.ramGb);
  const normalized = Math.min(capacityScore / CAPACITY_CEIL, 1.0);
  return MIN_RADIUS + normalized * (MAX_RADIUS - MIN_RADIUS);
}

export function getPrimaryRole(roles: NodeRole[]): NodeRole {
  for (const priority of ROLE_PRIORITY) {
    if (roles.includes(priority)) return priority;
  }
  return 'EPHEMERAL';
}

export function formatUptime(seconds: number): string {
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  if (days > 0) return `${days}d ${hours}h`;
  const minutes = Math.floor((seconds % 3600) / 60);
  if (hours > 0) return `${hours}h ${minutes}m`;
  return `${minutes}m`;
}
