// Marabunta - Licensed under the MIT License.
// ── Observatory barrel exports ──
// W5B: Swarm Observatory (Spec S21 - Mission Control)

export { ObservatoryScene } from './ObservatoryScene';
export { NodeSphere, computePulse, computeGlow, getRoleColor } from './NodeSphere';
export { NodeCloud } from './NodeCloud';
export { RoleCorona } from './RoleCorona';
export { GossipEdges } from './GossipEdges';
export { ReplicationArcs } from './ReplicationArcs';
export { DatabaseGeometry } from './DatabaseGeometry';
export { DeadNodeGhost } from './DeadNodeGhost';
export { PartitionMembrane } from './PartitionMembrane';
export { NodeTooltip } from './NodeTooltip';

export type {
  SwarmNode,
  GossipEdge,
  ReplicationLink,
  DatabaseObject,
  DeadNodeInfo,
  PartitionInfo,
  LayoutEngine,
  NodeRole,
  MessageType,
  DatabaseType,
} from './types';

export {
  ROLE_COLORS,
  ROLE_PRIORITY,
  MESSAGE_PARTICLE_COLORS,
  DATABASE_COLORS,
  computeRadius,
  getPrimaryRole,
  formatUptime,
} from './types';
