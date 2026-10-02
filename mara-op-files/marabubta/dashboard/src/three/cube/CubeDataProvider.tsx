// Marabunta - Licensed under the MIT License.
// ── CubeDataProvider ──
// Bridges domain data (from REST API responses or WebSocket updates) to
// the typed face data structures. Accepts either a WorkflowInstance or a
// SwarmNode and produces a unified CubeData object that the InspectionCube
// component consumes.

import { useMemo } from 'react';
import { formatDuration } from './types';
import type {
  CubeData,
  StateFaceData,
  ActorsFaceData,
  AuditFaceData,
  ContextFaceData,
  InfraFaceData,
  TimersFaceData,
} from './types';

// ── Domain Types ──
// These represent the shapes we expect from the REST API / context layer.
// In a production codebase these would come from a shared API types module.

export interface WorkflowInstance {
  currentState: string;
  availableTransitions: Array<{
    targetState: string;
    action: string;
    requiredRole?: string;
  }>;
  stateEnteredAt: string;
  stateIndex: number;
  definition: {
    states: Array<{ name: string }>;
  };
  assignedActors: Array<{
    displayName: string;
    role: string;
    permissions: string[];
    lastSeen: number;
  }>;
  auditTrail: Array<{
    timestamp: string;
    actorName: string;
    action: string;
    signatureValid: boolean;
  }>;
  witnessSignatures: Array<{ nodeId: string; signature: string }>;
  contextData: Record<string, unknown>;
  assignedNodeId: string;
  nodeTier: string;
  replicationFactor: number;
  replicaHealthRatio: number;
  replicas: Array<{
    id: string;
    status: 'healthy' | 'syncing' | 'failed';
  }>;
  region: string;
  activeTimers: Array<{
    label: string;
    deadline: string;
    totalDuration: number;
  }>;
}

export interface SwarmNodeEntity {
  id: string;
  status: string;
  statusSince: string;
  tier: string;
  replicationFactor: number;
  replicaHealth: number;
  replicas: Array<{
    id: string;
    status: 'healthy' | 'syncing' | 'failed';
  }>;
  region: string;
  cpuPercent: number;
  memoryMb: number;
  diskGb: number;
  uptimeSeconds: number;
  assignedWorkloads: Array<{
    name: string;
    running: boolean;
  }>;
  eventLog: Array<{
    timestamp: string;
    description: string;
  }>;
  activeTimers?: Array<{
    label: string;
    deadline: string;
    totalDuration: number;
    remaining: number;
  }>;
}

// ── Source Entity Discriminated Union ──

export type SourceEntity =
  | { kind: 'workflow'; instance: WorkflowInstance }
  | { kind: 'node'; node: SwarmNodeEntity };

// ── State Color Mapping ──

function stateToColor(state: string): string {
  const lower = state.toLowerCase();
  if (lower.includes('error') || lower.includes('fail') || lower.includes('reject')) {
    return '#fb7185';
  }
  if (lower.includes('wait') || lower.includes('pending') || lower.includes('pause')) {
    return '#fbbf24';
  }
  if (lower.includes('complete') || lower.includes('done') || lower.includes('approved')) {
    return '#34d399';
  }
  // Default: active/in-progress
  return '#22d3ee';
}

// ── Mapping Functions ──

function mapWorkflowToCube(wf: WorkflowInstance): CubeData {
  const state: StateFaceData = {
    currentState: wf.currentState,
    stateColor: stateToColor(wf.currentState),
    transitions: wf.availableTransitions,
    enteredAt: wf.stateEnteredAt,
    stateIndex: wf.stateIndex,
    totalStates: wf.definition.states.length,
  };

  const actors: ActorsFaceData = {
    actors: wf.assignedActors.map((a) => ({
      name: a.displayName,
      role: a.role,
      permissions: a.permissions,
      active: a.lastSeen > Date.now() - 300_000,
    })),
  };

  const audit: AuditFaceData = {
    totalEvents: wf.auditTrail.length,
    witnessCount: wf.witnessSignatures.length,
    recentEvents: wf.auditTrail
      .slice(-4)
      .reverse()
      .map((e) => ({
        timestamp: e.timestamp,
        actor: e.actorName,
        action: e.action,
        verified: e.signatureValid,
      })),
  };

  const context: ContextFaceData = {
    entries: Object.entries(wf.contextData).map(([k, v]) => ({
      key: k,
      value: String(v),
    })),
    totalKeys: Object.keys(wf.contextData).length,
  };

  const infra: InfraFaceData = {
    nodeId: wf.assignedNodeId,
    nodeTier: wf.nodeTier,
    replicationFactor: wf.replicationFactor,
    replicaHealth: wf.replicaHealthRatio,
    replicas: wf.replicas.map((r) => ({
      id: r.id,
      status: r.status,
    })),
    region: wf.region,
  };

  const timers: TimersFaceData = {
    timers: wf.activeTimers.map((t) => ({
      label: t.label,
      deadline: t.deadline,
      totalDuration: t.totalDuration,
      remaining: Math.max(
        0,
        (new Date(t.deadline).getTime() - Date.now()) / 1000,
      ),
    })),
  };

  return { state, actors, audit, context, infra, timers };
}

function mapNodeToCube(node: SwarmNodeEntity): CubeData {
  const state: StateFaceData = {
    currentState: node.status,
    stateColor: node.status === 'healthy' ? '#34d399' : '#fb7185',
    transitions: [],
    enteredAt: node.statusSince,
    stateIndex: 1,
    totalStates: 1,
  };

  const actors: ActorsFaceData = {
    actors: node.assignedWorkloads.map((w) => ({
      name: w.name,
      role: 'WORKLOAD',
      permissions: [],
      active: w.running,
    })),
  };

  const audit: AuditFaceData = {
    totalEvents: node.eventLog.length,
    witnessCount: 0,
    recentEvents: node.eventLog
      .slice(-4)
      .reverse()
      .map((e) => ({
        timestamp: e.timestamp,
        actor: 'system',
        action: e.description,
        verified: true,
      })),
  };

  const context: ContextFaceData = {
    entries: [
      { key: 'CPU', value: `${node.cpuPercent}%` },
      { key: 'Memory', value: `${node.memoryMb} MB` },
      { key: 'Disk', value: `${node.diskGb} GB` },
      { key: 'Uptime', value: formatDuration(node.uptimeSeconds) },
    ],
    totalKeys: 4,
  };

  const infra: InfraFaceData = {
    nodeId: node.id,
    nodeTier: node.tier,
    replicationFactor: node.replicationFactor,
    replicaHealth: node.replicaHealth,
    replicas: node.replicas,
    region: node.region,
  };

  const timers: TimersFaceData = {
    timers: node.activeTimers ?? [],
  };

  return { state, actors, audit, context, infra, timers };
}

// ── Hook ──

export function useCubeData(source: SourceEntity): CubeData {
  return useMemo(() => {
    switch (source.kind) {
      case 'workflow':
        return mapWorkflowToCube(source.instance);
      case 'node':
        return mapNodeToCube(source.node);
    }
  }, [source]);
}
