// Marabunta - Licensed under the MIT License.
// dashboard/src/three/SwarmDataContext.tsx
// Single source of truth for all swarm topology data within the 3D view.
// Uses React useReducer for predictable state transitions.

import { createContext, useContext, useReducer, type ReactNode } from 'react';

// ──────── Data Types ────────

/** A single node in the swarm topology */
export interface NodeData {
  /** Unique node identifier */
  id: string;
  /** Assigned roles, e.g. ['AGGREGATOR', 'WITNESS'] */
  roles: string[];
  /** CPU utilization 0.0 - 1.0 */
  cpu: number;
  /** RAM utilization 0.0 - 1.0 */
  ram: number;
  /** Failure detector state */
  health: 'alive' | 'suspect' | 'dead';
  /** Reputation score 0.0 - 1.0 */
  reputation: number;
  /** 3D position set by the layout engine [x, y, z] */
  position?: [number, number, number];
  /** Geographic coordinates for geo-aware visualization */
  geo?: { lat: number; lon: number };
}

/** A gossip connection between two nodes */
export interface EdgeData {
  /** Source node ID */
  source: string;
  /** Target node ID */
  target: string;
  /** Gossip frequency weight — higher = thicker/brighter edge */
  weight: number;
}

// ──────── State ────────

export interface SwarmState {
  /** Map of nodeId -> NodeData for O(1) lookup */
  nodes: Map<string, NodeData>;
  /** Array of undirected edges */
  edges: EdgeData[];
  /** Whether the topology WebSocket is connected */
  wsConnected: boolean;
  /** Currently selected node ID (null = no selection) */
  selectedNodeId: string | null;
}

// ──────── Actions ────────

export type SwarmAction =
  | { type: 'WS_CONNECTED' }
  | { type: 'WS_DISCONNECTED' }
  | { type: 'TOPOLOGY_SNAPSHOT'; payload: { nodes: NodeData[]; edges: EdgeData[] } }
  | { type: 'NODE_UPDATE'; payload: NodeData }
  | { type: 'NODE_REMOVED'; payload: { node_id: string } }
  | { type: 'EDGE_UPDATE'; payload: EdgeData[] }
  | { type: 'HEALTH_TICK'; payload: { node_id: string; cpu: number; ram: number } }
  | { type: 'SELECT_NODE'; payload: string | null }
  | { type: 'LAYOUT_UPDATE'; payload: Map<string, [number, number, number]> };

// ──────── Reducer ────────

export function swarmReducer(state: SwarmState, action: SwarmAction): SwarmState {
  switch (action.type) {
    case 'WS_CONNECTED':
      return { ...state, wsConnected: true };

    case 'WS_DISCONNECTED':
      return { ...state, wsConnected: false };

    case 'TOPOLOGY_SNAPSHOT': {
      const nodes = new Map<string, NodeData>();
      for (const n of action.payload.nodes) {
        nodes.set(n.id, n);
      }
      return { ...state, nodes, edges: action.payload.edges };
    }

    case 'NODE_UPDATE': {
      const nodes = new Map(state.nodes);
      const existing = nodes.get(action.payload.id);
      // Preserve position from layout engine if not provided in update
      if (existing?.position && !action.payload.position) {
        nodes.set(action.payload.id, { ...action.payload, position: existing.position });
      } else {
        nodes.set(action.payload.id, action.payload);
      }
      return { ...state, nodes };
    }

    case 'NODE_REMOVED': {
      const nodes = new Map(state.nodes);
      nodes.delete(action.payload.node_id);
      // Also remove edges referencing the removed node
      const edges = state.edges.filter(
        e => e.source !== action.payload.node_id && e.target !== action.payload.node_id
      );
      // Clear selection if the removed node was selected
      const selectedNodeId = state.selectedNodeId === action.payload.node_id
        ? null
        : state.selectedNodeId;
      return { ...state, nodes, edges, selectedNodeId };
    }

    case 'EDGE_UPDATE':
      return { ...state, edges: action.payload };

    case 'HEALTH_TICK': {
      const nodes = new Map(state.nodes);
      const existing = nodes.get(action.payload.node_id);
      if (existing) {
        nodes.set(action.payload.node_id, {
          ...existing,
          cpu: action.payload.cpu,
          ram: action.payload.ram,
        });
      }
      return { ...state, nodes };
    }

    case 'SELECT_NODE':
      return { ...state, selectedNodeId: action.payload };

    case 'LAYOUT_UPDATE': {
      const nodes = new Map(state.nodes);
      for (const [id, pos] of action.payload) {
        const node = nodes.get(id);
        if (node) {
          nodes.set(id, { ...node, position: pos });
        }
      }
      return { ...state, nodes };
    }

    default:
      return state;
  }
}

// ──────── Initial State ────────

const initialState: SwarmState = {
  nodes: new Map(),
  edges: [],
  wsConnected: false,
  selectedNodeId: null,
};

// ──────── Context ────────

interface SwarmDataContextValue {
  state: SwarmState;
  dispatch: React.Dispatch<SwarmAction>;
}

const SwarmDataCtx = createContext<SwarmDataContextValue | null>(null);

/** Provider component that wraps the R3F Canvas subtree */
export function SwarmDataProvider({ children }: { children: ReactNode }) {
  const [state, dispatch] = useReducer(swarmReducer, initialState);
  return (
    <SwarmDataCtx.Provider value={{ state, dispatch }}>
      {children}
    </SwarmDataCtx.Provider>
  );
}

/**
 * Hook to access swarm topology state and dispatch actions.
 * Must be called within a SwarmDataProvider.
 */
export function useSwarmData(): SwarmDataContextValue {
  const ctx = useContext(SwarmDataCtx);
  if (!ctx) {
    throw new Error('useSwarmData must be used within a SwarmDataProvider');
  }
  return ctx;
}
