// Marabunta - Licensed under the MIT License.
// ── ObservatoryScene ──
// Root component for Spec S21: Mission Control "Swarm Observatory" mode.
// Composes all observatory visual layers into a single scene graph:
//   L0: Replication arcs (data plane backbone)
//   L1: Gossip edges with flowing particles (control plane)
//   L2: Database geometry (infrastructure layer)
//   L3: Node cloud (InstancedMesh compute layer)
//   L4: Role corona rings (multi-role accents)
//   L5: Dead node ghosts (death animations)
//   L6: Partition membranes (failure visualization)
//   L7: Node tooltip (HTML overlay on hover)

import { useState, useCallback, useReducer, useEffect, useRef, useMemo } from 'react';
import * as THREE from 'three';
import { NodeCloud } from './NodeCloud';
import { RoleCorona } from './RoleCorona';
import { GossipEdges } from './GossipEdges';
import { ReplicationArcs } from './ReplicationArcs';
import { TrainingLinks } from './TrainingLinks';
import { DatabaseGeometry } from './DatabaseGeometry';
import { DeadNodeGhost } from './DeadNodeGhost';
import { PartitionMembrane } from './PartitionMembrane';
import { NodeTooltip } from './NodeTooltip';
import {
  type SwarmNode,
  type GossipEdge,
  type ReplicationLink,
  type DatabaseObject,
  type DeadNodeInfo,
  type PartitionInfo,
  type LayoutEngine,
  ROLE_COLORS,
  computeRadius,
} from './types';

// ── Observatory data state ──

interface ObservatoryState {
  nodes: SwarmNode[];
  edges: GossipEdge[];
  replLinks: ReplicationLink[];
  databases: DatabaseObject[];
  deadNodes: DeadNodeInfo[];
  partitions: PartitionInfo[];
}

type ObservatoryAction =
  | {
      type: 'BULK_UPDATE';
      nodes: SwarmNode[];
      edges: GossipEdge[];
      replLinks: ReplicationLink[];
      databases: DatabaseObject[];
    }
  | { type: 'NODE_DEATH'; payload: DeadNodeInfo }
  | { type: 'PARTITION_DETECTED'; payload: PartitionInfo }
  | { type: 'PARTITION_HEALED'; payload: { index: number } }
  | { type: 'GHOST_COMPLETE'; payload: { nodeId: string } };

const initialState: ObservatoryState = {
  nodes: [],
  edges: [],
  replLinks: [],
  databases: [],
  deadNodes: [],
  partitions: [],
};

function observatoryReducer(
  state: ObservatoryState,
  action: ObservatoryAction,
): ObservatoryState {
  switch (action.type) {
    case 'BULK_UPDATE':
      return {
        ...state,
        nodes: action.nodes,
        edges: action.edges,
        replLinks: action.replLinks,
        databases: action.databases,
      };
    case 'NODE_DEATH': {
      // Idempotent: ignore duplicate death events.
      if (state.deadNodes.some((dn) => dn.id === action.payload.id)) {
        return state;
      }
      return {
        ...state,
        deadNodes: [...state.deadNodes, action.payload].slice(-20), // cap at 20
      };
    }
    case 'PARTITION_DETECTED':
      return {
        ...state,
        partitions: [...state.partitions, action.payload],
      };
    case 'PARTITION_HEALED':
      return {
        ...state,
        partitions: state.partitions.filter((_, i) => i !== action.payload.index),
      };
    case 'GHOST_COMPLETE':
      return {
        ...state,
        deadNodes: state.deadNodes.filter((dn) => dn.id !== action.payload.nodeId),
      };
    default:
      return state;
  }
}

// ── Simple force layout stub ──
// In production this would use d3-force-3d on a Web Worker.
// This stub distributes nodes in a sphere for immediate usability.

function createSimpleLayout(nodes: SwarmNode[]): LayoutEngine {
  const positions = new Map<string, THREE.Vector3>();

  const phi = (1 + Math.sqrt(5)) / 2; // golden ratio for spiral
  nodes.forEach((node, i) => {
    const theta = 2 * Math.PI * i / phi;
    const y = 1 - (i / Math.max(nodes.length - 1, 1)) * 2;
    const radiusAtY = Math.sqrt(1 - y * y);
    const spread = Math.cbrt(nodes.length) * 3;

    positions.set(
      node.id,
      new THREE.Vector3(
        Math.cos(theta) * radiusAtY * spread,
        y * spread,
        Math.sin(theta) * radiusAtY * spread,
      ),
    );
  });

  return {
    getPosition(nodeId: string): THREE.Vector3 {
      return positions.get(nodeId) ?? new THREE.Vector3(0, 0, 0);
    },
  };
}

// ── Data fetching hook ──

function useObservatoryData(): [ObservatoryState, React.Dispatch<ObservatoryAction>] {
  const [state, dispatch] = useReducer(observatoryReducer, initialState);
  const wsRef = useRef<WebSocket | null>(null);
  const pollFailures = useRef(0);

  // Poll for bulk state every 2 seconds.
  useEffect(() => {
    let active = true;

    async function poll() {
      try {
        const [nodes, edges, replLinks, databases] = await Promise.all([
          fetch('/api/v1/nodes').then((r) => r.json()),
          fetch('/api/v1/gossip/edges').then((r) => r.json()),
          fetch('/api/v1/replication/links').then((r) => r.json()),
          fetch('/api/v1/databases').then((r) => r.json()),
        ]);
        if (active) {
          dispatch({ type: 'BULK_UPDATE', nodes, edges, replLinks, databases });
          pollFailures.current = 0;
        }
      } catch {
        pollFailures.current++;
      }
    }

    const interval = setInterval(poll, 2000);
    poll(); // Initial fetch.

    return () => {
      active = false;
      clearInterval(interval);
    };
  }, []);

  // WebSocket for real-time events.
  useEffect(() => {
    let reconnectDelay = 1000;
    let active = true;

    function connect() {
      if (!active) return;
      const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
      const ws = new WebSocket(`${protocol}//${location.host}/ws/events`);
      wsRef.current = ws;

      ws.onmessage = (msg) => {
        try {
          const event = JSON.parse(msg.data);
          if (event.type === 'NODE_DEATH') {
            dispatch({ type: 'NODE_DEATH', payload: event.payload });
          } else if (event.type === 'PARTITION_DETECTED') {
            dispatch({ type: 'PARTITION_DETECTED', payload: event.payload });
          }
        } catch {
          // Ignore malformed messages.
        }
      };

      ws.onopen = () => {
        reconnectDelay = 1000; // Reset on successful connect.
      };

      ws.onclose = () => {
        if (!active) return;
        // Exponential backoff reconnect.
        setTimeout(connect, reconnectDelay);
        reconnectDelay = Math.min(reconnectDelay * 2, 30000);
      };

      ws.onerror = () => {
        ws.close();
      };
    }

    connect();

    return () => {
      active = false;
      if (wsRef.current) {
        wsRef.current.close();
      }
    };
  }, []);

  return [state, dispatch];
}

// ── ObservatoryScene root ──

export function ObservatoryScene() {
  const [state, dispatch] = useObservatoryData();
  const { nodes, edges, replLinks, databases, deadNodes, partitions } = state;

  const layout = createSimpleLayout(nodes);
  const [hoveredNodeId, setHoveredNodeId] = useState<string | null>(null);
  const [_selectedNodeId, setSelectedNodeId] = useState<string | null>(null);

  // Derive PythonDiLoCo training links
  // Connect all nodes with isTraining=true to the primary AGGREGATOR
  const trainingLinks = useMemo(() => {
    const aggregator = nodes.find(n => n.roles.includes('AGGREGATOR'));
    if (!aggregator) return [];
    
    return nodes
      .filter(n => n.isTraining && n.id !== aggregator.id)
      .map(n => ({ sourceId: n.id, targetId: aggregator.id }));
  }, [nodes]);

  const hoveredNode = nodes.find((n) => n.id === hoveredNodeId) ?? null;
  const hoveredPos = hoveredNodeId ? layout.getPosition(hoveredNodeId) : null;

  const handleGhostComplete = useCallback(
    (nodeId: string) => {
      dispatch({ type: 'GHOST_COMPLETE', payload: { nodeId } });
    },
    [dispatch],
  );

  // Empty swarm message.
  if (nodes.length === 0) {
    return (
      <group>
        <ambientLight intensity={0.4} />
      </group>
    );
  }

  return (
    <>
      {/* Layer 0: Replication infrastructure */}
      <ReplicationArcs links={replLinks} layout={layout} />

      {/* Layer 1: Gossip network */}
      <GossipEdges edges={edges} layout={layout} />

      {/* Layer 1.5: PythonDiLoCo Training Links */}
      <TrainingLinks links={trainingLinks} layout={layout} />

      {/* Layer 2: Database objects */}
      <DatabaseGeometry databases={databases} layout={layout} />

      {/* Layer 3: Node spheres (InstancedMesh) */}
      <NodeCloud
        nodes={nodes}
        layout={layout}
        onNodeHover={setHoveredNodeId}
        onNodeClick={setSelectedNodeId}
        hoveredNodeId={hoveredNodeId}
      />

      {/* Layer 4: Role coronas (only multi-role nodes) */}
      {nodes
        .filter((n) => n.roles.length > 1)
        .map((n) => (
          <RoleCorona
            key={n.id}
            nodeId={n.id}
            roles={n.roles}
            position={layout.getPosition(n.id)}
            baseRadius={computeRadius(n)}
          />
        ))}

      {/* Layer 5: Death animations */}
      {deadNodes.map((dn) => (
        <DeadNodeGhost
          key={dn.id}
          nodeId={dn.id}
          position={layout.getPosition(dn.id)}
          originalColor={ROLE_COLORS[dn.primaryRole]}
          radius={computeRadius(dn)}
          rerouteTargets={dn.rerouteTargets}
          onFadeComplete={handleGhostComplete}
        />
      ))}

      {/* Layer 6: Partition membranes */}
      {partitions.map((p, i) => (
        <PartitionMembrane key={i} partition={p} layout={layout} />
      ))}

      {/* Layer 7: Tooltip overlay */}
      <NodeTooltip node={hoveredNode} position={hoveredPos} />
    </>
  );
}
