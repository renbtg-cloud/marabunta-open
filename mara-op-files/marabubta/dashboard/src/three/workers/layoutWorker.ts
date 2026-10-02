// Marabunta - Licensed under the MIT License.
// dashboard/src/three/workers/layoutWorker.ts
// Web Worker for d3-force-3d layout simulation.
// Runs the force-directed layout off the main thread to keep rendering smooth.
// Communicates with useForceLayout hook via postMessage.

import {
  forceSimulation,
  forceManyBody,
  forceLink,
  forceCenter,
  forceCollide,
  forceRadial,
} from 'd3-force-3d';

// ──────── Types ────────

interface LayoutNode {
  id: string;
  roles: string[];
  x?: number;
  y?: number;
  z?: number;
  vx?: number;
  vy?: number;
  vz?: number;
}

interface LayoutEdge {
  source: string;
  target: string;
  weight: number;
}

type WorkerMessage =
  | { type: 'init'; nodes: LayoutNode[]; edges: LayoutEdge[] }
  | { type: 'update_nodes'; nodes: LayoutNode[] }
  | { type: 'update_edges'; edges: LayoutEdge[] }
  | { type: 'tick_request' }
  | { type: 'set_alpha'; alpha: number };

// ──────── State ────────

// eslint-disable-next-line @typescript-eslint/no-explicit-any
let simulation: any = null;
let nodes: LayoutNode[] = [];
let edges: LayoutEdge[] = [];

// ──────── Role-based Y-layer gravity ────────
// Aggregators float up, storage sinks down — creates loose vertical stratification
const ROLE_Y_TARGET: Record<string, number> = {
  AGGREGATOR: 15,
  GATEWAY: 10,
  WITNESS: 5,
  RELAY: 0,
  STORAGE: -10,
  EPHEMERAL: -15,
};

function getRoleY(roles: string[]): number {
  if (roles.length === 0) return 0;
  const sum = roles.reduce((acc, r) => acc + (ROLE_Y_TARGET[r] ?? 0), 0);
  return sum / roles.length;
}

// ──────── Simulation ────────

function createSimulation() {
  simulation = forceSimulation(nodes, 3)
    .force(
      'charge',
      forceManyBody().strength(-80).distanceMax(100)
    )
    .force(
      'link',
      forceLink(edges)
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        .id((d: any) => d.id)
        .distance(8)
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        .strength((e: any) => e.weight * 0.3)
    )
    .force('center', forceCenter(0, 0, 0).strength(0.05))
    .force('collision', forceCollide().radius(1.5).iterations(2))
    .force(
      'role_y',
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      forceRadial((d: any) => getRoleY(d.roles), 0, 0).strength(0.02)
    )
    .alphaDecay(0.02)
    .velocityDecay(0.3)
    .stop(); // We tick manually, not on an internal timer
}

function emitPositions() {
  const positions: Record<string, [number, number, number]> = {};
  for (const node of nodes) {
    positions[node.id] = [node.x ?? 0, node.y ?? 0, node.z ?? 0];
  }
  self.postMessage({ type: 'positions', data: positions });
}

// ──────── Message Handler ────────

self.onmessage = (event: MessageEvent<WorkerMessage>) => {
  const msg = event.data;

  switch (msg.type) {
    case 'init':
      nodes = msg.nodes;
      edges = msg.edges;
      createSimulation();
      // Run warmup ticks for initial layout convergence
      for (let i = 0; i < 100; i++) simulation.tick();
      emitPositions();
      break;

    case 'update_nodes': {
      // Merge new nodes while preserving positions of existing ones
      const existingMap = new Map(nodes.map(n => [n.id, n]));
      nodes = msg.nodes.map(n => ({
        ...n,
        x: existingMap.get(n.id)?.x ?? (Math.random() - 0.5) * 20,
        y: existingMap.get(n.id)?.y ?? (Math.random() - 0.5) * 20,
        z: existingMap.get(n.id)?.z ?? (Math.random() - 0.5) * 20,
        vx: existingMap.get(n.id)?.vx ?? 0,
        vy: existingMap.get(n.id)?.vy ?? 0,
        vz: existingMap.get(n.id)?.vz ?? 0,
      }));
      createSimulation();
      simulation.alpha(0.3);
      break;
    }

    case 'update_edges':
      edges = msg.edges;
      if (simulation) {
        simulation.force(
          'link',
          forceLink(edges)
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            .id((d: any) => d.id)
            .distance(8)
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            .strength((e: any) => e.weight * 0.3)
        );
        simulation.alpha(0.1);
      }
      break;

    case 'tick_request':
      if (simulation && simulation.alpha() > 0.001) {
        simulation.tick(3); // 3 ticks per request for faster convergence
        emitPositions();
      }
      break;

    case 'set_alpha':
      if (simulation) simulation.alpha(msg.alpha);
      break;
  }
};
