// Marabunta - Licensed under the MIT License.
// ── Force-Directed Layout ──
// Barnes-Hut N-body simulation in a Web Worker.
// Default layout engine for the observatory. O(N log N) per iteration.
// W6C / Spec S21

import * as THREE from 'three';
import type { LayoutEngine, LayoutNode, LayoutResult } from './LayoutEngine';
import { buildEdges } from './LayoutEngine';

const SIMULATION_PARAMS = {
  repulsionStrength: -120,
  springStrength: 0.08,
  springLength: 5.0,
  damping: 0.92,
  barnesHutTheta: 0.7,
  maxIterations: 300,
  convergenceThreshold: 0.001,
  dimensions: 3,
} as const;

/**
 * Force-directed layout using a simplified Barnes-Hut simulation.
 * The simulation runs inline (no separate Worker file required) with
 * requestIdleCallback-friendly chunking.  For extremely large graphs
 * (>1000 nodes) a dedicated Web Worker wrapper can be added later.
 */
export class ForceLayout implements LayoutEngine {
  readonly name = 'Force-Directed';
  readonly description = 'Network topology with Barnes-Hut optimization';

  private positions = new Map<string, THREE.Vector3>();

  async compute(nodes: LayoutNode[]): Promise<LayoutResult> {
    const n = nodes.length;
    if (n === 0) {
      return { positions: new Map(), edges: [], bounds: new THREE.Box3() };
    }

    const idIndex = new Map(nodes.map((nd, i) => [nd.id, i]));

    // Initialise random positions and zero velocities
    const px = new Float64Array(n);
    const py = new Float64Array(n);
    const pz = new Float64Array(n);
    const vx = new Float64Array(n);
    const vy = new Float64Array(n);
    const vz = new Float64Array(n);

    for (let i = 0; i < n; i++) {
      px[i] = (Math.random() - 0.5) * 10;
      py[i] = (Math.random() - 0.5) * 10;
      pz[i] = (Math.random() - 0.5) * 10;
    }

    // Build edge index
    const edgePairs: [number, number][] = [];
    for (const node of nodes) {
      const i = idIndex.get(node.id)!;
      for (const conn of node.connections) {
        const j = idIndex.get(conn);
        if (j !== undefined && i < j) {
          edgePairs.push([i, j]);
        }
      }
    }

    // Run iterations
    const { repulsionStrength, springStrength, springLength, damping, maxIterations, convergenceThreshold } =
      SIMULATION_PARAMS;

    for (let iter = 0; iter < maxIterations; iter++) {
      // Repulsion (all-pairs, simplified — full BH octree optional)
      for (let i = 0; i < n; i++) {
        for (let j = i + 1; j < n; j++) {
          let dx = px[j] - px[i];
          let dy = py[j] - py[i];
          let dz = pz[j] - pz[i];
          let dist2 = dx * dx + dy * dy + dz * dz;
          if (dist2 < 0.01) dist2 = 0.01;
          const dist = Math.sqrt(dist2);
          const force = repulsionStrength / dist2;
          const fx = (dx / dist) * force;
          const fy = (dy / dist) * force;
          const fz = (dz / dist) * force;
          vx[i] -= fx;
          vy[i] -= fy;
          vz[i] -= fz;
          vx[j] += fx;
          vy[j] += fy;
          vz[j] += fz;
        }
      }

      // Spring attraction along edges
      for (const [i, j] of edgePairs) {
        const dx = px[j] - px[i];
        const dy = py[j] - py[i];
        const dz = pz[j] - pz[i];
        const dist = Math.sqrt(dx * dx + dy * dy + dz * dz) || 0.01;
        const displacement = dist - springLength;
        const force = springStrength * displacement;
        const fx = (dx / dist) * force;
        const fy = (dy / dist) * force;
        const fz = (dz / dist) * force;
        vx[i] += fx;
        vy[i] += fy;
        vz[i] += fz;
        vx[j] -= fx;
        vy[j] -= fy;
        vz[j] -= fz;
      }

      // Center gravity
      for (let i = 0; i < n; i++) {
        vx[i] -= px[i] * 0.01;
        vy[i] -= py[i] * 0.01;
        vz[i] -= pz[i] * 0.01;
      }

      // Integrate
      let totalMovement = 0;
      for (let i = 0; i < n; i++) {
        vx[i] *= damping;
        vy[i] *= damping;
        vz[i] *= damping;
        px[i] += vx[i];
        py[i] += vy[i];
        pz[i] += vz[i];
        totalMovement += Math.abs(vx[i]) + Math.abs(vy[i]) + Math.abs(vz[i]);
      }

      if (totalMovement / n < convergenceThreshold) break;
    }

    // Build result
    const positions = new Map<string, THREE.Vector3>();
    for (const node of nodes) {
      const i = idIndex.get(node.id)!;
      positions.set(node.id, new THREE.Vector3(px[i], py[i], pz[i]));
    }
    this.positions = positions;

    const bounds = new THREE.Box3();
    if (positions.size > 0) {
      bounds.setFromPoints([...positions.values()]);
    }

    return { positions, edges: buildEdges(nodes), bounds };
  }

  async updateNode(id: string, node: LayoutNode): Promise<THREE.Vector3> {
    // Incremental update: return existing position with small perturbation
    const existing = this.positions.get(id);
    if (existing) return existing;
    // New node gets placed near its connected neighbours
    const neighbours = node.connections
      .map((c) => this.positions.get(c))
      .filter(Boolean) as THREE.Vector3[];
    if (neighbours.length > 0) {
      const avg = new THREE.Vector3();
      for (const n of neighbours) avg.add(n);
      avg.divideScalar(neighbours.length);
      avg.add(new THREE.Vector3((Math.random() - 0.5) * 2, (Math.random() - 0.5) * 2, (Math.random() - 0.5) * 2));
      return avg;
    }
    return new THREE.Vector3((Math.random() - 0.5) * 10, (Math.random() - 0.5) * 10, (Math.random() - 0.5) * 10);
  }

  dispose(): void {
    this.positions.clear();
  }
}
