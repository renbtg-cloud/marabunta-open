// Marabunta - Licensed under the MIT License.
// ── Layout Engine Interface ──
// Shared types and contract for all six layout engines (W6C / Spec S21)
// Each layout engine positions cubes in 3D space using a different algorithm.

import * as THREE from 'three';

// ── Node Input ──

export interface LayoutNode {
  id: string;
  role: string;
  region?: string;
  lat?: number;
  lon?: number;
  depth?: number;
  connections: string[];
  metadata: Record<string, unknown>;
}

// ── Layout Output ──

export interface LayoutResult {
  positions: Map<string, THREE.Vector3>;
  edges: Array<{ from: string; to: string }>;
  bounds: THREE.Box3;
}

// ── Engine Contract ──

export type LayoutKey =
  | 'force'
  | 'geographic'
  | 'hierarchical'
  | 'spectral'
  | 'grid'
  | 'custom';

export interface LayoutEngine {
  readonly name: string;
  readonly description: string;

  /** Compute positions for all nodes. May be async for Web Worker layouts. */
  compute(nodes: LayoutNode[]): Promise<LayoutResult>;

  /** Update incrementally when a single node changes */
  updateNode(id: string, node: LayoutNode): Promise<THREE.Vector3>;

  /** Clean up resources (e.g. terminate Web Worker) */
  dispose(): void;
}

// ── Shared helpers used by multiple engines ──

/** Build edge list from node connection arrays (deduplicates). */
export function buildEdges(nodes: LayoutNode[]): Array<{ from: string; to: string }> {
  const seen = new Set<string>();
  const edges: Array<{ from: string; to: string }> = [];

  for (const node of nodes) {
    for (const conn of node.connections) {
      const key = [node.id, conn].sort().join('::');
      if (!seen.has(key)) {
        seen.add(key);
        edges.push({ from: node.id, to: conn });
      }
    }
  }

  return edges;
}
