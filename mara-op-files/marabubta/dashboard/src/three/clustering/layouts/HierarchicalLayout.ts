// Marabunta - Licensed under the MIT License.
// ── Hierarchical Layout ──
// Tree layout by role or org hierarchy. Modified Reingold-Tilford in 3D.
// GATEWAY at top, AGGREGATOR mid, WORKER/STORAGE lower, EPHEMERAL bottom.
// W6C / Spec S21

import * as THREE from 'three';
import type { LayoutEngine, LayoutNode, LayoutResult } from './LayoutEngine';
import { buildEdges } from './LayoutEngine';

/** Role-to-depth tier mapping. Lower number = higher in tree. */
const ROLE_DEPTH: Record<string, number> = {
  GATEWAY: 0,
  AGGREGATOR: 1,
  COORDIGLOBAL_ALLIANCE_T1R: 2,
  WITNESS: 2,
  RELAY: 2,
  WORKER: 3,
  STORAGE: 3,
  EPHEMERAL: 4,
};

const TIER_SPACING_Y = 5.0;
const SIBLING_SPACING_XZ = 3.0;

export class HierarchicalLayout implements LayoutEngine {
  readonly name = 'Hierarchical';
  readonly description = 'Tree layout by role or org hierarchy';

  async compute(nodes: LayoutNode[]): Promise<LayoutResult> {
    // Group nodes by depth tier
    const tiers = new Map<number, LayoutNode[]>();
    for (const node of nodes) {
      const depth = ROLE_DEPTH[node.role] ?? 3;
      if (!tiers.has(depth)) tiers.set(depth, []);
      tiers.get(depth)!.push(node);
    }

    const positions = new Map<string, THREE.Vector3>();

    for (const [depth, tierNodes] of tiers) {
      const y = -depth * TIER_SPACING_Y;
      const count = tierNodes.length;
      const cols = Math.max(1, Math.ceil(Math.sqrt(count)));
      const rows = Math.ceil(count / cols);

      tierNodes.forEach((node, i) => {
        const row = Math.floor(i / cols);
        const col = i % cols;
        const x = (col - (cols - 1) / 2) * SIBLING_SPACING_XZ;
        const z = (row - (rows - 1) / 2) * SIBLING_SPACING_XZ;
        positions.set(node.id, new THREE.Vector3(x, y, z));
      });
    }

    const bounds = new THREE.Box3();
    if (positions.size > 0) {
      bounds.setFromPoints([...positions.values()]);
    }

    return {
      positions,
      edges: buildEdges(nodes),
      bounds,
    };
  }

  async updateNode(id: string, node: LayoutNode): Promise<THREE.Vector3> {
    const depth = ROLE_DEPTH[node.role] ?? 3;
    return new THREE.Vector3(0, -depth * TIER_SPACING_Y, 0);
  }

  dispose(): void {
    /* nothing to clean up */
  }
}
