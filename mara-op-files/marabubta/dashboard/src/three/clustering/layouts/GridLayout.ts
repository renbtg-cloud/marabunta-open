// Marabunta - Licensed under the MIT License.
// ── Grid Layout ──
// Uniform 3D grid spacing sorted by role. Simplest engine.
// Ideal for very large swarms (500+) where maximum visibility is needed.
// W6C / Spec S21

import * as THREE from 'three';
import type { LayoutEngine, LayoutNode, LayoutResult } from './LayoutEngine';
import { buildEdges } from './LayoutEngine';

const GRID_SPACING = 3.0;

export class GridLayout implements LayoutEngine {
  readonly name = 'Grid';
  readonly description = 'Uniform 3D grid spacing';

  async compute(nodes: LayoutNode[]): Promise<LayoutResult> {
    if (nodes.length === 0) {
      return { positions: new Map(), edges: [], bounds: new THREE.Box3() };
    }

    // Sort by role for spatial grouping
    const sorted = [...nodes].sort((a, b) => a.role.localeCompare(b.role));
    const size = Math.max(1, Math.ceil(Math.cbrt(sorted.length)));
    const positions = new Map<string, THREE.Vector3>();
    const offset = (size * GRID_SPACING) / 2;

    sorted.forEach((node, i) => {
      const x = (i % size) * GRID_SPACING;
      const y = (Math.floor(i / size) % size) * GRID_SPACING;
      const z = Math.floor(i / (size * size)) * GRID_SPACING;
      positions.set(node.id, new THREE.Vector3(
        x - offset,
        y - offset,
        z - offset,
      ));
    });

    const bounds = new THREE.Box3();
    bounds.setFromPoints([...positions.values()]);

    return { positions, edges: buildEdges(nodes), bounds };
  }

  async updateNode(): Promise<THREE.Vector3> {
    // Grid layout requires full recomputation for correct placement
    return new THREE.Vector3();
  }

  dispose(): void {
    /* nothing to clean up */
  }
}
