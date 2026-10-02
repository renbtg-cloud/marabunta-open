// Marabunta - Licensed under the MIT License.
// ── Custom Layout ──
// Manual pin positions persisted to localStorage, keyed by config name.
// Operator can drag cubes to any position and save as named configurations.
// W6C / Spec S21

import * as THREE from 'three';
import type { LayoutEngine, LayoutNode, LayoutResult } from './LayoutEngine';
import { buildEdges } from './LayoutEngine';

const STORAGE_PREFIX = 'marabunta_custom_layout_';

export class CustomLayout implements LayoutEngine {
  readonly name = 'Custom';
  readonly description = 'Manual pin positions, saved as named config';

  private configName = 'default';
  private positions = new Map<string, THREE.Vector3>();

  /** Switch to a different named configuration. */
  setConfig(name: string): void {
    this.configName = name;
    this.loadFromStorage();
  }

  getConfig(): string {
    return this.configName;
  }

  async compute(nodes: LayoutNode[]): Promise<LayoutResult> {
    this.loadFromStorage();

    // Assign positions for new nodes not in saved config
    const centroid = this.computeCentroid();
    for (const node of nodes) {
      if (!this.positions.has(node.id)) {
        this.positions.set(
          node.id,
          centroid.clone().add(
            new THREE.Vector3(
              (Math.random() - 0.5) * 4,
              (Math.random() - 0.5) * 4,
              (Math.random() - 0.5) * 4,
            ),
          ),
        );
      }
    }

    const bounds = new THREE.Box3();
    if (this.positions.size > 0) {
      bounds.setFromPoints([...this.positions.values()]);
    }

    return {
      positions: new Map(this.positions),
      edges: buildEdges(nodes),
      bounds,
    };
  }

  /** Called when the operator drags a node to a new position. */
  pinNode(id: string, position: THREE.Vector3): void {
    this.positions.set(id, position.clone());
    this.saveToStorage();
  }

  /** List all saved configuration names. */
  listConfigs(): string[] {
    const configs: string[] = [];
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      if (key?.startsWith(STORAGE_PREFIX)) {
        configs.push(key.slice(STORAGE_PREFIX.length));
      }
    }
    return configs;
  }

  private loadFromStorage(): void {
    try {
      const raw = localStorage.getItem(STORAGE_PREFIX + this.configName);
      if (!raw) return;
      const data = JSON.parse(raw) as Record<string, [number, number, number]>;
      this.positions.clear();
      for (const [id, [x, y, z]] of Object.entries(data)) {
        this.positions.set(id, new THREE.Vector3(x, y, z));
      }
    } catch {
      // Corrupt data; start fresh
      this.positions.clear();
    }
  }

  private saveToStorage(): void {
    const data: Record<string, [number, number, number]> = {};
    for (const [id, pos] of this.positions) {
      data[id] = [pos.x, pos.y, pos.z];
    }
    try {
      localStorage.setItem(
        STORAGE_PREFIX + this.configName,
        JSON.stringify(data),
      );
    } catch {
      // localStorage quota exceeded; silently skip
    }
  }

  private computeCentroid(): THREE.Vector3 {
    const centroid = new THREE.Vector3();
    if (this.positions.size === 0) return centroid;
    for (const pos of this.positions.values()) {
      centroid.add(pos);
    }
    centroid.divideScalar(this.positions.size);
    return centroid;
  }

  async updateNode(id: string): Promise<THREE.Vector3> {
    return this.positions.get(id) ?? new THREE.Vector3();
  }

  dispose(): void {
    this.positions.clear();
  }
}
