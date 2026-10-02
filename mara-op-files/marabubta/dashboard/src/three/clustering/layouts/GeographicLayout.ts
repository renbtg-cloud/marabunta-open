// Marabunta - Licensed under the MIT License.
// ── Geographic Layout ──
// Places cubes on a 3D globe or Mercator flat map based on lat/lon metadata.
// W6C / Spec S21

import * as THREE from 'three';
import type { LayoutEngine, LayoutNode, LayoutResult } from './LayoutEngine';
import { buildEdges } from './LayoutEngine';

const GLOBE_RADIUS = 15;
const ELEVATION = 0.5;
const FLAT_MAP_SCALE = 20;

/** Convert latitude/longitude to a position on a 3D sphere. */
function latLonToPosition(lat: number, lon: number): THREE.Vector3 {
  const phi = (90 - lat) * (Math.PI / 180);
  const theta = (lon + 180) * (Math.PI / 180);
  const r = GLOBE_RADIUS + ELEVATION;

  return new THREE.Vector3(
    -r * Math.sin(phi) * Math.cos(theta),
     r * Math.cos(phi),
     r * Math.sin(phi) * Math.sin(theta),
  );
}

/** Mercator projection to flat plane. */
function flatMapPosition(lat: number, lon: number): THREE.Vector3 {
  const x = (lon / 180) * FLAT_MAP_SCALE;
  const latRad = (45 + lat / 2) * (Math.PI / 180);
  // Clamp to avoid log(0) at poles
  const clamped = Math.max(0.01, Math.tan(latRad));
  const y = (Math.log(clamped) / Math.PI) * FLAT_MAP_SCALE;
  return new THREE.Vector3(x, y, 0);
}

export class GeographicLayout implements LayoutEngine {
  readonly name = 'Geographic';
  readonly description = 'Lat/lon globe or flat map projection';

  private mode: 'globe' | 'flat' = 'globe';

  setMode(mode: 'globe' | 'flat'): void {
    this.mode = mode;
  }

  getMode(): 'globe' | 'flat' {
    return this.mode;
  }

  async compute(nodes: LayoutNode[]): Promise<LayoutResult> {
    const positions = new Map<string, THREE.Vector3>();

    for (const node of nodes) {
      const lat = node.lat ?? 0;
      const lon = node.lon ?? 0;

      positions.set(
        node.id,
        this.mode === 'globe'
          ? latLonToPosition(lat, lon)
          : flatMapPosition(lat, lon),
      );
    }

    const edges = buildEdges(nodes);
    const bounds = new THREE.Box3();
    if (positions.size > 0) {
      bounds.setFromPoints([...positions.values()]);
    }

    return { positions, edges, bounds };
  }

  async updateNode(id: string, node: LayoutNode): Promise<THREE.Vector3> {
    const lat = node.lat ?? 0;
    const lon = node.lon ?? 0;
    return this.mode === 'globe'
      ? latLonToPosition(lat, lon)
      : flatMapPosition(lat, lon);
  }

  dispose(): void {
    /* no resources to clean up */
  }
}
