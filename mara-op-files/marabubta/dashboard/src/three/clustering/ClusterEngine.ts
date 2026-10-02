// Marabunta - Licensed under the MIT License.
// ── Cluster Engine ──
// Deterministic group-by clustering on a user-selected dimension.
// Partitions cubes into clusters, computes centroids, bounding boxes,
// and face-data aggregations for mega-cube collapse.
// W6C / Spec S21

import * as THREE from 'three';

// ── Types ──

export interface FaceData {
  label: string;
  numericValue: number;
}

export interface CubeEntry {
  id: string;
  position: THREE.Vector3;
  /** Dimension key-value pairs, e.g. { role: 'AGGREGATOR', region: 'us-east-1' } */
  dimensions: Record<string, string>;
  faceData: FaceData[];
}

export type AggregationType = 'SUM' | 'AVG' | 'COUNT' | 'MIN' | 'MAX';

export interface AggregateFaceData {
  faceIndex: number;
  label: string;
  aggregation: AggregationType;
  value: number;
  memberValues: number[];
}

export interface Cluster {
  id: string;
  dimension: string;
  value: string;
  members: CubeEntry[];
  centroid: THREE.Vector3;
  boundingBox: THREE.Box3;
  aggregatedFaces: AggregateFaceData[];
  collapsed: boolean;
}

// ── Bounding Box Padding ──

const BOUNDING_BOX_PADDING = 1.5;

// ── Minimum cluster size for mega-cube collapse ──

const MIN_COLLAPSE_SIZE = 3;

// ── Engine ──

export class ClusterEngine {
  private clusters = new Map<string, Cluster>();
  private cubes: CubeEntry[] = [];
  private activeDimension: string | null = null;

  /** Replace the full cube list. Call before groupBy(). */
  setCubes(cubes: CubeEntry[]): void {
    this.cubes = cubes;
  }

  /** Get the active grouping dimension. */
  getActiveDimension(): string | null {
    return this.activeDimension;
  }

  /** Get available dimensions from the current cube set. */
  getAvailableDimensions(): string[] {
    const dims = new Set<string>();
    for (const cube of this.cubes) {
      for (const key of Object.keys(cube.dimensions)) {
        dims.add(key);
      }
    }
    return [...dims].sort();
  }

  /**
   * Group all cubes by a dimension, computing centroids, bounding boxes,
   * and aggregate face data for each cluster.
   */
  groupBy(dimension: string): Cluster[] {
    this.activeDimension = dimension;
    this.clusters.clear();

    // Partition cubes by dimension value
    const groups = new Map<string, CubeEntry[]>();
    for (const cube of this.cubes) {
      const value = cube.dimensions[dimension] ?? 'unknown';
      if (!groups.has(value)) groups.set(value, []);
      groups.get(value)!.push(cube);
    }

    // Build cluster objects
    for (const [value, members] of groups) {
      const id = `cluster::${dimension}::${value}`;
      const centroid = this.computeCentroid(members);
      const boundingBox = this.computeBoundingBox(members);
      const aggregatedFaces = this.aggregateFaces(members);

      this.clusters.set(id, {
        id,
        dimension,
        value,
        members,
        centroid,
        boundingBox,
        aggregatedFaces,
        collapsed: false,
      });
    }

    return this.getClusters();
  }

  /** Clear all grouping and return to ungrouped state. */
  clearGrouping(): void {
    this.activeDimension = null;
    this.clusters.clear();
  }

  /** Collapse a cluster into a mega-cube (if it has enough members). */
  collapse(clusterId: string): boolean {
    const cluster = this.clusters.get(clusterId);
    if (!cluster) return false;
    if (cluster.members.length < MIN_COLLAPSE_SIZE) return false;
    cluster.collapsed = true;
    return true;
  }

  /** Expand a mega-cube back into individual cubes. */
  expand(clusterId: string): void {
    const cluster = this.clusters.get(clusterId);
    if (cluster) {
      cluster.collapsed = false;
    }
  }

  /** Get a snapshot of all current clusters. */
  getClusters(): Cluster[] {
    return [...this.clusters.values()];
  }

  /** Get a single cluster by ID. */
  getCluster(id: string): Cluster | undefined {
    return this.clusters.get(id);
  }

  /** Compute the centroid (mean position) of cluster members. */
  private computeCentroid(members: CubeEntry[]): THREE.Vector3 {
    const centroid = new THREE.Vector3();
    for (const member of members) {
      centroid.add(member.position);
    }
    if (members.length > 0) {
      centroid.divideScalar(members.length);
    }
    return centroid;
  }

  /** Compute axis-aligned bounding box with padding. */
  private computeBoundingBox(members: CubeEntry[]): THREE.Box3 {
    const box = new THREE.Box3();
    for (const member of members) {
      box.expandByPoint(member.position);
    }
    box.expandByScalar(BOUNDING_BOX_PADDING);
    return box;
  }

  /** Aggregate face data across all cluster members. */
  private aggregateFaces(members: CubeEntry[]): AggregateFaceData[] {
    const faceCount = Math.max(6, ...members.map((m) => m.faceData.length));

    return Array.from({ length: faceCount }, (_, faceIndex) => {
      const memberValues = members.map(
        (m) => m.faceData[faceIndex]?.numericValue ?? 0,
      );
      const label =
        members[0]?.faceData[faceIndex]?.label ?? `Face ${faceIndex}`;
      const aggregation = ClusterEngine.inferAggregation(label);

      let value: number;
      switch (aggregation) {
        case 'SUM':
          value = memberValues.reduce((a, b) => a + b, 0);
          break;
        case 'AVG':
          value =
            memberValues.length > 0
              ? memberValues.reduce((a, b) => a + b, 0) / memberValues.length
              : 0;
          break;
        case 'COUNT':
          value = memberValues.length;
          break;
        case 'MIN':
          value = memberValues.length > 0 ? Math.min(...memberValues) : 0;
          break;
        case 'MAX':
          value = memberValues.length > 0 ? Math.max(...memberValues) : 0;
          break;
      }

      return { faceIndex, label, aggregation, value, memberValues };
    });
  }

  /**
   * Infer the aggregation type from a face label using keyword heuristics.
   * Labels containing "count", "total", or "throughput" -> SUM
   * Labels with "latency", "duration", or "avg" -> AVG
   * Labels with "error" or "failure" -> MAX (worst-case)
   * Default -> SUM
   */
  static inferAggregation(label: string): AggregationType {
    const lower = label.toLowerCase();
    if (/latency|duration|avg|average|mean/.test(lower)) return 'AVG';
    if (/error|failure|fault/.test(lower)) return 'MAX';
    if (/min|minimum|floor/.test(lower)) return 'MIN';
    if (/count|total|throughput|sum/.test(lower)) return 'SUM';
    return 'SUM';
  }
}
