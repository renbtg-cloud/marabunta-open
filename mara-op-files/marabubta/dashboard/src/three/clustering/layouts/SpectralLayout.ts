// Marabunta - Licensed under the MIT License.
// ── Spectral Layout ──
// Eigenvector decomposition of the graph Laplacian.
// The first three non-trivial eigenvectors become X, Y, Z coordinates.
// Reveals mathematical community structure in the network.
// W6C / Spec S21

import * as THREE from 'three';
import type { LayoutEngine, LayoutNode, LayoutResult } from './LayoutEngine';
import { buildEdges } from './LayoutEngine';

const POSITION_SCALE = 15;
const POWER_ITERATIONS = 100;
const EIGENVECTOR_COUNT = 4; // 0 is trivial, use 1..3

export class SpectralLayout implements LayoutEngine {
  readonly name = 'Spectral';
  readonly description = 'Eigenvector-based mathematical structure';

  async compute(nodes: LayoutNode[]): Promise<LayoutResult> {
    const n = nodes.length;

    if (n === 0) {
      return { positions: new Map(), edges: [], bounds: new THREE.Box3() };
    }

    const idIndex = new Map(nodes.map((nd, i) => [nd.id, i]));

    // Build adjacency matrix and degree vector
    const adjacency: number[][] = Array.from({ length: n }, () =>
      new Array(n).fill(0),
    );
    const degree = new Float64Array(n);

    for (const node of nodes) {
      const i = idIndex.get(node.id)!;
      for (const conn of node.connections) {
        const j = idIndex.get(conn);
        if (j !== undefined) {
          adjacency[i][j] = 1;
          adjacency[j][i] = 1;
          degree[i]++;
        }
      }
    }

    // Laplacian L = D - A
    const laplacian: number[][] = Array.from({ length: n }, (_, i) =>
      Array.from({ length: n }, (_, j) =>
        (i === j ? degree[i] : 0) - adjacency[i][j],
      ),
    );

    // Compute eigenvectors via power iteration with deflation
    const eigenvectors = this.powerIteration(laplacian, n, EIGENVECTOR_COUNT);

    // Use eigenvectors 1, 2, 3 (skip trivial eigenvector 0)
    const positions = new Map<string, THREE.Vector3>();
    for (const node of nodes) {
      const i = idIndex.get(node.id)!;
      const x = (eigenvectors[1]?.[i] ?? 0) * POSITION_SCALE;
      const y = (eigenvectors[2]?.[i] ?? 0) * POSITION_SCALE;
      const z = (eigenvectors[3]?.[i] ?? 0) * POSITION_SCALE;
      positions.set(node.id, new THREE.Vector3(x, y, z));
    }

    const bounds = new THREE.Box3();
    if (positions.size > 0) {
      bounds.setFromPoints([...positions.values()]);
    }

    return { positions, edges: buildEdges(nodes), bounds };
  }

  /**
   * Simplified power iteration with deflation to extract the smallest
   * eigenvectors of a symmetric matrix. For production graphs >500 nodes,
   * a Lanczos / ARPACK-style solver is recommended.
   */
  private powerIteration(
    matrix: number[][],
    n: number,
    count: number,
  ): number[][] {
    if (n <= 1) {
      return Array.from({ length: count }, () => [0]);
    }

    const eigenvectors: number[][] = [];

    // We need smallest eigenvectors; shift the matrix: M' = maxEig*I - M
    // then largest eigenvectors of M' correspond to smallest of M.
    const maxEig = this.estimateMaxEigenvalue(matrix, n);
    const shifted: number[][] = Array.from({ length: n }, (_, i) =>
      Array.from({ length: n }, (_, j) =>
        (i === j ? maxEig : 0) - matrix[i][j],
      ),
    );

    for (let ev = 0; ev < count; ev++) {
      // Random initial vector
      let v = new Float64Array(n);
      for (let i = 0; i < n; i++) v[i] = Math.random() - 0.5;

      // Orthogonalise against previously found eigenvectors
      for (const prev of eigenvectors) {
        const dot = this.dot(v, prev, n);
        for (let i = 0; i < n; i++) v[i] -= dot * prev[i];
      }
      this.normalize(v, n);

      for (let iter = 0; iter < POWER_ITERATIONS; iter++) {
        // Multiply: w = shifted * v
        const w = new Float64Array(n);
        for (let i = 0; i < n; i++) {
          let sum = 0;
          for (let j = 0; j < n; j++) {
            sum += shifted[i][j] * v[j];
          }
          w[i] = sum;
        }

        // Orthogonalise against previous eigenvectors
        for (const prev of eigenvectors) {
          const d = this.dot(w, prev, n);
          for (let i = 0; i < n; i++) w[i] -= d * prev[i];
        }

        this.normalize(w, n);
        v = w;
      }

      eigenvectors.push(Array.from(v));
    }

    return eigenvectors;
  }

  private estimateMaxEigenvalue(matrix: number[][], n: number): number {
    // Gershgorin circle estimate
    let maxEig = 0;
    for (let i = 0; i < n; i++) {
      let rowSum = 0;
      for (let j = 0; j < n; j++) {
        if (i !== j) rowSum += Math.abs(matrix[i][j]);
      }
      maxEig = Math.max(maxEig, matrix[i][i] + rowSum);
    }
    return maxEig + 1; // small margin
  }

  private dot(a: Float64Array, b: number[] | Float64Array, n: number): number {
    let sum = 0;
    for (let i = 0; i < n; i++) sum += a[i] * (b[i] ?? 0);
    return sum;
  }

  private normalize(v: Float64Array, n: number): void {
    let norm = 0;
    for (let i = 0; i < n; i++) norm += v[i] * v[i];
    norm = Math.sqrt(norm) || 1;
    for (let i = 0; i < n; i++) v[i] /= norm;
  }

  async updateNode(): Promise<THREE.Vector3> {
    // Spectral layout requires full recomputation
    return new THREE.Vector3();
  }

  dispose(): void {
    /* nothing to clean up */
  }
}
