// Marabunta - Licensed under the MIT License.
// ── DecompositionTree ──
// State manager for infinite drill-down. Maintains a tree of decomposition
// nodes where the root is the original cube and each child records the
// operation that created it, the data it represents, and its visual state.
// Collapsing back removes children and re-aggregates to the parent level.

import * as THREE from 'three';

// ── Types ──

export type DecomposeOperation =
  | 'ROOT'
  | 'SUM'
  | 'AVG'
  | 'COUNT'
  | 'GROUP_BY'
  | 'UNGROUP';

export type AggregateType = 'SUM' | 'AVG' | 'COUNT' | 'RAW';

export interface DecomposedItem {
  id: string;
  label: string;
  value: number;
  percentage: number; // 0..1
  subCubeId?: string; // for further drill-down
  color?: string;     // optional color override
}

export interface DecomposeResult {
  parentCubeId: string;
  faceIndex: number;
  aggregateType: AggregateType;
  items: DecomposedItem[];
  totalValue: number;
  canDrillDeeper: boolean;
}

export interface DistributionBin {
  low: number;
  high: number;
  count: number;
}

export interface DistributionResult {
  bins: DistributionBin[];
  mean: number;
  median: number;
  stdDev: number;
  min: number;
  max: number;
  outlierThreshold: number;
  outlierIndices: number[];
  totalCount: number;
}

export interface CountResult {
  items: DecomposedItem[];
  totalCount: number;
}

export interface TrendPoint {
  timestamp: number;
  value: number;
}

export interface CompareData {
  currentValue: number;
  populationAvg: number;
  sampleSize: number;
}

export interface FilterCriterion {
  field: string;
  operator: 'eq' | 'gt' | 'lt' | 'contains';
  value: string | number;
  sourceLabel: string;
}

export interface GroupByCluster {
  dimension: string;
  label: string;
  cubeIds: string[];
  aggregateValue: number;
}

// ── Tree Node ──

export interface TreeNode {
  id: string;
  cubeId: string;
  parentId: string | null;
  operation: DecomposeOperation;
  depth: number;
  data: DecomposeResult | DistributionResult | CountResult | DecomposedItem | null;
  children: TreeNode[];
  collapsed: boolean;
  worldPosition: THREE.Vector3;
}

// ── Tree Root Structure ──

export interface DecompositionTree {
  root: TreeNode;
  activeNodeId: string;
  maxDepth: number;
  breadcrumbs: TreeNode[];
}

// ── Manager ──

export class DecompositionTreeManager {
  private tree: DecompositionTree;

  constructor(rootCubeId: string, worldPosition?: THREE.Vector3) {
    this.tree = {
      root: {
        id: rootCubeId,
        cubeId: rootCubeId,
        parentId: null,
        operation: 'ROOT',
        depth: 0,
        data: null,
        children: [],
        collapsed: false,
        worldPosition: worldPosition?.clone() ?? new THREE.Vector3(),
      },
      activeNodeId: rootCubeId,
      maxDepth: 0,
      breadcrumbs: [],
    };
    this.updateBreadcrumbs();
  }

  /** Get the full tree state (read-only snapshot) */
  getTree(): Readonly<DecompositionTree> {
    return this.tree;
  }

  /** Get the currently active node */
  getActiveNode(): TreeNode | null {
    return this.findNode(this.tree.activeNodeId);
  }

  /** Set the active node by ID */
  setActiveNode(nodeId: string): void {
    if (this.findNode(nodeId)) {
      this.tree.activeNodeId = nodeId;
      this.updateBreadcrumbs();
    }
  }

  /** Decompose a parent node: add child nodes from the result */
  decompose(parentId: string, result: DecomposeResult): TreeNode[] {
    const parent = this.findNode(parentId);
    if (!parent) throw new Error(`Node ${parentId} not found in tree`);

    const children: TreeNode[] = result.items.map((item, i) => ({
      id: item.id,
      cubeId: item.subCubeId ?? item.id,
      parentId: parent.id,
      operation: result.aggregateType as DecomposeOperation,
      depth: parent.depth + 1,
      data: item,
      children: [],
      collapsed: false,
      worldPosition: new THREE.Vector3(),
    }));

    parent.children = children;
    parent.collapsed = false;
    this.tree.maxDepth = Math.max(this.tree.maxDepth, parent.depth + 1);
    this.tree.activeNodeId = parentId;
    this.updateBreadcrumbs();
    return children;
  }

  /** Collapse a node: remove all children and mark as collapsed */
  collapseBack(nodeId: string): void {
    const node = this.findNode(nodeId);
    if (!node) return;

    // Recursively clear children
    this.clearChildren(node);
    node.collapsed = true;

    // Navigate to parent if available
    this.tree.activeNodeId = node.parentId ?? node.id;
    this.recalculateMaxDepth();
    this.updateBreadcrumbs();
  }

  /** Collapse to a specific level: remove all descendants below the target node */
  collapseToNode(targetNodeId: string): void {
    const target = this.findNode(targetNodeId);
    if (!target) return;

    this.clearChildren(target);
    target.collapsed = true;
    this.tree.activeNodeId = targetNodeId;
    this.recalculateMaxDepth();
    this.updateBreadcrumbs();
  }

  /** Find a node by ID via depth-first search */
  findNode(id: string): TreeNode | null {
    return this.dfs(this.tree.root, id);
  }

  /** Get all leaf nodes in the tree */
  getLeaves(): TreeNode[] {
    const leaves: TreeNode[] = [];
    this.walkTree(this.tree.root, (node) => {
      if (node.children.length === 0) {
        leaves.push(node);
      }
    });
    return leaves;
  }

  /** Walk the entire tree, calling the visitor for each node */
  walkTree(node: TreeNode, visitor: (node: TreeNode) => void): void {
    visitor(node);
    for (const child of node.children) {
      this.walkTree(child, visitor);
    }
  }

  /** Get the total number of nodes in the tree */
  getNodeCount(): number {
    let count = 0;
    this.walkTree(this.tree.root, () => { count++; });
    return count;
  }

  // ── Private helpers ──

  private dfs(node: TreeNode, id: string): TreeNode | null {
    if (node.id === id) return node;
    for (const child of node.children) {
      const found = this.dfs(child, id);
      if (found) return found;
    }
    return null;
  }

  private clearChildren(node: TreeNode): void {
    for (const child of node.children) {
      this.clearChildren(child);
    }
    node.children = [];
  }

  private updateBreadcrumbs(): void {
    const crumbs: TreeNode[] = [];
    let current = this.findNode(this.tree.activeNodeId);
    while (current) {
      crumbs.unshift(current);
      current = current.parentId ? this.findNode(current.parentId) : null;
    }
    this.tree.breadcrumbs = crumbs;
  }

  private recalculateMaxDepth(): void {
    let max = 0;
    this.walkTree(this.tree.root, (node) => {
      if (node.depth > max) max = node.depth;
    });
    this.tree.maxDepth = max;
  }
}

// ── Utility ──

/** Extract a human-readable label from a tree node */
export function getNodeLabel(node: TreeNode): string {
  if (node.operation === 'ROOT') return 'Root';
  if (node.data && 'label' in node.data) {
    return (node.data as DecomposedItem).label;
  }
  return node.cubeId;
}

/** Extract the numeric value from node data if available */
export function extractValue(data: TreeNode['data']): number | null {
  if (!data) return null;
  if ('value' in data && typeof data.value === 'number') return data.value;
  if ('totalValue' in data) return (data as DecomposeResult).totalValue;
  if ('totalCount' in data) return (data as CountResult).totalCount;
  if ('mean' in data) return (data as DistributionResult).mean;
  return null;
}
