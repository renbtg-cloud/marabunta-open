// Marabunta - Licensed under the MIT License.
// ── ExportManager ──
// Exports decomposed data to CSV, JSON, or Parquet. CSV and JSON are
// generated client-side. Parquet uses a server-side endpoint. The exported
// data includes the full decomposition path for each leaf node so the
// export is self-documenting (e.g. "Salary R$1.8M > Engineering R$720K
// > Backend R$320K > Alice R$95K").

import type {
  DecompositionTree,
  TreeNode,
  DecomposeOperation,
} from './DecompositionTree';
import { getNodeLabel, extractValue } from './DecompositionTree';

// ── Types ──

export type ExportFormat = 'csv' | 'json' | 'parquet';

export interface ExportRow {
  path: string;
  depth: number;
  operation: DecomposeOperation;
  value: number | null;
  label: string;
}

// ── Tree Flattening ──

/** Walk the decomposition tree and produce a flat array of rows.
 *  Each row represents a leaf node with its full path from root. */
export function flattenTreeToRows(tree: DecompositionTree): ExportRow[] {
  const rows: ExportRow[] = [];

  function walk(node: TreeNode, pathParts: string[]): void {
    const label = getNodeLabel(node);
    const currentPath = [...pathParts, label];

    if (node.children.length === 0) {
      // Leaf node: emit a row
      rows.push({
        path: currentPath.join(' > '),
        depth: node.depth,
        operation: node.operation,
        value: extractValue(node.data),
        label,
      });
    } else {
      // Internal node: recurse into children
      for (const child of node.children) {
        walk(child, currentPath);
      }
    }
  }

  walk(tree.root, []);
  return rows;
}

// ── CSV Serialization ──

/** Escape a CSV field: wrap in double quotes if it contains commas,
 *  newlines, or double quotes. Double quotes are escaped by doubling. */
function escapeCSVField(field: string): string {
  if (/[",\n\r]/.test(field)) {
    return `"${field.replace(/"/g, '""')}"`;
  }
  return field;
}

/** Convert rows to CSV string with header row */
export function rowsToCSV(rows: ExportRow[]): string {
  const header = 'path,depth,operation,value,label';
  const lines = rows.map(
    (r) =>
      [
        escapeCSVField(r.path),
        String(r.depth),
        escapeCSVField(r.operation),
        r.value !== null ? String(r.value) : '',
        escapeCSVField(r.label),
      ].join(','),
  );
  return [header, ...lines].join('\n');
}

// ── JSON Serialization ──

/** Convert tree to a JSON-serializable structure preserving the tree shape */
function treeToJSON(tree: DecompositionTree): unknown {
  function nodeToJSON(node: TreeNode): unknown {
    return {
      id: node.id,
      cubeId: node.cubeId,
      label: getNodeLabel(node),
      operation: node.operation,
      depth: node.depth,
      value: extractValue(node.data),
      children: node.children.map(nodeToJSON),
    };
  }

  return {
    exportedAt: new Date().toISOString(),
    maxDepth: tree.maxDepth,
    root: nodeToJSON(tree.root),
    flatRows: flattenTreeToRows(tree),
  };
}

// ── Download Helper ──

function downloadBlob(
  content: string | Blob,
  filename: string,
  mime: string,
): void {
  const blob =
    content instanceof Blob
      ? content
      : new Blob([content], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

// ── Main Export Function ──

/** Export the decomposition tree in the specified format.
 *  CSV and JSON are generated client-side.
 *  Parquet delegates to the server-side endpoint. */
export async function exportDecomposition(
  tree: DecompositionTree,
  format: ExportFormat,
  filename?: string,
): Promise<void> {
  const name = filename ?? `decomposition-${Date.now()}`;

  switch (format) {
    case 'csv': {
      const rows = flattenTreeToRows(tree);
      const csv = rowsToCSV(rows);
      downloadBlob(csv, `${name}.csv`, 'text/csv');
      break;
    }

    case 'json': {
      const jsonData = treeToJSON(tree);
      const json = JSON.stringify(jsonData, null, 2);
      downloadBlob(json, `${name}.json`, 'application/json');
      break;
    }

    case 'parquet': {
      // Server-side Parquet generation
      const rows = flattenTreeToRows(tree);
      const resp = await fetch('/api/v1/export/parquet', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ rows }),
      });
      if (!resp.ok) {
        throw new Error(`Parquet export failed with status ${resp.status}`);
      }
      const blob = await resp.blob();
      downloadBlob(blob, `${name}.parquet`, 'application/vnd.apache.parquet');
      break;
    }
  }
}
