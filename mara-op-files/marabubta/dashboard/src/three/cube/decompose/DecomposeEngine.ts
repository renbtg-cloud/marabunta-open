// Marabunta - Licensed under the MIT License.
// ── DecomposeEngine ──
// API client for all decomposition operations. Sends REST requests to the
// W4A workflow API and returns typed results for SUM, AVG, COUNT breakdown,
// population average comparisons, time-series trends, and group-by queries.

import type {
  AggregateType,
  DecomposeResult,
  DistributionResult,
  CountResult,
  TrendPoint,
  CompareData,
  GroupByCluster,
  DecomposedItem,
} from './DecompositionTree';

// ── Error ──

export class DecomposeError extends Error {
  constructor(
    public readonly status: number,
    message?: string,
  ) {
    super(message ?? `Decomposition request failed with status ${status}`);
    this.name = 'DecomposeError';
  }
}

// ── Internal helpers ──

async function fetchJSON<T>(url: string): Promise<T> {
  const resp = await fetch(url);
  if (!resp.ok) throw new DecomposeError(resp.status);
  return resp.json() as Promise<T>;
}

async function postJSON<T>(url: string, body: unknown): Promise<T> {
  const resp = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!resp.ok) throw new DecomposeError(resp.status);
  return resp.json() as Promise<T>;
}

// ── SUM Decomposition ──

export async function decomposeSUM(
  cubeId: string,
  faceIndex: number,
): Promise<DecomposeResult> {
  return fetchJSON<DecomposeResult>(
    `/api/v1/cubes/${encodeURIComponent(cubeId)}/decompose?face=${faceIndex}&type=SUM`,
  );
}

// ── AVG Decomposition ──

export async function decomposeAVG(
  cubeId: string,
  faceIndex: number,
): Promise<DistributionResult> {
  return fetchJSON<DistributionResult>(
    `/api/v1/cubes/${encodeURIComponent(cubeId)}/decompose?face=${faceIndex}&type=AVG`,
  );
}

// ── COUNT Decomposition ──

export async function decomposeCOUNT(
  cubeId: string,
  faceIndex: number,
): Promise<CountResult> {
  return fetchJSON<CountResult>(
    `/api/v1/cubes/${encodeURIComponent(cubeId)}/decompose?face=${faceIndex}&type=COUNT`,
  );
}

// ── Population Average (Compare) ──

export async function fetchPopulationAverage(
  cubeId: string,
  faceIndex: number,
): Promise<CompareData> {
  return fetchJSON<CompareData>(
    `/api/v1/cubes/${encodeURIComponent(cubeId)}/population-avg?face=${faceIndex}`,
  );
}

// ── Trend (Time Series) ──

export async function fetchTrend(
  cubeId: string,
  faceIndex: number,
  opts?: { points?: number },
): Promise<TrendPoint[]> {
  const pts = opts?.points ?? 60;
  return fetchJSON<TrendPoint[]>(
    `/api/v1/cubes/${encodeURIComponent(cubeId)}/trend?face=${faceIndex}&points=${pts}`,
  );
}

// ── Group By ──

export async function fetchGroupBy(
  cubeId: string,
  dimension: string,
): Promise<GroupByCluster[]> {
  return fetchJSON<GroupByCluster[]>(
    `/api/v1/cubes/${encodeURIComponent(cubeId)}/group-by?dimension=${encodeURIComponent(dimension)}`,
  );
}

// ── Available Group-By Dimensions ──

export async function fetchAvailableDimensions(
  cubeId: string,
): Promise<string[]> {
  return fetchJSON<string[]>(
    `/api/v1/cubes/${encodeURIComponent(cubeId)}/dimensions`,
  );
}

// ── Determine aggregate type for a face ──

/** Given face data, determine whether it is SUM, AVG, COUNT, or RAW */
export function detectAggregateType(
  faceIndex: number,
  cubeId: string,
  faceMetadata?: Record<number, AggregateType>,
): AggregateType {
  if (faceMetadata && faceIndex in faceMetadata) {
    return faceMetadata[faceIndex];
  }
  // Default heuristic: faces 0-1 are often SUM, 2 is COUNT, others RAW
  // In production this would come from the cube's metadata endpoint
  return 'RAW';
}

// ── Unified decompose dispatcher ──

export type DecomposeAnyResult =
  | { type: 'SUM'; data: DecomposeResult }
  | { type: 'AVG'; data: DistributionResult }
  | { type: 'COUNT'; data: CountResult };

export async function decomposeByType(
  cubeId: string,
  faceIndex: number,
  aggregateType: AggregateType,
): Promise<DecomposeAnyResult> {
  switch (aggregateType) {
    case 'SUM': {
      const data = await decomposeSUM(cubeId, faceIndex);
      return { type: 'SUM', data };
    }
    case 'AVG': {
      const data = await decomposeAVG(cubeId, faceIndex);
      return { type: 'AVG', data };
    }
    case 'COUNT': {
      const data = await decomposeCOUNT(cubeId, faceIndex);
      return { type: 'COUNT', data };
    }
    default:
      throw new DecomposeError(400, `Cannot decompose aggregate type: ${aggregateType}`);
  }
}
