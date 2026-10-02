// Marabunta - Licensed under the MIT License.
import { useQuery } from '@tanstack/react-query';
import { api } from '../api/client';
import type {
  NodeList,
  NodeDetailResponse,
  RoleDistribution,
  StorageTiersResponse,
  AuditEventList,
  AuditQueryParams,
  NodeFilters,
  ConfigSchemaResponse,
  CompliancePostureResponse,
} from '../api/types';

function filtersToParams(filters?: NodeFilters): Record<string, string> {
  if (!filters) return {};
  const params: Record<string, string> = {};
  if (filters.status?.length) params.status = filters.status.join(',');
  if (filters.roles?.length) params.roles = filters.roles.join(',');
  if (filters.os?.length) params.os = filters.os.join(',');
  if (filters.search) params.search = filters.search;
  if (filters.sort_by) params.sort_by = filters.sort_by;
  if (filters.sort_dir) params.sort_dir = filters.sort_dir;
  return params;
}

function auditParamsToRecord(params: AuditQueryParams): Record<string, string> {
  const record: Record<string, string> = {
    page: String(params.page),
    per_page: String(params.per_page),
  };
  if (params.node_id) record.node_id = params.node_id;
  if (params.actor) record.actor = params.actor;
  if (params.action) record.action = params.action;
  if (params.criticality) record.criticality = params.criticality;
  if (params.from_date) record.from_date = params.from_date;
  if (params.to_date) record.to_date = params.to_date;
  return record;
}

export function useNodes(page: number, perPage: number, filters?: NodeFilters) {
  return useQuery({
    queryKey: ['nodes', page, perPage, filters],
    queryFn: () =>
      api.get<NodeList>('/api/v1/nodes', {
        page: String(page),
        per_page: String(perPage),
        ...filtersToParams(filters),
      }),
    staleTime: 10_000,
    refetchInterval: 30_000,
  });
}

export function useNodeDetail(nodeId: string) {
  return useQuery({
    queryKey: ['node', nodeId],
    queryFn: () => api.get<NodeDetailResponse>(`/api/v1/nodes/${nodeId}`),
    staleTime: 5_000,
    enabled: !!nodeId,
  });
}

export function useNodeSoftware(nodeId: string) {
  return useQuery({
    queryKey: ['node', nodeId, 'software'],
    queryFn: () =>
      api.get<{ software: Array<{ name: string; version: string; port: number; status: string; tier_role?: string }> }>(
        `/api/v1/nodes/${nodeId}/software`,
      ),
    staleTime: 10_000,
    enabled: !!nodeId,
  });
}

export function useRoleDistribution() {
  return useQuery({
    queryKey: ['roles', 'distribution'],
    queryFn: () => api.get<RoleDistribution>('/api/v1/roles/distribution'),
    staleTime: 15_000,
  });
}

export function useStorageTiers() {
  return useQuery({
    queryKey: ['storage', 'tiers'],
    queryFn: () => api.get<StorageTiersResponse>('/api/v1/storage/tiers'),
    staleTime: 10_000,
    refetchInterval: 30_000,
  });
}

export function useAuditEvents(params: AuditQueryParams) {
  return useQuery({
    queryKey: ['audit', params],
    queryFn: () =>
      api.get<AuditEventList>(
        '/api/v1/audit/events',
        auditParamsToRecord(params),
      ),
    staleTime: 5_000,
  });
}

export function useConfigSchema() {
  return useQuery({
    queryKey: ['config', 'schema'],
    queryFn: () => api.get<ConfigSchemaResponse>('/api/v1/config/schema'),
    staleTime: 60_000,
  });
}

export function useCompliancePosture() {
  return useQuery({
    queryKey: ['compliance', 'posture'],
    queryFn: () =>
      api.get<CompliancePostureResponse>('/api/v1/compliance/posture'),
    staleTime: 15_000,
    refetchInterval: 30_000,
  });
}
