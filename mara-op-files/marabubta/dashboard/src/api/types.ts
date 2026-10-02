// Marabunta - Licensed under the MIT License.
// ── Node Types ──

export type NodeStatus = 'online' | 'idle' | 'offline';

export type RoleName =
  | 'AGGREGATOR'
  | 'WITNESS'
  | 'GATEWAY'
  | 'RELAY'
  | 'STORAGE'
  | 'EPHEMERAL';

export interface NodeRole {
  role: RoleName;
  pinned: boolean;
  pinned_by?: string;
  pinned_at?: string;
}

export interface HardwareInfo {
  os: string;
  cpu: string;
  cpu_cores: number;
  ram_total_gb: number;
  ram_used_gb: number;
  disk_total_gb: number;
  disk_free_gb: number;
  gpu?: string;
  gpu_vram_gb?: number;
}

export interface DetectedSoftware {
  name: string;
  version: string;
  port: number;
  status: 'active' | 'inactive' | 'error';
  tier_role?: string;
}

export interface Node {
  id: string;
  name: string;
  status: NodeStatus;
  roles: NodeRole[];
  hardware: HardwareInfo;
  software: DetectedSoftware[];
  uptime_seconds: number;
  last_seen: string;
  gossip_connections: string[];
  is_training: boolean;
  is_pgwire_active: boolean;
  chaos_state: {
    is_active: boolean;
    current_death?: any;
  };
}

export interface NodeList {
  nodes: Node[];
  total: number;
  page: number;
  per_page: number;
}

export interface NodeDetailResponse extends Node {
  recipes: DeployableRecipe[];
}

export interface DeployableRecipe {
  id: string;
  name: string;
  description: string;
  yaml_preview: string;
  requires_auth: boolean;
}

// ── Role Distribution ──

export interface RoleCount {
  role: RoleName;
  count: number;
}

export type RoleDistribution = RoleCount[];

// ── Storage Types ──

export type TierHealth = 'healthy' | 'degraded' | 'critical';

export interface Tier1Database {
  name: string;
  node_id: string;
  node_name: string;
  status: TierHealth;
  connections_active: number;
  connections_max: number;
  tier_role: string;
}

export interface Tier2Container {
  name: string;
  node_id: string;
  node_name: string;
  cpu_percent: number;
  ram_used_gb: number;
  ram_limit_gb: number;
}

export interface Tier3CDE {
  replication_factor: number;
  fragment_count: number;
  distribution_percent: number;
  latency_p50_ms: number;
  latency_p95_ms: number;
  latency_p99_ms: number;
}

export interface StorageTier {
  tier: 1 | 2 | 3;
  health: TierHealth;
  databases?: Tier1Database[];
  containers?: Tier2Container[];
  cde?: Tier3CDE;
}

export interface StorageTiersResponse {
  tiers: StorageTier[];
}

// ── Audit Types ──

export type Criticality = 'INFO' | 'WARN' | 'AUDIT' | 'ALERT' | 'CRITICAL';

export interface AuditEvent {
  event_id: string;
  action: string;
  actor: string;
  node_id: string | null;
  node_name: string | null;
  criticality: Criticality;
  witness_verified: boolean;
  witness_count: number;
  timestamp: string;
  details?: Record<string, unknown>;
}

export interface AuditEventList {
  events: AuditEvent[];
  total: number;
  page: number;
  per_page: number;
}

export interface AuditQueryParams {
  page: number;
  per_page: number;
  node_id?: string;
  actor?: string;
  action?: string;
  criticality?: Criticality;
  from_date?: string;
  to_date?: string;
}

export interface NodeFilters {
  status?: NodeStatus[];
  roles?: RoleName[];
  os?: string[];
  search?: string;
  sort_by?: string;
  sort_dir?: 'asc' | 'desc';
}

// ── WebSocket Types ──

export type WsMessage =
  | {
      type: 'node_status';
      node_id: string;
      status: NodeStatus;
      roles: string[];
      timestamp: string;
    }
  | {
      type: 'audit_event';
      event_id: string;
      action: string;
      actor: string;
      node_id: string | null;
      criticality: Criticality;
      witness_verified: boolean;
      witness_count: number;
      timestamp: string;
    }
  | {
      type: 'storage_update';
      tier: 1 | 2 | 3;
      health: TierHealth;
      details: Record<string, unknown>;
      timestamp: string;
    }
  | {
      type: 'metric_update';
      metric: string;
      value: number;
      previous: number;
      timestamp: string;
    }
  | {
      type: 'heartbeat';
      server_time: string;
    };

export type WsState = 'connecting' | 'connected' | 'reconnecting' | 'disconnected';

// ── Config Types ──

export type ConfigTier = 'hardwired' | 'startup' | 'runtime';
export type UiVisibility = 'visible_mutable' | 'visible_readonly' | 'hidden';

export interface ConfigSchemaEntry {
  key: string;
  tier: ConfigTier;
  ui_visibility: UiVisibility;
  category: string;
  description: string;
  restart_required: boolean;
  compliance_tags: string[];
  value_type: string;
  constraints: Record<string, unknown>;
  default_value: unknown;
}

export interface ConfigSchemaResponse {
  entries: ConfigSchemaEntry[];
}

export interface ConfigFullResponse {
  config: Record<string, unknown>;
}

export interface ConfigHistoryEntry {
  id: number;
  key: string;
  old_value: unknown;
  new_value: unknown;
  changed_by: string;
  source: string;
  changed_at: string;
  row_hash: string;
}

export interface ConfigHistoryResponse {
  entries: ConfigHistoryEntry[];
}

// ── Compliance Types ──

export type ControlStatus = 'passing' | 'warning' | 'violation' | 'unknown';

export interface ComplianceControl {
  key: string;
  status: ControlStatus;
  description?: string;
  expected?: unknown;
  actual?: unknown;
  compliance_tags?: string[];
}

export interface CompliancePostureResponse {
  overall_status: ControlStatus;
  controls: ComplianceControl[];
  active_profiles: string[];
  evaluated_at: string;
}

export interface ComplianceManifestResponse {
  profiles: Array<{
    name: string;
    version: string;
    description: string;
    controls_count: number;
  }>;
  binary_features: string[];
}

export interface ComplianceViolation {
  id: number;
  control_key: string;
  severity: string;
  detected_at: string;
  resolved_at?: string;
  details: Record<string, unknown>;
}

export interface ComplianceViolationsResponse {
  violations: ComplianceViolation[];
}

// ── Time-Travel Types ──

export interface TimeTravelBookmark {
  label: string;
  timestamp: string;
  createdAt: string;
}
