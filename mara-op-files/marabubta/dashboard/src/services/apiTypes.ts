// Marabunta - Licensed under the MIT License.
// marabunta-compute/dashboard/src/services/apiTypes.ts
// In a real app, these would be generated from the backend spec (e.g., OpenAPI/gRPC)

export interface CheckResult {
  step: string;
  passed: boolean;
  detail: string;
}

export interface BlindAuditEventSummary {
  sequence: number;
  timestamp: string;
  kind: string;
  signed: boolean;
}

// Swarm Psyche types from original management-ui
export type PsycheLevel = 1 | 2 | 3 | 4 | 5; // 1: Critical, 5: Thriving
export type PsycheTrend = 'rising' | 'falling' | 'stable' | '';

export interface PsycheFacet {
  level: PsycheLevel;
  score: number; // 0-100
  trend: PsycheTrend;
}

export interface SwarmPsyche {
  archetype: string;
  description: string;
  facets: {
    resilience: PsycheFacet;
    efficiency: PsycheFacet;
    coherence: PsycheFacet;
    vitality: PsycheFacet;
    intelligence: PsycheFacet;
    reach: PsycheFacet;
    integrity: PsycheFacet;
  };
}

// Zone/Region Health (derived from Constellation insights)
export type ZoneStatus = 'OPERATIONAL' | 'DEGRADED' | 'CRITICAL' | 'ISOLATED';
export interface ZoneHealth {
  id: string;
  name: string;
  status: ZoneStatus;
  activeNodes: number;
  criticalAlerts: number;
}
