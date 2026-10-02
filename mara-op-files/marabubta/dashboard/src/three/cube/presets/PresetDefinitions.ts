// Marabunta - Licensed under the MIT License.
// ── Preset Definitions ──
// All 7 dimension presets as fully typed objects (W6A / Spec S21)
// Single source of truth for what each preset shows on the cube faces.

import type { PresetDefinition } from './types';
import { PresetName } from './types';

// ── Operational ──

const OPERATIONAL: PresetDefinition = {
  name: PresetName.Operational,
  label: 'Operational',
  description: 'Workflow state, actors, audit trail, context, infra, timers',
  icon: 'activity',
  shortcut: '1',
  faces: {
    front:  { metricId: 'state-transitions',  label: 'State + Transitions',
              description: 'Current state and available transitions',
              renderer: 'StateMachineRenderer', accent: '#22d3ee' },
    top:    { metricId: 'actors-roles',        label: 'Actors + Roles',
              description: 'Assigned actors and their current roles',
              renderer: 'ActorGridRenderer',    accent: '#a78bfa' },
    right:  { metricId: 'audit-trail',         label: 'Audit Trail',
              description: 'Chronological event log for this instance',
              renderer: 'TimelineRenderer',     accent: '#34d399' },
    back:   { metricId: 'context-data',        label: 'Context Data',
              description: 'Workflow payload and environment variables',
              renderer: 'JsonTreeRenderer',     accent: '#60a5fa' },
    left:   { metricId: 'infrastructure',      label: 'Infrastructure',
              description: 'Node placement, resource usage, network path',
              renderer: 'InfraMapRenderer',     accent: '#fbbf24' },
    bottom: { metricId: 'timers',              label: 'Timers',
              description: 'Active timers, deadlines, and SLA countdowns',
              renderer: 'TimerBarRenderer',     accent: '#fb7185' },
  },
};

// ── Risk ──

const RISK: PresetDefinition = {
  name: PresetName.Risk,
  label: 'Risk',
  description: 'Risk score, compliance, rejections, violations, anomalies, escalation',
  icon: 'shield-alert',
  shortcut: '2',
  faces: {
    front:  { metricId: 'risk-score',          label: 'Risk Score',
              description: 'Composite risk score with color gradient',
              renderer: 'GradientScoreRenderer', accent: '#fb7185' },
    top:    { metricId: 'compliance-flags',    label: 'Compliance Flags',
              description: 'Active compliance violations and warnings',
              renderer: 'FlagGridRenderer',      accent: '#fbbf24' },
    right:  { metricId: 'rejection-history',   label: 'Rejection History',
              description: 'Past rejections with reasons and timestamps',
              renderer: 'TimelineRenderer',      accent: '#fb7185' },
    back:   { metricId: 'policy-violations',   label: 'Policy Violations',
              description: 'Triggered policy rules and severity levels',
              renderer: 'ViolationListRenderer', accent: '#f43f5e' },
    left:   { metricId: 'anomaly-indicators',  label: 'Anomaly Indicators',
              description: 'Statistical anomalies in behavior patterns',
              renderer: 'AnomalyChartRenderer',  accent: '#a78bfa' },
    bottom: { metricId: 'escalation-path',     label: 'Escalation Path',
              description: 'Current escalation level and next responders',
              renderer: 'EscalationTreeRenderer', accent: '#fbbf24' },
  },
};

// ── Performance ──

const PERFORMANCE: PresetDefinition = {
  name: PresetName.Performance,
  label: 'Performance',
  description: 'State time, completion, bottlenecks, SLA, response, queue depth',
  icon: 'gauge',
  shortcut: '3',
  faces: {
    front:  { metricId: 'time-in-state',       label: 'Time in Current State',
              description: 'Duration in the current workflow state',
              renderer: 'DurationGaugeRenderer', accent: '#22d3ee',
              unit: 'ms' },
    top:    { metricId: 'avg-completion',       label: 'Avg Completion Time',
              description: 'Moving average completion across recent instances',
              renderer: 'SparklineRenderer',     accent: '#34d399',
              unit: 'ms' },
    right:  { metricId: 'bottleneck-score',    label: 'Bottleneck Score',
              description: 'Computed bottleneck probability for this state',
              renderer: 'HeatScoreRenderer',     accent: '#fbbf24' },
    back:   { metricId: 'sla-status',          label: 'SLA Status',
              description: 'SLA compliance: green/amber/red with time remaining',
              renderer: 'SlaGaugeRenderer',      accent: '#34d399' },
    left:   { metricId: 'node-response-time',  label: 'Node Response Time',
              description: 'p50/p95/p99 response times for the assigned node',
              renderer: 'PercentilesRenderer',   accent: '#60a5fa',
              unit: 'ms' },
    bottom: { metricId: 'queue-depth',         label: 'Queue Depth',
              description: 'Items waiting in the processing queue',
              renderer: 'BarChartRenderer',      accent: '#a78bfa' },
  },
};

// ── Financial ──

const FINANCIAL: PresetDefinition = {
  name: PresetName.Financial,
  label: 'Financial',
  description: 'Total amount, budget, approvals, cost centers, currency, forecast',
  icon: 'banknote',
  shortcut: '4',
  faces: {
    front:  { metricId: 'total-amount',        label: 'Total Amount',
              description: 'Principal value of this workflow instance',
              renderer: 'CurrencyRenderer',      accent: '#34d399',
              unit: 'USD' },
    top:    { metricId: 'budget-vs-actual',    label: 'Budget vs Actual',
              description: 'Planned budget against actual expenditure',
              renderer: 'DualBarRenderer',       accent: '#fbbf24' },
    right:  { metricId: 'approval-chain',      label: 'Approval Chain',
              description: 'Sequential approvers with status indicators',
              renderer: 'ChainRenderer',         accent: '#a78bfa' },
    back:   { metricId: 'cost-center',         label: 'Cost Center Breakdown',
              description: 'Allocation across cost centers and GL codes',
              renderer: 'PieChartRenderer',      accent: '#60a5fa' },
    left:   { metricId: 'currency-exposure',   label: 'Currency Exposure',
              description: 'Multi-currency positions and FX risk',
              renderer: 'ExposureMapRenderer',   accent: '#fb7185' },
    bottom: { metricId: 'forecast-impact',     label: 'Forecast Impact',
              description: 'Projected impact on quarterly financial forecast',
              renderer: 'ForecastLineRenderer',  accent: '#22d3ee' },
  },
};

// ── Audit ──

const AUDIT: PresetDefinition = {
  name: PresetName.Audit,
  label: 'Audit',
  description: 'Latest event, witnesses, signatures, criticality, tamper, reconstruction',
  icon: 'file-search',
  shortcut: '5',
  faces: {
    front:  { metricId: 'latest-event',        label: 'Latest Event',
              description: 'Most recent audit event with full detail',
              renderer: 'EventDetailRenderer',   accent: '#22d3ee' },
    top:    { metricId: 'witness-map',         label: 'Witness Map',
              description: 'Nodes that witnessed and co-signed this event',
              renderer: 'WitnessGridRenderer',   accent: '#a78bfa' },
    right:  { metricId: 'signature-chain',     label: 'Signature Chain',
              description: 'Cryptographic signature chain with verification status',
              renderer: 'ChainRenderer',         accent: '#34d399' },
    back:   { metricId: 'criticality-timeline', label: 'Criticality Timeline',
              description: 'Time-series of criticality level changes',
              renderer: 'TimelineRenderer',      accent: '#fbbf24' },
    left:   { metricId: 'tamper-check',        label: 'Tamper Check Status',
              description: 'Integrity verification result for all fragments',
              renderer: 'IntegrityRenderer',     accent: '#fb7185' },
    bottom: { metricId: 'reconstruction-health', label: 'Reconstruction Health',
              description: 'CDE fragment availability and reconstruction capability',
              renderer: 'HealthBarRenderer',     accent: '#34d399' },
  },
};

// ── Hedge Fund ──

const HEDGE_FUND: PresetDefinition = {
  name: PresetName.HedgeFund,
  label: 'Hedge Fund',
  description: 'Position, Greeks, P&L, risk limits, counterparty, liquidity',
  icon: 'trending-up',
  shortcut: '6',
  faces: {
    front:  { metricId: 'position-value',      label: 'Position Value',
              description: 'Current mark-to-market position value',
              renderer: 'CurrencyRenderer',      accent: '#34d399',
              unit: 'USD' },
    top:    { metricId: 'greeks',              label: 'Greeks',
              description: 'Delta, Gamma, Theta, Vega for this position',
              renderer: 'GreeksQuadRenderer',    accent: '#a78bfa' },
    right:  { metricId: 'pnl-attribution',     label: 'P&L Attribution',
              description: 'Breakdown of P&L by factor: delta, gamma, vega, theta, residual',
              renderer: 'WaterfallRenderer',     accent: '#22d3ee' },
    back:   { metricId: 'risk-limits',         label: 'Risk Limits',
              description: 'VaR, stress test results, limit utilization',
              renderer: 'LimitGaugeRenderer',    accent: '#fb7185' },
    left:   { metricId: 'counterparty-exposure', label: 'Counterparty Exposure',
              description: 'Exposure by counterparty with credit quality',
              renderer: 'ExposureMapRenderer',   accent: '#fbbf24' },
    bottom: { metricId: 'liquidity-score',     label: 'Liquidity Score',
              description: 'Position liquidity score based on volume and bid-ask spread',
              renderer: 'LiquidityGaugeRenderer', accent: '#60a5fa' },
  },
};

// ── Custom (Template) ──

const CUSTOM_TEMPLATE: PresetDefinition = {
  name: PresetName.Custom,
  label: 'Custom',
  description: 'User-defined: drag any metric to any face slot',
  icon: 'palette',
  shortcut: '7',
  faces: {
    front:  { metricId: 'placeholder', label: 'Drop Metric Here',
              description: 'Drag a metric from the sidebar',
              renderer: 'PlaceholderRenderer', accent: '#64748b' },
    top:    { metricId: 'placeholder', label: 'Drop Metric Here',
              description: 'Drag a metric from the sidebar',
              renderer: 'PlaceholderRenderer', accent: '#64748b' },
    right:  { metricId: 'placeholder', label: 'Drop Metric Here',
              description: 'Drag a metric from the sidebar',
              renderer: 'PlaceholderRenderer', accent: '#64748b' },
    back:   { metricId: 'placeholder', label: 'Drop Metric Here',
              description: 'Drag a metric from the sidebar',
              renderer: 'PlaceholderRenderer', accent: '#64748b' },
    left:   { metricId: 'placeholder', label: 'Drop Metric Here',
              description: 'Drag a metric from the sidebar',
              renderer: 'PlaceholderRenderer', accent: '#64748b' },
    bottom: { metricId: 'placeholder', label: 'Drop Metric Here',
              description: 'Drag a metric from the sidebar',
              renderer: 'PlaceholderRenderer', accent: '#64748b' },
  },
};

// ── Exports ──

/** All built-in presets, indexed by name */
export const PRESETS: Readonly<Record<PresetName, PresetDefinition>> = {
  [PresetName.Operational]: OPERATIONAL,
  [PresetName.Risk]:        RISK,
  [PresetName.Performance]: PERFORMANCE,
  [PresetName.Financial]:   FINANCIAL,
  [PresetName.Audit]:       AUDIT,
  [PresetName.HedgeFund]:   HEDGE_FUND,
  [PresetName.Custom]:      CUSTOM_TEMPLATE,
};

/** Ordered list for toolbar rendering */
export const PRESET_ORDER: readonly PresetName[] = [
  PresetName.Operational,
  PresetName.Risk,
  PresetName.Performance,
  PresetName.Financial,
  PresetName.Audit,
  PresetName.HedgeFund,
  PresetName.Custom,
];
