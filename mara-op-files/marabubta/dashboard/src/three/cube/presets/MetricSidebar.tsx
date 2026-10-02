// Marabunta - Licensed under the MIT License.
// ── Metric Sidebar ──
// Draggable metric sidebar for the Custom preset editor.
// Slides in from the right when Custom preset is active.
// All available metrics organized by category in an accordion.
// Metrics are draggable using the HTML5 Drag and Drop API.
// (W6A / Spec S21)

import React, { useState, useCallback } from 'react';
import type { MetricCategory, MetricEntry, MetricDragPayload } from './types';

// ── Full Metric Catalog (6 categories, 36 metrics) ──

export const METRIC_CATALOG: MetricCategory[] = [
  {
    id: 'operational',
    label: 'Operational',
    icon: 'activity',
    metrics: [
      { metricId: 'state-transitions',  label: 'State + Transitions',
        description: 'Current state and transition map',
        renderer: 'StateMachineRenderer', defaultAccent: '#22d3ee',
        category: 'operational' },
      { metricId: 'actors-roles',        label: 'Actors + Roles',
        description: 'Assigned actors and roles',
        renderer: 'ActorGridRenderer', defaultAccent: '#a78bfa',
        category: 'operational' },
      { metricId: 'audit-trail',         label: 'Audit Trail',
        description: 'Chronological audit events',
        renderer: 'TimelineRenderer', defaultAccent: '#34d399',
        category: 'operational' },
      { metricId: 'context-data',        label: 'Context Data',
        description: 'Workflow payload and env vars',
        renderer: 'JsonTreeRenderer', defaultAccent: '#60a5fa',
        category: 'operational' },
      { metricId: 'infrastructure',      label: 'Infrastructure',
        description: 'Node placement and resources',
        renderer: 'InfraMapRenderer', defaultAccent: '#fbbf24',
        category: 'operational' },
      { metricId: 'timers',              label: 'Timers',
        description: 'Active timers and deadlines',
        renderer: 'TimerBarRenderer', defaultAccent: '#fb7185',
        category: 'operational' },
    ],
  },
  {
    id: 'risk',
    label: 'Risk & Compliance',
    icon: 'shield-alert',
    metrics: [
      { metricId: 'risk-score',         label: 'Risk Score',
        description: 'Composite risk with color gradient',
        renderer: 'GradientScoreRenderer', defaultAccent: '#fb7185',
        category: 'risk' },
      { metricId: 'compliance-flags',   label: 'Compliance Flags',
        description: 'Active compliance violations',
        renderer: 'FlagGridRenderer', defaultAccent: '#fbbf24',
        category: 'risk' },
      { metricId: 'rejection-history',  label: 'Rejection History',
        description: 'Past rejections with reasons',
        renderer: 'TimelineRenderer', defaultAccent: '#fb7185',
        category: 'risk' },
      { metricId: 'policy-violations',  label: 'Policy Violations',
        description: 'Triggered policies and severity',
        renderer: 'ViolationListRenderer', defaultAccent: '#f43f5e',
        category: 'risk' },
      { metricId: 'anomaly-indicators', label: 'Anomaly Indicators',
        description: 'Statistical behavior anomalies',
        renderer: 'AnomalyChartRenderer', defaultAccent: '#a78bfa',
        category: 'risk' },
      { metricId: 'escalation-path',    label: 'Escalation Path',
        description: 'Escalation level and responders',
        renderer: 'EscalationTreeRenderer', defaultAccent: '#fbbf24',
        category: 'risk' },
    ],
  },
  {
    id: 'performance',
    label: 'Performance',
    icon: 'gauge',
    metrics: [
      { metricId: 'time-in-state',      label: 'Time in State',
        description: 'Duration in current state',
        renderer: 'DurationGaugeRenderer', defaultAccent: '#22d3ee',
        category: 'performance', unit: 'ms' },
      { metricId: 'avg-completion',     label: 'Avg Completion',
        description: 'Moving average completion time',
        renderer: 'SparklineRenderer', defaultAccent: '#34d399',
        category: 'performance', unit: 'ms' },
      { metricId: 'bottleneck-score',   label: 'Bottleneck Score',
        description: 'Computed bottleneck probability',
        renderer: 'HeatScoreRenderer', defaultAccent: '#fbbf24',
        category: 'performance' },
      { metricId: 'sla-status',         label: 'SLA Status',
        description: 'SLA compliance with time remaining',
        renderer: 'SlaGaugeRenderer', defaultAccent: '#34d399',
        category: 'performance' },
      { metricId: 'node-response-time', label: 'Node Response Time',
        description: 'p50/p95/p99 response times',
        renderer: 'PercentilesRenderer', defaultAccent: '#60a5fa',
        category: 'performance', unit: 'ms' },
      { metricId: 'queue-depth',        label: 'Queue Depth',
        description: 'Processing queue size',
        renderer: 'BarChartRenderer', defaultAccent: '#a78bfa',
        category: 'performance' },
    ],
  },
  {
    id: 'financial',
    label: 'Financial',
    icon: 'banknote',
    metrics: [
      { metricId: 'total-amount',       label: 'Total Amount',
        description: 'Principal value of this instance',
        renderer: 'CurrencyRenderer', defaultAccent: '#34d399',
        category: 'financial', unit: 'USD' },
      { metricId: 'budget-vs-actual',   label: 'Budget vs Actual',
        description: 'Planned vs actual expenditure',
        renderer: 'DualBarRenderer', defaultAccent: '#fbbf24',
        category: 'financial' },
      { metricId: 'approval-chain',     label: 'Approval Chain',
        description: 'Sequential approvers and status',
        renderer: 'ChainRenderer', defaultAccent: '#a78bfa',
        category: 'financial' },
      { metricId: 'cost-center',        label: 'Cost Center Breakdown',
        description: 'Allocation across cost centers',
        renderer: 'PieChartRenderer', defaultAccent: '#60a5fa',
        category: 'financial' },
      { metricId: 'currency-exposure',  label: 'Currency Exposure',
        description: 'Multi-currency positions and FX risk',
        renderer: 'ExposureMapRenderer', defaultAccent: '#fb7185',
        category: 'financial' },
      { metricId: 'forecast-impact',    label: 'Forecast Impact',
        description: 'Quarterly financial forecast impact',
        renderer: 'ForecastLineRenderer', defaultAccent: '#22d3ee',
        category: 'financial' },
    ],
  },
  {
    id: 'audit',
    label: 'Audit & Provenance',
    icon: 'file-search',
    metrics: [
      { metricId: 'latest-event',       label: 'Latest Event',
        description: 'Most recent audit event detail',
        renderer: 'EventDetailRenderer', defaultAccent: '#22d3ee',
        category: 'audit' },
      { metricId: 'witness-map',        label: 'Witness Map',
        description: 'Witness nodes and co-signatures',
        renderer: 'WitnessGridRenderer', defaultAccent: '#a78bfa',
        category: 'audit' },
      { metricId: 'signature-chain',    label: 'Signature Chain',
        description: 'Cryptographic signature verification',
        renderer: 'ChainRenderer', defaultAccent: '#34d399',
        category: 'audit' },
      { metricId: 'criticality-timeline', label: 'Criticality Timeline',
        description: 'Time-series criticality changes',
        renderer: 'TimelineRenderer', defaultAccent: '#fbbf24',
        category: 'audit' },
      { metricId: 'tamper-check',       label: 'Tamper Check Status',
        description: 'Fragment integrity verification',
        renderer: 'IntegrityRenderer', defaultAccent: '#fb7185',
        category: 'audit' },
      { metricId: 'reconstruction-health', label: 'Reconstruction Health',
        description: 'CDE fragment availability',
        renderer: 'HealthBarRenderer', defaultAccent: '#34d399',
        category: 'audit' },
    ],
  },
  {
    id: 'hedge-fund',
    label: 'Hedge Fund',
    icon: 'trending-up',
    metrics: [
      { metricId: 'position-value',     label: 'Position Value',
        description: 'Mark-to-market position value',
        renderer: 'CurrencyRenderer', defaultAccent: '#34d399',
        category: 'hedge-fund', unit: 'USD' },
      { metricId: 'greeks',             label: 'Greeks',
        description: 'Delta, Gamma, Theta, Vega',
        renderer: 'GreeksQuadRenderer', defaultAccent: '#a78bfa',
        category: 'hedge-fund' },
      { metricId: 'pnl-attribution',    label: 'P&L Attribution',
        description: 'P&L breakdown by factor',
        renderer: 'WaterfallRenderer', defaultAccent: '#22d3ee',
        category: 'hedge-fund' },
      { metricId: 'risk-limits',        label: 'Risk Limits',
        description: 'VaR, stress tests, limit utilization',
        renderer: 'LimitGaugeRenderer', defaultAccent: '#fb7185',
        category: 'hedge-fund' },
      { metricId: 'counterparty-exposure', label: 'Counterparty Exposure',
        description: 'Exposure by counterparty and credit',
        renderer: 'ExposureMapRenderer', defaultAccent: '#fbbf24',
        category: 'hedge-fund' },
      { metricId: 'liquidity-score',    label: 'Liquidity Score',
        description: 'Liquidity based on volume and spread',
        renderer: 'LiquidityGaugeRenderer', defaultAccent: '#60a5fa',
        category: 'hedge-fund' },
    ],
  },
];

// ── Renderer Lookup ──
// Maps metricId to renderer component key. Built from the catalog at import time.

const RENDERER_MAP: Record<string, string> = {};
const ACCENT_MAP: Record<string, string> = {};

for (const cat of METRIC_CATALOG) {
  for (const m of cat.metrics) {
    RENDERER_MAP[m.metricId] = m.renderer;
    ACCENT_MAP[m.category] = m.defaultAccent;
  }
}

/** Resolve the renderer key for a given metricId */
export function resolveRenderer(metricId: string): string {
  return RENDERER_MAP[metricId] ?? 'PlaceholderRenderer';
}

/** Resolve accent color from category */
export function resolveAccent(category: string): string {
  return ACCENT_MAP[category] ?? '#64748b';
}

// ── Inline Styles ──

const sidebarStyle: React.CSSProperties = {
  width: '280px',
  maxHeight: '100%',
  overflowY: 'auto',
  background: '#111827',
  borderLeft: '1px solid #1e293b',
  padding: '12px',
  fontFamily: "'DM Sans', sans-serif",
  fontSize: '0.85rem',
  color: '#e8ecf4',
};

const sidebarTitleStyle: React.CSSProperties = {
  fontFamily: "'IBM Plex Mono', monospace",
  fontSize: '0.8rem',
  fontWeight: 600,
  letterSpacing: '0.1em',
  textTransform: 'uppercase' as const,
  color: '#22d3ee',
  marginBottom: '12px',
};

const categoryHeaderStyle: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  width: '100%',
  padding: '8px 10px',
  border: '1px solid #1e293b',
  borderRadius: '6px',
  background: '#1a2234',
  color: '#e8ecf4',
  cursor: 'pointer',
  fontFamily: "'DM Sans', sans-serif",
  fontSize: '0.82rem',
  fontWeight: 600,
  marginBottom: '4px',
  transition: 'border-color 0.2s',
};

const metricListStyle: React.CSSProperties = {
  display: 'flex',
  flexDirection: 'column',
  gap: '4px',
  padding: '4px 0 8px',
};

const metricCardStyle: React.CSSProperties = {
  display: 'flex',
  flexDirection: 'column',
  gap: '2px',
  padding: '8px 10px',
  background: '#0d1321',
  border: '1px solid #1e293b',
  borderRadius: '6px',
  cursor: 'grab',
  transition: 'border-color 0.15s, transform 0.15s',
};

const mcLabelStyle: React.CSSProperties = {
  fontWeight: 600,
  fontSize: '0.8rem',
  color: '#e8ecf4',
};

const mcDescStyle: React.CSSProperties = {
  fontSize: '0.72rem',
  color: '#64748b',
  lineHeight: '1.4',
};

// ── Component ──

export function MetricSidebar() {
  const [expandedCategory, setExpandedCategory] = useState<string | null>(
    'operational',
  );

  const handleDragStart = useCallback(
    (e: React.DragEvent, metric: MetricEntry) => {
      const payload: MetricDragPayload = {
        metricId: metric.metricId,
        sourceCategory: metric.category,
        label: metric.label,
      };
      e.dataTransfer.setData('application/json', JSON.stringify(payload));
      e.dataTransfer.effectAllowed = 'copy';
    },
    [],
  );

  const toggleCategory = useCallback(
    (catId: string) => {
      setExpandedCategory(prev => (prev === catId ? null : catId));
    },
    [],
  );

  return (
    <div className="metric-sidebar" style={sidebarStyle}>
      <h3 style={sidebarTitleStyle}>Available Metrics</h3>
      {METRIC_CATALOG.map(cat => (
        <div key={cat.id} className="metric-category">
          <button
            style={{
              ...categoryHeaderStyle,
              borderColor:
                expandedCategory === cat.id ? '#22d3ee40' : '#1e293b',
            }}
            onClick={() => toggleCategory(cat.id)}
            aria-expanded={expandedCategory === cat.id}
          >
            <span>
              {cat.icon} {cat.label}
            </span>
            <span style={{ fontSize: '0.7rem', color: '#64748b' }}>
              {expandedCategory === cat.id ? '\u25B2' : '\u25BC'}
            </span>
          </button>
          {expandedCategory === cat.id && (
            <div style={metricListStyle}>
              {cat.metrics.map(m => (
                <div
                  key={m.metricId}
                  className="metric-card"
                  style={metricCardStyle}
                  draggable
                  onDragStart={e => handleDragStart(e, m)}
                >
                  <span style={mcLabelStyle}>{m.label}</span>
                  <span style={mcDescStyle}>{m.description}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      ))}
    </div>
  );
}
