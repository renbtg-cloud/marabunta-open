// Marabunta - Licensed under the MIT License.
import { useCallback } from 'react';
import clsx from 'clsx';
import { useStorageTiers } from '../hooks/useSwarmData';
import { useWsSubscription } from '../hooks/useWebSocket';
import { MetricCard } from '../components/MetricCard';
import type { StorageTier, TierHealth, WsMessage } from '../api/types';
import { useQueryClient } from '@tanstack/react-query';

const TIER_COLORS: Record<number, { border: string; text: string; bg: string; label: string }> = {
  1: {
    border: 'border-cyan-500/40',
    text: 'text-marabunta-cyan',
    bg: 'bg-cyan-500/10',
    label: 'Tier 1 -- Connected Databases',
  },
  2: {
    border: 'border-amber-500/40',
    text: 'text-marabunta-amber',
    bg: 'bg-amber-500/10',
    label: 'Tier 2 -- Deployed Containers',
  },
  3: {
    border: 'border-violet-500/40',
    text: 'text-marabunta-violet',
    bg: 'bg-violet-500/10',
    label: 'Tier 3 -- CDE (Collective Data Engine)',
  },
};

const HEALTH_STYLES: Record<TierHealth, { dot: string; label: string }> = {
  healthy: { dot: 'bg-emerald-400', label: 'Healthy' },
  degraded: { dot: 'bg-amber-400 animate-pulse', label: 'Degraded' },
  critical: { dot: 'bg-rose-400 animate-pulse', label: 'Critical' },
};

function ProgressBar({
  value,
  max,
  color,
  className,
}: {
  value: number;
  max: number;
  color: string;
  className?: string;
}) {
  const pct = max > 0 ? Math.min(100, (value / max) * 100) : 0;
  return (
    <div className={clsx('h-2 bg-marabunta-bg3 rounded-full overflow-hidden', className)}>
      <div
        className="h-full rounded-full transition-all duration-500"
        style={{ width: `${pct}%`, backgroundColor: color }}
      />
    </div>
  );
}

function CascadeArrow() {
  return (
    <div className="flex items-center justify-center px-2">
      <div className="flex items-center gap-0.5 rotate-90 sm:rotate-0">
        <div className="w-8 h-0.5 bg-marabunta-border" />
        <svg
          className="w-4 h-4 text-marabunta-muted animate-pulse"
          viewBox="0 0 16 16"
          fill="currentColor"
        >
          <path d="M6 3l5 5-5 5V3z" />
        </svg>
      </div>
    </div>
  );
}

function TierBox({ tier, health }: { tier: number; health: TierHealth }) {
  const colors = TIER_COLORS[tier] ?? TIER_COLORS[1];
  const healthStyle = HEALTH_STYLES[health] ?? HEALTH_STYLES.healthy;

  return (
    <div
      className={clsx(
        'flex items-center gap-2 px-4 py-3 rounded-xl border',
        health === 'critical' || health === 'degraded'
          ? 'border-rose-500/40 bg-rose-500/5'
          : colors.border,
        colors.bg,
      )}
    >
      <span className={clsx('inline-block w-2.5 h-2.5 rounded-full', healthStyle.dot)} />
      <span className={clsx('font-mono text-sm font-semibold', colors.text)}>
        Tier {tier}
      </span>
    </div>
  );
}

export function StorageTierStatus() {
  const { data, isLoading, error } = useStorageTiers();
  const queryClient = useQueryClient();

  useWsSubscription(
    'storage_update',
    useCallback(
      (_msg: WsMessage) => {
        queryClient.invalidateQueries({ queryKey: ['storage', 'tiers'] });
      },
      [queryClient],
    ),
  );

  const tiers = data?.tiers ?? [];
  const tier1 = tiers.find((t) => t.tier === 1);
  const tier2 = tiers.find((t) => t.tier === 2);
  const tier3 = tiers.find((t) => t.tier === 3);

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-full">
        <span className="text-marabunta-muted font-mono text-sm animate-pulse">
          Loading storage tiers...
        </span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-full">
        <span className="text-marabunta-rose font-mono text-sm">
          Error loading storage data: {(error as Error).message}
        </span>
      </div>
    );
  }

  return (
    <div className="p-4 sm:p-6 max-w-5xl mx-auto">
      {/* Header */}
      <h1 className="font-serif text-2xl text-marabunta-t1 mb-1">Storage Tier Status</h1>
      <p className="text-sm text-marabunta-t2 mb-6">
        Cascade flow across three storage tiers
      </p>

      {/* Cascade Flow Visualization */}
      <div className="flex flex-col sm:flex-row items-center justify-center gap-2 sm:gap-0 mb-8">
        <TierBox tier={1} health={tier1?.health ?? 'healthy'} />
        <CascadeArrow />
        <TierBox tier={2} health={tier2?.health ?? 'healthy'} />
        <CascadeArrow />
        <TierBox tier={3} health={tier3?.health ?? 'healthy'} />
      </div>

      {/* Tier 1: Connected Databases */}
      <TierSection tier={1}>
        {tier1?.databases && tier1.databases.length > 0 ? (
          <div className="space-y-3">
            {tier1.databases.map((db) => {
              const healthStyle = HEALTH_STYLES[db.status] ?? HEALTH_STYLES.healthy;
              return (
                <div
                  key={`${db.name}-${db.node_id}`}
                  className="flex flex-col sm:flex-row sm:items-center gap-2 sm:gap-4 p-3 bg-marabunta-bg3 rounded-lg"
                >
                  <span
                    className={clsx(
                      'inline-block w-2.5 h-2.5 rounded-full shrink-0',
                      healthStyle.dot,
                    )}
                  />
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2">
                      <span className="text-sm text-marabunta-t1 font-semibold truncate">
                        {db.name}
                      </span>
                      <span className="text-xs text-marabunta-muted font-mono">
                        @ {db.node_name}
                      </span>
                    </div>
                    <div className="text-xs text-marabunta-t2 mt-0.5">
                      {db.tier_role}
                    </div>
                  </div>
                  <div className="w-full sm:w-32 sm:shrink-0">
                    <div className="flex items-center justify-between text-xs font-mono mb-1">
                      <span className="text-marabunta-muted">Conns</span>
                      <span className="text-marabunta-t2">
                        {db.connections_active}/{db.connections_max}
                      </span>
                    </div>
                    <ProgressBar
                      value={db.connections_active}
                      max={db.connections_max}
                      color={
                        db.connections_active / db.connections_max > 0.9
                          ? '#fb7185'
                          : db.connections_active / db.connections_max > 0.7
                            ? '#fbbf24'
                            : '#22d3ee'
                      }
                    />
                  </div>
                </div>
              );
            })}
          </div>
        ) : (
          <EmptyState message="No connected databases" />
        )}
      </TierSection>

      {/* Tier 2: Deployed Containers */}
      <TierSection tier={2}>
        {tier2?.containers && tier2.containers.length > 0 ? (
          <div className="space-y-3">
            {tier2.containers.map((ct) => (
              <div
                key={`${ct.name}-${ct.node_id}`}
                className="flex flex-col sm:flex-row sm:items-center gap-2 sm:gap-4 p-3 bg-marabunta-bg3 rounded-lg"
              >
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2">
                    <span className="text-sm text-marabunta-t1 font-semibold truncate">
                      {ct.name}
                    </span>
                    <span className="text-xs text-marabunta-muted font-mono">
                      @ {ct.node_name}
                    </span>
                  </div>
                </div>
                <div className="w-full sm:w-28 sm:shrink-0">
                  <div className="flex items-center justify-between text-xs font-mono mb-1">
                    <span className="text-marabunta-muted">CPU</span>
                    <span className="text-marabunta-t2">{ct.cpu_percent}%</span>
                  </div>
                  <ProgressBar
                    value={ct.cpu_percent}
                    max={100}
                    color={ct.cpu_percent > 80 ? '#fb7185' : '#22d3ee'}
                  />
                </div>
                <div className="w-full sm:w-28 sm:shrink-0">
                  <div className="flex items-center justify-between text-xs font-mono mb-1">
                    <span className="text-marabunta-muted">RAM</span>
                    <span className="text-marabunta-t2">
                      {ct.ram_used_gb.toFixed(1)}/{ct.ram_limit_gb}G
                    </span>
                  </div>
                  <ProgressBar
                    value={ct.ram_used_gb}
                    max={ct.ram_limit_gb}
                    color={
                      ct.ram_used_gb / ct.ram_limit_gb > 0.9
                        ? '#fb7185'
                        : '#fbbf24'
                    }
                  />
                </div>
              </div>
            ))}
          </div>
        ) : (
          <EmptyState message="No deployed containers" />
        )}
      </TierSection>

      {/* Tier 3: CDE */}
      <TierSection tier={3}>
        {tier3?.cde ? (
          <div>
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-4">
              <MetricCard
                value={tier3.cde.replication_factor}
                label="Replication Factor"
              />
              <MetricCard
                value={tier3.cde.fragment_count.toLocaleString()}
                label="Fragments"
              />
              <MetricCard
                value={`${tier3.cde.distribution_percent}%`}
                label="Balanced"
                trend={
                  tier3.cde.distribution_percent >= 90
                    ? 'up'
                    : tier3.cde.distribution_percent >= 70
                      ? 'flat'
                      : 'down'
                }
              />
              <MetricCard
                value={`${tier3.cde.latency_p50_ms}ms`}
                label="p50 Latency"
              />
            </div>

            <div className="p-3 bg-marabunta-bg3 rounded-lg">
              <div className="flex items-center justify-between text-xs font-mono mb-2">
                <span className="text-marabunta-muted">Distribution Balance</span>
                <span className="text-marabunta-t2">
                  {tier3.cde.distribution_percent}%
                </span>
              </div>
              <ProgressBar
                value={tier3.cde.distribution_percent}
                max={100}
                color="#a78bfa"
                className="h-3"
              />
              <div className="flex items-center justify-between mt-3 text-xs font-mono text-marabunta-muted">
                <span>p50: {tier3.cde.latency_p50_ms}ms</span>
                <span>p95: {tier3.cde.latency_p95_ms}ms</span>
                <span>p99: {tier3.cde.latency_p99_ms}ms</span>
              </div>
            </div>
          </div>
        ) : (
          <EmptyState message="CDE data unavailable" />
        )}
      </TierSection>
    </div>
  );
}

function TierSection({
  tier,
  children,
}: {
  tier: number;
  children: React.ReactNode;
}) {
  const colors = TIER_COLORS[tier] ?? TIER_COLORS[1];
  return (
    <div className={clsx('mb-6 border rounded-xl p-4', colors.border, 'bg-marabunta-bgc')}>
      <h2 className={clsx('font-sans font-semibold mb-3', colors.text)}>
        {colors.label}
      </h2>
      {children}
    </div>
  );
}

function EmptyState({ message }: { message: string }) {
  return (
    <div className="text-center py-6 text-sm text-marabunta-muted font-mono">
      {message}
    </div>
  );
}
