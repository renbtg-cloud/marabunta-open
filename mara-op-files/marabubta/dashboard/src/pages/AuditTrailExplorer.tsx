// Marabunta - Licensed under the MIT License.
import { useState, useCallback, useEffect, useMemo } from 'react';
import clsx from 'clsx';
import { useAuditEvents } from '../hooks/useSwarmData';
import { usePagination } from '../hooks/usePagination';
import { useWsSubscription } from '../hooks/useWebSocket';
import { Pagination } from '../components/Pagination';
import { CriticalityBadge } from '../components/CriticalityBadge';
import { FilterDropdown } from '../components/FilterDropdown';
import { api } from '../api/client';
import type { AuditEvent, AuditQueryParams, Criticality, WsMessage } from '../api/types';
import { useQueryClient } from '@tanstack/react-query';

const ALL_CRITICALITIES: Criticality[] = [
  'INFO',
  'WARN',
  'AUDIT',
  'ALERT',
  'CRITICAL',
];

const COMMON_ACTIONS = [
  'node_joined',
  'node_left',
  'container_deployed',
  'witness_quorum',
  'role_pinned',
  'role_unpinned',
  'auth_failed',
  'config_changed',
  'recipe_deployed',
];

export function AuditTrailExplorer() {
  const queryClient = useQueryClient();
  const pagination = usePagination(50);

  const [critFilter, setCritFilter] = useState<string[]>([]);
  const [actionFilter, setActionFilter] = useState<string[]>([]);
  const [fromDate, setFromDate] = useState('');
  const [toDate, setToDate] = useState('');
  const [liveEvents, setLiveEvents] = useState<AuditEvent[]>([]);

  const queryParams: AuditQueryParams = useMemo(
    () => ({
      page: pagination.page,
      per_page: pagination.perPage,
      criticality:
        critFilter.length === 1 ? (critFilter[0] as Criticality) : undefined,
      action: actionFilter.length === 1 ? actionFilter[0] : undefined,
      from_date: fromDate || undefined,
      to_date: toDate || undefined,
    }),
    [
      pagination.page,
      pagination.perPage,
      critFilter,
      actionFilter,
      fromDate,
      toDate,
    ],
  );

  const { data, isLoading, error } = useAuditEvents(queryParams);

  useEffect(() => {
    if (data?.total !== undefined) {
      pagination.setTotal(data.total);
    }
  }, [data?.total, pagination]);

  // Subscribe to live audit events
  useWsSubscription(
    'audit_event',
    useCallback(
      (msg: WsMessage) => {
        if (msg.type === 'audit_event') {
          const event: AuditEvent = {
            event_id: msg.event_id,
            action: msg.action,
            actor: msg.actor,
            node_id: msg.node_id,
            node_name: null,
            criticality: msg.criticality,
            witness_verified: msg.witness_verified,
            witness_count: msg.witness_count,
            timestamp: msg.timestamp,
          };
          // Prepend live events while on page 1
          if (pagination.page === 1) {
            setLiveEvents((prev) => [event, ...prev].slice(0, 10));
          }
          // Invalidate cached audit data
          queryClient.invalidateQueries({ queryKey: ['audit'] });
        }
      },
      [pagination.page, queryClient],
    ),
  );

  // Clear live events when changing pages or filters
  useEffect(() => {
    setLiveEvents([]);
  }, [pagination.page, critFilter, actionFilter, fromDate, toDate]);

  const handleExport = useCallback(
    async (format: 'csv' | 'json') => {
      try {
        const params: Record<string, string> = { format };
        if (critFilter.length === 1) params.criticality = critFilter[0];
        if (actionFilter.length === 1) params.action = actionFilter[0];
        if (fromDate) params.from_date = fromDate;
        if (toDate) params.to_date = toDate;

        const blob = await api.download('/api/v1/audit/export', params);
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `audit-export-${new Date().toISOString().slice(0, 10)}.${format}`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
      } catch (err) {
        console.error('Export failed:', err);
      }
    },
    [critFilter, actionFilter, fromDate, toDate],
  );

  // Combine live events (page 1 only) with fetched events, deduplicated
  const allEvents = useMemo(() => {
    const fetched = data?.events ?? [];
    if (liveEvents.length === 0) return fetched;

    const fetchedIds = new Set(fetched.map((e) => e.event_id));
    const uniqueLive = liveEvents.filter((e) => !fetchedIds.has(e.event_id));
    return [...uniqueLive, ...fetched];
  }, [data?.events, liveEvents]);

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="px-4 sm:px-6 pt-4 sm:pt-6 pb-4">
        <h1 className="font-serif text-2xl text-marabunta-t1 mb-1">
          Audit Trail Explorer
        </h1>
        <p className="text-sm text-marabunta-t2">
          {data
            ? `${data.total.toLocaleString()} events recorded`
            : 'Loading audit trail...'}
        </p>
      </div>

      {/* Filter Bar */}
      <div className="px-4 sm:px-6 pb-4 flex flex-col sm:flex-row sm:flex-wrap items-stretch sm:items-center gap-3">
        <div className="flex flex-wrap gap-2">
          <FilterDropdown
            label="Criticality"
            options={ALL_CRITICALITIES}
            selected={critFilter}
            onChange={setCritFilter}
          />
          <FilterDropdown
            label="Action"
            options={COMMON_ACTIONS}
            selected={actionFilter}
            onChange={setActionFilter}
          />
        </div>

        <div className="flex flex-wrap gap-2">
          <div className="flex items-center gap-2">
            <label className="text-xs font-mono text-marabunta-muted">From</label>
            <input
              type="date"
              value={fromDate}
              onChange={(e) => setFromDate(e.target.value)}
              className="bg-marabunta-bg3 border border-marabunta-border rounded px-2 py-2.5 sm:py-1.5 text-base sm:text-xs font-mono text-marabunta-t2 focus:outline-none focus:border-marabunta-cyan/50"
            />
          </div>
          <div className="flex items-center gap-2">
            <label className="text-xs font-mono text-marabunta-muted">To</label>
            <input
              type="date"
              value={toDate}
              onChange={(e) => setToDate(e.target.value)}
              className="bg-marabunta-bg3 border border-marabunta-border rounded px-2 py-2.5 sm:py-1.5 text-base sm:text-xs font-mono text-marabunta-t2 focus:outline-none focus:border-marabunta-cyan/50"
            />
          </div>
        </div>

        <div className="flex-1" />

        <div className="flex items-center gap-2">
          <button
            onClick={() => handleExport('csv')}
            className="px-3 py-1.5 text-xs font-mono rounded-lg border border-marabunta-border text-marabunta-t2 hover:border-marabunta-cyan/30 hover:text-marabunta-cyan transition-colors"
          >
            Export CSV
          </button>
          <button
            onClick={() => handleExport('json')}
            className="px-3 py-1.5 text-xs font-mono rounded-lg border border-marabunta-border text-marabunta-t2 hover:border-marabunta-cyan/30 hover:text-marabunta-cyan transition-colors"
          >
            Export JSON
          </button>
        </div>
      </div>

      {/* Events Table */}
      <div className="flex-1 px-4 sm:px-6 overflow-auto">
        {isLoading ? (
          <div className="flex items-center justify-center h-64">
            <span className="text-marabunta-muted font-mono text-sm animate-pulse">
              Loading audit events...
            </span>
          </div>
        ) : error ? (
          <div className="flex items-center justify-center h-64">
            <span className="text-marabunta-rose font-mono text-sm">
              Error loading audit trail: {(error as Error).message}
            </span>
          </div>
        ) : (
          <table className="w-full text-sm">
            <thead className="sticky top-0 bg-marabunta-bg z-10">
              <tr>
                <th className="font-mono text-[11px] uppercase tracking-wider text-marabunta-muted text-left px-4 py-3 border-b-2 border-marabunta-border w-24">
                  Time
                </th>
                <th className="font-mono text-[11px] uppercase tracking-wider text-marabunta-muted text-left px-4 py-3 border-b-2 border-marabunta-border">
                  Action
                </th>
                <th className="hidden sm:table-cell font-mono text-[11px] uppercase tracking-wider text-marabunta-muted text-left px-4 py-3 border-b-2 border-marabunta-border w-28">
                  Actor
                </th>
                <th className="hidden md:table-cell font-mono text-[11px] uppercase tracking-wider text-marabunta-muted text-left px-4 py-3 border-b-2 border-marabunta-border w-28">
                  Node
                </th>
                <th className="font-mono text-[11px] uppercase tracking-wider text-marabunta-muted text-left px-4 py-3 border-b-2 border-marabunta-border w-24">
                  Crit
                </th>
                <th className="hidden sm:table-cell font-mono text-[11px] uppercase tracking-wider text-marabunta-muted text-center px-4 py-3 border-b-2 border-marabunta-border w-12">
                  W
                </th>
              </tr>
            </thead>
            <tbody>
              {allEvents.map((event, idx) => {
                const isLive = liveEvents.some(
                  (e) => e.event_id === event.event_id,
                );
                return (
                  <tr
                    key={event.event_id ?? `${event.timestamp}-${idx}`}
                    className={clsx(
                      'border-b border-marabunta-border transition-colors',
                      isLive
                        ? 'bg-marabunta-cyan/5 animate-fade-in'
                        : 'hover:bg-marabunta-bg3',
                    )}
                  >
                    <td className="px-4 py-2.5 font-mono text-xs text-marabunta-muted">
                      {new Date(event.timestamp).toLocaleTimeString()}
                    </td>
                    <td className="px-4 py-2.5 text-marabunta-t2">
                      {event.action.replace(/_/g, ' ')}
                    </td>
                    <td className="hidden sm:table-cell px-4 py-2.5 font-mono text-xs text-marabunta-t2 truncate max-w-[120px]">
                      {event.actor}
                    </td>
                    <td className="hidden md:table-cell px-4 py-2.5 font-mono text-xs text-marabunta-t2 truncate max-w-[120px]">
                      {event.node_name ?? event.node_id ?? '\u2014'}
                    </td>
                    <td className="px-4 py-2.5">
                      <CriticalityBadge level={event.criticality} />
                    </td>
                    <td className="hidden sm:table-cell px-4 py-2.5 text-center">
                      {event.witness_verified ? (
                        <span
                          className="text-marabunta-green text-sm"
                          title={`Verified by ${event.witness_count} witnesses`}
                        >
                          &#10003;
                        </span>
                      ) : (
                        <span
                          className="text-marabunta-rose text-sm"
                          title="Witness verification failed"
                        >
                          &#10007;
                        </span>
                      )}
                    </td>
                  </tr>
                );
              })}
              {allEvents.length === 0 && (
                <tr>
                  <td
                    colSpan={6}
                    className="px-4 py-12 text-center text-marabunta-muted font-mono"
                  >
                    No audit events match the current filters
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        )}
      </div>

      {/* Footer */}
      <div className="px-4 sm:px-6 py-4 flex flex-col sm:flex-row items-center justify-between gap-3 border-t border-marabunta-border">
        <div className="flex items-center gap-4 text-xs text-marabunta-muted font-mono">
          <span>
            {data
              ? `Showing ${(pagination.page - 1) * pagination.perPage + 1}-${Math.min(pagination.page * pagination.perPage, data.total)} of ${data.total.toLocaleString()}`
              : '...'}
          </span>
          <span className="hidden sm:flex items-center gap-1">
            <span className="text-marabunta-green">&#10003;</span> = verified by 3+
            witnesses
          </span>
          <span className="hidden sm:flex items-center gap-1">
            <span className="text-marabunta-rose">&#10007;</span> = sig failed
          </span>
        </div>
        <Pagination
          page={pagination.page}
          totalPages={pagination.totalPages}
          onPageChange={pagination.setPage}
        />
      </div>
    </div>
  );
}
