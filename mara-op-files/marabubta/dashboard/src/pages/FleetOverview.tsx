// Marabunta - Licensed under the MIT License.
import { useState, useCallback, useMemo, useEffect } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import clsx from 'clsx';
import { useNodes } from '../hooks/useSwarmData';
import { usePagination } from '../hooks/usePagination';
import { SearchBar } from '../components/SearchBar';
import { FilterDropdown } from '../components/FilterDropdown';
import { Pagination } from '../components/Pagination';
import { NodeBadge } from '../components/NodeBadge';
import { StatusIndicator } from '../components/StatusIndicator';
import type { NodeFilters, NodeStatus, RoleName } from '../api/types';

const ALL_STATUSES: NodeStatus[] = ['online', 'idle', 'offline'];
const ALL_ROLES: RoleName[] = [
  'AGGREGATOR',
  'WITNESS',
  'GATEWAY',
  'RELAY',
  'STORAGE',
  'EPHEMERAL',
];

type SortColumn =
  | 'name'
  | 'os'
  | 'cpu'
  | 'ram'
  | 'software'
  | 'roles'
  | 'status';

interface SortState {
  column: SortColumn;
  direction: 'asc' | 'desc';
}

export function FleetOverview() {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();

  // Parse initial state from URL
  const [search, setSearch] = useState(searchParams.get('search') ?? '');
  const [statusFilter, setStatusFilter] = useState<string[]>(() => {
    const s = searchParams.get('status');
    return s ? s.split(',') : [];
  });
  const [roleFilter, setRoleFilter] = useState<string[]>(() => {
    const r = searchParams.get('roles');
    return r ? r.split(',') : [];
  });
  const [sort, setSort] = useState<SortState>({
    column: (searchParams.get('sort_by') as SortColumn) ?? 'name',
    direction: (searchParams.get('sort_dir') as 'asc' | 'desc') ?? 'asc',
  });

  const pagination = usePagination(25);

  const filters: NodeFilters = useMemo(
    () => ({
      search: search || undefined,
      status: statusFilter.length > 0 ? (statusFilter as NodeStatus[]) : undefined,
      roles: roleFilter.length > 0 ? (roleFilter as RoleName[]) : undefined,
      sort_by: sort.column,
      sort_dir: sort.direction,
    }),
    [search, statusFilter, roleFilter, sort],
  );

  const { data, isLoading, error } = useNodes(
    pagination.page,
    pagination.perPage,
    filters,
  );

  // Sync total from API response
  useEffect(() => {
    if (data?.total !== undefined) {
      pagination.setTotal(data.total);
    }
  }, [data?.total, pagination]);

  // Sync filters to URL
  useEffect(() => {
    const params = new URLSearchParams();
    if (search) params.set('search', search);
    if (statusFilter.length > 0) params.set('status', statusFilter.join(','));
    if (roleFilter.length > 0) params.set('roles', roleFilter.join(','));
    if (sort.column !== 'name') params.set('sort_by', sort.column);
    if (sort.direction !== 'asc') params.set('sort_dir', sort.direction);
    setSearchParams(params, { replace: true });
  }, [search, statusFilter, roleFilter, sort, setSearchParams]);

  const handleSort = useCallback(
    (column: SortColumn) => {
      setSort((prev) => ({
        column,
        direction:
          prev.column === column && prev.direction === 'asc' ? 'desc' : 'asc',
      }));
    },
    [],
  );

  const handleSearchChange = useCallback((value: string) => {
    setSearch(value);
  }, []);

  const nodes = data?.nodes ?? [];

  const SortHeader = ({
    column,
    label,
    className,
  }: {
    column: SortColumn;
    label: string;
    className?: string;
  }) => (
    <th
      onClick={() => handleSort(column)}
      className={clsx(
        'font-mono text-[11px] uppercase tracking-wider text-marabunta-muted text-left px-4 py-3 border-b-2 border-marabunta-border cursor-pointer hover:text-marabunta-t1 transition-colors select-none',
        className,
      )}
    >
      <span className="inline-flex items-center gap-1">
        {label}
        {sort.column === column && (
          <span className="text-marabunta-cyan">
            {sort.direction === 'asc' ? '\u2191' : '\u2193'}
          </span>
        )}
      </span>
    </th>
  );

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="px-4 sm:px-6 pt-4 sm:pt-6 pb-4 flex flex-col sm:flex-row sm:items-start sm:justify-between gap-4">
        <div>
          <h1 className="font-serif text-2xl text-marabunta-t1 mb-1">Fleet Overview</h1>
          <p className="text-sm text-marabunta-t2">
            {data ? `${data.total} nodes in swarm` : 'Loading fleet data...'}
          </p>
        </div>
        <div className="flex flex-col sm:flex-row items-stretch sm:items-center gap-3 w-full sm:w-auto">
          <SearchBar
            value={search}
            onChange={handleSearchChange}
            placeholder="Search nodes..."
            className="w-full sm:w-64"
          />
          <div className="flex gap-3">
            <FilterDropdown
              label="Status"
              options={ALL_STATUSES}
              selected={statusFilter}
              onChange={setStatusFilter}
            />
            <FilterDropdown
              label="Role"
              options={ALL_ROLES}
              selected={roleFilter}
              onChange={setRoleFilter}
            />
          </div>
        </div>
      </div>

      {/* Table */}
      <div className="flex-1 px-4 sm:px-6 overflow-auto">
        {isLoading ? (
          <div className="flex items-center justify-center h-64">
            <span className="text-marabunta-muted font-mono text-sm animate-pulse">
              Loading fleet data...
            </span>
          </div>
        ) : error ? (
          <div className="flex items-center justify-center h-64">
            <span className="text-marabunta-rose font-mono text-sm">
              Error loading fleet: {(error as Error).message}
            </span>
          </div>
        ) : (
          <table className="w-full text-sm">
            <thead className="sticky top-0 bg-marabunta-bg z-10">
              <tr>
                <SortHeader column="name" label="Name" />
                <SortHeader column="os" label="OS" className="hidden sm:table-cell" />
                <SortHeader column="cpu" label="CPU / RAM" />
                <SortHeader column="software" label="Software" className="hidden md:table-cell" />
                <SortHeader column="roles" label="Roles" />
                <SortHeader column="status" label="St" className="w-12" />
              </tr>
            </thead>
            <tbody>
              {nodes.map((node) => (
                <tr
                  key={node.id}
                  onClick={() => navigate(`/nodes/${node.id}`)}
                  className={clsx(
                    'cursor-pointer transition-colors border-b border-marabunta-border',
                    node.status === 'offline'
                      ? 'bg-rose-950/20 hover:bg-rose-950/30'
                      : 'hover:bg-marabunta-bg3',
                  )}
                >
                  <td className="px-3 sm:px-4 py-3 font-mono text-marabunta-t1">{node.name}</td>
                  <td className="hidden sm:table-cell px-3 sm:px-4 py-3 text-marabunta-t2">
                    {node.hardware.os}
                  </td>
                  <td className="px-3 sm:px-4 py-3 font-mono text-marabunta-t2">
                    {node.hardware.cpu_cores}c / {node.hardware.ram_total_gb}G
                  </td>
                  <td className="hidden md:table-cell px-3 sm:px-4 py-3">
                    {node.software.length === 0 ? (
                      <span className="text-marabunta-muted">&mdash;</span>
                    ) : (
                      <span className="text-marabunta-t2">
                        {node.software.map((sw) => sw.name).join(', ')}
                      </span>
                    )}
                  </td>
                  <td className="px-3 sm:px-4 py-3">
                    <div className="flex flex-wrap gap-1">
                      {node.roles.map((r) => (
                        <NodeBadge
                          key={r.role}
                          role={r.role}
                          pinned={r.pinned}
                        />
                      ))}
                    </div>
                  </td>
                  <td className="px-3 sm:px-4 py-3">
                    <StatusIndicator status={node.status} />
                  </td>
                </tr>
              ))}
              {nodes.length === 0 && (
                <tr>
                  <td
                    colSpan={6}
                    className="px-4 py-12 text-center text-marabunta-muted font-mono"
                  >
                    No nodes match the current filters
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        )}
      </div>

      {/* Footer: Pagination + Legend */}
      <div className="px-4 sm:px-6 py-4 flex flex-col sm:flex-row items-center justify-between gap-3 border-t border-marabunta-border">
        <div className="flex items-center gap-4 text-xs text-marabunta-muted font-mono">
          <span>
            Showing{' '}
            {data
              ? `${(pagination.page - 1) * pagination.perPage + 1}-${Math.min(pagination.page * pagination.perPage, data.total)} of ${data.total}`
              : '...'}
          </span>
          <span className="hidden sm:flex items-center gap-1">
            <StatusIndicator status="online" /> Online
          </span>
          <span className="hidden sm:flex items-center gap-1">
            <StatusIndicator status="idle" /> Idle
          </span>
          <span className="hidden sm:flex items-center gap-1">
            <StatusIndicator status="offline" /> Offline
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
