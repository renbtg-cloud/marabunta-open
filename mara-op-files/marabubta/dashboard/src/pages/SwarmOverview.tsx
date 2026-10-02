// Marabunta - Licensed under the MIT License.
import { useEffect, useRef, useState, useCallback, useMemo, lazy, Suspense } from 'react';
import { useNavigate } from 'react-router-dom';
import * as d3 from 'd3';
import { MetricCard } from '../components/MetricCard';
import { useNodes, useRoleDistribution } from '../hooks/useSwarmData';
import { useTimeTravelQuery } from '../hooks/useTimeTravelQuery';
import { useTimeTravel } from '../hooks/useTimeTravel';
import { useWsSubscription } from '../hooks/useWebSocket';
import { detectWebGLSupport } from '../three';
import type { Node, NodeList, RoleDistribution, WsMessage, RoleName, AuditEvent } from '../api/types';

// Lazy-load the 3D canvas so Three.js bundle is only fetched on demand
const SwarmCanvas = lazy(() =>
  import('../three/SwarmCanvas').then((mod) => ({ default: mod.SwarmCanvas }))
);

type ViewMode = '2d' | '3d';

interface GraphNode extends d3.SimulationNodeDatum {
  id: string;
  name: string;
  status: 'online' | 'idle' | 'offline';
  primaryRole: string;
  radius: number;
  isTraining: boolean;
  isPgWireActive: boolean;
  isChaos: boolean;
}

interface GraphLink extends d3.SimulationLinkDatum<GraphNode> {
  source: string | GraphNode;
  target: string | GraphNode;
}

const ROLE_COLORS: Record<string, string> = {
  AGGREGATOR: '#22d3ee',
  WITNESS: '#a78bfa',
  GATEWAY: '#34d399',
  RELAY: '#60a5fa',
  STORAGE: '#fbbf24',
  EPHEMERAL: '#fb7185',
};

const STATUS_OPACITY: Record<string, number> = {
  online: 1.0,
  idle: 0.6,
  offline: 0.3,
};

function formatUptime(seconds: number): string {
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  if (days > 0) return `${days}d ${hours}h`;
  const mins = Math.floor((seconds % 3600) / 60);
  return `${hours}h ${mins}m`;
}

// Color Legend
const WS_STATUS_STYLES: Record<string, { dot: string; label: string }> = {
  connected: { dot: 'bg-emerald-400', label: 'Live Topology Stream' },
  connecting: { dot: 'bg-amber-400 animate-pulse', label: 'Connecting to Swarm...' },
  disconnected: { dot: 'bg-red-400', label: 'Disconnected' },
};

export function SwarmOverview() {
  const navigate = useNavigate();
  const svgRef = useRef<SVGSVGElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const simulationRef = useRef<d3.Simulation<GraphNode, GraphLink> | null>(null);
  const [recentEvents, setRecentEvents] = useState<AuditEvent[]>([]);
  const [roleFilter, setRoleFilter] = useState<string>('all');
  const [viewMode, setViewMode] = useState<ViewMode>('2d');

  // Detect WebGL support — cached after first call
  const webglSupport = useMemo(() => detectWebGLSupport(), []);
  const canShow3D = !webglSupport.none;

  // Build the WebSocket URL for the 3D topology stream
  const wsUrl = useMemo(() => {
    const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    return `${proto}//${window.location.host}/ws/swarm-topology`;
  }, []);

  // Handler for 3D node selection — navigates to node detail
  const handleNodeSelect = useCallback((nodeId: string | null) => {
    if (nodeId) navigate(`/nodes/${nodeId}`);
  }, [navigate]);

  const { isLive } = useTimeTravel();

  // Use time-travel aware queries when viewing historical data, live queries otherwise
  const { data: nodeDataLive } = useNodes(1, 500);
  const { data: nodeDataTT } = useTimeTravelQuery<NodeList>(
    ['nodes', 'tt'],
    '/api/v1/nodes',
    { page: '1', per_page: '500' },
    { staleTime: 10_000, refetchInterval: 30_000, enabled: !isLive },
  );
  const nodeData = isLive ? nodeDataLive : nodeDataTT;

  const { data: roleDistributionLive } = useRoleDistribution();
  const { data: roleDistributionTT } = useTimeTravelQuery<RoleDistribution>(
    ['roles', 'distribution', 'tt'],
    '/api/v1/roles/distribution',
    undefined,
    { staleTime: 15_000, enabled: !isLive },
  );
  const roleDistribution = isLive ? roleDistributionLive : roleDistributionTT;

  const nodes = useMemo(() => nodeData?.nodes ?? [], [nodeData]);

  const metrics = useMemo(() => {
    const total = nodes.length;
    const healthy = nodes.filter((n) => n.status === 'online').length;
    const unhealthy = nodes.filter((n) => n.status === 'offline').length;
    const rolesSet = new Set<string>();
    nodes.forEach((n) => n.roles.forEach((r) => rolesSet.add(r.role)));
    return { total, healthy, unhealthy, roles: rolesSet.size };
  }, [nodes]);

  // Subscribe to live audit events for the recent events feed
  useWsSubscription(
    'audit_event',
    useCallback((msg: WsMessage) => {
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
        setRecentEvents((prev) => [event, ...prev].slice(0, 5));
      }
    }, []),
  );

  // Build D3 force-directed graph
  useEffect(() => {
    if (!svgRef.current || !containerRef.current || nodes.length === 0) return;

    const svg = d3.select(svgRef.current);
    const container = containerRef.current;
    const width = container.clientWidth;
    const height = container.clientHeight;

    svg.attr('viewBox', `0 0 ${width} ${height}`);
    svg.selectAll('*').remove();

    const g = svg.append('g');

    // Build graph data
    const graphNodes: GraphNode[] = nodes
      .filter(
        (n) => roleFilter === 'all' || n.roles.some((r) => r.role === roleFilter),
      )
      .map((n) => ({
        id: n.id,
        name: n.name,
        status: n.status,
        primaryRole: n.roles[0]?.role ?? 'EPHEMERAL',
        radius: Math.max(6, Math.min(14, 4 + n.hardware.cpu_cores * 0.8)),
        isTraining: n.is_training,
        isPgWireActive: n.is_pgwire_active,
        isChaos: n.chaos_state.is_active,
      }));

    const nodeIdSet = new Set(graphNodes.map((n) => n.id));
    const graphLinks: GraphLink[] = [];
    nodes.forEach((n) => {
      n.gossip_connections.forEach((targetId) => {
        if (nodeIdSet.has(n.id) && nodeIdSet.has(targetId) && n.id < targetId) {
          graphLinks.push({ source: n.id, target: targetId });
        }
      });
    });

    // Force simulation
    const simulation = d3
      .forceSimulation<GraphNode>(graphNodes)
      .force(
        'link',
        d3
          .forceLink<GraphNode, GraphLink>(graphLinks)
          .id((d) => d.id)
          .distance(80),
      )
      .force('charge', d3.forceManyBody().strength(-120))
      .force('center', d3.forceCenter(width / 2, height / 2))
      .force('collide', d3.forceCollide().radius(20))
      .alphaDecay(0.02)
      .velocityDecay(0.4);

    simulationRef.current = simulation;

    // Draw links
    const link = g
      .append('g')
      .attr('stroke', '#1e293b')
      .attr('stroke-opacity', 0.6)
      .selectAll('line')
      .data(graphLinks)
      .join('line')
      .attr('stroke-width', 1);

    // Draw nodes
    const node = g
      .append('g')
      .selectAll<SVGCircleElement, GraphNode>('circle')
      .data(graphNodes)
      .join('circle')
      .attr('r', (d) => d.radius)
      .attr('fill', (d) => d.isChaos ? '#ef4444' : (ROLE_COLORS[d.primaryRole] ?? '#64748b'))
      .attr('fill-opacity', (d) => STATUS_OPACITY[d.status] ?? 1.0)
      .attr('stroke', (d) => d.isPgWireActive ? '#22d3ee' : (d.isTraining ? '#fb7185' : '#0a0e17'))
      .attr('stroke-width', (d) => (d.isPgWireActive || d.isTraining || d.isChaos) ? 3 : 1.5)
      .attr('cursor', 'pointer')
      .on('click', (_event, d) => {
        navigate(`/nodes/${d.id}`);
      });

    // Add pulsing class for training nodes if they exist
    node.attr('class', (d) => d.isTraining ? 'node-training-pulse' : (d.isChaos ? 'node-chaos-glitch' : ''));

    // Tooltip
    node.append('title').text((d) => `${d.name} (${d.primaryRole}) - ${d.status}`);

    // Drag behavior
    const drag = d3
      .drag<SVGCircleElement, GraphNode>()
      .on('start', (event, d) => {
        if (!event.active) simulation.alphaTarget(0.3).restart();
        d.fx = d.x;
        d.fy = d.y;
      })
      .on('drag', (event, d) => {
        d.fx = event.x;
        d.fy = event.y;
      })
      .on('end', (event, d) => {
        if (!event.active) simulation.alphaTarget(0);
        d.fx = null;
        d.fy = null;
      });

    node.call(drag);

    // Zoom
    const zoom = d3
      .zoom<SVGSVGElement, unknown>()
      .scaleExtent([0.2, 5])
      .on('zoom', (event) => {
        g.attr('transform', event.transform);
      });

    svg.call(zoom);

    // Tick function
    simulation.on('tick', () => {
      link
        .attr('x1', (d) => (d.source as GraphNode).x ?? 0)
        .attr('y1', (d) => (d.source as GraphNode).y ?? 0)
        .attr('x2', (d) => (d.target as GraphNode).x ?? 0)
        .attr('y2', (d) => (d.target as GraphNode).y ?? 0);

      node
        .attr('cx', (d) => d.x ?? 0)
        .attr('cy', (d) => d.y ?? 0);
    });

    return () => {
      simulation.stop();
    };
  }, [nodes, roleFilter, navigate]);

  const roleBarData = useMemo(() => {
    if (roleDistribution) return roleDistribution;
    // Fallback: compute from nodes data
    const counts = new Map<string, number>();
    nodes.forEach((n) => {
      n.roles.forEach((r) => {
        counts.set(r.role, (counts.get(r.role) ?? 0) + 1);
      });
    });
    return Array.from(counts.entries()).map(([role, count]) => ({
      role: role as RoleName,
      count,
    }));
  }, [roleDistribution, nodes]);

  const maxRoleCount = Math.max(1, ...roleBarData.map((r) => r.count));

  return (
    <div className="flex flex-col h-full">
      <style dangerouslySetInnerHTML={{ __html: `
        @keyframes nodePulse {
          0% { stroke-opacity: 1; stroke-width: 3px; }
          50% { stroke-opacity: 0.4; stroke-width: 8px; }
          100% { stroke-opacity: 1; stroke-width: 3px; }
        }
        .node-training-pulse {
          animation: nodePulse 0.5s infinite ease-in-out;
        }
        @keyframes nodeGlitch {
          0% { transform: translate(0,0); }
          20% { transform: translate(-2px, 1px); }
          40% { transform: translate(2px, -1px); }
          60% { transform: translate(-1px, -2px); }
          80% { transform: translate(1px, 2px); }
          100% { transform: translate(0,0); }
        }
        .node-chaos-glitch {
          animation: nodeGlitch 0.2s infinite linear;
        }
      `}} />
      {/* Page Header */}
      <div className="px-4 sm:px-6 pt-4 sm:pt-6 pb-3 sm:pb-4">
        <h1 className="font-serif text-2xl text-marabunta-t1 mb-1">Swarm Overview</h1>
        <p className="text-sm text-marabunta-t2">
          Real-time topology and health status of the Marabunta Compute swarm
        </p>
      </div>

      {/* Metric Cards */}
      <div className="px-4 sm:px-6 grid grid-cols-2 lg:grid-cols-4 gap-3 sm:gap-4 mb-4">
        <MetricCard
          value={metrics.total}
          label="Total Nodes"
          trend={metrics.total > 0 ? 'flat' : undefined}
        />
        <MetricCard
          value={metrics.healthy}
          label="Healthy"
          trend={metrics.healthy === metrics.total ? 'up' : 'flat'}
        />
        <MetricCard
          value={metrics.unhealthy}
          label="Unhealthy"
          trend={metrics.unhealthy > 0 ? 'down' : 'flat'}
        />
        <MetricCard value={metrics.roles} label="Active Roles" />
      </div>

      {/* Main Content: Topology + Sidebar */}
      <div className="flex-1 flex px-4 sm:px-6 pb-4 sm:pb-6 gap-4 min-h-0">
        {/* Topology Graph */}
        <div className="flex-1 flex flex-col min-w-0">
          <div className="flex items-center gap-3 mb-2">
            <span className="label">Topology</span>
            <select
              value={roleFilter}
              onChange={(e) => setRoleFilter(e.target.value)}
              className="bg-marabunta-bg3 border border-marabunta-border rounded px-2 py-1 text-xs font-mono text-marabunta-t2 focus:outline-none focus:border-marabunta-cyan/50"
            >
              <option value="all">All Roles</option>
              <option value="AGGREGATOR">Aggregator</option>
              <option value="WITNESS">Witness</option>
              <option value="GATEWAY">Gateway</option>
              <option value="RELAY">Relay</option>
              <option value="STORAGE">Storage</option>
              <option value="EPHEMERAL">Ephemeral</option>
            </select>
            {/* View Mode Toggle — hidden if WebGL is unavailable */}
            {canShow3D && (
              <div className="ml-auto flex items-center bg-marabunta-bg3 border border-marabunta-border rounded overflow-hidden">
                <button
                  onClick={() => setViewMode('2d')}
                  className={`px-3 py-1 text-xs font-mono transition-colors ${
                    viewMode === '2d'
                      ? 'bg-marabunta-cyan/20 text-marabunta-cyan border-r border-marabunta-border'
                      : 'text-marabunta-muted hover:text-marabunta-t2 border-r border-marabunta-border'
                  }`}
                >
                  2D
                </button>
                <button
                  onClick={() => setViewMode('3d')}
                  className={`px-3 py-1 text-xs font-mono transition-colors ${
                    viewMode === '3d'
                      ? 'bg-marabunta-cyan/20 text-marabunta-cyan'
                      : 'text-marabunta-muted hover:text-marabunta-t2'
                  }`}
                >
                  3D
                </button>
              </div>
            )}
          </div>

          {/* 2D Topology View (D3 force-directed SVG) */}
          {viewMode === '2d' && (
            <div
              ref={containerRef}
              className="flex-1 bg-marabunta-bgd border border-marabunta-border rounded-xl overflow-hidden relative"
            >
              <svg ref={svgRef} className="w-full h-full" />
              {nodes.length === 0 && (
                <div className="absolute inset-0 flex items-center justify-center text-marabunta-muted text-sm font-mono">
                  No nodes connected
                </div>
              )}
            </div>
          )}

          {/* 3D Topology View (Three.js R3F Canvas) */}
          {viewMode === '3d' && (
            <div className="flex-1 bg-marabunta-bgd border border-marabunta-border rounded-xl overflow-hidden relative">
              <Suspense
                fallback={
                  <div className="absolute inset-0 flex items-center justify-center text-marabunta-muted text-sm font-mono">
                    Loading 3D view...
                  </div>
                }
              >
                <SwarmCanvas
                  wsUrl={wsUrl}
                  className="w-full h-full"
                  onNodeSelect={handleNodeSelect}
                />
              </Suspense>
            </div>
          )}

          {/* Recent Events */}
          <div className="mt-4">
            <span className="label mb-2 block">Recent Events</span>
            <div className="bg-marabunta-bgc border border-marabunta-border rounded-xl overflow-hidden">
              {recentEvents.length === 0 ? (
                <div className="px-4 py-3 text-sm text-marabunta-muted font-mono">
                  Waiting for events...
                </div>
              ) : (
                recentEvents.map((event) => (
                  <div
                    key={event.event_id}
                    className="flex items-center gap-3 px-3 sm:px-4 py-2 border-b border-marabunta-border last:border-b-0 text-sm"
                  >
                    <span className="font-mono text-xs text-marabunta-muted w-20 shrink-0">
                      {new Date(event.timestamp).toLocaleTimeString()}
                    </span>
                    <span className="text-marabunta-t2 truncate flex-1">
                      {event.action.replace(/_/g, ' ')}
                    </span>
                    <span
                      className={`text-xs font-mono px-1.5 py-0.5 rounded ${
                        event.criticality === 'INFO'
                          ? 'text-marabunta-blue'
                          : event.criticality === 'WARN'
                            ? 'text-marabunta-amber'
                            : event.criticality === 'ALERT'
                              ? 'text-marabunta-rose'
                              : event.criticality === 'CRITICAL'
                                ? 'text-marabunta-rose animate-pulse'
                                : 'text-marabunta-violet'
                      }`}
                    >
                      {event.criticality}
                    </span>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>

        {/* Right Sidebar: Role Distribution */}
        <div className="w-52 shrink-0 hidden lg:block">
          <span className="label mb-3 block">Role Distribution</span>
          <div className="space-y-2">
            {roleBarData.map((item) => (
              <div key={item.role}>
                <div className="flex items-center justify-between mb-1">
                  <span className="font-mono text-xs text-marabunta-t2">{item.role}</span>
                  <span className="font-mono text-xs text-marabunta-muted">{item.count}</span>
                </div>
                <div className="h-2 bg-marabunta-bg3 rounded-full overflow-hidden">
                  <div
                    className="h-full rounded-full transition-all duration-500"
                    style={{
                      width: `${(item.count / maxRoleCount) * 100}%`,
                      backgroundColor: ROLE_COLORS[item.role] ?? '#64748b',
                    }}
                  />
                </div>
              </div>
            ))}
          </div>

          {/* Color Legend */}
          <div className="mt-6">
            <span className="label mb-2 block">Legend</span>
            <div className="space-y-1.5">
              {Object.entries(ROLE_COLORS).map(([role, color]) => (
                <div key={role} className="flex items-center gap-2">
                  <span
                    className="w-3 h-3 rounded-full shrink-0"
                    style={{ backgroundColor: color }}
                  />
                  <span className="font-mono text-xs text-marabunta-t2">{role}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
