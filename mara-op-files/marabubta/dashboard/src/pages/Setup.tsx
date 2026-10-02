// Marabunta - Licensed under the MIT License.
import { useState, useCallback, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { MetricCard } from '../components/MetricCard';
import { useSetupStatus } from '../hooks/useSetupStatus';

function formatUptime(seconds: number): string {
  if (seconds < 60) return `${seconds}s`;
  const mins = Math.floor(seconds / 60);
  if (mins < 60) return `${mins}m ${seconds % 60}s`;
  const hours = Math.floor(mins / 60);
  return `${hours}h ${mins % 60}m`;
}

export function Setup() {
  const navigate = useNavigate();
  const [targetNodes, setTargetNodes] = useState(10);
  const [copied, setCopied] = useState(false);

  const { data: status } = useSetupStatus();

  const knownNodes = status?.known_nodes ?? 0;
  const progressPercent = Math.min(100, (knownNodes / Math.max(1, targetNodes)) * 100);
  const listenAddr = status?.listen_address ?? 'this-node:4200';

  const joinCommand = useMemo(() => {
    const parts = [
      'marabunta-swarm',
      '--listen 0.0.0.0:4200',
      `--bootstrap ${listenAddr}`,
      '--node-type bare-metal',
      '--max-concurrent 1',
      '--api-port 8080',
    ];
    return parts.join(' \\\n  ');
  }, [listenAddr]);

  const joinCommandOneLine = useMemo(
    () =>
      `marabunta-swarm --listen 0.0.0.0:4200 --bootstrap ${listenAddr} --node-type bare-metal --max-concurrent 1 --api-port 8080`,
    [listenAddr],
  );

  const handleCopy = useCallback(() => {
    navigator.clipboard.writeText(joinCommandOneLine);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }, [joinCommandOneLine]);

  const swarmReady = knownNodes >= targetNodes && targetNodes > 1;

  return (
    <div className="flex flex-col h-full overflow-y-auto">
      {/* Page Header */}
      <div className="px-4 sm:px-6 pt-6 sm:pt-8 pb-4">
        <h1 className="font-serif text-3xl text-marabunta-t1 mb-2">Swarm Setup</h1>
        <p className="text-sm text-marabunta-t2 max-w-2xl">
          Your node is running. Add more machines to build the swarm. Nodes
          discover each other automatically via the bootstrap address.
        </p>
      </div>

      <div className="px-4 sm:px-6 max-w-4xl">
        {/* Metric cards */}
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 sm:gap-4 mb-6">
          <MetricCard
            value={knownNodes}
            label="Nodes Joined"
            trend={knownNodes > 1 ? 'up' : 'flat'}
          />
          <MetricCard value={targetNodes} label="Target" />
          <MetricCard value={status?.known_jobs ?? 0} label="Jobs Queued" />
          <MetricCard
            value={status ? formatUptime(status.uptime_secs) : '--'}
            label="Uptime"
          />
        </div>

        {/* Progress bar */}
        <div className="card mb-6">
          <div className="flex items-center justify-between mb-2">
            <span className="label">Join Progress</span>
            <span className="font-mono text-sm text-marabunta-cyan">
              {knownNodes} / {targetNodes} nodes
            </span>
          </div>
          <div className="h-3 bg-marabunta-bg3 rounded-full overflow-hidden">
            <div
              className="h-full rounded-full transition-all duration-700 ease-out"
              style={{
                width: `${progressPercent}%`,
                backgroundColor: swarmReady ? '#34d399' : '#22d3ee',
              }}
            />
          </div>
          {swarmReady && (
            <p className="text-sm text-marabunta-green mt-3 font-mono">
              Target reached. Your swarm is ready.
            </p>
          )}
        </div>

        {/* Join Command */}
        <div className="card mb-6">
          <div className="flex items-center justify-between mb-3">
            <span className="label">Run this on each machine</span>
            <button
              onClick={handleCopy}
              className="px-3 py-1.5 text-xs font-mono bg-marabunta-cyan/10 text-marabunta-cyan border border-marabunta-cyan/30 rounded-lg hover:bg-marabunta-cyan/20 transition-colors"
            >
              {copied ? 'Copied!' : 'Copy'}
            </button>
          </div>
          <div className="bg-marabunta-bgd border border-marabunta-border rounded-lg px-4 py-3 overflow-x-auto">
            <pre className="font-mono text-sm text-marabunta-cyan whitespace-pre-wrap break-all">
              {joinCommand}
            </pre>
          </div>
          <p className="text-xs text-marabunta-muted mt-2">
            Each node auto-discovers the swarm via the bootstrap address and starts
            gossiping within seconds. Adjust <code className="text-marabunta-t2">--max-concurrent</code> and{' '}
            <code className="text-marabunta-t2">--node-type</code> to match your hardware.
          </p>
        </div>

        {/* TOML config alternative */}
        <div className="card mb-6">
          <span className="label mb-3 block">Or use a config file</span>
          <div className="bg-marabunta-bgd border border-marabunta-border rounded-lg px-4 py-3 overflow-x-auto">
            <pre className="font-mono text-xs text-marabunta-t2 whitespace-pre">
{`# swarm.toml
listen_addr = "0.0.0.0:4200"
bootstrap_servers = ["${listenAddr}"]
max_concurrent_chunks = 1
chunk_timeout = "10m"
enable_neuromancer = false
enable_management_layer = false
enable_postgres = false`}
            </pre>
          </div>
          <p className="text-xs text-marabunta-muted mt-2">
            Save as <code className="text-marabunta-t2">swarm.toml</code> and
            run: <code className="text-marabunta-t2">marabunta-swarm --config swarm.toml</code>
          </p>
        </div>

        {/* Target size */}
        <div className="card mb-6">
          <div className="flex items-center justify-between">
            <div>
              <span className="label block mb-1">Expected Swarm Size</span>
              <p className="text-xs text-marabunta-muted">
                Sets the progress bar target
              </p>
            </div>
            <input
              type="number"
              min={2}
              max={10000}
              value={targetNodes}
              onChange={(e) =>
                setTargetNodes(Math.max(2, Number(e.target.value) || 2))
              }
              className="w-24 bg-marabunta-bg3 border border-marabunta-border rounded px-3 py-1.5 text-sm font-mono text-marabunta-t2 text-right focus:outline-none focus:border-marabunta-cyan/50"
            />
          </div>
        </div>

        {/* Node identity */}
        {status && (
          <div className="card mb-6">
            <span className="label mb-2 block">This Node</span>
            <div className="space-y-1.5 text-sm">
              <div className="flex justify-between">
                <span className="text-marabunta-muted">Node ID</span>
                <span className="font-mono text-marabunta-t2 text-xs">
                  {status.node_id}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-marabunta-muted">Listen Address</span>
                <span className="font-mono text-marabunta-t2 text-xs">
                  {status.listen_address ?? 'unknown'}
                </span>
              </div>
              {status.bootstrap_seeds.length > 0 && (
                <div className="flex justify-between">
                  <span className="text-marabunta-muted">Bootstrap Seeds</span>
                  <span className="font-mono text-marabunta-t2 text-xs">
                    {status.bootstrap_seeds.join(', ')}
                  </span>
                </div>
              )}
            </div>
          </div>
        )}

        {/* Navigation */}
        <div className="flex items-center justify-between pb-8">
          <button
            onClick={() => navigate('/')}
            className="text-sm text-marabunta-muted hover:text-marabunta-cyan transition-colors font-mono"
          >
            Skip to Dashboard
          </button>
          {swarmReady && (
            <button
              onClick={() => navigate('/')}
              className="px-4 py-2 text-sm font-mono bg-marabunta-cyan/10 text-marabunta-cyan border border-marabunta-cyan/30 rounded-lg hover:bg-marabunta-cyan/20 transition-colors"
            >
              Open Dashboard
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
