// Marabunta - Licensed under the MIT License.
import { useState, useCallback } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useNodeDetail } from '../hooks/useSwarmData';
import { NodeBadge } from '../components/NodeBadge';
import { StatusIndicator } from '../components/StatusIndicator';
import { api } from '../api/client';

function formatUptime(seconds: number): string {
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const mins = Math.floor((seconds % 3600) / 60);
  if (days > 0) return `${days}d ${hours}h ${mins}m`;
  return `${hours}h ${mins}m`;
}

function formatSize(gb: number): string {
  if (gb >= 1024) return `${(gb / 1024).toFixed(1)} TB`;
  return `${gb} GB`;
}

interface DeployModalProps {
  recipeName: string;
  yamlPreview: string;
  onConfirm: () => void;
  onCancel: () => void;
}

function DeployModal({ recipeName, yamlPreview, onConfirm, onCancel }: DeployModalProps) {
  const [authorized, setAuthorized] = useState(false);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm">
      <div className="bg-marabunta-bg2 border border-marabunta-border rounded-xl w-full max-w-lg mx-4 overflow-hidden">
        <div className="px-6 py-4 border-b border-marabunta-border">
          <h3 className="font-sans font-semibold text-marabunta-t1">
            Deploy Recipe: {recipeName}
          </h3>
        </div>
        <div className="px-6 py-4">
          <span className="label mb-2 block">Recipe Preview</span>
          <pre className="bg-marabunta-bgd border border-marabunta-border rounded-lg p-3 text-xs font-mono text-marabunta-t2 overflow-x-auto max-h-48">
            {yamlPreview}
          </pre>
          <label className="flex items-center gap-2 mt-4 cursor-pointer">
            <input
              type="checkbox"
              checked={authorized}
              onChange={(e) => setAuthorized(e.target.checked)}
              className="w-4 h-4 rounded border-marabunta-border bg-marabunta-bg3 accent-marabunta-cyan"
            />
            <span className="text-sm text-marabunta-amber">
              I authorize this deployment
            </span>
          </label>
        </div>
        <div className="flex items-center justify-end gap-3 px-6 py-4 border-t border-marabunta-border">
          <button
            onClick={onCancel}
            className="px-4 py-2 text-sm text-marabunta-t2 hover:text-marabunta-t1 transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={onConfirm}
            disabled={!authorized}
            className="px-4 py-2 text-sm font-semibold rounded-lg bg-marabunta-cyan/20 text-marabunta-cyan border border-marabunta-cyan/30 disabled:opacity-40 disabled:cursor-not-allowed hover:bg-marabunta-cyan/30 transition-colors"
          >
            Deploy
          </button>
        </div>
      </div>
    </div>
  );
}

export function NodeDetail() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const { data: node, isLoading, error } = useNodeDetail(id ?? '');
  const [deployRecipe, setDeployRecipe] = useState<{
    id: string;
    name: string;
    yaml: string;
  } | null>(null);

  const handlePin = useCallback(
    async (role: string) => {
      if (!id) return;
      try {
        await api.post(`/api/v1/nodes/${id}/roles/${role}/pin`);
      } catch (err) {
        console.error('Failed to pin role:', err);
      }
    },
    [id],
  );

  const handleUnpin = useCallback(
    async (role: string) => {
      if (!id) return;
      try {
        await api.post(`/api/v1/nodes/${id}/roles/${role}/unpin`);
      } catch (err) {
        console.error('Failed to unpin role:', err);
      }
    },
    [id],
  );

  const handleDeploy = useCallback(async () => {
    if (!id || !deployRecipe) return;
    try {
      await api.post(`/api/v1/nodes/${id}/recipes/${deployRecipe.id}/deploy`);
      setDeployRecipe(null);
    } catch (err) {
      console.error('Failed to deploy recipe:', err);
    }
  }, [id, deployRecipe]);

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-full">
        <span className="text-marabunta-muted font-mono text-sm animate-pulse">
          Loading node details...
        </span>
      </div>
    );
  }

  if (error || !node) {
    return (
      <div className="flex flex-col items-center justify-center h-full gap-4">
        <span className="text-marabunta-rose font-mono text-sm">
          {error ? `Error loading node: ${(error as Error).message}` : 'Node not found'}
        </span>
        <button
          onClick={() => navigate('/fleet')}
          className="text-sm text-marabunta-cyan hover:underline"
        >
          Back to Fleet
        </button>
      </div>
    );
  }

  const hasPinnedRoles = node.roles.some((r) => r.pinned);

  return (
    <div className="p-4 sm:p-6 max-w-5xl mx-auto">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center gap-2 sm:gap-4 mb-6">
        <button
          onClick={() => navigate('/fleet')}
          className="text-marabunta-muted hover:text-marabunta-t1 transition-colors text-sm font-mono"
        >
          &larr; Back to Fleet
        </button>
        <h1 className="font-serif text-2xl text-marabunta-t1 flex-1">{node.name}</h1>
        <StatusIndicator status={node.status} showLabel />
      </div>

      {/* Status Bar */}
      <div className="flex flex-wrap items-center gap-4 sm:gap-6 mb-6 p-3 sm:p-4 bg-marabunta-bgc border border-marabunta-border rounded-xl">
        <div>
          <span className="label block">Status</span>
          <StatusIndicator status={node.status} showLabel />
        </div>
        <div>
          <span className="label block">Uptime</span>
          <span className="text-sm text-marabunta-t1 font-mono">
            {formatUptime(node.uptime_seconds)}
          </span>
        </div>
        <div>
          <span className="label block">Roles</span>
          <span className="text-sm text-marabunta-t1 font-mono">{node.roles.length}</span>
        </div>
        <div>
          <span className="label block">Last Seen</span>
          <span className="text-sm text-marabunta-t1 font-mono">
            {new Date(node.last_seen).toLocaleString()}
          </span>
        </div>
      </div>

      {/* Human-pinned Warning */}
      {hasPinnedRoles && (
        <div className="mb-6 p-3 bg-amber-500/10 border border-amber-500/30 rounded-xl flex items-start gap-2">
          <span className="text-marabunta-amber text-lg shrink-0">&#9888;</span>
          <div className="text-sm">
            <span className="text-marabunta-amber font-semibold">
              Human-pinned roles detected
            </span>
            {node.roles
              .filter((r) => r.pinned)
              .map((r) => (
                <div key={r.role} className="text-marabunta-t2 mt-1">
                  {r.role} pinned by{' '}
                  <span className="text-marabunta-t1">{r.pinned_by ?? 'unknown'}</span>
                  {r.pinned_at && (
                    <span className="text-marabunta-muted">
                      {' '}
                      on {new Date(r.pinned_at).toLocaleDateString()}
                    </span>
                  )}
                </div>
              ))}
          </div>
        </div>
      )}

      {/* Hardware + Software */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
        {/* Hardware */}
        <div className="bg-marabunta-bgc border border-marabunta-border rounded-xl p-4">
          <h2 className="font-sans font-semibold text-marabunta-t1 mb-3">Hardware</h2>
          <div className="space-y-2 text-sm">
            <Row label="OS" value={node.hardware.os} />
            <Row
              label="CPU"
              value={`${node.hardware.cpu} (${node.hardware.cpu_cores}c)`}
            />
            <Row
              label="RAM"
              value={`${formatSize(node.hardware.ram_total_gb)} (${formatSize(node.hardware.ram_used_gb)} used)`}
            />
            <Row
              label="Disk"
              value={`${formatSize(node.hardware.disk_total_gb)} (${formatSize(node.hardware.disk_free_gb)} free)`}
            />
            {node.hardware.gpu && (
              <Row
                label="GPU"
                value={`${node.hardware.gpu}${node.hardware.gpu_vram_gb ? ` (${node.hardware.gpu_vram_gb} GB)` : ''}`}
              />
            )}
          </div>
        </div>

        {/* Software */}
        <div className="bg-marabunta-bgc border border-marabunta-border rounded-xl p-4">
          <h2 className="font-sans font-semibold text-marabunta-t1 mb-3">
            Detected Software
          </h2>
          {node.software.length === 0 ? (
            <span className="text-sm text-marabunta-muted font-mono">
              No software detected
            </span>
          ) : (
            <div className="space-y-3">
              {node.software.map((sw) => (
                <div
                  key={`${sw.name}-${sw.port}`}
                  className="flex items-start justify-between"
                >
                  <div>
                    <div className="text-sm text-marabunta-t1 font-semibold">
                      {sw.name} {sw.version}
                    </div>
                    <div className="text-xs text-marabunta-muted font-mono">
                      :{sw.port} &mdash;{' '}
                      <span
                        className={
                          sw.status === 'active'
                            ? 'text-marabunta-green'
                            : sw.status === 'error'
                              ? 'text-marabunta-rose'
                              : 'text-marabunta-amber'
                        }
                      >
                        {sw.status.toUpperCase()}
                      </span>
                    </div>
                    {sw.tier_role && (
                      <div className="text-xs text-marabunta-t2 mt-0.5">
                        {sw.tier_role}
                      </div>
                    )}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Roles */}
      <div className="bg-marabunta-bgc border border-marabunta-border rounded-xl p-4 mb-6">
        <h2 className="font-sans font-semibold text-marabunta-t1 mb-3">Roles</h2>
        <div className="flex flex-wrap gap-2 mb-4">
          {node.roles.map((r) => (
            <NodeBadge key={r.role} role={r.role} pinned={r.pinned} />
          ))}
        </div>
        <div className="flex flex-wrap gap-2">
          {node.roles.map((r) =>
            r.pinned ? (
              <button
                key={`unpin-${r.role}`}
                onClick={() => handleUnpin(r.role)}
                className="px-3 py-2 sm:py-1.5 text-xs font-mono rounded-lg border border-amber-500/30 text-marabunta-amber hover:bg-amber-500/10 transition-colors"
              >
                Unpin {r.role}
              </button>
            ) : (
              <button
                key={`pin-${r.role}`}
                onClick={() => handlePin(r.role)}
                className="px-3 py-2 sm:py-1.5 text-xs font-mono rounded-lg border border-marabunta-border text-marabunta-t2 hover:border-marabunta-cyan/30 hover:text-marabunta-cyan transition-colors"
              >
                Pin {r.role}
              </button>
            ),
          )}
        </div>
      </div>

      {/* Deployable Recipes */}
      {node.recipes && node.recipes.length > 0 && (
        <div className="bg-marabunta-bgc border border-marabunta-border rounded-xl p-4">
          <h2 className="font-sans font-semibold text-marabunta-t1 mb-3">
            Deployable Recipes
          </h2>
          <div className="space-y-2">
            {node.recipes.map((recipe) => (
              <div
                key={recipe.id}
                className="flex items-center justify-between p-3 bg-marabunta-bg3 rounded-lg"
              >
                <div>
                  <div className="text-sm text-marabunta-t1 font-semibold">
                    {recipe.name}
                  </div>
                  <div className="text-xs text-marabunta-t2">{recipe.description}</div>
                </div>
                <button
                  onClick={() =>
                    setDeployRecipe({
                      id: recipe.id,
                      name: recipe.name,
                      yaml: recipe.yaml_preview,
                    })
                  }
                  className="px-3 py-2 sm:py-1.5 text-xs font-mono rounded-lg bg-marabunta-cyan/15 text-marabunta-cyan border border-marabunta-cyan/30 hover:bg-marabunta-cyan/25 transition-colors shrink-0"
                >
                  Deploy {recipe.requires_auth && '(auth req.)'}
                </button>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Deploy Confirmation Modal */}
      {deployRecipe && (
        <DeployModal
          recipeName={deployRecipe.name}
          yamlPreview={deployRecipe.yaml}
          onConfirm={handleDeploy}
          onCancel={() => setDeployRecipe(null)}
        />
      )}
    </div>
  );
}

function Row({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-start gap-2">
      <span className="font-mono text-xs text-marabunta-muted w-12 shrink-0 uppercase">
        {label}
      </span>
      <span className="text-marabunta-t2">{value}</span>
    </div>
  );
}
