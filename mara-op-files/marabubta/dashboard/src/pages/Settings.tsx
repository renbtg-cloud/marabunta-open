// Marabunta - Licensed under the MIT License.
import { useLocalStorage } from '../hooks/useLocalStorage';
import { useCompliancePosture, useConfigSchema } from '../hooks/useSwarmData';
import clsx from 'clsx';

// ── Settings categories ──

type SettingsTab = 'display' | 'data' | 'compliance';

const TABS: { key: SettingsTab; label: string }[] = [
  { key: 'display', label: 'Display' },
  { key: 'data', label: 'Data & Refresh' },
  { key: 'compliance', label: 'Compliance' },
];

// ── Component ──

export function Settings() {
  const [activeTab, setActiveTab] = useLocalStorage<SettingsTab>('marabunta-settings-tab', 'display');

  // Display preferences
  const [defaultView, setDefaultView] = useLocalStorage<'2d' | '3d'>('marabunta-pref-view', '2d');
  const [colorScheme, setColorScheme] = useLocalStorage<'role' | 'status' | 'load'>(
    'marabunta-pref-color',
    'role',
  );
  const [nodeLabels, setNodeLabels] = useLocalStorage<boolean>('marabunta-pref-labels', false);
  const [animationsEnabled, setAnimationsEnabled] = useLocalStorage<boolean>(
    'marabunta-pref-animations',
    true,
  );

  // Data preferences
  const [refreshInterval, setRefreshInterval] = useLocalStorage<number>(
    'marabunta-pref-refresh',
    30,
  );
  const [maxEvents, setMaxEvents] = useLocalStorage<number>('marabunta-pref-max-events', 50);
  const [autoReconnect, setAutoReconnect] = useLocalStorage<boolean>(
    'marabunta-pref-reconnect',
    true,
  );

  // Hooks
  const { data: posture } = useCompliancePosture();
  const { data: schema } = useConfigSchema();

  const overallStatus = posture?.overall_status ?? 'unknown';
  const controls = posture?.controls ?? [];
  const activeProfiles = posture?.active_profiles ?? [];
  const schemaEntries = schema?.entries ?? [];

  return (
    <div className="flex flex-col h-full">
      {/* Page Header */}
      <div className="px-4 sm:px-6 pt-4 sm:pt-6 pb-3 sm:pb-4">
        <h1 className="font-serif text-2xl text-marabunta-t1 mb-1">Settings</h1>
        <p className="text-sm text-marabunta-t2">
          Dashboard preferences, data refresh configuration, and compliance overview
        </p>
      </div>

      {/* Tabs */}
      <div className="px-4 sm:px-6 flex gap-1 mb-4">
        {TABS.map((tab) => (
          <button
            key={tab.key}
            onClick={() => setActiveTab(tab.key)}
            className={clsx(
              'px-4 py-2 text-sm font-mono rounded-lg transition-colors',
              activeTab === tab.key
                ? 'bg-marabunta-cyan/10 text-marabunta-cyan border border-marabunta-cyan/30'
                : 'text-marabunta-t2 hover:text-marabunta-t1 hover:bg-marabunta-bg3 border border-transparent',
            )}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* Tab Content */}
      <div className="flex-1 overflow-y-auto px-4 sm:px-6 pb-6">
        {activeTab === 'display' && (
          <div className="space-y-6 max-w-2xl">
            <SettingsSection title="Visualization">
              <SettingRow label="Default topology view" description="Initial view mode when opening the swarm overview">
                <select
                  value={defaultView}
                  onChange={(e) => setDefaultView(e.target.value as '2d' | '3d')}
                  className="bg-marabunta-bg3 border border-marabunta-border rounded px-3 py-1.5 text-sm font-mono text-marabunta-t2 focus:outline-none focus:border-marabunta-cyan/50"
                >
                  <option value="2d">2D (D3 Force Graph)</option>
                  <option value="3d">3D (Three.js WebGL)</option>
                </select>
              </SettingRow>

              <SettingRow label="Color scheme" description="How nodes are colored in the topology view">
                <select
                  value={colorScheme}
                  onChange={(e) => setColorScheme(e.target.value as 'role' | 'status' | 'load')}
                  className="bg-marabunta-bg3 border border-marabunta-border rounded px-3 py-1.5 text-sm font-mono text-marabunta-t2 focus:outline-none focus:border-marabunta-cyan/50"
                >
                  <option value="role">By Role</option>
                  <option value="status">By Status</option>
                  <option value="load">By Load</option>
                </select>
              </SettingRow>

              <SettingRow label="Show node labels" description="Display node names in the topology graph">
                <ToggleSwitch checked={nodeLabels} onChange={setNodeLabels} />
              </SettingRow>

              <SettingRow label="Animations" description="Enable transition animations and effects">
                <ToggleSwitch checked={animationsEnabled} onChange={setAnimationsEnabled} />
              </SettingRow>
            </SettingsSection>
          </div>
        )}

        {activeTab === 'data' && (
          <div className="space-y-6 max-w-2xl">
            <SettingsSection title="Refresh & Polling">
              <SettingRow label="Auto-refresh interval" description="How often to poll the API for updates (seconds)">
                <select
                  value={refreshInterval}
                  onChange={(e) => setRefreshInterval(Number(e.target.value))}
                  className="bg-marabunta-bg3 border border-marabunta-border rounded px-3 py-1.5 text-sm font-mono text-marabunta-t2 focus:outline-none focus:border-marabunta-cyan/50"
                >
                  <option value={5}>5s</option>
                  <option value={10}>10s</option>
                  <option value={15}>15s</option>
                  <option value={30}>30s (default)</option>
                  <option value={60}>60s</option>
                  <option value={0}>Disabled</option>
                </select>
              </SettingRow>

              <SettingRow label="Max audit events" description="Maximum number of recent events to display in feeds">
                <input
                  type="number"
                  min={5}
                  max={200}
                  value={maxEvents}
                  onChange={(e) => setMaxEvents(Number(e.target.value))}
                  className="w-24 bg-marabunta-bg3 border border-marabunta-border rounded px-3 py-1.5 text-sm font-mono text-marabunta-t2 focus:outline-none focus:border-marabunta-cyan/50"
                />
              </SettingRow>

              <SettingRow label="Auto-reconnect WebSocket" description="Automatically reconnect when the WebSocket connection drops">
                <ToggleSwitch checked={autoReconnect} onChange={setAutoReconnect} />
              </SettingRow>
            </SettingsSection>
          </div>
        )}

        {activeTab === 'compliance' && (
          <div className="space-y-6 max-w-3xl">
            {/* Overall Status */}
            <SettingsSection title="Compliance Posture">
              <div className="flex items-center gap-3 px-4 py-3 bg-marabunta-bgc border border-marabunta-border rounded-xl">
                <span
                  className={clsx(
                    'w-4 h-4 rounded-full',
                    overallStatus === 'passing' && 'bg-emerald-400',
                    overallStatus === 'warning' && 'bg-amber-400',
                    overallStatus === 'violation' && 'bg-rose-400',
                    overallStatus === 'unknown' && 'bg-gray-400',
                  )}
                />
                <span className="text-marabunta-t1 font-mono text-sm capitalize">
                  {overallStatus}
                </span>
                {activeProfiles.length > 0 && (
                  <span className="text-marabunta-muted text-xs ml-2">
                    Profiles: {activeProfiles.join(', ')}
                  </span>
                )}
                <span className="ml-auto text-marabunta-muted text-xs font-mono">
                  {controls.length} controls
                </span>
              </div>
            </SettingsSection>

            {/* Controls Grid */}
            {controls.length > 0 && (
              <SettingsSection title="Controls">
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
                  {controls.map((ctrl: { key: string; status: string; description?: string }, i: number) => (
                    <div
                      key={i}
                      className={clsx(
                        'px-3 py-2 border rounded-lg text-sm',
                        ctrl.status === 'passing' && 'border-emerald-500/30 bg-emerald-500/5',
                        ctrl.status === 'warning' && 'border-amber-500/30 bg-amber-500/5',
                        ctrl.status === 'violation' && 'border-rose-500/30 bg-rose-500/5',
                        ctrl.status === 'unknown' && 'border-gray-500/30 bg-gray-500/5',
                      )}
                    >
                      <div className="flex items-center gap-2">
                        <span
                          className={clsx(
                            'w-2 h-2 rounded-full shrink-0',
                            ctrl.status === 'passing' && 'bg-emerald-400',
                            ctrl.status === 'warning' && 'bg-amber-400',
                            ctrl.status === 'violation' && 'bg-rose-400',
                            ctrl.status === 'unknown' && 'bg-gray-400',
                          )}
                        />
                        <span className="text-marabunta-t2 font-mono text-xs truncate">
                          {ctrl.key}
                        </span>
                      </div>
                      {ctrl.description && (
                        <p className="text-marabunta-muted text-xs mt-1 pl-4">{ctrl.description}</p>
                      )}
                    </div>
                  ))}
                </div>
              </SettingsSection>
            )}

            {/* Config Schema Summary */}
            {schemaEntries.length > 0 && (
              <SettingsSection title="Configuration Schema">
                <div className="text-xs text-marabunta-muted mb-2">
                  {schemaEntries.length} registered configuration keys
                </div>
                <div className="space-y-1 max-h-64 overflow-y-auto">
                  {schemaEntries.slice(0, 30).map((entry: { key: string; tier: string; category: string }, i: number) => (
                    <div key={i} className="flex items-center gap-2 px-2 py-1 text-xs">
                      <span
                        className={clsx(
                          'px-1.5 py-0.5 rounded text-[10px] font-mono shrink-0',
                          entry.tier === 'hardwired' && 'bg-gray-500/20 text-gray-400',
                          entry.tier === 'startup' && 'bg-amber-500/20 text-amber-400',
                          entry.tier === 'runtime' && 'bg-emerald-500/20 text-emerald-400',
                        )}
                      >
                        {entry.tier}
                      </span>
                      <span className="text-marabunta-t2 font-mono truncate">{entry.key}</span>
                      <span className="text-marabunta-muted ml-auto shrink-0">{entry.category}</span>
                    </div>
                  ))}
                  {schemaEntries.length > 30 && (
                    <div className="text-marabunta-muted text-xs px-2 py-1">
                      ...and {schemaEntries.length - 30} more
                    </div>
                  )}
                </div>
              </SettingsSection>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

// ── Sub-components ──

function SettingsSection({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div>
      <h2 className="font-mono text-sm text-marabunta-t1 mb-3 pb-2 border-b border-marabunta-border">
        {title}
      </h2>
      <div className="space-y-4">{children}</div>
    </div>
  );
}

function SettingRow({
  label,
  description,
  children,
}: {
  label: string;
  description: string;
  children: React.ReactNode;
}) {
  return (
    <div className="flex items-center justify-between gap-4 py-2">
      <div className="min-w-0">
        <div className="text-sm text-marabunta-t1">{label}</div>
        <div className="text-xs text-marabunta-muted">{description}</div>
      </div>
      <div className="shrink-0">{children}</div>
    </div>
  );
}

function ToggleSwitch({
  checked,
  onChange,
}: {
  checked: boolean;
  onChange: (val: boolean) => void;
}) {
  return (
    <button
      role="switch"
      aria-checked={checked}
      onClick={() => onChange(!checked)}
      className={clsx(
        'relative inline-flex h-6 w-11 items-center rounded-full transition-colors',
        checked ? 'bg-marabunta-cyan' : 'bg-marabunta-bg3 border border-marabunta-border',
      )}
    >
      <span
        className={clsx(
          'inline-block h-4 w-4 rounded-full bg-white transition-transform',
          checked ? 'translate-x-6' : 'translate-x-1',
        )}
      />
    </button>
  );
}
