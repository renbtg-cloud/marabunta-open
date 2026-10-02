// Marabunta - Licensed under the MIT License.
import { useState, useEffect } from 'react';
import { NavLink, Outlet } from 'react-router-dom';
import clsx from 'clsx';
import { useWebSocket } from '../hooks/useWebSocket';
import { TimeTravelContext, useTimeTravelState } from '../hooks/useTimeTravel';
import { TimeTravelScrubber } from './TimeTravelScrubber';
import { ComplianceWidget } from './ComplianceWidget';
import type { WsState } from '../api/types';

interface NavItem {
  path: string;
  label: string;
  icon: string;
}

const NAV_ITEMS: NavItem[] = [
  { path: '/setup', label: 'Setup', icon: '\u2726' },
  { path: '/', label: 'Swarm Overview', icon: '\u2B21' },
  { path: '/fleet', label: 'Fleet Overview', icon: '\u2630' },
  { path: '/storage', label: 'Storage Tiers', icon: '\u2395' },
  { path: '/audit', label: 'Audit Trail', icon: '\u2691' },
  { path: '/wargames', label: 'War Games', icon: '\u2623' },
  { path: '/settings', label: 'Settings', icon: '\u2699' },
];

const WS_STATUS_STYLES: Record<WsState, { dot: string; label: string }> = {
  connected: { dot: 'bg-emerald-400', label: 'Connected' },
  connecting: { dot: 'bg-amber-400 animate-pulse', label: 'Connecting...' },
  reconnecting: { dot: 'bg-amber-400 animate-pulse', label: 'Reconnecting...' },
  disconnected: { dot: 'bg-rose-400', label: 'Disconnected' },
};

const COLLAPSED_KEY = 'marabunta-sidebar-collapsed';

export function Layout() {
  const [collapsed, setCollapsed] = useState(() => {
    try {
      return localStorage.getItem(COLLAPSED_KEY) === 'true';
    } catch {
      return false;
    }
  });
  const [mobileOpen, setMobileOpen] = useState(false);
  const { state: wsState } = useWebSocket();
  const timeTravelState = useTimeTravelState();

  useEffect(() => {
    try {
      localStorage.setItem(COLLAPSED_KEY, String(collapsed));
    } catch {
      // localStorage unavailable
    }
  }, [collapsed]);

  // Auto-close mobile drawer when resizing above md breakpoint
  useEffect(() => {
    const mq = window.matchMedia('(min-width: 768px)');
    const handler = () => {
      if (mq.matches) setMobileOpen(false);
    };
    mq.addEventListener('change', handler);
    return () => mq.removeEventListener('change', handler);
  }, []);

  const wsStyle = WS_STATUS_STYLES[wsState];

  return (
    <TimeTravelContext.Provider value={timeTravelState}>
      <div className="flex flex-col md:flex-row h-screen bg-marabunta-bg overflow-hidden">
        {/* Mobile Header Bar */}
        <div className="md:hidden flex items-center justify-between px-4 py-3 bg-marabunta-bg2 border-b border-marabunta-border shrink-0">
          <button
            onClick={() => setMobileOpen((prev) => !prev)}
            className="flex items-center justify-center w-10 h-10 rounded-lg text-marabunta-t2 hover:bg-marabunta-bg3 transition-colors"
            aria-label="Toggle navigation"
          >
            {mobileOpen ? (
              <svg className="w-5 h-5" viewBox="0 0 20 20" fill="currentColor">
                <path d="M4.293 4.293a1 1 0 011.414 0L10 8.586l4.293-4.293a1 1 0 111.414 1.414L11.414 10l4.293 4.293a1 1 0 01-1.414 1.414L10 11.414l-4.293 4.293a1 1 0 01-1.414-1.414L8.586 10 4.293 5.707a1 1 0 010-1.414z" />
              </svg>
            ) : (
              <svg className="w-5 h-5" viewBox="0 0 20 20" fill="currentColor">
                <path fillRule="evenodd" d="M3 5a1 1 0 011-1h12a1 1 0 110 2H4a1 1 0 01-1-1zm0 5a1 1 0 011-1h12a1 1 0 110 2H4a1 1 0 01-1-1zm0 5a1 1 0 011-1h12a1 1 0 110 2H4a1 1 0 01-1-1z" clipRule="evenodd" />
              </svg>
            )}
          </button>
          <span className="font-mono font-semibold text-marabunta-cyan text-sm tracking-wide">
            MARABUNTA::COMPUTE
          </span>
          <div className="flex items-center gap-2">
            <span className={clsx('inline-block w-2 h-2 rounded-full', wsStyle.dot)} />
          </div>
        </div>

        {/* Mobile Backdrop */}
        {mobileOpen && (
          <div
            className="fixed inset-0 z-40 bg-black/50 md:hidden"
            onClick={() => setMobileOpen(false)}
          />
        )}

        {/* Sidebar */}
        <aside
          className={clsx(
            'flex flex-col bg-marabunta-bg2 transition-all duration-200 shrink-0',
            // Desktop: normal sidebar with border
            'md:relative md:border-r md:border-marabunta-border',
            collapsed ? 'md:w-16' : 'md:w-60',
            // Mobile: fixed overlay or hidden
            mobileOpen
              ? 'fixed inset-y-0 left-0 z-50 w-60 border-r border-marabunta-border'
              : 'hidden md:flex',
          )}
        >
          {/* Logo / Header */}
          <div className="flex items-center gap-2 px-4 py-4 border-b border-marabunta-border min-h-[56px]">
            <span className="font-mono font-semibold text-marabunta-cyan text-sm tracking-wide whitespace-nowrap">
              {collapsed ? 'H' : 'MARABUNTA'}
            </span>
            {!collapsed && (
              <span className="font-mono text-marabunta-muted text-sm">::COMPUTE</span>
            )}
          </div>

          {/* Navigation Links */}
          <nav className="flex-1 py-2">
            {NAV_ITEMS.map((item) => (
              <NavLink
                key={item.path}
                to={item.path}
                end={item.path === '/'}
                onClick={() => setMobileOpen(false)}
                className={({ isActive }) =>
                  clsx(
                    'flex items-center gap-3 px-4 py-2.5 mx-2 rounded-lg text-sm transition-colors',
                    isActive
                      ? 'bg-marabunta-cyan/10 text-marabunta-cyan border border-marabunta-cyan/30'
                      : 'text-marabunta-t2 hover:text-marabunta-t1 hover:bg-marabunta-bg3 border border-transparent',
                  )
                }
              >
                <span className="text-lg shrink-0 w-5 text-center">{item.icon}</span>
                {!collapsed && <span className="truncate">{item.label}</span>}
              </NavLink>
            ))}
          </nav>

          {/* Compliance Widget */}
          <div className="border-t border-marabunta-border">
            <ComplianceWidget collapsed={collapsed} />
          </div>

          {/* Collapse Toggle (desktop only) */}
          <button
            onClick={() => setCollapsed((prev) => !prev)}
            className="hidden md:flex items-center justify-center px-4 py-2 text-marabunta-muted hover:text-marabunta-t1 transition-colors"
            title={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
          >
            <span className="text-sm font-mono">
              {collapsed ? '\u00BB' : '\u00AB'}
            </span>
          </button>

          {/* Connection Status */}
          <div className="flex items-center gap-2 px-4 py-3 border-t border-marabunta-border">
            <span
              className={clsx('inline-block w-2 h-2 rounded-full shrink-0', wsStyle.dot)}
            />
            {!collapsed && (
              <span className="font-mono text-xs text-marabunta-muted truncate">
                {wsStyle.label}
              </span>
            )}
          </div>
        </aside>

        {/* Main Content + Time Travel Scrubber */}
        <div className="flex-1 flex flex-col min-h-0">
          <main className="flex-1 overflow-y-auto min-h-0">
            <Outlet />
          </main>
          <TimeTravelScrubber />
        </div>
      </div>
    </TimeTravelContext.Provider>
  );
}
