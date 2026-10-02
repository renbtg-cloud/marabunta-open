// Marabunta - Licensed under the MIT License.
import { useState } from 'react';
import clsx from 'clsx';
import { useCompliancePosture } from '../hooks/useSwarmData';

const POSTURE_STYLES = {
  passing: { dot: 'bg-emerald-400', label: 'All Clear', text: 'text-emerald-400' },
  warning: { dot: 'bg-amber-400', label: 'Warnings', text: 'text-amber-400' },
  violation: { dot: 'bg-rose-400', label: 'Violations', text: 'text-rose-400' },
  unknown: { dot: 'bg-gray-400', label: 'Unknown', text: 'text-gray-400' },
};

export function ComplianceWidget({ collapsed }: { collapsed: boolean }) {
  const [expanded, setExpanded] = useState(false);
  const { data: posture } = useCompliancePosture();

  if (!posture) return null;

  const overallStatus = posture.overall_status ?? 'unknown';
  const style = POSTURE_STYLES[overallStatus as keyof typeof POSTURE_STYLES] ?? POSTURE_STYLES.unknown;

  const passingCount = posture.controls?.filter((c: { status: string }) => c.status === 'passing').length ?? 0;
  const warningCount = posture.controls?.filter((c: { status: string }) => c.status === 'warning').length ?? 0;
  const violationCount = posture.controls?.filter((c: { status: string }) => c.status === 'violation').length ?? 0;
  const totalControls = posture.controls?.length ?? 0;

  if (collapsed) {
    return (
      <div className="flex items-center justify-center px-4 py-2" title={`Compliance: ${style.label}`}>
        <span className={clsx('inline-block w-2.5 h-2.5 rounded-full', style.dot)} />
      </div>
    );
  }

  return (
    <div className="px-3 py-2">
      <button
        onClick={() => setExpanded((p) => !p)}
        className="flex items-center gap-2 w-full text-left"
      >
        <span className={clsx('inline-block w-2 h-2 rounded-full shrink-0', style.dot)} />
        <span className={clsx('font-mono text-xs', style.text)}>
          Compliance: {style.label}
        </span>
        <span className="ml-auto text-marabunta-muted text-xs">{expanded ? '\u25B4' : '\u25BE'}</span>
      </button>

      {expanded && (
        <div className="mt-2 pl-4 space-y-1.5 text-xs">
          {posture.active_profiles?.length > 0 && (
            <div>
              <span className="text-marabunta-muted">Profiles: </span>
              <span className="text-marabunta-t2 font-mono">
                {posture.active_profiles.join(', ')}
              </span>
            </div>
          )}
          <div className="flex items-center gap-3">
            <span className="text-emerald-400 font-mono">{passingCount} passing</span>
            {warningCount > 0 && (
              <span className="text-amber-400 font-mono">{warningCount} warn</span>
            )}
            {violationCount > 0 && (
              <span className="text-rose-400 font-mono">{violationCount} fail</span>
            )}
          </div>
          <div className="h-1.5 bg-marabunta-bg3 rounded-full overflow-hidden flex">
            {totalControls > 0 && (
              <>
                <div
                  className="h-full bg-emerald-400"
                  style={{ width: `${(passingCount / totalControls) * 100}%` }}
                />
                <div
                  className="h-full bg-amber-400"
                  style={{ width: `${(warningCount / totalControls) * 100}%` }}
                />
                <div
                  className="h-full bg-rose-400"
                  style={{ width: `${(violationCount / totalControls) * 100}%` }}
                />
              </>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
