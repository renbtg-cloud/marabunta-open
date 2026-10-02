// Marabunta - Licensed under the MIT License.
import clsx from 'clsx';
import type { NodeStatus } from '../api/types';

const STATUS_STYLES: Record<NodeStatus, { dot: string; label: string; symbol: string }> = {
  online: { dot: 'bg-emerald-400', label: 'Online', symbol: '\u25CF' },
  idle: { dot: 'bg-amber-400 animate-pulse', label: 'Idle', symbol: '\u25D0' },
  offline: { dot: 'bg-rose-400', label: 'Offline', symbol: '\u25CB' },
};

interface StatusIndicatorProps {
  status: NodeStatus;
  showLabel?: boolean;
  className?: string;
}

export function StatusIndicator({ status, showLabel, className }: StatusIndicatorProps) {
  const style = STATUS_STYLES[status] ?? STATUS_STYLES.offline;

  return (
    <span className={clsx('inline-flex items-center gap-1.5', className)}>
      <span
        className={clsx('inline-block w-2.5 h-2.5 rounded-full', style.dot)}
        title={style.label}
      />
      {showLabel && (
        <span className="text-xs text-marabunta-t2">{style.label}</span>
      )}
    </span>
  );
}
