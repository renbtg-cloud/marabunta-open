// Marabunta - Licensed under the MIT License.
import clsx from 'clsx';
import type { Criticality } from '../api/types';

const CRITICALITY_STYLES: Record<Criticality, string> = {
  INFO: 'bg-blue-500/15 text-marabunta-blue border-blue-500/30',
  WARN: 'bg-amber-500/15 text-marabunta-amber border-amber-500/30',
  AUDIT: 'bg-violet-500/15 text-marabunta-violet border-violet-500/30',
  ALERT: 'bg-rose-500/15 text-marabunta-rose border-rose-500/30',
  CRITICAL: 'bg-rose-500/20 text-marabunta-rose border-rose-500/40 animate-pulse',
};

interface CriticalityBadgeProps {
  level: Criticality;
  className?: string;
}

export function CriticalityBadge({ level, className }: CriticalityBadgeProps) {
  const style = CRITICALITY_STYLES[level] ?? CRITICALITY_STYLES.INFO;

  return (
    <span
      className={clsx(
        'inline-block px-2 py-0.5 rounded text-xs font-mono border',
        style,
        className,
      )}
    >
      {level}
    </span>
  );
}
