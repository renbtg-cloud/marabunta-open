// Marabunta - Licensed under the MIT License.
import clsx from 'clsx';

const ROLE_COLORS: Record<string, string> = {
  AGGREGATOR: 'bg-cyan-500/20 text-cyan-400 border-cyan-500/40',
  WITNESS: 'bg-violet-500/20 text-violet-400 border-violet-500/40',
  GATEWAY: 'bg-emerald-500/20 text-emerald-400 border-emerald-500/40',
  RELAY: 'bg-blue-500/20 text-blue-400 border-blue-500/40',
  STORAGE: 'bg-amber-500/20 text-amber-400 border-amber-500/40',
  EPHEMERAL: 'bg-rose-500/20 text-rose-400 border-rose-500/40',
};

interface NodeBadgeProps {
  role: string;
  pinned?: boolean;
  className?: string;
}

export function NodeBadge({ role, pinned, className }: NodeBadgeProps) {
  const colorClasses =
    ROLE_COLORS[role] ?? 'bg-gray-500/20 text-gray-400 border-gray-500/40';

  return (
    <span
      className={clsx(
        'inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-mono border',
        colorClasses,
        className,
      )}
    >
      {pinned && (
        <svg
          className="w-3 h-3"
          viewBox="0 0 16 16"
          fill="currentColor"
          aria-label="Pinned role"
        >
          <path d="M4 4a4 4 0 0 1 8 0v2h.5A1.5 1.5 0 0 1 14 7.5v.5H2v-.5A1.5 1.5 0 0 1 3.5 6H4V4zm2 0v2h4V4a2 2 0 1 0-4 0zm-1 6h6v4.5a.5.5 0 0 1-.5.5h-5a.5.5 0 0 1-.5-.5V10z" />
        </svg>
      )}
      {role}
    </span>
  );
}
