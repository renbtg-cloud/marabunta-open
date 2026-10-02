// Marabunta - Licensed under the MIT License.
import clsx from 'clsx';

export type TrendDirection = 'up' | 'down' | 'flat';

interface MetricCardProps {
  value: string | number;
  label: string;
  trend?: TrendDirection;
  className?: string;
}

export function MetricCard({ value, label, trend, className }: MetricCardProps) {
  return (
    <div className={clsx('bg-marabunta-bgc border border-marabunta-border rounded-xl p-4 text-center', className)}>
      <div className="font-serif text-3xl text-marabunta-t1">
        {value}
        {trend === 'up' && (
          <svg
            className="inline w-4 h-4 text-emerald-400 ml-1"
            viewBox="0 0 16 16"
            fill="currentColor"
          >
            <path d="M8 3l5 6H3l5-6z" />
          </svg>
        )}
        {trend === 'down' && (
          <svg
            className="inline w-4 h-4 text-rose-400 ml-1"
            viewBox="0 0 16 16"
            fill="currentColor"
          >
            <path d="M8 13l5-6H3l5 6z" />
          </svg>
        )}
      </div>
      <div className="font-mono text-xs uppercase tracking-wider text-marabunta-muted mt-1">
        {label}
      </div>
    </div>
  );
}
