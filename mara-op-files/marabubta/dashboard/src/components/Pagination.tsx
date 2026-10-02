// Marabunta - Licensed under the MIT License.
import clsx from 'clsx';

interface PaginationProps {
  page: number;
  totalPages: number;
  onPageChange: (page: number) => void;
  className?: string;
}

function getPageNumbers(current: number, total: number): (number | 'ellipsis')[] {
  if (total <= 7) {
    return Array.from({ length: total }, (_, i) => i + 1);
  }

  const pages: (number | 'ellipsis')[] = [1];

  if (current > 3) {
    pages.push('ellipsis');
  }

  const start = Math.max(2, current - 1);
  const end = Math.min(total - 1, current + 1);

  for (let i = start; i <= end; i++) {
    pages.push(i);
  }

  if (current < total - 2) {
    pages.push('ellipsis');
  }

  if (total > 1) {
    pages.push(total);
  }

  return pages;
}

export function Pagination({ page, totalPages, onPageChange, className }: PaginationProps) {
  const pages = getPageNumbers(page, totalPages);

  return (
    <div className={clsx('flex items-center gap-1 flex-wrap', className)}>
      <button
        onClick={() => onPageChange(page - 1)}
        disabled={page <= 1}
        className={clsx(
          'px-3 py-2 sm:py-1.5 rounded text-xs font-mono transition-colors min-h-[44px] sm:min-h-0',
          page <= 1
            ? 'text-marabunta-muted/50 cursor-not-allowed'
            : 'text-marabunta-t2 hover:text-marabunta-t1 hover:bg-marabunta-bg3',
        )}
      >
        Prev
      </button>

      {pages.map((p, idx) =>
        p === 'ellipsis' ? (
          <span
            key={`ellipsis-${idx}`}
            className="px-2 py-1.5 text-xs font-mono text-marabunta-muted"
          >
            ...
          </span>
        ) : (
          <button
            key={p}
            onClick={() => onPageChange(p)}
            className={clsx(
              'px-3 py-2 sm:py-1.5 rounded text-xs font-mono transition-colors min-h-[44px] sm:min-h-0',
              p === page
                ? 'bg-marabunta-cyan/15 text-marabunta-cyan border border-marabunta-cyan/30'
                : 'text-marabunta-t2 hover:text-marabunta-t1 hover:bg-marabunta-bg3 border border-transparent',
            )}
          >
            {p}
          </button>
        ),
      )}

      <button
        onClick={() => onPageChange(page + 1)}
        disabled={page >= totalPages}
        className={clsx(
          'px-3 py-2 sm:py-1.5 rounded text-xs font-mono transition-colors min-h-[44px] sm:min-h-0',
          page >= totalPages
            ? 'text-marabunta-muted/50 cursor-not-allowed'
            : 'text-marabunta-t2 hover:text-marabunta-t1 hover:bg-marabunta-bg3',
        )}
      >
        Next
      </button>
    </div>
  );
}
