// Marabunta - Licensed under the MIT License.
import { useState, useRef, useEffect } from 'react';
import clsx from 'clsx';

interface FilterDropdownProps {
  label: string;
  options: string[];
  selected: string[];
  onChange: (selected: string[]) => void;
  className?: string;
}

export function FilterDropdown({
  label,
  options,
  selected,
  onChange,
  className,
}: FilterDropdownProps) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  const toggleOption = (opt: string) => {
    if (selected.includes(opt)) {
      onChange(selected.filter((s) => s !== opt));
    } else {
      onChange([...selected, opt]);
    }
  };

  const toggleAll = () => {
    if (selected.length === options.length) {
      onChange([]);
    } else {
      onChange([...options]);
    }
  };

  const activeCount = selected.length;

  return (
    <div ref={ref} className={clsx('relative', className)}>
      <button
        onClick={() => setOpen((prev) => !prev)}
        className={clsx(
          'flex items-center gap-2 px-3 py-2.5 rounded-lg text-sm font-mono border transition-colors',
          activeCount > 0
            ? 'bg-marabunta-cyan/10 text-marabunta-cyan border-marabunta-cyan/30'
            : 'bg-marabunta-bg3 text-marabunta-t2 border-marabunta-border hover:border-marabunta-cyan/30',
        )}
      >
        {label}
        {activeCount > 0 && (
          <span className="bg-marabunta-cyan/20 text-marabunta-cyan text-xs px-1.5 py-0.5 rounded-full min-w-[20px] text-center">
            {activeCount}
          </span>
        )}
        <svg
          className={clsx(
            'w-3 h-3 transition-transform',
            open ? 'rotate-180' : '',
          )}
          viewBox="0 0 16 16"
          fill="currentColor"
        >
          <path d="M4 6l4 4 4-4" />
        </svg>
      </button>

      {open && (
        <div className="absolute top-full mt-1 left-0 z-50 min-w-[200px] max-w-[calc(100vw-2rem)] bg-marabunta-bg2 border border-marabunta-border rounded-lg shadow-xl overflow-hidden">
          {/* Select All */}
          <button
            onClick={toggleAll}
            className="flex items-center gap-2 w-full px-3 py-2.5 text-sm text-marabunta-t2 hover:bg-marabunta-bg3 transition-colors border-b border-marabunta-border"
          >
            <span
              className={clsx(
                'w-4 h-4 rounded border flex items-center justify-center text-[10px]',
                selected.length === options.length
                  ? 'bg-marabunta-cyan/20 border-marabunta-cyan text-marabunta-cyan'
                  : 'border-marabunta-border',
              )}
            >
              {selected.length === options.length && '\u2713'}
            </span>
            Select All
          </button>

          {/* Options */}
          <div className="max-h-[240px] overflow-y-auto">
            {options.map((opt) => {
              const isSelected = selected.includes(opt);
              return (
                <button
                  key={opt}
                  onClick={() => toggleOption(opt)}
                  className="flex items-center gap-2 w-full px-3 py-2.5 text-sm text-marabunta-t2 hover:bg-marabunta-bg3 transition-colors"
                >
                  <span
                    className={clsx(
                      'w-4 h-4 rounded border flex items-center justify-center text-[10px]',
                      isSelected
                        ? 'bg-marabunta-cyan/20 border-marabunta-cyan text-marabunta-cyan'
                        : 'border-marabunta-border',
                    )}
                  >
                    {isSelected && '\u2713'}
                  </span>
                  <span className="font-mono text-xs">{opt}</span>
                </button>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}
