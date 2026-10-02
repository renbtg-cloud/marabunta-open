// Marabunta - Licensed under the MIT License.
import { useState, useEffect, useRef } from 'react';
import clsx from 'clsx';
import { useDebounce } from '../hooks/useDebounce';

interface SearchBarProps {
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  delay?: number;
  className?: string;
}

export function SearchBar({
  value: externalValue,
  onChange,
  placeholder = 'Search...',
  delay = 300,
  className,
}: SearchBarProps) {
  const [localValue, setLocalValue] = useState(externalValue);
  const debouncedValue = useDebounce(localValue, delay);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    onChange(debouncedValue);
  }, [debouncedValue, onChange]);

  useEffect(() => {
    setLocalValue(externalValue);
  }, [externalValue]);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === '/' && document.activeElement?.tagName !== 'INPUT') {
        e.preventDefault();
        inputRef.current?.focus();
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, []);

  return (
    <div className={clsx('relative', className)}>
      <svg
        className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-marabunta-muted"
        viewBox="0 0 16 16"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
      >
        <circle cx="6.5" cy="6.5" r="5" />
        <path d="M10.5 10.5L14.5 14.5" />
      </svg>
      <input
        ref={inputRef}
        type="text"
        value={localValue}
        onChange={(e) => setLocalValue(e.target.value)}
        placeholder={placeholder}
        className="w-full pl-9 pr-8 py-2.5 bg-marabunta-bg3 border border-marabunta-border rounded-lg text-base sm:text-sm text-marabunta-t1 placeholder:text-marabunta-muted focus:outline-none focus:border-marabunta-cyan/50 transition-colors"
      />
      {localValue && (
        <button
          onClick={() => {
            setLocalValue('');
            inputRef.current?.focus();
          }}
          className="absolute right-2 top-1/2 -translate-y-1/2 text-marabunta-muted hover:text-marabunta-t1 transition-colors"
          title="Clear search"
        >
          <svg className="w-4 h-4" viewBox="0 0 16 16" fill="currentColor">
            <path d="M4.646 4.646a.5.5 0 0 1 .708 0L8 7.293l2.646-2.647a.5.5 0 0 1 .708.708L8.707 8l2.647 2.646a.5.5 0 0 1-.708.708L8 8.707l-2.646 2.647a.5.5 0 0 1-.708-.708L7.293 8 4.646 5.354a.5.5 0 0 1 0-.708z" />
          </svg>
        </button>
      )}
      {!localValue && (
        <span className="absolute right-2 top-1/2 -translate-y-1/2 text-[10px] font-mono text-marabunta-muted/50 px-1 py-0.5 border border-marabunta-border rounded">
          /
        </span>
      )}
    </div>
  );
}
