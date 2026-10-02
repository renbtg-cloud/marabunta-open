// Marabunta - Licensed under the MIT License.
import { useState, useCallback, useRef, useEffect } from 'react';
import clsx from 'clsx';
import { useTimeTravel } from '../hooks/useTimeTravel';

/** Duration of the visible time window in milliseconds (default: 24 hours). */
const WINDOW_MS = 24 * 60 * 60 * 1000;

function formatTimestamp(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

function formatRelative(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime();
  const absDiff = Math.abs(diff);
  if (absDiff < 60_000) return `${Math.round(absDiff / 1000)}s ago`;
  if (absDiff < 3_600_000) return `${Math.round(absDiff / 60_000)}m ago`;
  if (absDiff < 86_400_000) return `${(absDiff / 3_600_000).toFixed(1)}h ago`;
  return `${(absDiff / 86_400_000).toFixed(1)}d ago`;
}

export function TimeTravelScrubber() {
  const {
    isLive,
    timestamp,
    setTimestamp,
    goLive,
    stepBack,
    stepForward,
    bookmarks,
    addBookmark,
    removeBookmark,
  } = useTimeTravel();

  const [jumpInput, setJumpInput] = useState('');
  const [showBookmarks, setShowBookmarks] = useState(false);
  const [bookmarkLabel, setBookmarkLabel] = useState('');
  const trackRef = useRef<HTMLDivElement>(null);

  // Compute thumb position as fraction [0, 1]
  const now = Date.now();
  const windowStart = now - WINDOW_MS;
  const thumbFraction = isLive
    ? 1
    : timestamp
      ? Math.max(0, Math.min(1, (new Date(timestamp).getTime() - windowStart) / WINDOW_MS))
      : 1;

  // Handle track click / drag
  const handleTrackInteraction = useCallback(
    (clientX: number) => {
      if (!trackRef.current) return;
      const rect = trackRef.current.getBoundingClientRect();
      const fraction = Math.max(0, Math.min(1, (clientX - rect.left) / rect.width));
      const ts = new Date(Date.now() - WINDOW_MS + fraction * WINDOW_MS);
      if (fraction >= 0.999) {
        goLive();
      } else {
        setTimestamp(ts.toISOString());
      }
    },
    [goLive, setTimestamp],
  );

  const [dragging, setDragging] = useState(false);

  const handleMouseDown = useCallback(
    (e: React.MouseEvent) => {
      e.preventDefault();
      setDragging(true);
      handleTrackInteraction(e.clientX);
    },
    [handleTrackInteraction],
  );

  useEffect(() => {
    if (!dragging) return;
    const handleMouseMove = (e: MouseEvent) => handleTrackInteraction(e.clientX);
    const handleMouseUp = () => setDragging(false);
    window.addEventListener('mousemove', handleMouseMove);
    window.addEventListener('mouseup', handleMouseUp);
    return () => {
      window.removeEventListener('mousemove', handleMouseMove);
      window.removeEventListener('mouseup', handleMouseUp);
    };
  }, [dragging, handleTrackInteraction]);

  const handleJump = useCallback(() => {
    if (!jumpInput.trim()) return;
    try {
      const d = new Date(jumpInput.trim());
      if (isNaN(d.getTime())) return;
      if (d.getTime() >= Date.now()) {
        goLive();
      } else {
        setTimestamp(d.toISOString());
      }
      setJumpInput('');
    } catch {
      // Invalid date — ignore
    }
  }, [jumpInput, goLive, setTimestamp]);

  const handleAddBookmark = useCallback(() => {
    if (!bookmarkLabel.trim() || !timestamp) return;
    addBookmark(bookmarkLabel.trim());
    setBookmarkLabel('');
  }, [bookmarkLabel, timestamp, addBookmark]);

  return (
    <div className="bg-marabunta-bg2 border-t border-marabunta-border px-4 py-2">
      {/* Status Banner */}
      {!isLive && timestamp && (
        <div className="flex items-center gap-2 mb-2 px-2 py-1.5 bg-amber-500/10 border border-amber-500/30 rounded-lg text-xs">
          <span className="text-amber-400 font-mono">TIME TRAVEL</span>
          <span className="text-marabunta-t2">
            Viewing state at {formatTimestamp(timestamp)} ({formatRelative(timestamp)})
          </span>
          <button
            onClick={goLive}
            className="ml-auto text-emerald-400 hover:text-emerald-300 font-mono text-xs"
          >
            Return to Now
          </button>
        </div>
      )}

      <div className="flex items-center gap-3">
        {/* Step Controls */}
        <div className="flex items-center gap-1 shrink-0">
          <button
            onClick={() => stepBack(3600)}
            className="px-1.5 py-1 text-xs font-mono text-marabunta-muted hover:text-marabunta-t1 bg-marabunta-bg3 rounded transition-colors"
            title="1 hour back"
          >
            &laquo;
          </button>
          <button
            onClick={() => stepBack(300)}
            className="px-1.5 py-1 text-xs font-mono text-marabunta-muted hover:text-marabunta-t1 bg-marabunta-bg3 rounded transition-colors"
            title="5 minutes back"
          >
            &lsaquo;
          </button>
        </div>

        {/* Track */}
        <div
          ref={trackRef}
          className="flex-1 h-6 relative cursor-pointer select-none"
          onMouseDown={handleMouseDown}
        >
          {/* Track background */}
          <div className="absolute top-1/2 -translate-y-1/2 left-0 right-0 h-1.5 bg-marabunta-bg3 rounded-full" />
          {/* Filled portion */}
          <div
            className="absolute top-1/2 -translate-y-1/2 left-0 h-1.5 rounded-full bg-marabunta-cyan/40"
            style={{ width: `${thumbFraction * 100}%` }}
          />
          {/* Thumb */}
          <div
            className={clsx(
              'absolute top-1/2 -translate-y-1/2 -translate-x-1/2 w-3.5 h-3.5 rounded-full border-2 transition-colors',
              isLive
                ? 'bg-emerald-400 border-emerald-500'
                : 'bg-amber-400 border-amber-500',
            )}
            style={{ left: `${thumbFraction * 100}%` }}
          />
          {/* Time labels */}
          <div className="absolute -bottom-3.5 left-0 text-[10px] text-marabunta-muted font-mono">
            -24h
          </div>
          <div className="absolute -bottom-3.5 right-0 text-[10px] text-marabunta-muted font-mono">
            Now
          </div>
        </div>

        {/* Forward Steps */}
        <div className="flex items-center gap-1 shrink-0">
          <button
            onClick={() => stepForward(300)}
            className="px-1.5 py-1 text-xs font-mono text-marabunta-muted hover:text-marabunta-t1 bg-marabunta-bg3 rounded transition-colors"
            title="5 minutes forward"
          >
            &rsaquo;
          </button>
          <button
            onClick={() => stepForward(3600)}
            className="px-1.5 py-1 text-xs font-mono text-marabunta-muted hover:text-marabunta-t1 bg-marabunta-bg3 rounded transition-colors"
            title="1 hour forward"
          >
            &raquo;
          </button>
        </div>

        {/* Now Button */}
        <button
          onClick={goLive}
          className={clsx(
            'px-2.5 py-1 text-xs font-mono rounded transition-colors shrink-0',
            isLive
              ? 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30'
              : 'bg-marabunta-bg3 text-marabunta-muted hover:text-emerald-400 border border-marabunta-border',
          )}
        >
          LIVE
        </button>

        {/* Jump-to Input */}
        <div className="flex items-center gap-1 shrink-0">
          <input
            type="text"
            value={jumpInput}
            onChange={(e) => setJumpInput(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && handleJump()}
            placeholder="Jump to..."
            className="w-36 px-2 py-1 text-xs font-mono bg-marabunta-bg3 border border-marabunta-border rounded text-marabunta-t2 placeholder:text-marabunta-muted focus:outline-none focus:border-marabunta-cyan/50"
          />
        </div>

        {/* Bookmarks */}
        <div className="relative shrink-0">
          <button
            onClick={() => setShowBookmarks((p) => !p)}
            className="px-2 py-1 text-xs font-mono text-marabunta-muted hover:text-marabunta-t1 bg-marabunta-bg3 border border-marabunta-border rounded transition-colors"
            title="Bookmarks"
          >
            {'\u2605'} {bookmarks.length}
          </button>

          {showBookmarks && (
            <div className="absolute bottom-full right-0 mb-1 w-64 bg-marabunta-bg2 border border-marabunta-border rounded-lg shadow-xl z-50 overflow-hidden">
              <div className="px-3 py-2 border-b border-marabunta-border text-xs font-mono text-marabunta-t2">
                Bookmarks
              </div>
              {bookmarks.length === 0 ? (
                <div className="px-3 py-2 text-xs text-marabunta-muted">
                  No bookmarks saved
                </div>
              ) : (
                <div className="max-h-48 overflow-y-auto">
                  {bookmarks.map((bm, i) => (
                    <div
                      key={i}
                      className="flex items-center gap-2 px-3 py-1.5 hover:bg-marabunta-bg3 cursor-pointer text-xs"
                      onClick={() => {
                        setTimestamp(bm.timestamp);
                        setShowBookmarks(false);
                      }}
                    >
                      <span className="text-marabunta-t2 truncate flex-1">{bm.label}</span>
                      <span className="text-marabunta-muted font-mono shrink-0">
                        {formatRelative(bm.timestamp)}
                      </span>
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          removeBookmark(i);
                        }}
                        className="text-marabunta-muted hover:text-marabunta-rose shrink-0"
                      >
                        {'\u2715'}
                      </button>
                    </div>
                  ))}
                </div>
              )}
              {!isLive && timestamp && (
                <div className="flex items-center gap-1 px-3 py-2 border-t border-marabunta-border">
                  <input
                    type="text"
                    value={bookmarkLabel}
                    onChange={(e) => setBookmarkLabel(e.target.value)}
                    onKeyDown={(e) => e.key === 'Enter' && handleAddBookmark()}
                    placeholder="Bookmark label..."
                    className="flex-1 px-2 py-1 text-xs font-mono bg-marabunta-bg3 border border-marabunta-border rounded text-marabunta-t2 placeholder:text-marabunta-muted focus:outline-none"
                  />
                  <button
                    onClick={handleAddBookmark}
                    className="px-2 py-1 text-xs font-mono text-marabunta-cyan hover:text-marabunta-cyan/80 transition-colors"
                  >
                    Save
                  </button>
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
