// Marabunta - Licensed under the MIT License.
import { createContext, useContext, useCallback } from 'react';
import { useLocalStorage } from './useLocalStorage';

// ── Types ──

export interface Bookmark {
  label: string;
  timestamp: string;
  createdAt: string;
}

export interface TimeTravelState {
  /** Whether viewing live (real-time) data. */
  isLive: boolean;
  /** ISO timestamp when viewing historical data, null when live. */
  timestamp: string | null;
  /** Set a specific timestamp to view (exits live mode). */
  setTimestamp(ts: string | null): void;
  /** Return to live/real-time mode. */
  goLive(): void;
  /** Step backward by the given number of seconds. */
  stepBack(seconds: number): void;
  /** Step forward by the given number of seconds. */
  stepForward(seconds: number): void;
  /** Saved timestamp bookmarks. */
  bookmarks: Bookmark[];
  /** Add a bookmark at the current time-travel timestamp. */
  addBookmark(label: string): void;
  /** Remove a bookmark by index. */
  removeBookmark(index: number): void;
}

// ── Context ──

export const TimeTravelContext = createContext<TimeTravelState | null>(null);

export function useTimeTravel(): TimeTravelState {
  const ctx = useContext(TimeTravelContext);
  if (!ctx) {
    throw new Error('useTimeTravel must be used within a TimeTravelProvider');
  }
  return ctx;
}

// ── Provider hook (used inside TimeTravelProvider component) ──

export function useTimeTravelState(): TimeTravelState {
  const [timestamp, setTimestampRaw] = useLocalStorage<string | null>(
    'marabunta-timetravel-ts',
    null,
  );
  const [bookmarks, setBookmarks] = useLocalStorage<Bookmark[]>(
    'marabunta-timetravel-bookmarks',
    [],
  );

  const isLive = timestamp === null;

  const setTimestamp = useCallback(
    (ts: string | null) => {
      setTimestampRaw(ts);
    },
    [setTimestampRaw],
  );

  const goLive = useCallback(() => {
    setTimestampRaw(null);
  }, [setTimestampRaw]);

  const stepBack = useCallback(
    (seconds: number) => {
      const base = timestamp ? new Date(timestamp) : new Date();
      base.setSeconds(base.getSeconds() - seconds);
      setTimestampRaw(base.toISOString());
    },
    [timestamp, setTimestampRaw],
  );

  const stepForward = useCallback(
    (seconds: number) => {
      const base = timestamp ? new Date(timestamp) : new Date();
      base.setSeconds(base.getSeconds() + seconds);
      const now = new Date();
      if (base >= now) {
        setTimestampRaw(null); // Snap to live if we pass current time
      } else {
        setTimestampRaw(base.toISOString());
      }
    },
    [timestamp, setTimestampRaw],
  );

  const addBookmark = useCallback(
    (label: string) => {
      if (!timestamp) return;
      setBookmarks((prev) => [
        ...prev,
        { label, timestamp, createdAt: new Date().toISOString() },
      ]);
    },
    [timestamp, setBookmarks],
  );

  const removeBookmark = useCallback(
    (index: number) => {
      setBookmarks((prev) => prev.filter((_, i) => i !== index));
    },
    [setBookmarks],
  );

  return {
    isLive,
    timestamp,
    setTimestamp,
    goLive,
    stepBack,
    stepForward,
    bookmarks,
    addBookmark,
    removeBookmark,
  };
}
