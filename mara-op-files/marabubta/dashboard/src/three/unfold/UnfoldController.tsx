// Marabunta - Licensed under the MIT License.
// ── UnfoldController ──
// Four-state finite state machine governing the unfold/fold animation.
// States: Folded -> Unfolding -> Unfolded -> Folding -> Folded
// Owns a single elapsed timer that ticks forward during Unfolding and backward
// during Folding. All subsystems derive their activation from the same
// progress value, guaranteeing perfect synchronization. (W5D / Spec S21)

import { useState, useCallback, useRef, useEffect, useMemo, createContext, useContext } from 'react';
import { useFrame } from '@react-three/fiber';
import { easeOutCubic, easeInCubic } from './easing';
import { computeTimeline, type TimelineSlice } from './UnfoldTimeline';

// ── Types ──

export type UnfoldState = 'Folded' | 'Unfolding' | 'Unfolded' | 'Folding';

export interface UnfoldContext {
  /** Current state machine state */
  state: UnfoldState;
  /** Normalized progress: 0.0 = fully folded, 1.0 = fully unfolded */
  progress: number;
  /** Wall-clock elapsed time in seconds (0 to DURATION) */
  elapsed: number;
  /** Request a state transition (toggle fold/unfold) */
  toggle: () => void;
  /** Whether user interaction should be blocked (animation in progress) */
  isAnimating: boolean;
  /** Per-subsystem timeline slice derived from current progress */
  timeline: TimelineSlice;
  /** Whether prefers-reduced-motion is active */
  reducedMotion: boolean;
}

// ── Constants ──

/** Default animation duration in seconds */
const DURATION = 2.0;

/** Reduced-motion fallback duration in seconds */
const REDUCED_MOTION_DURATION = 0.3;

// ── Reduced Motion Detection ──

function useReducedMotion(): boolean {
  const [reduced, setReduced] = useState(() => {
    if (typeof window === 'undefined') return false;
    return window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  });

  useEffect(() => {
    if (typeof window === 'undefined') return;
    const mql = window.matchMedia('(prefers-reduced-motion: reduce)');
    const handler = (e: MediaQueryListEvent) => setReduced(e.matches);
    mql.addEventListener('change', handler);
    return () => mql.removeEventListener('change', handler);
  }, []);

  return reduced;
}

// ── Direction-Aware Easing ──

/**
 * Apply easing based on the current animation direction.
 * Unfold: easeOutCubic (fast start, smooth deceleration).
 * Fold: easeInCubic (slow start, fast finish).
 * Static states: return raw progress (0 or 1).
 */
export function directionalEase(progress: number, state: UnfoldState): number {
  if (state === 'Unfolding') return easeOutCubic(progress);
  if (state === 'Folding') return easeInCubic(progress);
  return progress;
}

// ── Hook ──

export function useUnfoldController(): UnfoldContext {
  const [state, setState] = useState<UnfoldState>('Folded');
  const [progress, setProgress] = useState(0);
  const elapsedRef = useRef(0);
  const reducedMotion = useReducedMotion();

  const duration = reducedMotion ? REDUCED_MOTION_DURATION : DURATION;

  const toggle = useCallback(() => {
    if (state === 'Folded') {
      setState('Unfolding');
    } else if (state === 'Unfolded') {
      setState('Folding');
    }
    // No-op during Unfolding or Folding states.
    // The animation must complete before it can be reversed.
  }, [state]);

  useFrame((_, delta) => {
    if (state === 'Unfolding') {
      elapsedRef.current = Math.min(elapsedRef.current + delta, duration);
      const newProgress = elapsedRef.current / duration;
      setProgress(newProgress);
      if (elapsedRef.current >= duration) {
        setState('Unfolded');
        setProgress(1);
        elapsedRef.current = duration;
      }
    } else if (state === 'Folding') {
      elapsedRef.current = Math.max(elapsedRef.current - delta, 0);
      const newProgress = elapsedRef.current / duration;
      setProgress(newProgress);
      if (elapsedRef.current <= 0) {
        setState('Folded');
        setProgress(0);
        elapsedRef.current = 0;
      }
    }
  });

  const timeline = useMemo(() => computeTimeline(progress), [progress]);

  return {
    state,
    progress,
    elapsed: elapsedRef.current,
    toggle,
    isAnimating: state === 'Unfolding' || state === 'Folding',
    timeline,
    reducedMotion,
  };
}

// ── React Context ──
// Allows any descendant component to access the unfold state without prop drilling.

const UnfoldCtx = createContext<UnfoldContext | null>(null);

export const UnfoldProvider = UnfoldCtx.Provider;

/**
 * Hook to consume the unfold context.
 * Must be called within a component tree that has an UnfoldProvider ancestor.
 */
export function useUnfold(): UnfoldContext {
  const ctx = useContext(UnfoldCtx);
  if (!ctx) {
    throw new Error('useUnfold must be used within an UnfoldProvider');
  }
  return ctx;
}
