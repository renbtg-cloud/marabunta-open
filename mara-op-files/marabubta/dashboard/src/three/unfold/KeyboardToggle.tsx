// Marabunta - Licensed under the MIT License.
// ── KeyboardToggle ──
// Listens for keyboard events to toggle the unfold/fold animation.
// Primary trigger: Space key. Alternative fold trigger: Escape key.
// Includes input guards to prevent triggering when the user is typing
// in form elements. (W5D / Spec S21)

import { useEffect } from 'react';
import type { UnfoldState } from './UnfoldController';

// ── Props ──

interface KeyboardToggleProps {
  /** Callback to toggle the unfold/fold state */
  onToggle: () => void;
  /** Whether the toggle should be disabled (animation in progress) */
  disabled: boolean;
  /** Current unfold state (used for Escape key behavior) */
  state: UnfoldState;
}

// ── Tags that should suppress Space key handling ──

const INPUT_TAGS = new Set(['INPUT', 'TEXTAREA', 'SELECT']);

// ── Component ──

/**
 * Pure side-effect component: attaches keyboard listeners to `window`.
 * Renders nothing to the DOM.
 *
 * Space key: toggles between Folded <-> Unfolded (no-op during animation).
 * Escape key: folds back to 2D when in Unfolded state.
 *
 * Guards:
 * - Ignores events when the target is an INPUT, TEXTAREA, or SELECT element.
 * - Ignores events when the target has contentEditable="true".
 * - Ignores events when `disabled` is true (animation in progress).
 * - Prevents default on Space to avoid page scroll.
 */
export function KeyboardToggle({ onToggle, disabled, state }: KeyboardToggleProps) {
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      // Only handle Space and Escape keys
      if (e.code !== 'Space' && e.code !== 'Escape') return;

      // Don't trigger if user is typing in a form element
      const target = e.target as HTMLElement;
      if (target && INPUT_TAGS.has(target.tagName)) return;

      // Don't trigger if the target is contentEditable
      if (target?.isContentEditable) return;

      // Don't trigger during animation states
      if (disabled) return;

      if (e.code === 'Space') {
        e.preventDefault(); // Prevent page scroll on Space
        onToggle();
      } else if (e.code === 'Escape' && state === 'Unfolded') {
        // Escape key only triggers fold (not unfold)
        e.preventDefault();
        onToggle();
      }
    };

    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onToggle, disabled, state]);

  return null;
}

// ── Button Label Utility ──

/**
 * Returns the appropriate button label for the current unfold state.
 * Used by the UI toolbar button outside the R3F canvas.
 */
export function getUnfoldButtonLabel(state: UnfoldState): string {
  switch (state) {
    case 'Folded':    return 'Unfold 3D';
    case 'Unfolding': return 'Unfolding...';
    case 'Unfolded':  return 'Fold 2D';
    case 'Folding':   return 'Folding...';
  }
}

/**
 * Returns the tooltip text for the current unfold state.
 */
export function getUnfoldTooltip(state: UnfoldState): string {
  switch (state) {
    case 'Folded':    return 'Press Space to unfold into 3D view';
    case 'Unfolding': return 'Animation in progress';
    case 'Unfolded':  return 'Press Space to fold back to 2D view';
    case 'Folding':   return 'Animation in progress';
  }
}

/**
 * Returns the ARIA label for the unfold toggle button.
 */
export function getUnfoldAriaLabel(state: UnfoldState): string {
  return `Toggle between 2D and 3D visualization. Currently ${
    state === 'Folded' || state === 'Folding' ? '2D' : '3D'
  } mode.`;
}
