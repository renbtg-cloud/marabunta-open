// Marabunta - Licensed under the MIT License.
// ── Easing Utilities ──
// Pure mathematical functions for the unfold/fold animation system (W5D / Spec S21).
// Stateless, side-effect-free, and easily testable.
// No external dependencies.

/**
 * Cubic ease-out: fast start, smooth deceleration.
 * Used for the unfold direction (2D -> 3D).
 * Formula: 1 - (1 - x)^3
 */
export function easeOutCubic(x: number): number {
  const clamped = Math.max(0, Math.min(1, x));
  return 1 - Math.pow(1 - clamped, 3);
}

/**
 * Cubic ease-in: slow start, fast finish.
 * Used for the fold direction (3D -> 2D).
 * Formula: x^3
 */
export function easeInCubic(x: number): number {
  const clamped = Math.max(0, Math.min(1, x));
  return clamped * clamped * clamped;
}

/**
 * Linear interpolation between two values.
 * @param a Start value
 * @param b End value
 * @param t Interpolation factor (0-1)
 */
export function lerp(a: number, b: number, t: number): number {
  return a + (b - a) * t;
}

/**
 * Inverse linear interpolation: given a value between a and b,
 * return the t factor (0-1).
 */
export function inverseLerp(a: number, b: number, value: number): number {
  if (a === b) return 0;
  return (value - a) / (b - a);
}

/**
 * Remap a value from one range to another.
 * Input outside [inMin, inMax] is clamped.
 * Used by UnfoldTimeline to map global progress to per-subsystem activation.
 */
export function remap(
  value: number,
  inMin: number,
  inMax: number,
  outMin: number,
  outMax: number,
): number {
  const t = Math.max(0, Math.min(1, inverseLerp(inMin, inMax, value)));
  return lerp(outMin, outMax, t);
}

/**
 * Clamp a value between min and max.
 */
export function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

/**
 * Smooth step: Hermite interpolation for smoother transitions.
 * Returns 0 for x <= edge0, 1 for x >= edge1.
 */
export function smoothStep(edge0: number, edge1: number, x: number): number {
  const t = clamp((x - edge0) / (edge1 - edge0), 0, 1);
  return t * t * (3 - 2 * t);
}

/**
 * Vector3 lerp helper (for use outside R3F frame loops).
 */
export function lerpVec3(
  a: [number, number, number],
  b: [number, number, number],
  t: number,
): [number, number, number] {
  return [
    lerp(a[0], b[0], t),
    lerp(a[1], b[1], t),
    lerp(a[2], b[2], t),
  ];
}
