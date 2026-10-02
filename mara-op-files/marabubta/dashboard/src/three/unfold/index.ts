// Marabunta - Licensed under the MIT License.
// ── Unfold Animation — Barrel Export ──
// W5D: Unfold 2D->3D Animation System (Spec S21 - The Signature Moment)
// Choreographed two-second transition from flat topology graph to interactive
// three-dimensional observatory.

// Core state machine and context
export {
  useUnfoldController,
  useUnfold,
  UnfoldProvider,
  directionalEase,
} from './UnfoldController';
export type { UnfoldState, UnfoldContext } from './UnfoldController';

// Timeline mapper
export { computeTimeline } from './UnfoldTimeline';
export type { TimelineSlice } from './UnfoldTimeline';

// Animation subsystems
export { CameraAnimation } from './CameraAnimation';
export { NodeLift, getRoleWeight, getTargetZ } from './NodeLift';
export { EdgeCurve } from './EdgeCurve';
export { CoronaFadeIn } from './CoronaFadeIn';
export { CubeCondensation } from './CubeCondensation';

// Post-processing
export { PostProcessingToggle } from './PostProcessingToggle';

// Input handling
export {
  KeyboardToggle,
  getUnfoldButtonLabel,
  getUnfoldTooltip,
  getUnfoldAriaLabel,
} from './KeyboardToggle';

// Easing utilities
export {
  easeOutCubic,
  easeInCubic,
  lerp,
  inverseLerp,
  remap,
  clamp,
  smoothStep,
  lerpVec3,
} from './easing';
