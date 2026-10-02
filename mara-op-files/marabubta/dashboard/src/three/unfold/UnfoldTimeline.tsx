// Marabunta - Licensed under the MIT License.
// ── UnfoldTimeline ──
// Maps global animation progress (0-1) to per-subsystem activation values.
// Each subsystem has a start and end window within the 2-second animation.
// The timeline creates a cascading wave effect: subsystems overlap so that
// motion is continuous throughout the full animation. (W5D / Spec S21)

import { remap } from './easing';

/**
 * Per-subsystem activation values, each in the [0, 1] range.
 * At progress=0 all values are 0; at progress=1 all values are 1.
 */
export interface TimelineSlice {
  /** Camera pullback progress */
  camera: number;
  /** Node Z-lift progress */
  nodeLift: number;
  /** Edge curvature progress */
  edgeCurve: number;
  /** Gossip particle opacity */
  particles: number;
  /** Role corona opacity */
  corona: number;
  /** Database geometry scale */
  dbGeometry: number;
  /** Cube condensation scale */
  cubeScale: number;
  /** Cube face data opacity */
  cubeFaces: number;
  /** Bloom intensity */
  bloom: number;
  /** SSAO intensity */
  ssao: number;
  /** Cross-cube arc opacity */
  crossArcs: number;
  /** Audit heat overlay opacity */
  heatOverlay: number;
}

/**
 * Compute the timeline slice for a given global progress value.
 *
 * Timing windows (from Spec S21):
 *   camera:      0.00 - 0.30   (t=0.0s - 0.6s)
 *   nodeLift:    0.10 - 0.40   (t=0.2s - 0.8s)
 *   edgeCurve:   0.20 - 0.40   (t=0.4s - 0.8s)
 *   particles:   0.30 - 0.40   (t=0.6s - 0.8s)
 *   corona:      0.30 - 0.50   (t=0.6s - 1.0s)
 *   dbGeometry:  0.40 - 0.50   (t=0.8s - 1.0s)
 *   cubeScale:   0.40 - 0.60   (t=0.8s - 1.2s)
 *   cubeFaces:   0.50 - 0.60   (t=1.0s - 1.2s)
 *   bloom:       0.50 - 0.70   (t=1.0s - 1.4s)
 *   ssao:        0.50 - 0.70   (t=1.0s - 1.4s)
 *   crossArcs:   0.60 - 0.80   (t=1.2s - 1.6s)
 *   heatOverlay: 0.70 - 0.90   (t=1.4s - 1.8s)
 */
export function computeTimeline(progress: number): TimelineSlice {
  return {
    camera:      remap(progress, 0.00, 0.30, 0, 1),
    nodeLift:    remap(progress, 0.10, 0.40, 0, 1),
    edgeCurve:   remap(progress, 0.20, 0.40, 0, 1),
    particles:   remap(progress, 0.30, 0.40, 0, 1),
    corona:      remap(progress, 0.30, 0.50, 0, 1),
    dbGeometry:  remap(progress, 0.40, 0.50, 0, 1),
    cubeScale:   remap(progress, 0.40, 0.60, 0, 1),
    cubeFaces:   remap(progress, 0.50, 0.60, 0, 1),
    bloom:       remap(progress, 0.50, 0.70, 0, 1),
    ssao:        remap(progress, 0.50, 0.70, 0, 1),
    crossArcs:   remap(progress, 0.60, 0.80, 0, 1),
    heatOverlay: remap(progress, 0.70, 0.90, 0, 1),
  };
}
