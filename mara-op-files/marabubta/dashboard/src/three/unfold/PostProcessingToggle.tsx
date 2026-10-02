// Marabunta - Licensed under the MIT License.
// ── PostProcessingToggle ──
// Conditionally mounts the EffectComposer with Bloom and SSAO effects during
// unfold. The composer is NOT rendered when the scene is fully folded, avoiding
// any post-processing overhead in 2D mode. An audit heat overlay fades in
// during the final phase of the animation. (W5D / Spec S21)

import { useMemo } from 'react';
import { EffectComposer, Bloom, SSAO } from '@react-three/postprocessing';
import { BlendFunction } from 'postprocessing';

// ── Constants ──

/** Maximum bloom intensity at full unfold */
const BLOOM_MAX_INTENSITY = 1.5;

/** Maximum SSAO intensity at full unfold */
const SSAO_MAX_INTENSITY = 2.0;

/** Bloom luminance threshold: only emissive elements bloom */
const BLOOM_THRESHOLD = 0.8;

/** Bloom luminance smoothing for softer cutoff edges */
const BLOOM_SMOOTHING = 0.025;

/** Bloom spread radius */
const BLOOM_RADIUS = 0.4;

/** SSAO sampling radius */
const SSAO_RADIUS = 4;

/** SSAO luminance influence: bright areas get less AO */
const SSAO_LUMINANCE_INFLUENCE = 0.9;

// ── Props ──

interface PostProcessingToggleProps {
  /** 0-1 from timeline.bloom */
  bloomProgress: number;
  /** 0-1 from timeline.ssao */
  ssaoProgress: number;
  /** 0-1 from timeline.heatOverlay */
  heatProgress: number;
  /** Whether to skip non-essential effects (prefers-reduced-motion) */
  reducedMotion?: boolean;
}

// ── Component ──

export function PostProcessingToggle({
  bloomProgress,
  ssaoProgress,
  heatProgress,
  reducedMotion = false,
}: PostProcessingToggleProps) {
  // Determine effective values (skip bloom and heat in reduced-motion mode)
  const effectiveBloom = reducedMotion ? 0 : bloomProgress;
  const effectiveHeat = reducedMotion ? 0 : heatProgress;

  // Don't render the EffectComposer at all when fully folded.
  // Post-processing passes are GPU-expensive; mounting them only when needed
  // means the 2D view runs with zero post-processing overhead.
  const anyActive = effectiveBloom > 0 || ssaoProgress > 0;

  // Compute current effect intensities
  const bloomIntensity = useMemo(
    () => effectiveBloom * BLOOM_MAX_INTENSITY,
    [effectiveBloom],
  );
  const ssaoIntensity = useMemo(
    () => ssaoProgress * SSAO_MAX_INTENSITY,
    [ssaoProgress],
  );

  if (!anyActive) return null;

  return (
    <EffectComposer multisampling={0}>
      <Bloom
        intensity={bloomIntensity}
        luminanceThreshold={BLOOM_THRESHOLD}
        luminanceSmoothing={BLOOM_SMOOTHING}
        radius={BLOOM_RADIUS}
        blendFunction={BlendFunction.ADD}
        mipmapBlur
      />
      <SSAO
        radius={SSAO_RADIUS}
        intensity={ssaoIntensity}
        luminanceInfluence={SSAO_LUMINANCE_INFLUENCE}
        color="#000000"
      />
      {/* Audit heat overlay: subtle warmth on high-audit-activity nodes */}
      {effectiveHeat > 0 && (
        <Bloom
          intensity={effectiveHeat * 0.3}
          luminanceThreshold={0.5}
          luminanceSmoothing={0.3}
          radius={0.8}
          blendFunction={BlendFunction.SCREEN}
        />
      )}
    </EffectComposer>
  );
}
