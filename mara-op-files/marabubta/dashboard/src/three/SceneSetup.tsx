// Marabunta - Licensed under the MIT License.
// dashboard/src/three/SceneSetup.tsx
// Configures the 3D visual environment: lighting, camera controls,
// starfield background, fog, and post-processing.
// Intentionally contains no meshes or data-driven content.

import { useThree } from '@react-three/fiber';
import { OrbitControls, Stars } from '@react-three/drei';
import { EffectComposer, Bloom } from '@react-three/postprocessing';
import { useRef, useEffect } from 'react';
import { SCENE_CONFIG, BLOOM_CONFIG } from './constants';

interface SceneSetupProps {
  /** Callback when a node is selected (forwarded from parent) */
  onNodeSelect?: (nodeId: string | null) => void;
}

export function SceneSetup({ onNodeSelect: _onNodeSelect }: SceneSetupProps) {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const controlsRef = useRef<any>(null);
  const { camera } = useThree();

  useEffect(() => {
    // Reset camera to default position on mount
    camera.position.set(...SCENE_CONFIG.CAMERA_START);
    camera.lookAt(0, 0, 0);
  }, [camera]);

  return (
    <>
      {/* Ambient light fills shadows so nodes are never fully dark */}
      <ambientLight
        intensity={SCENE_CONFIG.AMBIENT_INTENSITY}
        color={SCENE_CONFIG.AMBIENT_COLOR}
      />

      {/* Key light from upper-right provides directional shading and volume */}
      <directionalLight
        position={SCENE_CONFIG.KEY_LIGHT_POS}
        intensity={SCENE_CONFIG.KEY_LIGHT_INTENSITY}
        color={SCENE_CONFIG.KEY_LIGHT_COLOR}
      />

      {/* Subtle cyan accent light at origin for thematic glow */}
      <pointLight
        position={[0, 0, 0]}
        intensity={SCENE_CONFIG.ACCENT_INTENSITY}
        color={SCENE_CONFIG.ACCENT_COLOR}
        decay={2}
        distance={50}
      />

      {/* Starfield background for depth and immersion */}
      <Stars
        radius={200}
        depth={60}
        count={1500}
        factor={3}
        fade
        speed={0.5}
      />

      {/* Fog for depth attenuation — distant nodes fade into background */}
      <fog attach="fog" args={[SCENE_CONFIG.FOG_COLOR, 40, 180]} />

      {/* Camera controls with constrained orbit */}
      <OrbitControls
        ref={controlsRef}
        enableDamping
        dampingFactor={0.08}
        minDistance={5}
        maxDistance={200}
        enablePan
        panSpeed={0.8}
        rotateSpeed={0.6}
        zoomSpeed={1.2}
        makeDefault
      />

      {/* Post-processing: bloom for glowing nodes */}
      <EffectComposer multisampling={0}>
        <Bloom
          luminanceThreshold={BLOOM_CONFIG.THRESHOLD}
          luminanceSmoothing={BLOOM_CONFIG.SMOOTHING}
          intensity={BLOOM_CONFIG.INTENSITY}
          mipmapBlur
        />
      </EffectComposer>
    </>
  );
}
