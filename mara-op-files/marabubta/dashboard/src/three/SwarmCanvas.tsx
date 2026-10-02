// Marabunta - Licensed under the MIT License.
// dashboard/src/three/SwarmCanvas.tsx
// Root R3F Canvas component for the 3D swarm visualization.
// Lazy-loaded via React.lazy() so the Three.js bundle (~600KB gzipped)
// is only downloaded when the user activates the 3D toggle.

import { Canvas } from '@react-three/fiber';
import { Suspense, useMemo } from 'react';
import { SceneSetup } from './SceneSetup';
import { SwarmDataProvider } from './SwarmDataContext';
import { WebSocketFeed } from './WebSocketFeed';
import { CANVAS_CONFIG } from './constants';

// ──────── WebGL Detection ────────

interface WebGLSupport {
  webgl2: boolean;
  webgl1: boolean;
  none: boolean;
}

// Cached result — detection runs once per page load
let cachedSupport: WebGLSupport | null = null;

/**
 * Detect WebGL support level.
 * Returns cached result on subsequent calls.
 */
export function detectWebGLSupport(): WebGLSupport {
  if (cachedSupport) return cachedSupport;

  const canvas = document.createElement('canvas');

  const gl2 = canvas.getContext('webgl2');
  if (gl2) {
    cachedSupport = { webgl2: true, webgl1: true, none: false };
    return cachedSupport;
  }

  const gl1 = canvas.getContext('webgl') || canvas.getContext('experimental-webgl');
  if (gl1) {
    cachedSupport = { webgl2: false, webgl1: true, none: false };
    return cachedSupport;
  }

  cachedSupport = { webgl2: false, webgl1: false, none: true };
  return cachedSupport;
}

// ──────── Props ────────

interface SwarmCanvasProps {
  /** WebSocket URL for the swarm topology stream */
  wsUrl: string;
  /** Optional CSS class name for the container div */
  className?: string;
  /** Callback when a node is selected/deselected in the 3D view */
  onNodeSelect?: (nodeId: string | null) => void;
}

// ──────── Component ────────

export function SwarmCanvas({ wsUrl, className, onNodeSelect }: SwarmCanvasProps) {
  const support = detectWebGLSupport();

  const gl = useMemo(() => ({
    antialias: true,
    alpha: false,
    powerPreference: 'high-performance' as const,
    stencil: false,
    depth: true,
  }), []);

  // No WebGL at all — show a graceful message
  if (support.none) {
    return (
      <div
        className={className}
        style={{
          width: '100%',
          height: '100%',
          minHeight: 400,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          background: CANVAS_CONFIG.BACKGROUND_COLOR,
          borderRadius: '0.75rem',
          border: '1px solid #1e293b',
        }}
      >
        <div style={{ textAlign: 'center', color: '#64748b', fontFamily: 'monospace', fontSize: '0.85rem' }}>
          <p style={{ marginBottom: '0.5rem', color: '#94a3b8' }}>3D View Unavailable</p>
          <p>WebGL is not supported in this browser.</p>
          <p>The 2D view provides full access to all swarm data.</p>
        </div>
      </div>
    );
  }

  return (
    <div className={className} style={{ width: '100%', height: '100%', minHeight: 400 }}>
      <Canvas
        gl={gl}
        camera={{
          fov: CANVAS_CONFIG.FOV,
          near: CANVAS_CONFIG.NEAR,
          far: CANVAS_CONFIG.FAR,
          position: CANVAS_CONFIG.INITIAL_POSITION,
        }}
        dpr={[1, 2]}
        frameloop="demand"
        flat={false}
        shadows={false}
        onCreated={({ gl: renderer }) => {
          renderer.setClearColor(CANVAS_CONFIG.BACKGROUND_COLOR);
        }}
      >
        <Suspense fallback={null}>
          <SwarmDataProvider>
            <WebSocketFeed url={wsUrl} />
            <SceneSetup onNodeSelect={onNodeSelect} />
          </SwarmDataProvider>
        </Suspense>
      </Canvas>
    </div>
  );
}
