// Marabunta - Licensed under the MIT License.
// dashboard/src/three/hooks/useForceLayout.ts
// Hook that wraps the Web Worker layout engine.
// Creates the worker on mount, feeds it node/edge data,
// requests ticks on each animation frame, and dispatches
// position updates into SwarmDataContext.

import { useEffect, useRef, useCallback } from 'react';
import { useSwarmData } from '../SwarmDataContext';
import { useFrame } from '@react-three/fiber';

/**
 * Manages the d3-force-3d layout simulation running in a Web Worker.
 * Automatically initializes when nodes arrive and requests ticks
 * on each R3F animation frame.
 *
 * @returns Object with `reheat` function to manually restart the layout
 */
export function useForceLayout() {
  const workerRef = useRef<Worker | null>(null);
  const { state, dispatch } = useSwarmData();
  const initializedRef = useRef(false);
  const nodeCountRef = useRef(0);

  // Create worker on mount
  useEffect(() => {
    const worker = new Worker(
      new URL('../workers/layoutWorker.ts', import.meta.url),
      { type: 'module' }
    );

    worker.onmessage = (event) => {
      if (event.data.type === 'positions') {
        const posMap = new Map<string, [number, number, number]>();
        for (const [id, pos] of Object.entries(event.data.data)) {
          posMap.set(id, pos as [number, number, number]);
        }
        dispatch({ type: 'LAYOUT_UPDATE', payload: posMap });
      }
    };

    worker.onerror = (err) => {
      console.error('[useForceLayout] Worker error:', err);
    };

    workerRef.current = worker;

    return () => {
      worker.terminate();
      workerRef.current = null;
      initializedRef.current = false;
    };
  }, [dispatch]);

  // Initialize or update layout when nodes/edges change
  useEffect(() => {
    const worker = workerRef.current;
    if (!worker) return;

    const nodeArray = Array.from(state.nodes.values());
    const nodes = nodeArray.map(n => ({
      id: n.id,
      roles: n.roles,
    }));
    const edges = state.edges;

    if (!initializedRef.current && nodes.length > 0) {
      // First data arrival — initialize the simulation
      worker.postMessage({ type: 'init', nodes, edges });
      initializedRef.current = true;
      nodeCountRef.current = nodes.length;
    } else if (initializedRef.current && nodes.length !== nodeCountRef.current) {
      // Node count changed — update the simulation
      worker.postMessage({ type: 'update_nodes', nodes });
      nodeCountRef.current = nodes.length;
    }
  }, [state.nodes, state.edges]);

  // Send edge updates when edges change (separate from node updates)
  useEffect(() => {
    const worker = workerRef.current;
    if (!worker || !initializedRef.current) return;

    worker.postMessage({ type: 'update_edges', edges: state.edges });
  }, [state.edges]);

  // Request layout ticks on each animation frame
  // The worker only computes if alpha > 0.001 (simulation still active)
  useFrame(() => {
    workerRef.current?.postMessage({ type: 'tick_request' });
  });

  /**
   * Manually reheat the layout simulation.
   * Useful after bulk node additions or topology changes.
   * @param alpha Target alpha value (default 0.5)
   */
  const reheat = useCallback((alpha = 0.5) => {
    workerRef.current?.postMessage({ type: 'set_alpha', alpha });
  }, []);

  return { reheat };
}
