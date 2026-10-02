// Marabunta - Licensed under the MIT License.
// dashboard/src/three/WebSocketFeed.tsx
// Real-time data bridge: manages a WebSocket connection to /ws/swarm-topology
// and dispatches topology deltas into the shared SwarmDataContext.
// This component renders nothing — it is a side-effect-only bridge.

import { useEffect, useRef, useCallback } from 'react';
import { useSwarmData } from './SwarmDataContext';

const DEBUG_KEY = 'swarm3d.debug';

function isDebug(): boolean {
  try {
    return localStorage.getItem(DEBUG_KEY) === 'true';
  } catch {
    return false;
  }
}

interface WebSocketFeedProps {
  /** WebSocket URL for the swarm topology stream */
  url: string;
}

export function WebSocketFeed({ url }: WebSocketFeedProps) {
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimer = useRef<ReturnType<typeof setTimeout>>();
  const reconnectAttempt = useRef(0);
  const { dispatch } = useSwarmData();

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;

    const ws = new WebSocket(url);
    wsRef.current = ws;

    ws.onopen = () => {
      if (isDebug()) console.log('[SwarmWS] Connected to', url);
      reconnectAttempt.current = 0;
      dispatch({ type: 'WS_CONNECTED' });
      // Request full snapshot on connect
      ws.send(JSON.stringify({ type: 'subscribe', topics: ['topology', 'health'] }));
    };

    ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        if (isDebug()) console.log('[SwarmWS] Received:', msg.type, msg);

        switch (msg.type) {
          case 'topology_snapshot':
            dispatch({ type: 'TOPOLOGY_SNAPSHOT', payload: msg.data });
            break;
          case 'node_update':
            dispatch({ type: 'NODE_UPDATE', payload: msg.data });
            break;
          case 'node_removed':
            dispatch({ type: 'NODE_REMOVED', payload: msg.data });
            break;
          case 'edge_update':
            dispatch({ type: 'EDGE_UPDATE', payload: msg.data });
            break;
          case 'health_tick':
            dispatch({ type: 'HEALTH_TICK', payload: msg.data });
            break;
          default:
            if (isDebug()) console.log('[SwarmWS] Unknown message type:', msg.type);
        }
      } catch (e) {
        console.warn('[SwarmWS] Failed to parse message:', e);
      }
    };

    ws.onclose = (event) => {
      if (isDebug()) console.log('[SwarmWS] Disconnected, code:', event.code);
      dispatch({ type: 'WS_DISCONNECTED' });

      // Exponential backoff reconnect: 1s, 2s, 4s, 8s, ..., max 30s
      const delay = Math.min(
        30000,
        1000 * Math.pow(2, reconnectAttempt.current)
      );
      reconnectAttempt.current++;
      if (isDebug()) console.log(`[SwarmWS] Reconnecting in ${delay}ms (attempt ${reconnectAttempt.current})`);
      reconnectTimer.current = setTimeout(connect, delay);
    };

    ws.onerror = () => {
      // onclose will fire after onerror, which handles reconnection
      ws.close();
    };
  }, [url, dispatch]);

  useEffect(() => {
    connect();
    return () => {
      clearTimeout(reconnectTimer.current);
      wsRef.current?.close(1000, 'Component unmounted');
      wsRef.current = null;
    };
  }, [connect]);

  // This component renders nothing — it is a side-effect-only bridge
  return null;
}
