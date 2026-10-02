// Marabunta - Licensed under the MIT License.
import { useEffect, useState, useCallback, useRef } from 'react';
import { wsManager } from '../api/websocket';
import type { WsMessage, WsState } from '../api/types';

let connectionCount = 0;

export function useWebSocket() {
  const [state, setState] = useState<WsState>(wsManager.state);

  useEffect(() => {
    connectionCount++;
    if (connectionCount === 1) {
      wsManager.connect();
    }

    const unsubState = wsManager.onStateChange((newState) => {
      setState(newState);
    });

    setState(wsManager.state);

    return () => {
      unsubState();
      connectionCount--;
      if (connectionCount === 0) {
        wsManager.disconnect();
      }
    };
  }, []);

  const subscribe = useCallback(
    (type: string, cb: (msg: WsMessage) => void) => {
      return wsManager.on(type, cb);
    },
    [],
  );

  return {
    state,
    subscribe,
    manager: wsManager,
  };
}

export function useWsSubscription(
  type: string,
  callback: (msg: WsMessage) => void,
) {
  const callbackRef = useRef(callback);
  callbackRef.current = callback;

  useEffect(() => {
    const handler = (msg: WsMessage) => callbackRef.current(msg);
    return wsManager.on(type, handler);
  }, [type]);
}
