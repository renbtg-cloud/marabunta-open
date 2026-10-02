// Marabunta - Licensed under the MIT License.
import type { WsMessage, WsState } from './types';

type WsListener = (msg: WsMessage) => void;

export class WebSocketManager {
  private ws: WebSocket | null = null;
  private url: string;
  private listeners: Map<string, Set<WsListener>> = new Map();
  private stateListeners: Set<(state: WsState) => void> = new Set();
  private reconnectAttempt = 0;
  private maxReconnectDelay = 30_000;
  private heartbeatInterval: number | null = null;
  private lastHeartbeat = 0;
  private reconnectTimer: number | null = null;
  private _state: WsState = 'disconnected';

  get state(): WsState {
    return this._state;
  }

  private setState(newState: WsState): void {
    this._state = newState;
    this.stateListeners.forEach((cb) => cb(newState));
  }

  constructor(url?: string) {
    const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    this.url = url ?? `${proto}//${window.location.host}/ws/dashboard`;
  }

  connect(): void {
    if (this._state === 'connected' || this._state === 'connecting') {
      return;
    }
    this.setState('connecting');

    try {
      this.ws = new WebSocket(this.url);
    } catch {
      this.setState('reconnecting');
      this.scheduleReconnect();
      return;
    }

    this.ws.onopen = () => {
      this.setState('connected');
      this.reconnectAttempt = 0;
      this.startHeartbeatMonitor();
    };

    this.ws.onmessage = (event) => {
      try {
        const msg: WsMessage = JSON.parse(event.data);
        if (msg.type === 'heartbeat') {
          this.lastHeartbeat = Date.now();
          return;
        }
        this.emit(msg.type, msg);
        this.emit('*', msg);
      } catch {
        // Ignore malformed messages
      }
    };

    this.ws.onclose = () => {
      this.stopHeartbeatMonitor();
      if (this._state !== 'disconnected') {
        this.setState('reconnecting');
        this.scheduleReconnect();
      }
    };

    this.ws.onerror = () => {
      this.ws?.close();
    };
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer !== null) return;
    const delay = Math.min(
      1000 * Math.pow(2, this.reconnectAttempt),
      this.maxReconnectDelay,
    );
    this.reconnectAttempt++;
    this.reconnectTimer = window.setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delay);
  }

  private startHeartbeatMonitor(): void {
    this.lastHeartbeat = Date.now();
    this.heartbeatInterval = window.setInterval(() => {
      if (Date.now() - this.lastHeartbeat > 15_000) {
        this.ws?.close();
      }
    }, 5_000);
  }

  private stopHeartbeatMonitor(): void {
    if (this.heartbeatInterval !== null) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }
  }

  on(type: string, callback: WsListener): () => void {
    if (!this.listeners.has(type)) {
      this.listeners.set(type, new Set());
    }
    this.listeners.get(type)!.add(callback);
    return () => {
      this.listeners.get(type)?.delete(callback);
    };
  }

  onStateChange(callback: (state: WsState) => void): () => void {
    this.stateListeners.add(callback);
    return () => {
      this.stateListeners.delete(callback);
    };
  }

  private emit(type: string, msg: WsMessage): void {
    this.listeners.get(type)?.forEach((cb) => cb(msg));
  }

  disconnect(): void {
    this.setState('disconnected');
    this.stopHeartbeatMonitor();
    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.ws?.close();
    this.ws = null;
  }
}

export const wsManager = new WebSocketManager();
