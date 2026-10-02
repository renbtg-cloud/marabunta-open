# Marabunta - Licensed under the MIT License.
"""
Swarm Client Library for Marabunta Compute Plugins (Python).

Provides SwarmConnection, a client for communicating with the Marabunta
Swarm plugin host over Unix sockets or TCP using length-delimited JSON
frames (4-byte big-endian length prefix + JSON body).

Wire protocol contract:
    All messages are PluginWireMessage envelopes:
        {"type": "<Variant>", "payload": { ... }}
    Framing: 4-byte big-endian length prefix followed by a JSON body.
    Transport: Unix domain socket (default) or TCP.

Usage:
    conn = SwarmConnection.unix("/tmp/marabunta/plugins/my-plugin.sock")
    resp = conn.register("my-plugin", "1.0.0", ["CanExecute"], [])
    print(resp["plugin_id"])
"""

from __future__ import annotations

import asyncio
import json
import socket
import struct
import threading
from typing import Any, AsyncIterator, Dict, List, Optional, Union


# Maximum wire message size (16 MB, matches PLUGIN_MAX_MESSAGE_SIZE in Rust).
MAX_MESSAGE_SIZE = 16 * 1024 * 1024

# Length-prefix format: 4 bytes, big-endian unsigned int.
LENGTH_PREFIX_FMT = "!I"
LENGTH_PREFIX_SIZE = struct.calcsize(LENGTH_PREFIX_FMT)


class SwarmError(Exception):
    """Raised when a swarm operation fails."""
    pass


class SwarmConnection:
    """Bidirectional connection to the Marabunta Swarm plugin host.

    Supports both Unix domain sockets and TCP connections.  All messages
    use 4-byte big-endian length-delimited JSON framing.
    """

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._lock = threading.Lock()
        self._recv_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Factory constructors
    # ------------------------------------------------------------------

    @classmethod
    def unix(cls, path: str) -> "SwarmConnection":
        """Connect to the swarm host via a Unix domain socket."""
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(path)
        return cls(sock)

    @classmethod
    def tcp(cls, host: str, port: int) -> "SwarmConnection":
        """Connect to the swarm host via TCP."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        return cls(sock)

    # ------------------------------------------------------------------
    # Low-level framing
    # ------------------------------------------------------------------

    def send_message(self, msg: Dict[str, Any]) -> None:
        """Send a length-delimited JSON message.

        Frame layout:
            [4 bytes big-endian length][JSON payload bytes]
        """
        payload = json.dumps(msg, separators=(",", ":")).encode("utf-8")
        if len(payload) > MAX_MESSAGE_SIZE:
            raise SwarmError(
                f"message size {len(payload)} exceeds maximum {MAX_MESSAGE_SIZE}"
            )
        header = struct.pack(LENGTH_PREFIX_FMT, len(payload))
        with self._lock:
            self._sock.sendall(header + payload)

    def recv_message(self) -> Dict[str, Any]:
        """Receive a length-delimited JSON message.

        Reads the 4-byte length prefix, then reads exactly that many
        bytes of JSON payload.
        """
        with self._recv_lock:
            header = self._recv_exact(LENGTH_PREFIX_SIZE)
            (msg_len,) = struct.unpack(LENGTH_PREFIX_FMT, header)
            if msg_len > MAX_MESSAGE_SIZE:
                raise SwarmError(
                    f"incoming message size {msg_len} exceeds maximum {MAX_MESSAGE_SIZE}"
                )
            payload = self._recv_exact(msg_len)
        return json.loads(payload.decode("utf-8"))

    def _recv_exact(self, n: int) -> bytes:
        """Read exactly *n* bytes from the socket."""
        buf = bytearray()
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                raise SwarmError("connection closed while reading")
            buf.extend(chunk)
        return bytes(buf)

    # ------------------------------------------------------------------
    # Request-response helper
    # ------------------------------------------------------------------

    def _request(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        """Send a message and wait for the response."""
        self.send_message(msg)
        return self.recv_message()

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(
        self,
        name: str,
        version: str,
        traits: Optional[List[str]] = None,
        endpoints: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Register this plugin with the swarm.

        Returns the RegisterResponse payload containing plugin_id,
        node_id, and swarm_config.
        """
        resp = self._request({
            "type": "RegisterReq",
            "payload": {
                "name": name,
                "version": version,
                "traits": traits or [],
                "endpoints": endpoints or [],
            },
        })
        if resp.get("type") != "RegisterResp":
            raise SwarmError(f"expected RegisterResp, got {resp.get('type')}")
        return resp["payload"]

    # ------------------------------------------------------------------
    # Storage API
    # ------------------------------------------------------------------

    def store(
        self,
        key: Union[str, bytes],
        value: Union[str, bytes],
        consistency: str = "eventual",
        replicas: int = 3,
        ttl_seconds: int = 0,
    ) -> Dict[str, Any]:
        """Store a key-value pair in the swarm's distributed storage."""
        resp = self._request({
            "type": "StoreReq",
            "payload": {
                "key": _to_byte_list(key),
                "value": _to_byte_list(value),
                "options": {
                    "consistency": consistency,
                    "replicas": replicas,
                    "ttl_seconds": ttl_seconds,
                },
            },
        })
        if resp.get("type") != "StoreResp":
            raise SwarmError(f"expected StoreResp, got {resp.get('type')}")
        payload = resp["payload"]
        if not payload.get("success") and payload.get("error"):
            raise SwarmError(f"store failed: {payload['error']}")
        return payload

    def fetch(
        self,
        key: Union[str, bytes],
        consistency: str = "eventual",
        min_version: int = 0,
    ) -> Dict[str, Any]:
        """Fetch a value by key from the swarm's distributed storage."""
        resp = self._request({
            "type": "FetchReq",
            "payload": {
                "key": _to_byte_list(key),
                "options": {
                    "consistency": consistency,
                    "min_version": min_version,
                },
            },
        })
        if resp.get("type") != "FetchResp":
            raise SwarmError(f"expected FetchResp, got {resp.get('type')}")
        return resp["payload"]

    def delete(self, key: Union[str, bytes]) -> Dict[str, Any]:
        """Delete a key from the swarm's distributed storage."""
        resp = self._request({
            "type": "DeleteReq",
            "payload": {
                "key": _to_byte_list(key),
            },
        })
        if resp.get("type") != "DeleteResp":
            raise SwarmError(f"expected DeleteResp, got {resp.get('type')}")
        return resp["payload"]

    # ------------------------------------------------------------------
    # Scatter (Distributed Compute)
    # ------------------------------------------------------------------

    def scatter(
        self,
        units: List[Dict[str, Any]],
        hints: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Scatter work units across the swarm and gather results."""
        wire_units = []
        for u in units:
            wire_units.append({
                "target_node": u.get("target_node", ""),
                "payload": _to_byte_list(u.get("payload", b"")),
                "required_traits": u.get("required_traits", []),
            })
        resp = self._request({
            "type": "ScatterReq",
            "payload": {
                "units": wire_units,
                "hints": hints or {
                    "locality": "any",
                    "consistency": "eventual",
                    "priority": "normal",
                    "timeout_ms": 30000,
                },
            },
        })
        if resp.get("type") != "ScatterResp":
            raise SwarmError(f"expected ScatterResp, got {resp.get('type')}")
        return resp["payload"]

    # ------------------------------------------------------------------
    # Node Discovery
    # ------------------------------------------------------------------

    def find_nodes(
        self,
        traits: Optional[List[str]] = None,
        limit: int = 10,
        prefer_region: str = "",
    ) -> Dict[str, Any]:
        """Find nodes in the swarm matching the given criteria."""
        resp = self._request({
            "type": "FindNodesReq",
            "payload": {
                "required_traits": traits or [],
                "limit": limit,
                "prefer_region": prefer_region,
            },
        })
        if resp.get("type") != "FindNodesResp":
            raise SwarmError(f"expected FindNodesResp, got {resp.get('type')}")
        return resp["payload"]

    def is_node_alive(self, node_id: str) -> Dict[str, Any]:
        """Check whether a specific swarm node is alive."""
        resp = self._request({
            "type": "IsNodeAliveReq",
            "payload": {"node_id": node_id},
        })
        if resp.get("type") != "IsNodeAliveResp":
            raise SwarmError(f"expected IsNodeAliveResp, got {resp.get('type')}")
        return resp["payload"]

    def get_node_info(self) -> Dict[str, Any]:
        """Get this node's own information from the swarm."""
        resp = self._request({
            "type": "GetNodeInfoReq",
            "payload": {},
        })
        if resp.get("type") != "GetNodeInfoResp":
            raise SwarmError(f"expected GetNodeInfoResp, got {resp.get('type')}")
        return resp["payload"]

    # ------------------------------------------------------------------
    # Pub/Sub
    # ------------------------------------------------------------------

    def publish(self, topic: str, payload: Union[str, bytes]) -> Dict[str, Any]:
        """Publish a message to a topic."""
        resp = self._request({
            "type": "PublishReq",
            "payload": {
                "topic": topic,
                "payload": _to_byte_list(payload),
            },
        })
        if resp.get("type") != "PublishResp":
            raise SwarmError(f"expected PublishResp, got {resp.get('type')}")
        return resp["payload"]

    def subscribe(self, topic: str) -> Dict[str, Any]:
        """Subscribe to a topic.

        After calling subscribe(), use recv_message() in a loop to
        receive SubscribeEvt messages.  For async iteration, use
        subscribe_iter() instead.

        Returns the SubscribeResp payload.
        """
        resp = self._request({
            "type": "SubscribeReq",
            "payload": {"topic": topic},
        })
        if resp.get("type") != "SubscribeResp":
            raise SwarmError(f"expected SubscribeResp, got {resp.get('type')}")
        return resp["payload"]

    async def subscribe_iter(self, topic: str) -> AsyncIterator[Dict[str, Any]]:
        """Subscribe to a topic and yield events as an async generator.

        Runs recv_message() in a thread executor to avoid blocking the
        event loop.
        """
        self.subscribe(topic)
        loop = asyncio.get_running_loop()
        while True:
            msg = await loop.run_in_executor(None, self.recv_message)
            if msg.get("type") == "SubscribeEvt":
                yield msg["payload"]
            else:
                # Non-event message received; caller may want to handle it.
                break

    # ------------------------------------------------------------------
    # Data Channels
    # ------------------------------------------------------------------

    def push_channel(self, channel: str, payload: Any) -> Dict[str, Any]:
        """Push a data channel update to the swarm.

        Args:
            channel: Channel name (e.g. "fin.portfolio").
            payload: JSON-serializable payload.

        Returns:
            The DataChannelPushAck payload.
        """
        resp = self._request({
            "type": "DataChannelPush",
            "payload": {
                "channel": channel,
                "payload": payload,
            },
        })
        if resp.get("type") != "DataChannelPushAck":
            raise SwarmError(f"expected DataChannelPushAck, got {resp.get('type')}")
        payload_resp = resp["payload"]
        if not payload_resp.get("success"):
            raise SwarmError(f"data channel push failed for channel '{channel}'")
        return payload_resp

    def on_channel_query(
        self,
        channel: str,
        handler,  # Callable[[Dict[str, Any]], Any]
    ) -> None:
        """Register a handler for data channel queries and enter message loop.

        The handler receives the query payload and should return a
        JSON-serializable result.  This blocks in a message loop until
        the connection closes.

        Args:
            channel: Channel name to handle queries for.
            handler: Callback that receives the query dict and returns a result.
        """
        while True:
            try:
                msg = self.recv_message()
            except SwarmError:
                break

            if msg.get("type") == "DataChannelQuery":
                payload = msg.get("payload", {})
                query_channel = payload.get("channel", "")
                request_id = payload.get("request_id", "")

                if query_channel != channel:
                    continue

                try:
                    result = handler(payload.get("query", {}))
                except Exception as e:
                    result = {"error": str(e)}

                self.send_message({
                    "type": "DataChannelQueryResp",
                    "payload": {
                        "request_id": request_id,
                        "result": result,
                    },
                })
            elif msg.get("type") == "HealthReq":
                # Respond to health checks while in the query loop.
                self.send_message({
                    "type": "HealthResp",
                    "payload": {
                        "healthy": True,
                        "status": "running",
                        "details": {},
                    },
                })

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the connection."""
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._sock.close()

    def __enter__(self) -> "SwarmConnection":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


# ======================================================================
# Helpers
# ======================================================================

def _to_byte_list(data: Union[str, bytes, list]) -> List[int]:
    """Convert a string, bytes, or list to a JSON-serializable byte list.

    The Rust side expects byte fields as arrays of u8 integers
    (serde's default serialization for Vec<u8>).
    """
    if isinstance(data, str):
        return list(data.encode("utf-8"))
    if isinstance(data, (bytes, bytearray)):
        return list(data)
    if isinstance(data, list):
        return data
    raise TypeError(f"cannot convert {type(data).__name__} to byte list")


def bytes_from_list(data: Union[List[int], str, bytes]) -> bytes:
    """Convert a byte list received from the swarm back to bytes.

    Handles both the list-of-ints format and raw string/bytes.
    """
    if isinstance(data, list):
        return bytes(data)
    if isinstance(data, str):
        return data.encode("utf-8")
    if isinstance(data, (bytes, bytearray)):
        return bytes(data)
    raise TypeError(f"cannot convert {type(data).__name__} to bytes")
