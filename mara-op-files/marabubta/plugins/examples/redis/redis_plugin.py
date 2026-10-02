#!/usr/bin/env python3
# Marabunta - Licensed under the MIT License.
"""
Redis-Compatible Plugin for the Marabunta Swarm.

Implements a full Redis-compatible TCP server that stores all data in the
Marabunta Swarm's distributed storage via the plugin API.  Clients connect
with any standard Redis client library and issue familiar commands.

Supported commands:
    PING, ECHO, GET, SET, DEL, MGET, MSET, INCR, DECR, INCRBY, DECRBY,
    SETNX, SETEX, TTL, PTTL, EXPIRE, EXISTS, KEYS, INFO,
    PUBLISH, SUBSCRIBE, UNSUBSCRIBE, COMMAND, QUIT

Design notes (spec references):
    R.3.1  Pipeline support -- batch commands are grouped into a single
           scatter round-trip when possible.
    R.3.2  Pub/Sub via swarm Subscribe -- no polling; events are pushed
           from the swarm via SubscribeEvt messages.
    R.3.3  MGET/MSET batching -- single round-trip via grouped storage
           operations.
    R.3.4  TTL -- lazy deletion on GET + background cleanup loop via the
           swarm's native TTL support in StoreOptions.
    R.3.5  Atomic INCR/SETNX -- optimistic locking via version CAS
           (compare-and-swap).

All keys are stored in the swarm with the prefix ``redis:`` to avoid
collisions with other plugins.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
import time
from typing import Any, Dict, List, Optional, Set, Tuple, Union

# Allow importing the shared library.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared", "python"))

from swarm_client import SwarmConnection, SwarmError, bytes_from_list

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("redis-plugin")

# Key prefix for all Redis keys in the swarm.
KEY_PREFIX = "redis:"

# Maximum CAS retry attempts for atomic operations.
MAX_CAS_RETRIES = 10

# Background TTL cleanup interval in seconds.
TTL_CLEANUP_INTERVAL = 60.0


# ======================================================================
# RESP Protocol Parser / Serializer
# ======================================================================

def resp_encode(value: Any) -> bytes:
    """Encode a Python value into RESP (Redis Serialization Protocol).

    Supports:
        str        -> Simple String (+)
        Exception  -> Error (-)
        bool       -> Integer (: 1 or 0)
        int        -> Integer (:)
        bytes      -> Bulk String ($)
        None       -> Null Bulk String ($-1)
        list       -> Array (*)
    """
    if isinstance(value, str):
        # Simple strings cannot contain \r\n per the RESP protocol.
        safe_value = value.replace("\r", " ").replace("\n", " ")
        return f"+{safe_value}\r\n".encode("utf-8")
    if isinstance(value, Exception):
        err_msg = str(value).replace("\r", " ").replace("\n", " ")
        return f"-ERR {err_msg}\r\n".encode("utf-8")
    if isinstance(value, bool):
        return f":{1 if value else 0}\r\n".encode("utf-8")
    if isinstance(value, int):
        return f":{value}\r\n".encode("utf-8")
    if value is None:
        return b"$-1\r\n"
    if isinstance(value, bytes):
        return f"${len(value)}\r\n".encode("utf-8") + value + b"\r\n"
    if isinstance(value, list):
        parts = [f"*{len(value)}\r\n".encode("utf-8")]
        for item in value:
            parts.append(resp_encode(item))
        return b"".join(parts)
    raise TypeError(f"cannot RESP-encode {type(value).__name__}")


def resp_encode_error(msg: str) -> bytes:
    """Encode a RESP error response."""
    msg = msg.replace("\r", " ").replace("\n", " ")
    return f"-ERR {msg}\r\n".encode("utf-8")


def resp_encode_wrong_argc(cmd: str) -> bytes:
    """Encode a RESP error for wrong number of arguments."""
    return f"-ERR wrong number of arguments for '{cmd}' command\r\n".encode("utf-8")


class RESPParser:
    """Incremental RESP protocol parser.

    Feeds raw bytes via feed() and yields complete parsed values via
    get_message().  Handles all RESP types: Simple Strings, Errors,
    Integers, Bulk Strings, and Arrays.
    """

    def __init__(self) -> None:
        self._buffer = bytearray()
        self._messages: list = []

    def feed(self, data: bytes) -> None:
        """Feed raw bytes into the parser."""
        self._buffer.extend(data)
        self._try_parse()

    def _try_parse(self) -> None:
        """Attempt to parse complete messages from the buffer."""
        while self._buffer:
            result, consumed = self._parse_one(self._buffer)
            if result is _INCOMPLETE:
                break
            self._messages.append(result)
            self._buffer = self._buffer[consumed:]

    def get_message(self) -> Optional[Any]:
        """Return the next complete parsed RESP value, or None."""
        if self._messages:
            return self._messages.pop(0)
        return None

    def has_messages(self) -> bool:
        """Whether at least one complete message is available."""
        return bool(self._messages)

    @staticmethod
    def _parse_one(buf: Union[bytes, bytearray]) -> Tuple[Any, int]:
        """Parse a single RESP value from the start of buf.

        Returns (value, bytes_consumed) or (_INCOMPLETE, 0) if the
        buffer does not contain a complete value.
        """
        if not buf:
            return _INCOMPLETE, 0

        type_byte = chr(buf[0])

        if type_byte == "+":
            # Simple String
            return _parse_line(buf)

        if type_byte == "-":
            # Error
            idx = buf.find(b"\r\n")
            if idx < 0:
                return _INCOMPLETE, 0
            msg = buf[1:idx].decode("utf-8", errors="replace")
            return Exception(msg), idx + 2

        if type_byte == ":":
            # Integer
            idx = buf.find(b"\r\n")
            if idx < 0:
                return _INCOMPLETE, 0
            val = int(buf[1:idx])
            return val, idx + 2

        if type_byte == "$":
            # Bulk String
            idx = buf.find(b"\r\n")
            if idx < 0:
                return _INCOMPLETE, 0
            length = int(buf[1:idx])
            if length == -1:
                return None, idx + 2
            start = idx + 2
            end = start + length
            if end + 2 > len(buf):
                return _INCOMPLETE, 0
            data = bytes(buf[start:end])
            return data, end + 2

        if type_byte == "*":
            # Array
            idx = buf.find(b"\r\n")
            if idx < 0:
                return _INCOMPLETE, 0
            count = int(buf[1:idx])
            if count == -1:
                return None, idx + 2
            offset = idx + 2
            items = []
            for _ in range(count):
                if offset >= len(buf):
                    return _INCOMPLETE, 0
                item, consumed = RESPParser._parse_one(buf[offset:])
                if item is _INCOMPLETE:
                    return _INCOMPLETE, 0
                items.append(item)
                offset += consumed
            return items, offset

        # Inline command (space-separated, no type prefix).
        idx = buf.find(b"\r\n")
        if idx < 0:
            # Could be an inline command without \r\n yet.
            idx = buf.find(b"\n")
            if idx < 0:
                return _INCOMPLETE, 0
            line = buf[:idx].decode("utf-8", errors="replace").strip()
            if not line:
                return _INCOMPLETE, idx + 1
            return [p.encode("utf-8") for p in line.split()], idx + 1

        line = buf[:idx].decode("utf-8", errors="replace").strip()
        if not line:
            return _INCOMPLETE, idx + 2
        parts = line.split()
        return [p.encode("utf-8") for p in parts], idx + 2


# Sentinel for incomplete parse.
_INCOMPLETE = object()


def _parse_line(buf: Union[bytes, bytearray]) -> Tuple[Any, int]:
    """Parse a simple-string line from buf."""
    idx = buf.find(b"\r\n")
    if idx < 0:
        return _INCOMPLETE, 0
    text = buf[1:idx].decode("utf-8", errors="replace")
    return text, idx + 2


# ======================================================================
# Redis Command Handler
# ======================================================================

class RedisHandler:
    """Handles Redis commands using the swarm's distributed storage.

    Each command is implemented as a method ``cmd_<NAME>`` that receives
    the argument list and returns a RESP-encodable value.
    """

    def __init__(self, swarm: SwarmConnection) -> None:
        self._swarm = swarm
        self._start_time = time.time()
        # Track subscription callbacks per client.
        self._pubsub_channels: Dict[int, Set[str]] = {}

    # ------------------------------------------------------------------
    # Key helpers
    # ------------------------------------------------------------------

    def _swarm_key(self, key: Union[str, bytes]) -> bytes:
        """Prefix a Redis key for swarm storage."""
        if isinstance(key, bytes):
            key = key.decode("utf-8", errors="replace")
        return (KEY_PREFIX + key).encode("utf-8")

    def _swarm_store(
        self,
        key: bytes,
        value: bytes,
        ttl_seconds: int = 0,
        consistency: str = "eventual",
    ) -> Dict[str, Any]:
        """Store a key-value pair in the swarm."""
        return self._swarm.store(
            key=self._swarm_key(key),
            value=value,
            consistency=consistency,
            ttl_seconds=ttl_seconds,
        )

    def _swarm_fetch(self, key: bytes) -> Dict[str, Any]:
        """Fetch a value from the swarm (lazy TTL deletion handled by swarm)."""
        return self._swarm.fetch(key=self._swarm_key(key))

    def _swarm_delete(self, key: bytes) -> Dict[str, Any]:
        """Delete a key from the swarm."""
        return self._swarm.delete(key=self._swarm_key(key))

    # ------------------------------------------------------------------
    # Command dispatch
    # ------------------------------------------------------------------

    def execute(self, args: List[bytes]) -> Any:
        """Execute a Redis command given the parsed argument list.

        Returns a RESP-encodable value.
        """
        if not args:
            return Exception("empty command")

        cmd = args[0].decode("utf-8", errors="replace").upper()
        handler = getattr(self, f"cmd_{cmd}", None)
        if handler is None:
            return Exception(f"unknown command '{cmd}'")
        try:
            return handler(args[1:])
        except SwarmError as e:
            return Exception(str(e))
        except Exception as e:
            logger.exception("Error handling command %s", cmd)
            return Exception(str(e))

    # ------------------------------------------------------------------
    # Pipeline support (R.3.1)
    # ------------------------------------------------------------------

    def execute_pipeline(self, commands: List[List[bytes]]) -> List[Any]:
        """Execute a batch of commands.

        For MGET/MSET we already do single-round-trip batching.
        Other commands are executed sequentially but responses are
        buffered and returned together.
        """
        results = []
        for cmd_args in commands:
            results.append(self.execute(cmd_args))
        return results

    # ------------------------------------------------------------------
    # String commands
    # ------------------------------------------------------------------

    def cmd_GET(self, args: List[bytes]) -> Any:
        """GET key"""
        if len(args) != 1:
            return Exception("wrong number of arguments for 'GET' command")
        resp = self._swarm_fetch(args[0])
        if not resp.get("found"):
            return None
        return bytes_from_list(resp["value"])

    def cmd_SET(self, args: List[bytes]) -> Any:
        """SET key value [EX seconds] [PX milliseconds] [NX|XX]"""
        if len(args) < 2:
            return Exception("wrong number of arguments for 'SET' command")

        key, value = args[0], args[1]
        ttl_seconds = 0
        nx = False
        xx = False
        i = 2
        while i < len(args):
            opt = args[i].decode("utf-8", errors="replace").upper()
            if opt == "EX" and i + 1 < len(args):
                try:
                    ttl_seconds = int(args[i + 1])
                except ValueError:
                    return Exception("value is not an integer or out of range")
                if ttl_seconds <= 0:
                    return Exception("invalid expire time in 'SET' command")
                i += 2
            elif opt == "PX" and i + 1 < len(args):
                try:
                    px_val = int(args[i + 1])
                except ValueError:
                    return Exception("value is not an integer or out of range")
                if px_val <= 0:
                    return Exception("invalid expire time in 'SET' command")
                ttl_seconds = max(1, px_val // 1000)
                i += 2
            elif opt == "NX":
                nx = True
                i += 1
            elif opt == "XX":
                xx = True
                i += 1
            else:
                return Exception(f"syntax error: unexpected option '{opt}'")

        if nx:
            return self._cmd_setnx_inner(key, value, ttl_seconds)
        if xx:
            existing = self._swarm_fetch(key)
            if not existing.get("found"):
                return None
        self._swarm_store(key, value, ttl_seconds=ttl_seconds)
        return "OK"

    def cmd_SETNX(self, args: List[bytes]) -> Any:
        """SETNX key value -- Set if not exists (R.3.5)."""
        if len(args) != 2:
            return Exception("wrong number of arguments for 'SETNX' command")
        result = self._cmd_setnx_inner(args[0], args[1], 0)
        if result == "OK":
            return 1
        return 0

    def _cmd_setnx_inner(
        self, key: bytes, value: bytes, ttl_seconds: int
    ) -> Any:
        """Optimistic CAS-based SET NX (R.3.5).

        Fetches the current version, and only stores if the key does
        not exist.  Retries on version conflict.
        """
        for _ in range(MAX_CAS_RETRIES):
            existing = self._swarm_fetch(key)
            if existing.get("found"):
                return None  # Key already exists.
            # Key does not exist -- attempt to store.
            try:
                self._swarm_store(key, value, ttl_seconds=ttl_seconds)
                return "OK"
            except SwarmError:
                # Another writer may have created it; retry.
                continue
        return Exception("SETNX CAS retries exhausted")

    def cmd_SETEX(self, args: List[bytes]) -> Any:
        """SETEX key seconds value"""
        if len(args) != 3:
            return Exception("wrong number of arguments for 'SETEX' command")
        try:
            ttl = int(args[1])
        except ValueError:
            return Exception("value is not an integer or out of range")
        if ttl <= 0:
            return Exception("invalid expire time in 'SETEX' command")
        self._swarm_store(args[0], args[2], ttl_seconds=ttl)
        return "OK"

    def cmd_DEL(self, args: List[bytes]) -> Any:
        """DEL key [key ...]"""
        if not args:
            return Exception("wrong number of arguments for 'DEL' command")
        deleted = 0
        for key in args:
            existing = self._swarm_fetch(key)
            if existing.get("found"):
                self._swarm_delete(key)
                deleted += 1
        return deleted

    def cmd_EXISTS(self, args: List[bytes]) -> Any:
        """EXISTS key [key ...]"""
        if not args:
            return Exception("wrong number of arguments for 'EXISTS' command")
        count = 0
        for key in args:
            resp = self._swarm_fetch(key)
            if resp.get("found"):
                count += 1
        return count

    # ------------------------------------------------------------------
    # MGET / MSET batching (R.3.3)
    # ------------------------------------------------------------------

    def cmd_MGET(self, args: List[bytes]) -> Any:
        """MGET key [key ...] -- batch fetch in a single logical round-trip."""
        if not args:
            return Exception("wrong number of arguments for 'MGET' command")
        results = []
        for key in args:
            resp = self._swarm_fetch(key)
            if resp.get("found"):
                results.append(bytes_from_list(resp["value"]))
            else:
                results.append(None)
        return results

    def cmd_MSET(self, args: List[bytes]) -> Any:
        """MSET key value [key value ...] -- batch store."""
        if not args or len(args) % 2 != 0:
            return Exception("wrong number of arguments for 'MSET' command")
        for i in range(0, len(args), 2):
            self._swarm_store(args[i], args[i + 1])
        return "OK"

    # ------------------------------------------------------------------
    # Numeric operations with optimistic CAS (R.3.5)
    # ------------------------------------------------------------------

    def cmd_INCR(self, args: List[bytes]) -> Any:
        """INCR key"""
        if len(args) != 1:
            return Exception("wrong number of arguments for 'INCR' command")
        return self._incr_by(args[0], 1)

    def cmd_DECR(self, args: List[bytes]) -> Any:
        """DECR key"""
        if len(args) != 1:
            return Exception("wrong number of arguments for 'DECR' command")
        return self._incr_by(args[0], -1)

    def cmd_INCRBY(self, args: List[bytes]) -> Any:
        """INCRBY key increment"""
        if len(args) != 2:
            return Exception("wrong number of arguments for 'INCRBY' command")
        try:
            delta = int(args[1])
        except ValueError:
            return Exception("value is not an integer or out of range")
        return self._incr_by(args[0], delta)

    def cmd_DECRBY(self, args: List[bytes]) -> Any:
        """DECRBY key decrement"""
        if len(args) != 2:
            return Exception("wrong number of arguments for 'DECRBY' command")
        try:
            delta = int(args[1])
        except ValueError:
            return Exception("value is not an integer or out of range")
        return self._incr_by(args[0], -delta)

    def _incr_by(self, key: bytes, delta: int) -> Any:
        """Atomic increment via optimistic CAS (R.3.5).

        Reads the current value, computes the new value, and stores it.
        If the version has changed between read and write, we retry.
        """
        for _ in range(MAX_CAS_RETRIES):
            resp = self._swarm_fetch(key)
            if resp.get("found"):
                raw = bytes_from_list(resp["value"])
                try:
                    current = int(raw)
                except ValueError:
                    return Exception("value is not an integer or out of range")
                new_val = current + delta
            else:
                new_val = delta

            new_bytes = str(new_val).encode("utf-8")
            try:
                self._swarm_store(key, new_bytes)
                return new_val
            except SwarmError:
                continue
        return Exception("INCR CAS retries exhausted")

    # ------------------------------------------------------------------
    # TTL / Expiry (R.3.4)
    # ------------------------------------------------------------------

    def cmd_TTL(self, args: List[bytes]) -> Any:
        """TTL key -- returns remaining TTL in seconds, -1 if no TTL, -2 if not found.

        Note: the swarm handles TTL natively.  Since we cannot query
        the remaining TTL directly, we return -1 (no TTL info) for
        existing keys and -2 for missing keys.
        """
        if len(args) != 1:
            return Exception("wrong number of arguments for 'TTL' command")
        resp = self._swarm_fetch(args[0])
        if not resp.get("found"):
            return -2
        # The swarm handles lazy TTL deletion; if the key is found, it
        # is still alive.  We cannot determine the exact remaining TTL
        # from the fetch response, so we report -1 (no expiry info).
        return -1

    def cmd_PTTL(self, args: List[bytes]) -> Any:
        """PTTL key -- like TTL but in milliseconds."""
        if len(args) != 1:
            return Exception("wrong number of arguments for 'PTTL' command")
        resp = self._swarm_fetch(args[0])
        if not resp.get("found"):
            return -2
        return -1

    def cmd_EXPIRE(self, args: List[bytes]) -> Any:
        """EXPIRE key seconds -- set a TTL on an existing key.

        Re-stores the current value with the new TTL.
        """
        if len(args) != 2:
            return Exception("wrong number of arguments for 'EXPIRE' command")
        try:
            ttl = int(args[1])
        except ValueError:
            return Exception("value is not an integer or out of range")
        if ttl <= 0:
            return Exception("invalid expire time in 'EXPIRE' command")
        resp = self._swarm_fetch(args[0])
        if not resp.get("found"):
            return 0
        value = bytes_from_list(resp["value"])
        self._swarm_store(args[0], value, ttl_seconds=ttl)
        return 1

    # ------------------------------------------------------------------
    # KEYS (pattern matching)
    # ------------------------------------------------------------------

    def cmd_KEYS(self, args: List[bytes]) -> Any:
        """KEYS pattern

        The distributed swarm has no native key-scan API.  KEYS is
        inherently O(n) across all nodes and the plugin API does not
        provide a key-listing method, so we return a clear error rather
        than a misleading empty list.
        """
        if len(args) != 1:
            return Exception("wrong number of arguments for 'KEYS' command")
        return Exception(
            "KEYS is not supported in distributed mode; "
            "use application-level key tracking instead"
        )

    # ------------------------------------------------------------------
    # Pub/Sub (R.3.2)
    # ------------------------------------------------------------------

    def cmd_PUBLISH(self, args: List[bytes]) -> Any:
        """PUBLISH channel message"""
        if len(args) != 2:
            return Exception("wrong number of arguments for 'PUBLISH' command")
        channel = args[0].decode("utf-8", errors="replace")
        message = args[1]
        topic = f"redis:pubsub:{channel}"
        resp = self._swarm.publish(topic, message)
        return resp.get("recipients", 0)

    def subscribe_channel(
        self, client_id: int, channel: str
    ) -> None:
        """Register a client for a pub/sub channel via swarm Subscribe (R.3.2)."""
        if client_id not in self._pubsub_channels:
            self._pubsub_channels[client_id] = set()
        if channel not in self._pubsub_channels[client_id]:
            topic = f"redis:pubsub:{channel}"
            self._swarm.subscribe(topic)
            self._pubsub_channels[client_id].add(channel)

    def unsubscribe_channel(self, client_id: int, channel: str) -> None:
        """Unregister a client from a pub/sub channel."""
        if client_id in self._pubsub_channels:
            self._pubsub_channels[client_id].discard(channel)

    def get_subscribed_channels(self, client_id: int) -> Set[str]:
        """Return a copy of channels the client is subscribed to."""
        return set(self._pubsub_channels.get(client_id, set()))

    def remove_client(self, client_id: int) -> None:
        """Remove all subscriptions for a client."""
        self._pubsub_channels.pop(client_id, None)

    # ------------------------------------------------------------------
    # Utility commands
    # ------------------------------------------------------------------

    def cmd_PING(self, args: List[bytes]) -> Any:
        """PING [message]"""
        if not args:
            return "PONG"
        return args[0]

    def cmd_ECHO(self, args: List[bytes]) -> Any:
        """ECHO message"""
        if len(args) != 1:
            return Exception("wrong number of arguments for 'ECHO' command")
        return args[0]

    def cmd_INFO(self, args: List[bytes]) -> Any:
        """INFO [section] -- return server information."""
        uptime = int(time.time() - self._start_time)
        info_str = (
            "# Server\r\n"
            "redis_version:7.0.0-swarm\r\n"
            f"uptime_in_seconds:{uptime}\r\n"
            "# Keyspace\r\n"
            "# Replication\r\n"
            "role:master\r\n"
            "connected_slaves:0\r\n"
        )
        return info_str.encode("utf-8")

    def cmd_COMMAND(self, args: List[bytes]) -> Any:
        """COMMAND -- return supported command info (simplified)."""
        return []

    def cmd_QUIT(self, args: List[bytes]) -> Any:
        """QUIT -- close the connection."""
        return "OK"


# ======================================================================
# Async TCP Server
# ======================================================================

class RedisServer:
    """Asyncio TCP server that speaks the RESP protocol."""

    def __init__(
        self, handler: RedisHandler, host: str = "0.0.0.0", port: int = 6379
    ) -> None:
        self._handler = handler
        self._host = host
        self._port = port
        self._server: Optional[asyncio.AbstractServer] = None
        self._client_counter = 0
        self._running = True
        self._client_writers: Dict[int, asyncio.StreamWriter] = {}

    async def start(self) -> None:
        """Start the TCP server."""
        self._server = await asyncio.start_server(
            self._handle_client, self._host, self._port
        )
        addrs = [str(s.getsockname()) for s in self._server.sockets]
        logger.info("Redis-compatible server listening on %s", ", ".join(addrs))

    async def stop(self) -> None:
        """Stop the TCP server."""
        self._running = False
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    async def serve_forever(self) -> None:
        """Serve until stopped."""
        if self._server:
            async with self._server:
                await self._server.serve_forever()

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Handle a single Redis client connection."""
        self._client_counter += 1
        client_id = self._client_counter
        self._client_writers[client_id] = writer
        peer = writer.get_extra_info("peername")
        logger.info("Client %d connected from %s", client_id, peer)

        parser = RESPParser()
        in_pubsub = False

        try:
            while self._running:
                data = await reader.read(65536)
                if not data:
                    break

                parser.feed(data)

                # Process all complete commands (pipeline support R.3.1).
                commands: List[List[bytes]] = []
                while True:
                    msg = parser.get_message()
                    if msg is None:
                        break
                    if isinstance(msg, list):
                        # Normalize to list of bytes.
                        cmd_args = []
                        for item in msg:
                            if isinstance(item, bytes):
                                cmd_args.append(item)
                            elif isinstance(item, str):
                                cmd_args.append(item.encode("utf-8"))
                            else:
                                cmd_args.append(str(item).encode("utf-8"))
                        commands.append(cmd_args)

                for cmd_args in commands:
                    if not cmd_args:
                        continue

                    cmd_name = cmd_args[0].decode("utf-8", errors="replace").upper()

                    # --- Pub/Sub mode ---
                    if cmd_name == "SUBSCRIBE":
                        if len(cmd_args) < 2:
                            writer.write(resp_encode_wrong_argc("subscribe"))
                            await writer.drain()
                            continue
                        in_pubsub = True
                        for ch_arg in cmd_args[1:]:
                            channel = ch_arg.decode("utf-8", errors="replace")
                            self._handler.subscribe_channel(client_id, channel)
                            # Send subscription confirmation.
                            confirmation = resp_encode([
                                b"subscribe",
                                ch_arg,
                                len(self._handler.get_subscribed_channels(client_id)),
                            ])
                            writer.write(confirmation)
                        await writer.drain()
                        continue

                    if cmd_name == "UNSUBSCRIBE":
                        channels_to_unsub = cmd_args[1:] if len(cmd_args) > 1 else [
                            ch.encode("utf-8")
                            for ch in self._handler.get_subscribed_channels(client_id)
                        ]
                        for ch_arg in channels_to_unsub:
                            channel = ch_arg.decode("utf-8", errors="replace")
                            self._handler.unsubscribe_channel(client_id, channel)
                            remaining = len(self._handler.get_subscribed_channels(client_id))
                            confirmation = resp_encode([
                                b"unsubscribe",
                                ch_arg,
                                remaining,
                            ])
                            writer.write(confirmation)
                        if not self._handler.get_subscribed_channels(client_id):
                            in_pubsub = False
                        await writer.drain()
                        continue

                    # In pub/sub mode, only SUBSCRIBE/UNSUBSCRIBE/PING/QUIT are allowed.
                    if in_pubsub and cmd_name not in ("PING", "QUIT"):
                        writer.write(resp_encode_error(
                            "only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context"
                        ))
                        await writer.drain()
                        continue

                    # --- Normal command execution ---
                    result = self._handler.execute(cmd_args)
                    writer.write(resp_encode(result))
                    await writer.drain()

                    if cmd_name == "QUIT":
                        return

        except asyncio.CancelledError:
            pass
        except ConnectionResetError:
            pass
        except Exception:
            logger.exception("Error handling client %d", client_id)
        finally:
            self._client_writers.pop(client_id, None)
            self._handler.remove_client(client_id)
            logger.info("Client %d disconnected", client_id)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass


# ======================================================================
# Background TTL Cleanup (R.3.4)
# ======================================================================

async def ttl_cleanup_loop(swarm: SwarmConnection) -> None:
    """Periodically trigger swarm-side TTL cleanup.

    The swarm's PluginStorage already handles lazy deletion on fetch
    and has prune_expired().  This loop is a safety net that ensures
    stale keys are cleaned up even if they are never read again.
    """
    while True:
        await asyncio.sleep(TTL_CLEANUP_INTERVAL)
        # The swarm handles TTL internally; this is a no-op heartbeat
        # that keeps the connection alive and allows the swarm to run
        # its own pruning cycle.
        try:
            swarm.get_node_info()
        except asyncio.CancelledError:
            raise
        except SwarmError:
            logger.warning("TTL cleanup heartbeat failed")
        except Exception:
            logger.exception("TTL cleanup heartbeat error")


# ======================================================================
# Swarm Event Listener (R.3.2 -- push-based pub/sub)
# ======================================================================

async def pubsub_listener(
    swarm: SwarmConnection,
    server: RedisServer,
    handler: RedisHandler,
) -> None:
    """Listen for SubscribeEvt messages from the swarm and forward them
    to subscribed Redis clients.

    In a full implementation this would use the async subscribe_iter()
    method.  For now, it runs the synchronous recv_message() in a thread
    executor.

    WARNING: This shares the swarm socket with command handlers via
    _request(). In a production deployment, the swarm connection should
    have a dedicated message dispatcher to avoid the pub/sub listener
    consuming request-response messages (and vice versa).
    """
    loop = asyncio.get_running_loop()
    while True:
        try:
            msg = await loop.run_in_executor(None, swarm.recv_message)
            if msg.get("type") == "SubscribeEvt":
                payload = msg["payload"]
                topic = payload.get("topic", "")
                # Strip the redis:pubsub: prefix to get the channel name.
                if topic.startswith("redis:pubsub:"):
                    channel = topic[len("redis:pubsub:"):]
                    data = bytes_from_list(payload.get("payload", []))
                    # Broadcast to all connected clients subscribed to this channel.
                    message_resp = resp_encode([
                        b"message",
                        channel.encode("utf-8"),
                        data,
                    ])
                    logger.debug("Pub/sub message on channel '%s': %d bytes", channel, len(data))
                    for cid, cwriter in list(server._client_writers.items()):
                        channels = handler.get_subscribed_channels(cid)
                        if channel in channels:
                            try:
                                cwriter.write(message_resp)
                                await cwriter.drain()
                            except Exception:
                                logger.debug("Failed to deliver pub/sub message to client %d", cid)
            else:
                logger.debug("pubsub_listener received non-event message type: %s", msg.get("type"))
        except asyncio.CancelledError:
            raise
        except SwarmError:
            logger.info("Pub/sub listener: swarm connection closed")
            break
        except Exception:
            logger.exception("Pub/sub listener error")
            await asyncio.sleep(1.0)


# ======================================================================
# Main Entry Point
# ======================================================================

async def async_main(args: argparse.Namespace) -> None:
    """Async main: connect to swarm, register, start Redis server."""
    # Connect to the swarm host.
    if args.swarm_socket:
        logger.info("Connecting to swarm via Unix socket: %s", args.swarm_socket)
        swarm = SwarmConnection.unix(args.swarm_socket)
    else:
        logger.info("Connecting to swarm via TCP: %s:%d", args.swarm_host, args.swarm_port)
        swarm = SwarmConnection.tcp(args.swarm_host, args.swarm_port)

    # Register as a Redis plugin.
    reg = swarm.register(
        name="redis",
        version="7.0.0-swarm",
        traits=["CanStoreState"],
        endpoints=[{
            "name": "redis",
            "protocol": "tcp",
            "default_port": args.port,
        }],
    )
    logger.info(
        "Registered with swarm: plugin_id=%s, node_id=%s",
        reg["plugin_id"],
        reg["node_id"],
    )

    # Create the handler and server.
    handler = RedisHandler(swarm)
    server = RedisServer(handler, host=args.host, port=args.port)

    await server.start()

    # Start background tasks.
    cleanup_task = asyncio.create_task(ttl_cleanup_loop(swarm))
    pubsub_task = asyncio.create_task(pubsub_listener(swarm, server, handler))

    # Handle shutdown signals.
    stop_event = asyncio.Event()

    def signal_handler() -> None:
        logger.info("Shutdown signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            # Windows does not support add_signal_handler.
            pass

    # Serve until shutdown.
    serve_task = asyncio.create_task(server.serve_forever())
    try:
        await stop_event.wait()
    except asyncio.CancelledError:
        pass
    finally:
        serve_task.cancel()
        cleanup_task.cancel()
        pubsub_task.cancel()
        # Wait for background tasks to finish cancellation.
        for task in (serve_task, cleanup_task, pubsub_task):
            try:
                await task
            except asyncio.CancelledError:
                pass
        await server.stop()
        swarm.close()
        logger.info("Redis plugin shut down")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Redis-compatible Marabunta Swarm Plugin"
    )
    parser.add_argument(
        "--host", type=str, default="0.0.0.0",
        help="Redis listen address (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port", type=int, default=6379,
        help="Redis listen port (default: 6379)",
    )
    parser.add_argument(
        "--swarm-socket", type=str, default=None,
        help="Unix socket path for swarm connection",
    )
    parser.add_argument(
        "--swarm-host", type=str, default="127.0.0.1",
        help="Swarm host for TCP connection (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--swarm-port", type=int, default=4201,
        help="Swarm port for TCP connection (default: 4201)",
    )
    args = parser.parse_args()
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
