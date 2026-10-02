#!/usr/bin/env python3
# Marabunta - Licensed under the MIT License.
"""
Tests for the Redis-compatible Marabunta Swarm plugin.

Covers:
    - RESP protocol parsing and serialization
    - Key-value operations (GET, SET, DEL, EXISTS, MGET, MSET)
    - Numeric operations (INCR, DECR, INCRBY, DECRBY)
    - TTL / expiry commands (SETEX, EXPIRE, TTL, PTTL)
    - SETNX with optimistic CAS semantics
    - Utility commands (PING, ECHO, INFO, COMMAND, QUIT)
    - Pub/Sub (PUBLISH, SUBSCRIBE, UNSUBSCRIBE)
    - Pipeline execution

These tests mock the SwarmConnection to avoid requiring a live swarm.
"""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch
from typing import Any, Dict, List

import os
import sys

# Allow importing from plugin directories.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared", "python"))
sys.path.insert(0, os.path.dirname(__file__))

from redis_plugin import (
    RESPParser,
    RedisHandler,
    resp_encode,
    resp_encode_error,
    _INCOMPLETE,
    KEY_PREFIX,
)
from swarm_client import _to_byte_list, bytes_from_list


# ======================================================================
# RESP Protocol Tests
# ======================================================================


class TestRESPEncode(unittest.TestCase):
    """Tests for RESP serialization."""

    def test_simple_string(self) -> None:
        self.assertEqual(resp_encode("OK"), b"+OK\r\n")

    def test_simple_string_pong(self) -> None:
        self.assertEqual(resp_encode("PONG"), b"+PONG\r\n")

    def test_error(self) -> None:
        result = resp_encode(Exception("something went wrong"))
        self.assertEqual(result, b"-ERR something went wrong\r\n")

    def test_integer(self) -> None:
        self.assertEqual(resp_encode(42), b":42\r\n")

    def test_integer_zero(self) -> None:
        self.assertEqual(resp_encode(0), b":0\r\n")

    def test_integer_negative(self) -> None:
        self.assertEqual(resp_encode(-1), b":-1\r\n")

    def test_bulk_string(self) -> None:
        self.assertEqual(resp_encode(b"hello"), b"$5\r\nhello\r\n")

    def test_bulk_string_empty(self) -> None:
        self.assertEqual(resp_encode(b""), b"$0\r\n\r\n")

    def test_null(self) -> None:
        self.assertEqual(resp_encode(None), b"$-1\r\n")

    def test_array(self) -> None:
        result = resp_encode([b"foo", b"bar"])
        expected = b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
        self.assertEqual(result, expected)

    def test_array_empty(self) -> None:
        self.assertEqual(resp_encode([]), b"*0\r\n")

    def test_array_mixed(self) -> None:
        result = resp_encode([b"set", b"key", 42])
        expected = b"*3\r\n$3\r\nset\r\n$3\r\nkey\r\n:42\r\n"
        self.assertEqual(result, expected)

    def test_nested_array(self) -> None:
        result = resp_encode([[b"a"], [b"b"]])
        expected = b"*2\r\n*1\r\n$1\r\na\r\n*1\r\n$1\r\nb\r\n"
        self.assertEqual(result, expected)

    def test_error_encode(self) -> None:
        result = resp_encode_error("custom error")
        self.assertEqual(result, b"-ERR custom error\r\n")


class TestRESPParser(unittest.TestCase):
    """Tests for RESP deserialization."""

    def test_simple_string(self) -> None:
        parser = RESPParser()
        parser.feed(b"+OK\r\n")
        self.assertEqual(parser.get_message(), "OK")

    def test_error(self) -> None:
        parser = RESPParser()
        parser.feed(b"-ERR bad\r\n")
        msg = parser.get_message()
        self.assertIsInstance(msg, Exception)
        self.assertIn("bad", str(msg))

    def test_integer(self) -> None:
        parser = RESPParser()
        parser.feed(b":1000\r\n")
        self.assertEqual(parser.get_message(), 1000)

    def test_bulk_string(self) -> None:
        parser = RESPParser()
        parser.feed(b"$6\r\nfoobar\r\n")
        self.assertEqual(parser.get_message(), b"foobar")

    def test_null_bulk_string(self) -> None:
        parser = RESPParser()
        parser.feed(b"$-1\r\n")
        self.assertIsNone(parser.get_message())

    def test_array(self) -> None:
        parser = RESPParser()
        parser.feed(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
        msg = parser.get_message()
        self.assertEqual(msg, [b"foo", b"bar"])

    def test_null_array(self) -> None:
        parser = RESPParser()
        parser.feed(b"*-1\r\n")
        self.assertIsNone(parser.get_message())

    def test_empty_array(self) -> None:
        parser = RESPParser()
        parser.feed(b"*0\r\n")
        self.assertEqual(parser.get_message(), [])

    def test_incremental_feed(self) -> None:
        """Parser handles data arriving in chunks."""
        parser = RESPParser()
        parser.feed(b"*2\r\n$3\r\n")
        self.assertIsNone(parser.get_message())
        parser.feed(b"GET\r\n$4\r\n")
        self.assertIsNone(parser.get_message())
        parser.feed(b"name\r\n")
        msg = parser.get_message()
        self.assertEqual(msg, [b"GET", b"name"])

    def test_multiple_messages(self) -> None:
        """Parser handles multiple complete messages in one feed."""
        parser = RESPParser()
        parser.feed(b"+OK\r\n:42\r\n$5\r\nhello\r\n")
        self.assertEqual(parser.get_message(), "OK")
        self.assertEqual(parser.get_message(), 42)
        self.assertEqual(parser.get_message(), b"hello")
        self.assertIsNone(parser.get_message())

    def test_pipeline_commands(self) -> None:
        """Parser handles pipelined RESP commands."""
        cmd1 = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
        cmd2 = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"
        parser = RESPParser()
        parser.feed(cmd1 + cmd2)
        msg1 = parser.get_message()
        msg2 = parser.get_message()
        self.assertEqual(msg1, [b"SET", b"key", b"value"])
        self.assertEqual(msg2, [b"GET", b"key"])

    def test_inline_command(self) -> None:
        """Parser handles inline commands (space-separated)."""
        parser = RESPParser()
        parser.feed(b"PING\r\n")
        msg = parser.get_message()
        self.assertIsNotNone(msg)
        self.assertIsInstance(msg, list)

    def test_has_messages(self) -> None:
        parser = RESPParser()
        self.assertFalse(parser.has_messages())
        parser.feed(b"+OK\r\n")
        self.assertTrue(parser.has_messages())
        parser.get_message()
        self.assertFalse(parser.has_messages())


# ======================================================================
# Mock SwarmConnection for Handler Tests
# ======================================================================


def make_mock_swarm() -> MagicMock:
    """Create a mock SwarmConnection with sensible defaults."""
    swarm = MagicMock()
    _store: Dict[str, Dict[str, Any]] = {}

    def mock_store(key, value, consistency="eventual", ttl_seconds=0, replicas=3):
        k = repr(_to_byte_list(key))
        _store[k] = {
            "value": _to_byte_list(value),
            "version": _store.get(k, {}).get("version", 0) + 1,
        }
        return {
            "success": True,
            "error": "",
            "version": _store[k]["version"],
        }

    def mock_fetch(key, consistency="eventual", min_version=0):
        k = repr(_to_byte_list(key))
        if k in _store:
            return {
                "found": True,
                "value": _store[k]["value"],
                "version": _store[k]["version"],
                "error": "",
            }
        return {"found": False, "value": [], "version": 0, "error": ""}

    def mock_delete(key):
        k = repr(_to_byte_list(key))
        existed = k in _store
        _store.pop(k, None)
        return {"success": True, "error": ""}

    def mock_publish(topic, payload):
        return {"success": True, "recipients": 0}

    def mock_subscribe(topic):
        pass

    def mock_get_node_info():
        return {"node_id": "test-node", "region": "test", "traits": []}

    swarm.store = mock_store
    swarm.fetch = mock_fetch
    swarm.delete = mock_delete
    swarm.publish = mock_publish
    swarm.subscribe = mock_subscribe
    swarm.get_node_info = mock_get_node_info

    return swarm


# ======================================================================
# Redis Command Handler Tests
# ======================================================================


class TestRedisHandler(unittest.TestCase):
    """Tests for Redis command handling."""

    def setUp(self) -> None:
        self.swarm = make_mock_swarm()
        self.handler = RedisHandler(self.swarm)

    # --- PING / ECHO ---

    def test_ping(self) -> None:
        result = self.handler.execute([b"PING"])
        self.assertEqual(result, "PONG")

    def test_ping_with_message(self) -> None:
        result = self.handler.execute([b"PING", b"hello"])
        self.assertEqual(result, b"hello")

    def test_echo(self) -> None:
        result = self.handler.execute([b"ECHO", b"test"])
        self.assertEqual(result, b"test")

    def test_echo_wrong_args(self) -> None:
        result = self.handler.execute([b"ECHO"])
        self.assertIsInstance(result, Exception)

    # --- GET / SET ---

    def test_set_and_get(self) -> None:
        result = self.handler.execute([b"SET", b"mykey", b"myvalue"])
        self.assertEqual(result, "OK")
        result = self.handler.execute([b"GET", b"mykey"])
        self.assertEqual(result, b"myvalue")

    def test_get_nonexistent(self) -> None:
        result = self.handler.execute([b"GET", b"missing"])
        self.assertIsNone(result)

    def test_set_overwrites(self) -> None:
        self.handler.execute([b"SET", b"key", b"v1"])
        self.handler.execute([b"SET", b"key", b"v2"])
        result = self.handler.execute([b"GET", b"key"])
        self.assertEqual(result, b"v2")

    def test_set_with_ex(self) -> None:
        result = self.handler.execute([b"SET", b"key", b"val", b"EX", b"60"])
        self.assertEqual(result, "OK")

    def test_set_with_nx(self) -> None:
        result = self.handler.execute([b"SET", b"key", b"val", b"NX"])
        self.assertEqual(result, "OK")
        # Second SET NX should fail.
        result = self.handler.execute([b"SET", b"key", b"val2", b"NX"])
        self.assertIsNone(result)

    def test_set_with_xx_existing(self) -> None:
        self.handler.execute([b"SET", b"key", b"v1"])
        result = self.handler.execute([b"SET", b"key", b"v2", b"XX"])
        self.assertEqual(result, "OK")

    def test_set_with_xx_nonexistent(self) -> None:
        result = self.handler.execute([b"SET", b"key", b"val", b"XX"])
        self.assertIsNone(result)

    def test_get_wrong_args(self) -> None:
        result = self.handler.execute([b"GET"])
        self.assertIsInstance(result, Exception)

    def test_set_wrong_args(self) -> None:
        result = self.handler.execute([b"SET", b"key"])
        self.assertIsInstance(result, Exception)

    # --- SETNX ---

    def test_setnx_new_key(self) -> None:
        result = self.handler.execute([b"SETNX", b"key", b"val"])
        self.assertEqual(result, 1)

    def test_setnx_existing_key(self) -> None:
        self.handler.execute([b"SET", b"key", b"existing"])
        result = self.handler.execute([b"SETNX", b"key", b"new"])
        self.assertEqual(result, 0)

    # --- SETEX ---

    def test_setex(self) -> None:
        result = self.handler.execute([b"SETEX", b"key", b"60", b"val"])
        self.assertEqual(result, "OK")
        result = self.handler.execute([b"GET", b"key"])
        self.assertEqual(result, b"val")

    def test_setex_invalid_ttl(self) -> None:
        result = self.handler.execute([b"SETEX", b"key", b"0", b"val"])
        self.assertIsInstance(result, Exception)

    def test_setex_wrong_args(self) -> None:
        result = self.handler.execute([b"SETEX", b"key", b"60"])
        self.assertIsInstance(result, Exception)

    # --- DEL ---

    def test_del_existing(self) -> None:
        self.handler.execute([b"SET", b"key", b"val"])
        result = self.handler.execute([b"DEL", b"key"])
        self.assertEqual(result, 1)
        result = self.handler.execute([b"GET", b"key"])
        self.assertIsNone(result)

    def test_del_nonexistent(self) -> None:
        result = self.handler.execute([b"DEL", b"ghost"])
        self.assertEqual(result, 0)

    def test_del_multiple(self) -> None:
        self.handler.execute([b"SET", b"a", b"1"])
        self.handler.execute([b"SET", b"b", b"2"])
        result = self.handler.execute([b"DEL", b"a", b"b", b"c"])
        self.assertEqual(result, 2)

    # --- EXISTS ---

    def test_exists(self) -> None:
        self.handler.execute([b"SET", b"key", b"val"])
        result = self.handler.execute([b"EXISTS", b"key"])
        self.assertEqual(result, 1)

    def test_exists_nonexistent(self) -> None:
        result = self.handler.execute([b"EXISTS", b"ghost"])
        self.assertEqual(result, 0)

    def test_exists_multiple(self) -> None:
        self.handler.execute([b"SET", b"a", b"1"])
        self.handler.execute([b"SET", b"b", b"2"])
        result = self.handler.execute([b"EXISTS", b"a", b"b", b"c"])
        self.assertEqual(result, 2)

    # --- MGET / MSET (R.3.3) ---

    def test_mset_and_mget(self) -> None:
        result = self.handler.execute([b"MSET", b"k1", b"v1", b"k2", b"v2"])
        self.assertEqual(result, "OK")
        result = self.handler.execute([b"MGET", b"k1", b"k2", b"k3"])
        self.assertEqual(result, [b"v1", b"v2", None])

    def test_mset_odd_args(self) -> None:
        result = self.handler.execute([b"MSET", b"k1", b"v1", b"k2"])
        self.assertIsInstance(result, Exception)

    def test_mget_no_args(self) -> None:
        result = self.handler.execute([b"MGET"])
        self.assertIsInstance(result, Exception)

    # --- INCR / DECR (R.3.5) ---

    def test_incr_new_key(self) -> None:
        result = self.handler.execute([b"INCR", b"counter"])
        self.assertEqual(result, 1)

    def test_incr_existing(self) -> None:
        self.handler.execute([b"SET", b"counter", b"10"])
        result = self.handler.execute([b"INCR", b"counter"])
        self.assertEqual(result, 11)

    def test_decr(self) -> None:
        self.handler.execute([b"SET", b"counter", b"10"])
        result = self.handler.execute([b"DECR", b"counter"])
        self.assertEqual(result, 9)

    def test_incr_non_integer(self) -> None:
        self.handler.execute([b"SET", b"key", b"not_a_number"])
        result = self.handler.execute([b"INCR", b"key"])
        self.assertIsInstance(result, Exception)

    def test_incrby(self) -> None:
        self.handler.execute([b"SET", b"counter", b"5"])
        result = self.handler.execute([b"INCRBY", b"counter", b"3"])
        self.assertEqual(result, 8)

    def test_decrby(self) -> None:
        self.handler.execute([b"SET", b"counter", b"10"])
        result = self.handler.execute([b"DECRBY", b"counter", b"3"])
        self.assertEqual(result, 7)

    # --- TTL / EXPIRE (R.3.4) ---

    def test_ttl_nonexistent(self) -> None:
        result = self.handler.execute([b"TTL", b"ghost"])
        self.assertEqual(result, -2)

    def test_ttl_no_expiry(self) -> None:
        self.handler.execute([b"SET", b"key", b"val"])
        result = self.handler.execute([b"TTL", b"key"])
        self.assertEqual(result, -1)

    def test_pttl_nonexistent(self) -> None:
        result = self.handler.execute([b"PTTL", b"ghost"])
        self.assertEqual(result, -2)

    def test_expire(self) -> None:
        self.handler.execute([b"SET", b"key", b"val"])
        result = self.handler.execute([b"EXPIRE", b"key", b"60"])
        self.assertEqual(result, 1)

    def test_expire_nonexistent(self) -> None:
        result = self.handler.execute([b"EXPIRE", b"ghost", b"60"])
        self.assertEqual(result, 0)

    # --- KEYS ---

    def test_keys(self) -> None:
        # KEYS is not fully supported in distributed mode, returns [].
        result = self.handler.execute([b"KEYS", b"*"])
        self.assertEqual(result, [])

    # --- PUBLISH ---

    def test_publish(self) -> None:
        result = self.handler.execute([b"PUBLISH", b"channel", b"message"])
        self.assertEqual(result, 0)  # No subscribers.

    def test_publish_wrong_args(self) -> None:
        result = self.handler.execute([b"PUBLISH", b"channel"])
        self.assertIsInstance(result, Exception)

    # --- INFO ---

    def test_info(self) -> None:
        result = self.handler.execute([b"INFO"])
        self.assertIsInstance(result, bytes)
        self.assertIn(b"redis_version", result)

    # --- COMMAND ---

    def test_command(self) -> None:
        result = self.handler.execute([b"COMMAND"])
        self.assertIsInstance(result, list)

    # --- QUIT ---

    def test_quit(self) -> None:
        result = self.handler.execute([b"QUIT"])
        self.assertEqual(result, "OK")

    # --- Unknown command ---

    def test_unknown_command(self) -> None:
        result = self.handler.execute([b"FOOBAR"])
        self.assertIsInstance(result, Exception)

    # --- Empty command ---

    def test_empty_command(self) -> None:
        result = self.handler.execute([])
        self.assertIsInstance(result, Exception)

    # --- Pipeline (R.3.1) ---

    def test_pipeline(self) -> None:
        commands = [
            [b"SET", b"k1", b"v1"],
            [b"SET", b"k2", b"v2"],
            [b"GET", b"k1"],
            [b"GET", b"k2"],
        ]
        results = self.handler.execute_pipeline(commands)
        self.assertEqual(len(results), 4)
        self.assertEqual(results[0], "OK")
        self.assertEqual(results[1], "OK")
        self.assertEqual(results[2], b"v1")
        self.assertEqual(results[3], b"v2")

    # --- Pub/Sub tracking ---

    def test_subscribe_and_unsubscribe_tracking(self) -> None:
        self.handler.subscribe_channel(1, "ch1")
        self.handler.subscribe_channel(1, "ch2")
        channels = self.handler.get_subscribed_channels(1)
        self.assertEqual(channels, {"ch1", "ch2"})

        self.handler.unsubscribe_channel(1, "ch1")
        channels = self.handler.get_subscribed_channels(1)
        self.assertEqual(channels, {"ch2"})

    def test_remove_client(self) -> None:
        self.handler.subscribe_channel(1, "ch1")
        self.handler.remove_client(1)
        channels = self.handler.get_subscribed_channels(1)
        self.assertEqual(channels, set())


# ======================================================================
# Byte Conversion Helper Tests
# ======================================================================


class TestByteHelpers(unittest.TestCase):
    """Tests for swarm_client byte conversion helpers."""

    def test_to_byte_list_str(self) -> None:
        result = _to_byte_list("hello")
        self.assertEqual(result, [104, 101, 108, 108, 111])

    def test_to_byte_list_bytes(self) -> None:
        result = _to_byte_list(b"\x00\xff")
        self.assertEqual(result, [0, 255])

    def test_to_byte_list_passthrough(self) -> None:
        result = _to_byte_list([1, 2, 3])
        self.assertEqual(result, [1, 2, 3])

    def test_bytes_from_list(self) -> None:
        result = bytes_from_list([104, 101, 108, 108, 111])
        self.assertEqual(result, b"hello")

    def test_bytes_from_list_str(self) -> None:
        result = bytes_from_list("hello")
        self.assertEqual(result, b"hello")

    def test_bytes_from_list_bytes(self) -> None:
        result = bytes_from_list(b"hello")
        self.assertEqual(result, b"hello")


if __name__ == "__main__":
    unittest.main()
