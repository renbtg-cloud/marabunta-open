#!/usr/bin/env python3
# Marabunta - Licensed under the MIT License.
"""
Hello-World Plugin for the Marabunta Swarm.

A minimal plugin that demonstrates registration, handling incoming
requests (echo), and responding to health checks.

Usage:
    python hello_world.py --socket /tmp/marabunta/plugins/hello.sock
    python hello_world.py --host 127.0.0.1 --port 4201
"""

from __future__ import annotations

import argparse
import logging
import sys
import os

# Allow importing the shared library from the sibling directory.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared", "python"))

from swarm_client import SwarmConnection, bytes_from_list

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("hello-world")


def main() -> None:
    parser = argparse.ArgumentParser(description="Hello-World Swarm Plugin")
    parser.add_argument("--socket", type=str, help="Unix socket path")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="TCP host")
    parser.add_argument("--port", type=int, default=4201, help="TCP port")
    args = parser.parse_args()

    # Connect to the swarm host.
    if args.socket:
        logger.info("Connecting via Unix socket: %s", args.socket)
        conn = SwarmConnection.unix(args.socket)
    else:
        logger.info("Connecting via TCP: %s:%d", args.host, args.port)
        conn = SwarmConnection.tcp(args.host, args.port)

    # Register with the swarm.
    reg = conn.register(
        name="hello-world",
        version="1.0.0",
        traits=["CanExecute"],
        endpoints=[],
    )
    logger.info("Registered: plugin_id=%s, node_id=%s", reg["plugin_id"], reg["node_id"])

    # Main loop: handle incoming messages from the swarm.
    logger.info("Listening for incoming requests...")
    try:
        while True:
            msg = conn.recv_message()
            msg_type = msg.get("type", "")

            if msg_type == "HealthReq":
                conn.send_message({
                    "type": "HealthResp",
                    "payload": {
                        "healthy": True,
                        "status": "running",
                        "details": {"version": "1.0.0"},
                    },
                })

            elif msg_type == "HandleReq":
                payload = msg.get("payload", {})
                request_id = payload.get("request_id", "")
                data = bytes_from_list(payload.get("payload", []))
                logger.info("HandleReq id=%s payload=%s", request_id, data.decode("utf-8", errors="replace"))
                # Echo the payload back.
                conn.send_message({
                    "type": "HandleResp",
                    "payload": {
                        "payload": list(data),
                        "error": "",
                    },
                })

            elif msg_type == "StartReq":
                logger.info("Received StartReq")
                conn.send_message({
                    "type": "StartResp",
                    "payload": {"success": True, "error": ""},
                })

            elif msg_type == "StopReq":
                logger.info("Received StopReq, shutting down")
                conn.send_message({
                    "type": "StopResp",
                    "payload": {"clean": True},
                })
                break

            else:
                logger.warning("Unknown message type: %s", msg_type)

    except KeyboardInterrupt:
        logger.info("Interrupted, shutting down")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
