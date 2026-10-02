# Marabunta - Licensed under the MIT License.
import json
import base64
import requests
from typing import Dict, Any

class SwarmClient:
    """Client for interacting with the Marabunta Swarm API."""
    
    def __init__(self, api_url: str = "http://localhost:8080"):
        self.api_url = api_url.rstrip("/")

    def submit_diloco_job(
        self,
        name: str,
        wasm_bytes: bytes,
        sync_interval: int,
        outer_momentum: float,
        dataset_shard_uri: str,
        global_step: int,
        strategy: str,
        priority: int = 50,
        adaptive_timeout_ms: int = 5000,
        override_ip: str = ""
    ) -> Dict[str, Any]:
        """Submit a compiled WASM DiLoCo payload to the Swarm."""
        
        encoded_wasm = base64.b64encode(wasm_bytes).decode("utf-8")
        
        strategy_json = strategy
        if strategy == "adaptive":
            strategy_json = {"adaptive": {"timeout_ms": adaptive_timeout_ms}}
        elif strategy == "corporate_override":
            if not override_ip:
                raise ValueError("override_ip required for corporate_override strategy")
            strategy_json = {"corporate_override": {"target_ip": override_ip}}

        payload = {
            "name": name,
            "script_type": {
                "diloco_training": {
                    "sync_interval": sync_interval,
                    "outer_momentum": outer_momentum,
                    "dataset_shard_uri": dataset_shard_uri,
                    "global_step": global_step,
                    "strategy": strategy_json
                }
            },
            "script": encoded_wasm,
            "chunk_strategy": {"type": "single"},
            "priority": priority
        }

        response = requests.post(f"{self.api_url}/api/v1/jobs", json=payload)
        response.raise_for_status()
        return response.json()