# Marabunta - Licensed under the MIT License.
"""
Marabunta Distributed Low-Communication (DiLoCo) SDK for PyTorch.

This module intercepts standard PyTorch training loops, traces the computational
graph into ONNX/safetensors, bundles it with the Marabunta WASM execution engine,
and orchestrates the federated training state machine across the decentralized Swarm.
"""

from .optimizer import DistributedDiLoCo
from .client import SwarmClient

__version__ = "0.1.0"
__all__ = ["DistributedDiLoCo", "SwarmClient"]