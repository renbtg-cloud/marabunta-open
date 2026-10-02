# Marabunta - Licensed under the MIT License.
"""
Marabunta Radiation Python SDK — Module-specific client classes.

Provides typed wrappers around the CR REST API for all 22 pre-packaged
compute modules, plus a generic CRClient base class for custom usage.

Usage:
    from cr_modules import CRClient, MonteCarlo, Dock, Render

    client = CRClient("http://localhost:8080", token="...")
    mc = MonteCarlo(client)
    estimate = mc.estimate(simulations=1_000_000)
    job = mc.submit(script="simulate.py", simulations=1_000_000)
    status = client.status(job["job_id"])
    results = client.results(job["job_id"])
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, BinaryIO, Dict, List, Optional, Union

try:
    import requests
except ImportError:
    raise ImportError(
        "The 'requests' library is required. Install with: pip install requests"
    )

__version__ = "0.1.0"
__all__ = [
    "CRClient",
    "CRError",
    "MonteCarlo",
    "Brute",
    "Evolve",
    "Crunch",
    "Bootstrap",
    "Replicate",
    "Classify",
    "Validate",
    "Match",
    "Scan",
    "Grade",
    "Render",
    "Transcode",
    "OCR",
    "Dock",
    "Sweep",
    "Inference",
    "Embed",
    "Tune",
    "Eval",
    "Risk",
    "Price",
    "Route",
]

# =============================================================================
# Errors
# =============================================================================

class CRError(Exception):
    """Base exception for CR SDK errors."""

    def __init__(self, message: str, status_code: int = 0, body: str = ""):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


class CRAPIError(CRError):
    """Raised when the API returns a non-2xx response."""
    pass


class CRTimeoutError(CRError):
    """Raised when a polling wait times out."""
    pass


# =============================================================================
# CRClient -- base API client
# =============================================================================

class CRClient:
    """Low-level client for the Marabunta Radiation REST API."""

    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        token: Optional[str] = None,
        timeout: float = 30.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.token = token or os.environ.get("CR_TOKEN")
        self.timeout = timeout
        self._session = requests.Session()
        self._session.headers["User-Agent"] = f"cr-python-sdk/{__version__}"
        if self.token:
            self._session.headers["Authorization"] = f"Bearer {self.token}"

    def _url(self, path: str) -> str:
        return f"{self.base_url}/api/v1{path}"

    def _check(self, resp: requests.Response) -> Any:
        if resp.status_code >= 400:
            raise CRAPIError(
                f"API error {resp.status_code}: {resp.text}",
                status_code=resp.status_code,
                body=resp.text,
            )
        if resp.headers.get("Content-Type", "").startswith("application/json"):
            return resp.json()
        return resp.text

    def get(self, path: str, **params) -> Any:
        resp = self._session.get(self._url(path), params=params, timeout=self.timeout)
        return self._check(resp)

    def post(self, path: str, json_body: Optional[dict] = None) -> Any:
        resp = self._session.post(
            self._url(path), json=json_body, timeout=self.timeout
        )
        return self._check(resp)

    def post_multipart(
        self,
        path: str,
        params: dict,
        files: Optional[Dict[str, Union[str, Path, BinaryIO]]] = None,
    ) -> Any:
        form_files = [("params", (None, json.dumps(params), "application/json"))]
        if files:
            for name, f in files.items():
                if isinstance(f, (str, Path)):
                    p = Path(f)
                    form_files.append(("file", (p.name, open(p, "rb"))))
                else:
                    form_files.append(("file", (name, f)))

        resp = self._session.post(
            self._url(path), files=form_files, timeout=self.timeout
        )
        return self._check(resp)

    def delete(self, path: str) -> Any:
        resp = self._session.delete(self._url(path), timeout=self.timeout)
        return self._check(resp)

    # -----------------------------------------------------------------
    # High-level convenience methods
    # -----------------------------------------------------------------

    def modules(self) -> List[dict]:
        """List all available compute modules."""
        return self.get("/combo/modules")

    def module(self, module_id: str) -> dict:
        """Get detailed info for a module."""
        return self.get(f"/combo/modules/{module_id}")

    def module_example(self, module_id: str) -> dict:
        """Get a usage example for a module."""
        return self.get(f"/combo/modules/{module_id}/example")

    def categories(self) -> List[str]:
        """List module categories."""
        return self.get("/combo/modules/categories")

    def estimate(self, module_id: str, total_items: int = 1, **kwargs) -> dict:
        """Get a cost estimate for a job."""
        body = {"module_id": module_id, "total_items": total_items, **kwargs}
        return self.post("/combo/estimate", body)

    def submit(
        self,
        module_id: str,
        params: dict,
        files: Optional[Dict[str, Union[str, Path, BinaryIO]]] = None,
    ) -> dict:
        """Submit a job."""
        full_params = {"module": module_id, **params}
        return self.post_multipart("/combo/jobs", full_params, files)

    def status(self, job_id: str) -> dict:
        """Get job status."""
        return self.get(f"/combo/jobs/{job_id}")

    def cancel(self, job_id: str) -> dict:
        """Cancel a running job."""
        return self.post(f"/combo/jobs/{job_id}/cancel")

    def results(self, job_id: str) -> Any:
        """Get job results."""
        return self.get(f"/combo/jobs/{job_id}/results")

    def history(self, limit: int = 20, status: Optional[str] = None) -> List[dict]:
        """List recent jobs."""
        params = {"limit": limit}
        if status:
            params["status"] = status
        return self.get("/combo/jobs", **params)

    def wait(
        self,
        job_id: str,
        timeout: float = 3600,
        poll_interval: float = 5.0,
        on_progress: Optional[callable] = None,
    ) -> dict:
        """Poll until a job completes or times out."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            st = self.status(job_id)
            if on_progress:
                on_progress(st)
            if st.get("status") in ("completed", "failed", "cancelled"):
                return st
            time.sleep(poll_interval)
        raise CRTimeoutError(f"Job {job_id} did not complete within {timeout}s")


# =============================================================================
# Module-specific classes
# =============================================================================

class _ModuleBase:
    """Base class for module-specific wrappers."""

    MODULE_ID: str = ""

    def __init__(self, client: CRClient):
        self.client = client

    def info(self) -> dict:
        return self.client.module(self.MODULE_ID)

    def example(self) -> dict:
        return self.client.module_example(self.MODULE_ID)

    def estimate(self, total_items: int = 1, **kwargs) -> dict:
        return self.client.estimate(self.MODULE_ID, total_items=total_items, **kwargs)

    def submit(self, params: Optional[dict] = None, files: Optional[dict] = None, **kwargs) -> dict:
        p = {**(params or {}), **kwargs}
        return self.client.submit(self.MODULE_ID, p, files)


class MonteCarlo(_ModuleBase):
    """CR.MonteCarlo — Monte Carlo simulation."""
    MODULE_ID = "cr.montecarlo"

    def submit(self, simulations: int = 1_000_000, script: Optional[str] = None, **kwargs) -> dict:
        params = {"simulations": simulations, **kwargs}
        files = {"script": script} if script else None
        return self.client.submit(self.MODULE_ID, params, files)


class Brute(_ModuleBase):
    """CR.Brute — Brute-force search."""
    MODULE_ID = "cr.brute"

    def submit(self, target_hash: str, charset: str = "abcdefghijklmnopqrstuvwxyz0123456789", max_length: int = 8, **kwargs) -> dict:
        params = {"target_hash": target_hash, "charset": charset, "max_length": max_length, **kwargs}
        return self.client.submit(self.MODULE_ID, params)


class Evolve(_ModuleBase):
    """CR.Evolve — Evolutionary / genetic algorithms."""
    MODULE_ID = "cr.evolve"

    def submit(self, population_size: int = 1000, generations: int = 100, fitness_fn: Optional[str] = None, **kwargs) -> dict:
        params = {"population_size": population_size, "generations": generations, **kwargs}
        files = {"fitness_fn": fitness_fn} if fitness_fn else None
        return self.client.submit(self.MODULE_ID, params, files)


class Crunch(_ModuleBase):
    """CR.Crunch — Number crunching."""
    MODULE_ID = "cr.crunch"


class Bootstrap(_ModuleBase):
    """CR.Bootstrap — Statistical bootstrapping."""
    MODULE_ID = "cr.bootstrap"

    def submit(self, resamples: int = 10000, statistic: str = "mean", data_file: Optional[str] = None, **kwargs) -> dict:
        params = {"resamples": resamples, "statistic": statistic, **kwargs}
        files = {"data": data_file} if data_file else None
        return self.client.submit(self.MODULE_ID, params, files)


class Replicate(_ModuleBase):
    """CR.Replicate — Experiment replication."""
    MODULE_ID = "cr.replicate"


class Classify(_ModuleBase):
    """CR.Classify — Data classification."""
    MODULE_ID = "cr.classify"


class Validate(_ModuleBase):
    """CR.Validate — Data validation."""
    MODULE_ID = "cr.validate"


class Match(_ModuleBase):
    """CR.Match — Record matching."""
    MODULE_ID = "cr.match"


class Scan(_ModuleBase):
    """CR.Scan — Pattern scanning."""
    MODULE_ID = "cr.scan"


class Grade(_ModuleBase):
    """CR.Grade — Automated grading."""
    MODULE_ID = "cr.grade"


class Render(_ModuleBase):
    """CR.Render — 3D rendering."""
    MODULE_ID = "cr.render"

    def submit(self, scene: str, frames: str = "1-100", resolution: str = "1920x1080", samples: int = 128, **kwargs) -> dict:
        params = {"frames": frames, "resolution": resolution, "samples": samples, **kwargs}
        files = {"scene": scene}
        return self.client.submit(self.MODULE_ID, params, files)


class Transcode(_ModuleBase):
    """CR.Transcode — Media transcoding."""
    MODULE_ID = "cr.transcode"

    def submit(self, input_file: str, output_format: str = "webm", resolution: Optional[str] = None, **kwargs) -> dict:
        params = {"output_format": output_format, **kwargs}
        if resolution:
            params["resolution"] = resolution
        files = {"video": input_file}
        return self.client.submit(self.MODULE_ID, params, files)


class OCR(_ModuleBase):
    """CR.OCR — Optical character recognition."""
    MODULE_ID = "cr.ocr"


class Dock(_ModuleBase):
    """CR.Dock — Molecular docking."""
    MODULE_ID = "cr.dock"

    def submit(self, target: str, library: str, scoring_function: str = "vina", exhaustiveness: int = 8, **kwargs) -> dict:
        params = {"scoring_function": scoring_function, "exhaustiveness": exhaustiveness, **kwargs}
        files = {"target": target, "library": library}
        return self.client.submit(self.MODULE_ID, params, files)


class Sweep(_ModuleBase):
    """CR.Sweep — Parameter sweep."""
    MODULE_ID = "cr.sweep"


class Inference(_ModuleBase):
    """CR.Inference — Batch ML inference."""
    MODULE_ID = "cr.inference"


class Embed(_ModuleBase):
    """CR.Embed — Embedding generation."""
    MODULE_ID = "cr.embed"


class Tune(_ModuleBase):
    """CR.Tune — Hyperparameter tuning."""
    MODULE_ID = "cr.tune"


class Eval(_ModuleBase):
    """CR.Eval — Model evaluation."""
    MODULE_ID = "cr.eval"


class Risk(_ModuleBase):
    """CR.Risk — Risk analysis."""
    MODULE_ID = "cr.risk"

    def submit(self, portfolio: str, scenarios: int = 100000, var_confidence: float = 0.99, **kwargs) -> dict:
        params = {"scenarios": scenarios, "var_confidence": var_confidence, **kwargs}
        files = {"portfolio": portfolio}
        return self.client.submit(self.MODULE_ID, params, files)


class Price(_ModuleBase):
    """CR.Price — Option pricing."""
    MODULE_ID = "cr.price"


class Route(_ModuleBase):
    """CR.Route — Route optimisation."""
    MODULE_ID = "cr.route"


# =============================================================================
# Convenience: module registry
# =============================================================================

MODULE_CLASSES: Dict[str, type] = {
    "cr.montecarlo": MonteCarlo,
    "cr.brute": Brute,
    "cr.evolve": Evolve,
    "cr.crunch": Crunch,
    "cr.bootstrap": Bootstrap,
    "cr.replicate": Replicate,
    "cr.classify": Classify,
    "cr.validate": Validate,
    "cr.match": Match,
    "cr.scan": Scan,
    "cr.grade": Grade,
    "cr.render": Render,
    "cr.transcode": Transcode,
    "cr.ocr": OCR,
    "cr.dock": Dock,
    "cr.sweep": Sweep,
    "cr.inference": Inference,
    "cr.embed": Embed,
    "cr.tune": Tune,
    "cr.eval": Eval,
    "cr.risk": Risk,
    "cr.price": Price,
    "cr.route": Route,
}


def get_module(module_id: str, client: CRClient) -> _ModuleBase:
    """Get a module wrapper by ID."""
    cls = MODULE_CLASSES.get(module_id)
    if cls is None:
        raise CRError(f"Unknown module: {module_id}")
    return cls(client)
