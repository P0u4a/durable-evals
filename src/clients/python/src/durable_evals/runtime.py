from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any

import httpx


class RuntimeClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    @classmethod
    def ensure_started(cls, storage_dir: Path) -> "RuntimeClient":
        runtime_url = os.environ.get("DURABLE_EVALS_RUNTIME_URL")
        if runtime_url:
            return cls(runtime_url)

        storage_dir.mkdir(parents=True, exist_ok=True)
        metadata_path = storage_dir / "runtime.json"
        if metadata_path.exists():
            metadata = json.loads(metadata_path.read_text())
            client = cls(metadata["url"])
            if client.is_healthy():
                return client

        server_bin = os.environ.get("DURABLE_EVALS_SERVER_BIN", "durable-evals-server")
        db_path = storage_dir / "evals.sqlite"
        env = {**os.environ, "DURABLE_EVALS_DB": str(db_path)}
        process = subprocess.Popen(
            [server_bin],
            env=env,
            stdout=subprocess.PIPE,
            stderr=(storage_dir / "server.log").open("ab"),
            text=True,
        )
        if process.stdout is None:
            raise RuntimeError("durable evals server did not expose stdout")

        addr = process.stdout.readline().strip()
        if not addr:
            raise RuntimeError("durable evals server did not print a listening address")

        client = cls(f"http://{addr}")
        client.wait_until_healthy()
        metadata_path.write_text(json.dumps({"url": client.base_url, "pid": process.pid}))
        return client

    def is_healthy(self) -> bool:
        try:
            response = httpx.get(f"{self.base_url}/health", timeout=0.2)
            return response.status_code == 200 and response.json().get("ok") is True
        except httpx.HTTPError:
            return False

    def wait_until_healthy(self) -> None:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if self.is_healthy():
                return
            time.sleep(0.05)
        raise RuntimeError("durable evals server did not become healthy")

    def begin_step(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._post("/steps/begin", payload)

    def complete_step(self, payload: dict[str, Any]) -> None:
        self._post("/steps/complete", payload)

    def fail_step(self, payload: dict[str, Any]) -> None:
        self._post("/steps/fail", payload)

    def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        response = httpx.post(f"{self.base_url}{path}", json=payload, timeout=30)
        response.raise_for_status()
        return response.json()
