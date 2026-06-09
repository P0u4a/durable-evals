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
        self._async_client: httpx.AsyncClient | None = None

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

    def heartbeat_step(self, payload: dict[str, Any]) -> None:
        self._post("/steps/heartbeat", payload)

    def register_run(self, payload: dict[str, Any]) -> None:
        self._post("/runs/register", payload)

    def summary(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._post("/runs/summary", payload)

    def export(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._post("/runs/export", payload)

    def register_batch(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._post("/batches/register", payload)

    def list_cases(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        return self._post("/batches/cases/list", payload)

    def complete_case(self, payload: dict[str, Any]) -> None:
        self._post("/batches/cases/complete", payload)

    def fail_case(self, payload: dict[str, Any]) -> None:
        self._post("/batches/cases/fail", payload)

    def register_variants(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        return self._post("/variants/register", payload)

    def register_worker(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._post("/workers/register", payload)

    def trace_event(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._post("/traces/events", payload)

    def mark_reviewed(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._post("/reviews", payload)

    async def abegin_step(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._apost("/steps/begin", payload)

    async def acomplete_step(self, payload: dict[str, Any]) -> None:
        await self._apost("/steps/complete", payload)

    async def afail_step(self, payload: dict[str, Any]) -> None:
        await self._apost("/steps/fail", payload)

    async def aregister_batch(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._apost("/batches/register", payload)

    async def acomplete_case(self, payload: dict[str, Any]) -> None:
        await self._apost("/batches/cases/complete", payload)

    async def afail_case(self, payload: dict[str, Any]) -> None:
        await self._apost("/batches/cases/fail", payload)

    def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        response = httpx.post(f"{self.base_url}{path}", json=payload, timeout=30)
        response.raise_for_status()
        return response.json()

    async def _apost(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        response = await self._async_http().post(path, json=payload)
        response.raise_for_status()
        return response.json()

    def _async_http(self) -> httpx.AsyncClient:
        if self._async_client is None or self._async_client.is_closed:
            self._async_client = httpx.AsyncClient(base_url=self.base_url, timeout=30)
        return self._async_client

    async def aclose(self) -> None:
        if self._async_client is not None:
            await self._async_client.aclose()
