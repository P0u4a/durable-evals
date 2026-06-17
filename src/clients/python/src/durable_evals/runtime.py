from __future__ import annotations

import contextlib
import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any, Iterator

import httpx


class RuntimeClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self._async_client: httpx.AsyncClient | None = None
        token = os.environ.get("DURABLE_EVALS_TOKEN")
        self._headers = {"Authorization": f"Bearer {token}"} if token else {}

    @classmethod
    def ensure_started(cls, storage_dir: Path) -> "RuntimeClient":
        runtime_url = os.environ.get("DURABLE_EVALS_RUNTIME_URL")
        if runtime_url:
            return cls(runtime_url)

        storage_dir.mkdir(parents=True, exist_ok=True)
        metadata_path = storage_dir / "runtime.json"
        existing = cls._healthy_from_metadata(metadata_path)
        if existing is not None:
            return existing

        # Serialize startup so concurrent first-runs don't each spawn a server against
        # the same SQLite file; the winner writes runtime.json, the rest reuse it.
        with _startup_lock(storage_dir):
            existing = cls._healthy_from_metadata(metadata_path)
            if existing is not None:
                return existing
            return cls._spawn(storage_dir, metadata_path)

    @classmethod
    def _healthy_from_metadata(cls, metadata_path: Path) -> "RuntimeClient | None":
        if not metadata_path.exists():
            return None
        try:
            metadata = json.loads(metadata_path.read_text())
        except (OSError, ValueError):
            return None
        url = metadata.get("url")
        if not url:
            return None
        client = cls(url)
        return client if client.is_healthy() else None

    @classmethod
    def _spawn(cls, storage_dir: Path, metadata_path: Path) -> "RuntimeClient":
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
            response = httpx.get(
                f"{self.base_url}/health", headers=self._headers, timeout=0.2
            )
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

    def memo_get(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._post("/memos/get", payload)

    def memo_put(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._post("/memos/put", payload)

    async def abegin_step(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._apost("/steps/begin", payload)

    async def acomplete_step(self, payload: dict[str, Any]) -> None:
        await self._apost("/steps/complete", payload)

    async def afail_step(self, payload: dict[str, Any]) -> None:
        await self._apost("/steps/fail", payload)

    async def aregister_batch(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._apost("/batches/register", payload)

    async def alist_cases(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        return await self._apost("/batches/cases/list", payload)

    async def asummary(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._apost("/runs/summary", payload)

    async def acomplete_case(self, payload: dict[str, Any]) -> None:
        await self._apost("/batches/cases/complete", payload)

    async def afail_case(self, payload: dict[str, Any]) -> None:
        await self._apost("/batches/cases/fail", payload)

    async def amemo_get(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._apost("/memos/get", payload)

    async def amemo_put(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._apost("/memos/put", payload)

    def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        response = httpx.post(
            f"{self.base_url}{path}", json=payload, headers=self._headers, timeout=30
        )
        _raise_for_status(response)
        return response.json()

    async def _apost(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        response = await self._async_http().post(path, json=payload)
        _raise_for_status(response)
        return response.json()

    def _async_http(self) -> httpx.AsyncClient:
        if self._async_client is None or self._async_client.is_closed:
            self._async_client = httpx.AsyncClient(
                base_url=self.base_url, headers=self._headers, timeout=30
            )
        return self._async_client

    async def aclose(self) -> None:
        if self._async_client is not None:
            await self._async_client.aclose()


def _raise_for_status(response: httpx.Response) -> None:
    if response.is_success:
        return
    message = None
    try:
        message = response.json().get("message")
    except Exception:
        message = None
    raise RuntimeError(
        message or f"durable evals request failed with HTTP {response.status_code}"
    )


@contextlib.contextmanager
def _startup_lock(
    storage_dir: Path, timeout: float = 10.0, stale_after: float = 30.0
) -> Iterator[None]:
    """A best-effort cross-process lock backed by atomic directory creation."""
    lock_dir = storage_dir / "runtime.lock"
    deadline = time.monotonic() + timeout
    acquired = False
    while True:
        try:
            os.mkdir(lock_dir)
            acquired = True
            break
        except FileExistsError:
            try:
                age = time.time() - lock_dir.stat().st_mtime
            except OSError:
                age = 0.0
            if age > stale_after:
                # Reclaim a lock orphaned by a crashed process.
                with contextlib.suppress(OSError):
                    os.rmdir(lock_dir)
                continue
            if time.monotonic() > deadline:
                # Proceed without the lock rather than hang the caller forever.
                break
            time.sleep(0.05)
    try:
        yield
    finally:
        if acquired:
            with contextlib.suppress(OSError):
                os.rmdir(lock_dir)
