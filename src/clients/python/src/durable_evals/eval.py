from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any, Callable, Iterable

from .runtime import RuntimeClient


def _json_digest(value: Any) -> str:
    try:
        payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    except TypeError as exc:
        raise ValueError("durable eval payloads must be JSON-serializable") from exc
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _resolve_case_id(case: Any, case_id: str | None) -> str:
    if (case is None) == (case_id is None):
        raise ValueError("provide exactly one of case or case_id")
    return case_id if case_id is not None else _json_digest(case)


class DurableEval:
    def __init__(
        self,
        *,
        run_id: str | None = None,
        name: str | None = None,
        config: dict[str, Any] | None = None,
        storage_dir: str | Path = ".durable",
        runtime: RuntimeClient | None = None,
    ):
        self.run_id = run_id or str(uuid.uuid4())
        self.name = name
        self.config = config or {}
        self._runtime = runtime or RuntimeClient.ensure_started(Path(storage_dir))
        if hasattr(self._runtime, "register_run"):
            self._runtime.register_run(
                {"run_id": self.run_id, "name": self.name, "config": self.config}
            )

    def batch(self, batch_name: str, cases: Iterable[Any], **kwargs: Any) -> "Batch":
        batch = Batch(self, batch_name, list(cases))
        if kwargs:
            return batch.map(**kwargs)
        return batch

    def variants(self, dimension: str, variants: list[dict[str, Any]]) -> list[dict[str, Any]]:
        payload = []
        for variant in variants:
            config = variant.get("config", {})
            payload.append(
                {
                    "name": variant["name"],
                    "config": config,
                    "digest": variant.get("digest") or _json_digest(config),
                }
            )
        return self._runtime.register_variants(
            {"run_id": self.run_id, "dimension": dimension, "variants": payload}
        )

    def worker(self, *, id: str, resources: dict[str, Any] | None = None) -> "Worker":
        record = self._runtime.register_worker(
            {"worker_id": id, "resources": resources or {}}
        )
        return Worker(self, record)

    def trace_case(
        self,
        batch_name: str,
        *,
        case: Any = None,
        case_id: str | None = None,
        attempt: int = 1,
    ) -> "TraceCase":
        return TraceCase(self, batch_name, _resolve_case_id(case, case_id), attempt)

    def memo(self, key: Any, fn: Callable[[], Any]) -> Any:
        key_digest = _json_digest(key)
        record = self._runtime.memo_get({"run_id": self.run_id, "key_digest": key_digest})
        if record["found"]:
            return record["value"]
        value = fn()
        if inspect.isawaitable(value):
            if inspect.iscoroutine(value):
                value.close()
            raise TypeError("memo callback returned an awaitable")
        self._runtime.memo_put(
            {"run_id": self.run_id, "key_digest": key_digest, "key": key, "value": value}
        )
        return value

    async def amemo(self, key: Any, fn: Callable[[], Any]) -> Any:
        key_digest = _json_digest(key)
        record = await self._runtime.amemo_get(
            {"run_id": self.run_id, "key_digest": key_digest}
        )
        if record["found"]:
            return record["value"]
        value = fn()
        if inspect.isawaitable(value):
            value = await value
        await self._runtime.amemo_put(
            {"run_id": self.run_id, "key_digest": key_digest, "key": key, "value": value}
        )
        return value

    def summary(self) -> dict[str, Any]:
        return self._runtime.summary({"run_id": self.run_id})

    def export(self, kind: str = "manifest_json") -> str:
        return self._runtime.export({"run_id": self.run_id, "kind": kind})["body"]

    def mark_reviewed(
        self,
        *,
        batch_name: str,
        case: Any = None,
        case_id: str | None = None,
        decision: str,
        reviewer: str = "user",
        note: str | None = None,
    ) -> dict[str, Any]:
        return self._runtime.mark_reviewed(
            {
                "run_id": self.run_id,
                "batch_name": batch_name,
                "case_id": _resolve_case_id(case, case_id),
                "reviewer": reviewer,
                "decision": decision,
                "note": note,
            }
        )


class Batch:
    def __init__(self, eval_run: DurableEval, batch_name: str, cases: list[Any]):
        self.eval = eval_run
        self.batch_name = batch_name
        self.cases = cases

    def map(
        self,
        *,
        run: Callable[[Any], Any],
        id: Callable[[Any], str] | None = None,
        concurrency: int = 1,
        progress: Callable[[dict[str, int]], None] | None = None,
        max_attempts: int = 3,
    ) -> list[Any]:
        records = self._register(id)
        outputs: list[Any] = [None] * len(self.cases)
        runnable: list[tuple[list[int], Any, dict[str, Any]]] = []
        by_digest = {record["input_digest"]: record for record in records}
        for digest, indexes in self._positions().items():
            record = by_digest[digest]
            if record["status"] == "succeeded":
                for index in indexes:
                    outputs[index] = record.get("output")
            elif record["status"] != "terminal":
                runnable.append((indexes, self.cases[indexes[0]], record))

        if concurrency <= 1:
            for item in runnable:
                self._run_one(item, run, outputs, progress, max_attempts)
        else:
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = [
                    executor.submit(self._run_one, item, run, outputs, progress, max_attempts)
                    for item in runnable
                ]
                # Surfacing the first exception would abort sibling cases; a failing case
                # is already recorded durably, so let the batch run to completion.
                for future in as_completed(futures):
                    future.result()
        return outputs

    async def amap(
        self,
        *,
        run: Callable[[Any], Any],
        id: Callable[[Any], str] | None = None,
        concurrency: int = 10,
        progress: Callable[[dict[str, int]], None] | None = None,
        max_attempts: int = 3,
    ) -> list[Any]:
        records = await self._aregister(id)
        outputs: list[Any] = [None] * len(self.cases)
        semaphore = asyncio.Semaphore(concurrency)
        by_digest = {record["input_digest"]: record for record in records}

        async def run_one(indexes: list[int], case: Any, record: dict[str, Any]) -> None:
            async with semaphore:
                try:
                    result = run(case)
                    if inspect.isawaitable(result):
                        result = await result
                except Exception as exc:
                    # Record and move on so one bad case doesn't cancel the batch.
                    await self.eval._runtime.afail_case(
                        {
                            "run_id": self.eval.run_id,
                            "batch_name": self.batch_name,
                            "input_digest": record["input_digest"],
                            "error": _error_payload(exc),
                            "max_attempts": max_attempts,
                        }
                    )
                    return
                await self.eval._runtime.acomplete_case(
                    {
                        "run_id": self.eval.run_id,
                        "batch_name": self.batch_name,
                        "input_digest": record["input_digest"],
                        "output": result,
                    }
                )
                for index in indexes:
                    outputs[index] = result
                if progress:
                    progress(await self._asummary())

        tasks = []
        for digest, indexes in self._positions().items():
            record = by_digest[digest]
            if record["status"] == "succeeded":
                for index in indexes:
                    outputs[index] = record.get("output")
            elif record["status"] != "terminal":
                tasks.append(asyncio.create_task(run_one(indexes, self.cases[indexes[0]], record)))
        if tasks:
            await asyncio.gather(*tasks)
        return outputs

    def summary(self) -> dict[str, int]:
        return self._counts(
            self.eval._runtime.list_cases(
                {"run_id": self.eval.run_id, "batch_name": self.batch_name, "statuses": []}
            )
        )

    async def _asummary(self) -> dict[str, int]:
        return self._counts(
            await self.eval._runtime.alist_cases(
                {"run_id": self.eval.run_id, "batch_name": self.batch_name, "statuses": []}
            )
        )

    @staticmethod
    def _counts(records: list[dict[str, Any]]) -> dict[str, int]:
        counts = {status: 0 for status in ["pending", "running", "succeeded", "failed", "terminal"]}
        for record in records:
            counts[record["status"]] = counts.get(record["status"], 0) + 1
        counts["total"] = len(records)
        return counts

    def failed(self) -> list[dict[str, Any]]:
        return self._cases(["failed"])

    def terminal(self) -> list[dict[str, Any]]:
        return self._cases(["terminal"])

    def missing(self) -> list[dict[str, Any]]:
        return self._cases(["pending"])

    def _cases(self, statuses: list[str]) -> list[dict[str, Any]]:
        return self.eval._runtime.list_cases(
            {"run_id": self.eval.run_id, "batch_name": self.batch_name, "statuses": statuses}
        )

    def _positions(self) -> dict[str, list[int]]:
        positions: dict[str, list[int]] = {}
        for index, case in enumerate(self.cases):
            positions.setdefault(_json_digest(case), []).append(index)
        return positions

    def _case_payloads(self, id: Callable[[Any], str] | None) -> list[dict[str, Any]]:
        return [
            {
                "input_digest": _json_digest(case),
                "input": case,
                "label": str(id(case)) if id else None,
            }
            for case in self.cases
        ]

    def _register(self, id: Callable[[Any], str] | None) -> list[dict[str, Any]]:
        self.eval._runtime.register_batch(
            {
                "run_id": self.eval.run_id,
                "batch_name": self.batch_name,
                "cases": self._case_payloads(id),
            }
        )
        return self.eval._runtime.list_cases(
            {"run_id": self.eval.run_id, "batch_name": self.batch_name, "statuses": []}
        )

    async def _aregister(self, id: Callable[[Any], str] | None) -> list[dict[str, Any]]:
        await self.eval._runtime.aregister_batch(
            {
                "run_id": self.eval.run_id,
                "batch_name": self.batch_name,
                "cases": self._case_payloads(id),
            }
        )
        return await self.eval._runtime.alist_cases(
            {"run_id": self.eval.run_id, "batch_name": self.batch_name, "statuses": []}
        )

    def _run_one(
        self,
        item: tuple[list[int], Any, dict[str, Any]],
        run: Callable[[Any], Any],
        outputs: list[Any],
        progress: Callable[[dict[str, int]], None] | None,
        max_attempts: int,
    ) -> None:
        indexes, case, record = item
        try:
            result = run(case)
        except Exception as exc:
            # Record and move on so one bad case doesn't abort the batch.
            self.eval._runtime.fail_case(
                {
                    "run_id": self.eval.run_id,
                    "batch_name": self.batch_name,
                    "input_digest": record["input_digest"],
                    "error": _error_payload(exc),
                    "max_attempts": max_attempts,
                }
            )
            return
        if inspect.isawaitable(result):
            if inspect.iscoroutine(result):
                result.close()
            raise TypeError("sync batch callback returned an awaitable")
        self.eval._runtime.complete_case(
            {
                "run_id": self.eval.run_id,
                "batch_name": self.batch_name,
                "input_digest": record["input_digest"],
                "output": result,
            }
        )
        for index in indexes:
            outputs[index] = result
        if progress:
            progress(self.summary())


class Worker:
    def __init__(self, eval_run: DurableEval, record: dict[str, Any]):
        self.eval = eval_run
        self.record = record
        self.id = record["worker_id"]


class TraceCase(AbstractContextManager["TraceCase"]):
    def __init__(self, eval_run: DurableEval, batch_name: str, case_id: str, attempt: int):
        self.eval = eval_run
        self.batch_name = batch_name
        self.case_id = case_id
        self.attempt = attempt

    def event(
        self,
        event_type: str,
        payload: Any = None,
        *,
        artifact_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        return self.eval._runtime.trace_event(
            {
                "run_id": self.eval.run_id,
                "batch_name": self.batch_name,
                "case_id": self.case_id,
                "attempt": self.attempt,
                "event_type": event_type,
                "payload": payload,
                "artifact_ids": artifact_ids or [],
            }
        )

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        return None

    def model_request(self, payload: Any) -> dict[str, Any]:
        return self.event("model_request", payload)

    def model_response(self, payload: Any) -> dict[str, Any]:
        return self.event("model_response", payload)

    def tool_call(self, payload: Any) -> dict[str, Any]:
        return self.event("tool_call", payload)

    def tool_result(self, payload: Any) -> dict[str, Any]:
        return self.event("tool_result", payload)

    def state_snapshot(self, payload: Any, artifact_ids: list[str] | None = None) -> dict[str, Any]:
        return self.event("state_snapshot", payload, artifact_ids=artifact_ids)

    def scoring_event(self, payload: Any) -> dict[str, Any]:
        return self.event("scoring_event", payload)

    def termination_event(self, payload: Any) -> dict[str, Any]:
        return self.event("termination_event", payload)


def _error_payload(exc: Exception) -> dict[str, Any]:
    return {
        "error_type": type(exc).__name__,
        "message": str(exc),
        "failure_class": "user_code_error",
        "retryable": True,
    }
