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


def _resolve_task_id(task: Any, task_id: str | None) -> str:
    if (task is None) == (task_id is None):
        raise ValueError("provide exactly one of task or task_id")
    return task_id if task_id is not None else _json_digest(task)


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

    def dataset(self, dataset_name: str, tasks: Iterable[Any], **kwargs: Any) -> "Dataset":
        dataset = Dataset(self, dataset_name, list(tasks))
        if kwargs:
            return dataset.map(**kwargs)
        return dataset

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

    def trace_task(
        self,
        dataset_name: str,
        *,
        task: Any = None,
        task_id: str | None = None,
        attempt: int = 1,
    ) -> "TraceTask":
        return TraceTask(self, dataset_name, _resolve_task_id(task, task_id), attempt)

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


class Dataset:
    def __init__(self, eval_run: DurableEval, dataset_name: str, tasks: list[Any]):
        self.eval = eval_run
        self.dataset_name = dataset_name
        self.tasks = tasks

    def map(
        self,
        *,
        run: Callable[[Any], Any],
        id: Callable[[Any], str] | None = None,
        category: Callable[[Any], str] | None = None,
        categories: list[str] | None = None,
        concurrency: int = 1,
        progress: Callable[[dict[str, int]], None] | None = None,
        max_attempts: int = 3,
    ) -> list[Any]:
        records = self._register(id, category)
        outputs: list[Any] = [None] * len(self.tasks)
        runnable: list[tuple[list[int], Any, dict[str, Any]]] = []
        by_digest = {record["input_digest"]: record for record in records}
        for digest, indexes in self._positions().items():
            record = by_digest[digest]
            if record["status"] == "succeeded":
                for index in indexes:
                    outputs[index] = record.get("output")
            elif record["status"] != "terminal" and self._in_categories(record, categories):
                runnable.append((indexes, self.tasks[indexes[0]], record))

        if concurrency <= 1:
            for item in runnable:
                self._run_one(item, run, outputs, progress, max_attempts)
        else:
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = [
                    executor.submit(self._run_one, item, run, outputs, progress, max_attempts)
                    for item in runnable
                ]
                # Surfacing the first exception would abort sibling tasks; a failing task
                # is already recorded durably, so let the dataset run to completion.
                for future in as_completed(futures):
                    future.result()
        return outputs

    async def amap(
        self,
        *,
        run: Callable[[Any], Any],
        id: Callable[[Any], str] | None = None,
        category: Callable[[Any], str] | None = None,
        categories: list[str] | None = None,
        concurrency: int = 10,
        progress: Callable[[dict[str, int]], None] | None = None,
        max_attempts: int = 3,
    ) -> list[Any]:
        records = await self._aregister(id, category)
        outputs: list[Any] = [None] * len(self.tasks)
        semaphore = asyncio.Semaphore(concurrency)
        by_digest = {record["input_digest"]: record for record in records}

        async def run_one(indexes: list[int], task: Any, record: dict[str, Any]) -> None:
            async with semaphore:
                try:
                    result = run(task)
                    if inspect.isawaitable(result):
                        result = await result
                except Exception as exc:
                    # Record and move on so one bad task doesn't cancel the dataset.
                    await self.eval._runtime.afail_task(
                        {
                            "run_id": self.eval.run_id,
                            "dataset_name": self.dataset_name,
                            "input_digest": record["input_digest"],
                            "error": _error_payload(exc),
                            "max_attempts": max_attempts,
                        }
                    )
                    return
                await self.eval._runtime.acomplete_task(
                    {
                        "run_id": self.eval.run_id,
                        "dataset_name": self.dataset_name,
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
            elif record["status"] != "terminal" and self._in_categories(record, categories):
                tasks.append(asyncio.create_task(run_one(indexes, self.tasks[indexes[0]], record)))
        if tasks:
            await asyncio.gather(*tasks)
        return outputs

    @staticmethod
    def _in_categories(record: dict[str, Any], categories: list[str] | None) -> bool:
        if not categories:
            return True
        return record.get("category") in categories

    def summary(self) -> dict[str, int]:
        return self._counts(
            self.eval._runtime.list_tasks(
                {"run_id": self.eval.run_id, "dataset_name": self.dataset_name, "statuses": []}
            )
        )

    async def _asummary(self) -> dict[str, int]:
        return self._counts(
            await self.eval._runtime.alist_tasks(
                {"run_id": self.eval.run_id, "dataset_name": self.dataset_name, "statuses": []}
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
        return self._tasks(["failed"])

    def terminal(self) -> list[dict[str, Any]]:
        return self._tasks(["terminal"])

    def missing(self) -> list[dict[str, Any]]:
        return self._tasks(["pending"])

    def _tasks(self, statuses: list[str]) -> list[dict[str, Any]]:
        return self.eval._runtime.list_tasks(
            {"run_id": self.eval.run_id, "dataset_name": self.dataset_name, "statuses": statuses}
        )

    def _positions(self) -> dict[str, list[int]]:
        positions: dict[str, list[int]] = {}
        for index, task in enumerate(self.tasks):
            positions.setdefault(_json_digest(task), []).append(index)
        return positions

    def _task_payloads(
        self,
        id: Callable[[Any], str] | None,
        category: Callable[[Any], str] | None,
    ) -> list[dict[str, Any]]:
        payloads = []
        for task in self.tasks:
            payload = {
                "input_digest": _json_digest(task),
                "input": task,
                "label": str(id(task)) if id else None,
            }
            if category is not None:
                payload["category"] = str(category(task))
            payloads.append(payload)
        return payloads

    def _register(
        self,
        id: Callable[[Any], str] | None,
        category: Callable[[Any], str] | None,
    ) -> list[dict[str, Any]]:
        self.eval._runtime.register_dataset(
            {
                "run_id": self.eval.run_id,
                "dataset_name": self.dataset_name,
                "tasks": self._task_payloads(id, category),
            }
        )
        return self.eval._runtime.list_tasks(
            {"run_id": self.eval.run_id, "dataset_name": self.dataset_name, "statuses": []}
        )

    async def _aregister(
        self,
        id: Callable[[Any], str] | None,
        category: Callable[[Any], str] | None,
    ) -> list[dict[str, Any]]:
        await self.eval._runtime.aregister_dataset(
            {
                "run_id": self.eval.run_id,
                "dataset_name": self.dataset_name,
                "tasks": self._task_payloads(id, category),
            }
        )
        return await self.eval._runtime.alist_tasks(
            {"run_id": self.eval.run_id, "dataset_name": self.dataset_name, "statuses": []}
        )

    def _run_one(
        self,
        item: tuple[list[int], Any, dict[str, Any]],
        run: Callable[[Any], Any],
        outputs: list[Any],
        progress: Callable[[dict[str, int]], None] | None,
        max_attempts: int,
    ) -> None:
        indexes, task, record = item
        try:
            result = run(task)
        except Exception as exc:
            # Record and move on so one bad task doesn't abort the dataset.
            self.eval._runtime.fail_task(
                {
                    "run_id": self.eval.run_id,
                    "dataset_name": self.dataset_name,
                    "input_digest": record["input_digest"],
                    "error": _error_payload(exc),
                    "max_attempts": max_attempts,
                }
            )
            return
        if inspect.isawaitable(result):
            if inspect.iscoroutine(result):
                result.close()
            raise TypeError("sync dataset callback returned an awaitable")
        self.eval._runtime.complete_task(
            {
                "run_id": self.eval.run_id,
                "dataset_name": self.dataset_name,
                "input_digest": record["input_digest"],
                "output": result,
            }
        )
        for index in indexes:
            outputs[index] = result
        if progress:
            progress(self.summary())


class TraceTask(AbstractContextManager["TraceTask"]):
    def __init__(self, eval_run: DurableEval, dataset_name: str, task_id: str, attempt: int):
        self.eval = eval_run
        self.dataset_name = dataset_name
        self.task_id = task_id
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
                "dataset_name": self.dataset_name,
                "task_id": self.task_id,
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
        "failure_class": "eval_exception",
        "retryable": True,
    }
