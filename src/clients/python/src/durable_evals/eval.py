from __future__ import annotations

import asyncio
import datetime as _datetime
import hashlib
import inspect
import json
import time
import uuid
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any, Callable, Iterable

from .errors import Classifier, error_info
from .runtime import RuntimeClient

# The lifecycle hooks that bracket each task attempt. `setup` runs before `run`,
# `reset` runs after a failed attempt (to roll back partial side effects), and
# `teardown` always runs once the attempt resolves (to release what `setup` acquired).
_Lifecycle = namedtuple("_Lifecycle", ["setup", "reset", "teardown"])
_LIFECYCLE_ATTR = "_durable_lifecycle"


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


def durable_setup(fn: Callable) -> Callable:
    """Mark a DurableEval method as the per-attempt environment setup hook."""
    setattr(fn, _LIFECYCLE_ATTR, "setup")
    return fn


def durable_reset(fn: Callable) -> Callable:
    """Mark a DurableEval method as the hook that runs after a failed attempt."""
    setattr(fn, _LIFECYCLE_ATTR, "reset")
    return fn


def durable_teardown(fn: Callable) -> Callable:
    """Mark a DurableEval method as the hook that runs when an attempt resolves."""
    setattr(fn, _LIFECYCLE_ATTR, "teardown")
    return fn


def _accepts_ctx(fn: Callable) -> bool:
    """True if `fn` takes a second positional arg (the TaskContext) beyond the task."""
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return False
    positional = 0
    for param in sig.parameters.values():
        if param.kind is inspect.Parameter.VAR_POSITIONAL:
            return True
        if param.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            positional += 1
    return positional >= 2


def _invoke(fn: Callable | None, task: Any, ctx: "TaskContext") -> Any:
    if fn is None:
        return None
    return fn(task, ctx) if _accepts_ctx(fn) else fn(task)


async def _ainvoke(fn: Callable | None, task: Any, ctx: "TaskContext") -> Any:
    if fn is None:
        return None
    result = fn(task, ctx) if _accepts_ctx(fn) else fn(task)
    if inspect.isawaitable(result):
        result = await result
    return result


def _safe_invoke(fn: Callable | None, task: Any, ctx: "TaskContext") -> None:
    # Cleanup hooks are best-effort: a failing reset/teardown must not corrupt the
    # durable record of the attempt's real outcome, so its exception is suppressed.
    if fn is None:
        return
    try:
        _invoke(fn, task, ctx)
    except Exception:
        pass


async def _asafe_invoke(fn: Callable | None, task: Any, ctx: "TaskContext") -> None:
    if fn is None:
        return
    try:
        await _ainvoke(fn, task, ctx)
    except Exception:
        pass


def _collect_lifecycle(instance: Any) -> dict[str, Callable]:
    """Find methods decorated with the lifecycle decorators, subclass overriding base."""
    hooks: dict[str, Callable] = {}
    for klass in reversed(type(instance).__mro__):
        for name, attr in vars(klass).items():
            role = getattr(attr, _LIFECYCLE_ATTR, None)
            if role in ("setup", "reset", "teardown"):
                hooks[role] = getattr(instance, name)
    return hooks


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
        self.worker_id = str(uuid.uuid4())
        self._runtime = runtime or RuntimeClient.ensure_started(Path(storage_dir))
        self._lifecycle = _collect_lifecycle(self)
        if hasattr(self._runtime, "register_run"):
            self._runtime.register_run(
                {"run_id": self.run_id, "name": self.name, "config": self.config}
            )

    def dataset(self, dataset_name: str, tasks: Iterable[Any], **kwargs: Any) -> "Dataset":
        dataset = Dataset(self, dataset_name, list(tasks))
        if kwargs:
            return dataset.map(**kwargs)
        return dataset

    def trace_task(
        self,
        kind: str,
        *,
        task: Any = None,
        task_id: str | None = None,
        attempt: int = 1,
    ) -> "TraceTask":
        return TraceTask(self, kind, _resolve_task_id(task, task_id), attempt)

    def list_traces(
        self,
        *,
        kind: str | None = None,
        task: Any = None,
        task_id: str | None = None,
        event_type: str | Iterable[str] | None = None,
        attempt: int | None = None,
    ) -> list[dict[str, Any]]:
        return self._runtime.list_trace_events(
            self._trace_query(kind, task, task_id, event_type, attempt)
        )

    async def alist_traces(
        self,
        *,
        kind: str | None = None,
        task: Any = None,
        task_id: str | None = None,
        event_type: str | Iterable[str] | None = None,
        attempt: int | None = None,
    ) -> list[dict[str, Any]]:
        return await self._runtime.alist_trace_events(
            self._trace_query(kind, task, task_id, event_type, attempt)
        )

    def _trace_query(
        self,
        kind: str | None,
        task: Any,
        task_id: str | None,
        event_type: str | Iterable[str] | None,
        attempt: int | None,
    ) -> dict[str, Any]:
        if task is not None and task_id is not None:
            raise ValueError("provide at most one of task or task_id")
        resolved_task_id = _json_digest(task) if task is not None else task_id
        if isinstance(event_type, str):
            event_types = [event_type]
        elif event_type is None:
            event_types = []
        else:
            event_types = list(event_type)
        return {
            "run_id": self.run_id,
            "kind": kind,
            "task_id": resolved_task_id,
            "attempt": attempt,
            "event_type": event_types,
        }

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


class TaskContext:
    """Handed to a task's `run`/lifecycle callbacks (when they accept a second arg).

    It exposes a stable idempotency key for tagging external side effects, sequential
    durable memoization for sub-task recovery, and a trace handle scoped to this task.
    """

    def __init__(
        self,
        eval_run: DurableEval,
        kind: str,
        task: Any,
        task_id: str,
        attempt: int,
    ):
        self.eval = eval_run
        self.run_id = eval_run.run_id
        self.kind = kind
        self.task = task
        self.task_id = task_id
        self.attempt = attempt
        self._seq = 0
        self._trace: "TraceTask | None" = None

    @property
    def idempotency_key(self) -> str:
        """A stable key for this task, identical across every retry.

        Derived from `(run_id, kind, task_id)` and deliberately independent of the
        attempt, so an external side effect tagged with it (a container name, an output
        path, an API idempotency header) deduplicates when an attempt is retried.
        """
        return _json_digest(
            {"run_id": self.run_id, "kind": self.kind, "task_id": self.task_id}
        )

    def key(self, *parts: Any) -> str:
        """A stable sub-key derived from the idempotency key for a named resource."""
        return _json_digest({"idempotency_key": self.idempotency_key, "parts": list(parts)})

    @property
    def trace(self) -> "TraceTask":
        if self._trace is None:
            self._trace = TraceTask(self.eval, self.kind, self.task_id, self.attempt)
        return self._trace

    def _next_key(self, name: str | None) -> dict[str, Any]:
        # Keyed by call order within the task, not by attempt, so a retry replays the
        # same sequence and reuses each completed call's stored result.
        self._seq += 1
        return {
            "run_id": self.run_id,
            "kind": self.kind,
            "task_id": self.task_id,
            "seq": self._seq,
            "name": name,
        }

    def run(self, fn: Callable[..., Any], *args: Any, name: str | None = None, **kwargs: Any) -> Any:
        """Durably memoize a synchronous call: a retried task replays it from storage
        instead of re-executing (and re-paying for) it."""
        return self.eval.memo(self._next_key(name), lambda: fn(*args, **kwargs))

    async def arun(
        self, fn: Callable[..., Any], *args: Any, name: str | None = None, **kwargs: Any
    ) -> Any:
        """Async form of :meth:`run` for awaitable calls."""
        return await self.eval.amemo(self._next_key(name), lambda: fn(*args, **kwargs))

    def memo(self, key: Any, fn: Callable[[], Any]) -> Any:
        """Memoize by an explicit key, scoped to this task."""
        return self.eval.memo({"task_id": self.task_id, "kind": self.kind, "key": key}, fn)

    async def amemo(self, key: Any, fn: Callable[[], Any]) -> Any:
        return await self.eval.amemo(
            {"task_id": self.task_id, "kind": self.kind, "key": key}, fn
        )


class Dataset:
    def __init__(self, eval_run: DurableEval, dataset_name: str, tasks: list[Any]):
        self.eval = eval_run
        self.dataset_name = dataset_name
        self.tasks = tasks

    def map(
        self,
        *,
        run: Callable[..., Any],
        id: Callable[[Any], str] | None = None,
        category: Callable[[Any], str] | None = None,
        categories: list[str] | None = None,
        concurrency: int = 1,
        progress: Callable[[dict[str, int]], None] | None = None,
        max_attempts: int = 3,
        retry: dict[str, Any] | None = None,
        classify: Classifier | None = None,
        setup: Callable[..., Any] | None = None,
        reset: Callable[..., Any] | None = None,
        teardown: Callable[..., Any] | None = None,
    ) -> list[Any]:
        # Register the full set first so categories / generation are recorded, then
        # drive every unique task digest through the unified begin/complete/fail path.
        self._register(id, category)
        outputs: list[Any] = [None] * len(self.tasks)
        policy = self._retry_policy(max_attempts, retry)
        hooks = self._hooks(setup, reset, teardown)
        runnable = [
            (indexes, self.tasks[indexes[0]], digest)
            for digest, indexes in self._positions().items()
            if self._in_categories(self.tasks[indexes[0]], category, categories)
        ]

        if concurrency <= 1:
            for item in runnable:
                self._run_one(item, run, outputs, progress, policy, hooks, classify)
        else:
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = [
                    executor.submit(
                        self._run_one, item, run, outputs, progress, policy, hooks, classify
                    )
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
        run: Callable[..., Any],
        id: Callable[[Any], str] | None = None,
        category: Callable[[Any], str] | None = None,
        categories: list[str] | None = None,
        concurrency: int = 10,
        progress: Callable[[dict[str, int]], None] | None = None,
        max_attempts: int = 3,
        retry: dict[str, Any] | None = None,
        classify: Classifier | None = None,
        setup: Callable[..., Any] | None = None,
        reset: Callable[..., Any] | None = None,
        teardown: Callable[..., Any] | None = None,
    ) -> list[Any]:
        await self._aregister(id, category)
        outputs: list[Any] = [None] * len(self.tasks)
        policy = self._retry_policy(max_attempts, retry)
        hooks = self._hooks(setup, reset, teardown)
        semaphore = asyncio.Semaphore(concurrency)

        async def run_one(indexes: list[int], task: Any, digest: str) -> None:
            async with semaphore:
                while True:
                    outcome = await self.eval._runtime.abegin(
                        {
                            "run_id": self.eval.run_id,
                            "kind": self.dataset_name,
                            "input_digest": digest,
                            "retry": policy,
                            "worker_id": self.eval.worker_id,
                        }
                    )
                    kind = outcome["type"]
                    if kind == "skip_completed":
                        for index in indexes:
                            outputs[index] = outcome["output"]
                        return
                    if kind == "retry_later":
                        await _asleep_until(outcome["retry_at"])
                        continue
                    if kind != "execute":
                        # in_progress / failed_terminal: leave positions None.
                        return
                    attempt = outcome["attempt"]
                    ctx = TaskContext(self.eval, self.dataset_name, task, digest, attempt)
                    try:
                        await _ainvoke(hooks.setup, task, ctx)
                        result = await _ainvoke(run, task, ctx)
                    except Exception as exc:
                        # Record, roll back partial state, release the env, then retry
                        # according to the stored retry policy.
                        await self.eval._runtime.afail(
                            {
                                "run_id": self.eval.run_id,
                                "kind": self.dataset_name,
                                "input_digest": digest,
                                "attempt": attempt,
                                "worker_id": self.eval.worker_id,
                                "error": error_info(exc, classify),
                            }
                        )
                        await _asafe_invoke(hooks.reset, task, ctx)
                        await _asafe_invoke(hooks.teardown, task, ctx)
                        continue
                    await self.eval._runtime.acomplete(
                        {
                            "run_id": self.eval.run_id,
                            "kind": self.dataset_name,
                            "input_digest": digest,
                            "attempt": attempt,
                            "worker_id": self.eval.worker_id,
                            "output": result,
                        }
                    )
                    for index in indexes:
                        outputs[index] = result
                    if progress:
                        progress(await self._asummary())
                    await _asafe_invoke(hooks.teardown, task, ctx)
                    return

        tasks = [
            asyncio.create_task(run_one(indexes, self.tasks[indexes[0]], digest))
            for digest, indexes in self._positions().items()
            if self._in_categories(self.tasks[indexes[0]], category, categories)
        ]
        if tasks:
            await asyncio.gather(*tasks)
        return outputs

    def _retry_policy(
        self, max_attempts: int, retry: dict[str, Any] | None
    ) -> dict[str, Any]:
        policy: dict[str, Any] = {"max_attempts": max_attempts}
        if retry:
            policy.update(retry)
        return policy

    def _hooks(
        self,
        setup: Callable[..., Any] | None,
        reset: Callable[..., Any] | None,
        teardown: Callable[..., Any] | None,
    ) -> _Lifecycle:
        # Explicit hooks win; otherwise fall back to methods registered on the eval via
        # the @durable_setup / @durable_reset / @durable_teardown decorators.
        registered = self.eval._lifecycle
        return _Lifecycle(
            setup=setup if setup is not None else registered.get("setup"),
            reset=reset if reset is not None else registered.get("reset"),
            teardown=teardown if teardown is not None else registered.get("teardown"),
        )

    @staticmethod
    def _in_categories(
        task: Any,
        category: Callable[[Any], str] | None,
        categories: list[str] | None,
    ) -> bool:
        # Filter client-side against the category function applied to the
        # representative task for this digest, so we never begin() a task that
        # was filtered out of this run.
        if not categories or category is None:
            return True
        return str(category(task)) in categories

    def summary(self) -> dict[str, int]:
        return self._counts(
            self.eval._runtime.list(
                {"run_id": self.eval.run_id, "kind": self.dataset_name, "statuses": []}
            )
        )

    async def _asummary(self) -> dict[str, int]:
        return self._counts(
            await self.eval._runtime.alist(
                {"run_id": self.eval.run_id, "kind": self.dataset_name, "statuses": []}
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
        return self.eval._runtime.list(
            {"run_id": self.eval.run_id, "kind": self.dataset_name, "statuses": statuses}
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
    ) -> dict[str, Any]:
        return self.eval._runtime.register_dataset(
            {
                "run_id": self.eval.run_id,
                "kind": self.dataset_name,
                "tasks": self._task_payloads(id, category),
            }
        )

    async def _aregister(
        self,
        id: Callable[[Any], str] | None,
        category: Callable[[Any], str] | None,
    ) -> dict[str, Any]:
        return await self.eval._runtime.aregister_dataset(
            {
                "run_id": self.eval.run_id,
                "kind": self.dataset_name,
                "tasks": self._task_payloads(id, category),
            }
        )

    def _run_one(
        self,
        item: tuple[list[int], Any, str],
        run: Callable[..., Any],
        outputs: list[Any],
        progress: Callable[[dict[str, int]], None] | None,
        retry: dict[str, Any],
        hooks: _Lifecycle,
        classify: Classifier | None,
    ) -> None:
        indexes, task, digest = item
        while True:
            outcome = self.eval._runtime.begin(
                {
                    "run_id": self.eval.run_id,
                    "kind": self.dataset_name,
                    "input_digest": digest,
                    "retry": retry,
                    "worker_id": self.eval.worker_id,
                }
            )
            kind = outcome["type"]
            if kind == "skip_completed":
                for index in indexes:
                    outputs[index] = outcome["output"]
                return
            if kind == "retry_later":
                _sleep_until(outcome["retry_at"])
                continue
            if kind != "execute":
                # in_progress / failed_terminal: leave positions None.
                return
            attempt = outcome["attempt"]
            ctx = TaskContext(self.eval, self.dataset_name, task, digest, attempt)
            try:
                _invoke(hooks.setup, task, ctx)
                result = _invoke(run, task, ctx)
            except Exception as exc:
                # Record, roll back partial state, release the env, then retry
                # according to the stored retry policy.
                self.eval._runtime.fail(
                    {
                        "run_id": self.eval.run_id,
                        "kind": self.dataset_name,
                        "input_digest": digest,
                        "attempt": attempt,
                        "worker_id": self.eval.worker_id,
                        "error": error_info(exc, classify),
                    }
                )
                _safe_invoke(hooks.reset, task, ctx)
                _safe_invoke(hooks.teardown, task, ctx)
                continue
            if inspect.isawaitable(result):
                if inspect.iscoroutine(result):
                    result.close()
                raise TypeError("sync dataset callback returned an awaitable")
            self.eval._runtime.complete(
                {
                    "run_id": self.eval.run_id,
                    "kind": self.dataset_name,
                    "input_digest": digest,
                    "attempt": attempt,
                    "worker_id": self.eval.worker_id,
                    "output": result,
                }
            )
            for index in indexes:
                outputs[index] = result
            if progress:
                progress(self.summary())
            _safe_invoke(hooks.teardown, task, ctx)
            return


class TraceTask(AbstractContextManager["TraceTask"]):
    def __init__(self, eval_run: DurableEval, kind: str, task_id: str, attempt: int):
        self.eval = eval_run
        self.kind = kind
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
                "kind": self.kind,
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


def _retry_delay_seconds(retry_at: str) -> float:
    try:
        dt = _datetime.datetime.strptime(retry_at, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=_datetime.timezone.utc
        )
    except ValueError:
        return 0.0
    now = _datetime.datetime.now(_datetime.timezone.utc)
    return max(0.0, (dt - now).total_seconds())


def _sleep_until(retry_at: str) -> None:
    delay = _retry_delay_seconds(retry_at)
    if delay > 0:
        time.sleep(delay)


async def _asleep_until(retry_at: str) -> None:
    delay = _retry_delay_seconds(retry_at)
    if delay > 0:
        await asyncio.sleep(delay)
