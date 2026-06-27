from __future__ import annotations

import asyncio
import datetime as _datetime
import functools
import hashlib
import inspect
import json
import time
from typing import Any, Callable, TypeVar

F = TypeVar("F", bound=Callable[..., Any])
BeginResult = tuple[str, str] | tuple[str, Any] | tuple[str, str, str, int]


class DurableStepFailed(RuntimeError):
    pass


class DurableStepInProgress(RuntimeError):
    pass


def step(fn: F | None = None, *, name: str | None = None, retry: dict[str, Any] | None = None) -> F:
    if fn is None:
        return lambda wrapped: step(wrapped, name=name, retry=retry)  # type: ignore[return-value]

    def begin(self: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> BeginResult:
        kind = name or f"{fn.__module__}.{fn.__qualname__}"
        input_digest = input_digest_for(kind, args, kwargs)

        outcome = self._runtime.begin(
            {
                "run_id": self.run_id,
                "kind": kind,
                "input_digest": input_digest,
                "retry": retry or {},
                "worker_id": getattr(self, "worker_id", None),
            }
        )
        return handle_outcome(kind, input_digest, outcome)

    async def abegin(
        self: Any, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> BeginResult:
        kind = name or f"{fn.__module__}.{fn.__qualname__}"
        input_digest = input_digest_for(kind, args, kwargs)

        outcome = await self._runtime.abegin(
            {
                "run_id": self.run_id,
                "kind": kind,
                "input_digest": input_digest,
                "retry": retry or {},
                "worker_id": getattr(self, "worker_id", None),
            }
        )
        return handle_outcome(kind, input_digest, outcome)

    def input_digest_for(
        kind: str, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> str:
        try:
            input_json = json.dumps(
                {"args": args, "kwargs": kwargs},
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            )
        except TypeError as exc:
            raise ValueError(f"step input for {kind} must be JSON-serializable") from exc
        return hashlib.sha256(input_json.encode("utf-8")).hexdigest()

    def handle_outcome(
        kind: str, input_digest: str, outcome: dict[str, Any]
    ) -> BeginResult:
        match outcome["type"]:
            case "skip_completed":
                return "skip", outcome["output"]
            case "failed_terminal":
                error = outcome["error"]
                raise DurableStepFailed(f"{error['error_type']}: {error['message']}")
            case "retry_later":
                return "retry_later", outcome["retry_at"]
            case "in_progress":
                raise DurableStepInProgress(f"step is already running: {kind}")
            case "execute":
                return "execute", kind, input_digest, outcome["attempt"]
            case other:
                raise RuntimeError(f"unexpected step outcome: {other}")

    def complete(self: Any, kind: str, input_digest: str, attempt: int, result: Any) -> None:
        try:
            json.dumps(result)
        except TypeError as exc:
            raise ValueError(f"step output for {kind} must be JSON-serializable") from exc

        self._runtime.complete(
            {
                "run_id": self.run_id,
                "kind": kind,
                "input_digest": input_digest,
                "attempt": attempt,
                "worker_id": getattr(self, "worker_id", None),
                "output": result,
            }
        )

    async def acomplete(
        self: Any, kind: str, input_digest: str, attempt: int, result: Any
    ) -> None:
        try:
            json.dumps(result)
        except TypeError as exc:
            raise ValueError(f"step output for {kind} must be JSON-serializable") from exc

        await self._runtime.acomplete(
            {
                "run_id": self.run_id,
                "kind": kind,
                "input_digest": input_digest,
                "attempt": attempt,
                "worker_id": getattr(self, "worker_id", None),
                "output": result,
            }
        )

    def fail(self: Any, kind: str, input_digest: str, attempt: int, exc: Exception) -> None:
        self._runtime.fail(
            {
                "run_id": self.run_id,
                "kind": kind,
                "input_digest": input_digest,
                "attempt": attempt,
                "worker_id": getattr(self, "worker_id", None),
                "error": {
                    "error_type": type(exc).__name__,
                    "message": str(exc),
                    "failure_class": "eval_exception",
                    "retryable": True,
                },
            }
        )

    async def afail(
        self: Any, kind: str, input_digest: str, attempt: int, exc: Exception
    ) -> None:
        await self._runtime.afail(
            {
                "run_id": self.run_id,
                "kind": kind,
                "input_digest": input_digest,
                "attempt": attempt,
                "worker_id": getattr(self, "worker_id", None),
                "error": {
                    "error_type": type(exc).__name__,
                    "message": str(exc),
                    "failure_class": "eval_exception",
                    "retryable": True,
                },
            }
        )

    @functools.wraps(fn)
    async def async_wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        while True:
            begin_result = await abegin(self, args, kwargs)
            if begin_result[0] == "skip":
                return begin_result[1]
            if begin_result[0] == "retry_later":
                await _asleep_until(begin_result[1])
                continue
            _, kind, input_digest, attempt = begin_result

            try:
                result = fn(self, *args, **kwargs)
                if inspect.isawaitable(result):
                    result = await result
            except Exception as exc:
                await afail(self, kind, input_digest, attempt, exc)
                continue

            await acomplete(self, kind, input_digest, attempt, result)
            return result

    @functools.wraps(fn)
    def sync_wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        while True:
            begin_result = begin(self, args, kwargs)
            if begin_result[0] == "skip":
                return begin_result[1]
            if begin_result[0] == "retry_later":
                _sleep_until(begin_result[1])
                continue
            _, kind, input_digest, attempt = begin_result

            try:
                result = fn(self, *args, **kwargs)
            except Exception as exc:
                fail(self, kind, input_digest, attempt, exc)
                continue

            if inspect.isawaitable(result):
                if inspect.iscoroutine(result):
                    result.close()
                raise TypeError(f"sync step returned an awaitable: {kind}")

            complete(self, kind, input_digest, attempt, result)
            return result

    if inspect.iscoroutinefunction(fn):
        return async_wrapper  # type: ignore[return-value]
    return sync_wrapper  # type: ignore[return-value]


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
