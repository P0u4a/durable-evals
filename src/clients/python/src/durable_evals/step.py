from __future__ import annotations

import functools
import hashlib
import inspect
import json
from typing import Any, Callable, TypeVar

F = TypeVar("F", bound=Callable[..., Any])
BeginResult = tuple[str, str, str] | tuple[str, Any]


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
                raise DurableStepInProgress(f"step retry is scheduled at {outcome['retry_at']}: {kind}")
            case "in_progress":
                raise DurableStepInProgress(f"step is already running: {kind}")
            case "execute":
                return "execute", kind, input_digest
            case other:
                raise RuntimeError(f"unexpected step outcome: {other}")

    def complete(self: Any, kind: str, input_digest: str, result: Any) -> None:
        try:
            json.dumps(result)
        except TypeError as exc:
            raise ValueError(f"step output for {kind} must be JSON-serializable") from exc

        self._runtime.complete(
            {
                "run_id": self.run_id,
                "kind": kind,
                "input_digest": input_digest,
                "output": result,
            }
        )

    async def acomplete(
        self: Any, kind: str, input_digest: str, result: Any
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
                "output": result,
            }
        )

    def fail(self: Any, kind: str, input_digest: str, exc: Exception) -> None:
        self._runtime.fail(
            {
                "run_id": self.run_id,
                "kind": kind,
                "input_digest": input_digest,
                "error": {
                    "error_type": type(exc).__name__,
                    "message": str(exc),
                    "failure_class": "eval_exception",
                    "retryable": True,
                },
            }
        )

    async def afail(
        self: Any, kind: str, input_digest: str, exc: Exception
    ) -> None:
        await self._runtime.afail(
            {
                "run_id": self.run_id,
                "kind": kind,
                "input_digest": input_digest,
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
        begin_result = await abegin(self, args, kwargs)
        if begin_result[0] == "skip":
            return begin_result[1]
        _, kind, input_digest = begin_result

        try:
            result = fn(self, *args, **kwargs)
            if inspect.isawaitable(result):
                result = await result
        except Exception as exc:
            await afail(self, kind, input_digest, exc)
            raise

        await acomplete(self, kind, input_digest, result)
        return result

    @functools.wraps(fn)
    def sync_wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        begin_result = begin(self, args, kwargs)
        if begin_result[0] == "skip":
            return begin_result[1]
        _, kind, input_digest = begin_result

        try:
            result = fn(self, *args, **kwargs)
        except Exception as exc:
            fail(self, kind, input_digest, exc)
            raise

        if inspect.isawaitable(result):
            if inspect.iscoroutine(result):
                result.close()
            raise TypeError(f"sync step returned an awaitable: {kind}")

        complete(self, kind, input_digest, result)
        return result

    if inspect.iscoroutinefunction(fn):
        return async_wrapper  # type: ignore[return-value]
    return sync_wrapper  # type: ignore[return-value]
