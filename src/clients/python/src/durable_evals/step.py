from __future__ import annotations

import functools
import hashlib
import inspect
import json
from typing import Any, Callable, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


class DurableStepFailed(RuntimeError):
    pass


class DurableStepInProgress(RuntimeError):
    pass


def step(fn: F) -> F:
    def begin(self: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> tuple[str, str]:
        step_name = f"{fn.__module__}.{fn.__qualname__}"
        try:
            input_json = json.dumps({"args": args, "kwargs": kwargs}, sort_keys=True)
        except TypeError as exc:
            raise ValueError(f"step input for {step_name} must be JSON-serializable") from exc
        input_digest = hashlib.sha256(input_json.encode("utf-8")).hexdigest()

        outcome = self._runtime.begin_step(
            {
                "run_id": self.run_id,
                "step_name": step_name,
                "input_digest": input_digest,
            }
        )

        match outcome["type"]:
            case "skip_completed":
                return outcome["output"]
            case "failed_terminal":
                error = outcome["error"]
                raise DurableStepFailed(f"{error['error_type']}: {error['message']}")
            case "in_progress":
                raise DurableStepInProgress(f"step is already running: {step_name}")
            case "execute":
                pass
            case other:
                raise RuntimeError(f"unexpected step outcome: {other}")

        return step_name, input_digest

    def complete(self: Any, step_name: str, input_digest: str, result: Any) -> None:
        try:
            json.dumps(result)
        except TypeError as exc:
            raise ValueError(f"step output for {step_name} must be JSON-serializable") from exc

        self._runtime.complete_step(
            {
                "run_id": self.run_id,
                "step_name": step_name,
                "input_digest": input_digest,
                "output": result,
            }
        )

    def fail(self: Any, step_name: str, input_digest: str, exc: Exception) -> None:
        self._runtime.fail_step(
            {
                "run_id": self.run_id,
                "step_name": step_name,
                "input_digest": input_digest,
                "error": {
                    "error_type": type(exc).__name__,
                    "message": str(exc),
                },
            }
        )

    @functools.wraps(fn)
    async def async_wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        step_name, input_digest = begin(self, args, kwargs)

        try:
            result = fn(self, *args, **kwargs)
            if inspect.isawaitable(result):
                result = await result
        except Exception as exc:
            fail(self, step_name, input_digest, exc)
            raise

        complete(self, step_name, input_digest, result)
        return result

    @functools.wraps(fn)
    def sync_wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        step_name, input_digest = begin(self, args, kwargs)

        try:
            result = fn(self, *args, **kwargs)
        except Exception as exc:
            fail(self, step_name, input_digest, exc)
            raise

        if inspect.isawaitable(result):
            if inspect.iscoroutine(result):
                result.close()
            raise TypeError(f"sync step returned an awaitable: {step_name}")

        complete(self, step_name, input_digest, result)
        return result

    if inspect.iscoroutinefunction(fn):
        return async_wrapper  # type: ignore[return-value]
    return sync_wrapper  # type: ignore[return-value]
