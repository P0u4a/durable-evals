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
        input_digest = input_digest_for(step_name, args, kwargs)

        outcome = self._runtime.begin_step(
            {
                "run_id": self.run_id,
                "step_name": step_name,
                "input_digest": input_digest,
            }
        )
        handle_outcome(step_name, outcome)
        return step_name, input_digest

    async def abegin(
        self: Any, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> tuple[str, str]:
        step_name = f"{fn.__module__}.{fn.__qualname__}"
        input_digest = input_digest_for(step_name, args, kwargs)

        outcome = await self._runtime.abegin_step(
            {
                "run_id": self.run_id,
                "step_name": step_name,
                "input_digest": input_digest,
            }
        )
        handle_outcome(step_name, outcome)
        return step_name, input_digest

    def input_digest_for(
        step_name: str, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> str:
        try:
            input_json = json.dumps({"args": args, "kwargs": kwargs}, sort_keys=True)
        except TypeError as exc:
            raise ValueError(f"step input for {step_name} must be JSON-serializable") from exc
        return hashlib.sha256(input_json.encode("utf-8")).hexdigest()

    def handle_outcome(step_name: str, outcome: dict[str, Any]) -> None:
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

    async def acomplete(
        self: Any, step_name: str, input_digest: str, result: Any
    ) -> None:
        try:
            json.dumps(result)
        except TypeError as exc:
            raise ValueError(f"step output for {step_name} must be JSON-serializable") from exc

        await self._runtime.acomplete_step(
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

    async def afail(
        self: Any, step_name: str, input_digest: str, exc: Exception
    ) -> None:
        await self._runtime.afail_step(
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
        step_name, input_digest = await abegin(self, args, kwargs)

        try:
            result = fn(self, *args, **kwargs)
            if inspect.isawaitable(result):
                result = await result
        except Exception as exc:
            await afail(self, step_name, input_digest, exc)
            raise

        await acomplete(self, step_name, input_digest, result)
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
