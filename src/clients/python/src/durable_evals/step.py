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
    @functools.wraps(fn)
    async def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
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

        try:
            result = fn(self, *args, **kwargs)
            if inspect.isawaitable(result):
                result = await result
        except Exception as exc:
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
            raise

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
        return result

    return wrapper
