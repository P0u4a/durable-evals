from __future__ import annotations

import functools
import hashlib
import inspect
import json
from typing import Any, Callable, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


class DurableStepFailed(RuntimeError):
    pass


def step(fn: F) -> F:
    @functools.wraps(fn)
    async def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        step_name = f"{fn.__module__}.{fn.__qualname__}"
        input_json = json.dumps({"args": args, "kwargs": kwargs}, sort_keys=True)
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
