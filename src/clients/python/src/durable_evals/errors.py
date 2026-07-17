from __future__ import annotations

import traceback
from typing import Any, Callable

# Failure classes understood by the runtime (see FailureClass in the core). A task's
# class, together with the retry policy, decides whether a failed attempt retries.
TRANSIENT = "transient"
RESOURCE_UNAVAILABLE = "resource_unavailable"
EVAL_EXCEPTION = "eval_exception"
DURABLE_HARNESS_ERROR = "durable_harness_error"
ARTIFACT_ERROR = "artifact_error"


class DurableEvalError(Exception):
    """An error that carries a durability classification.

    ``failure_class`` maps to the runtime's failure classes and ``retryable``, when
    not ``None``, overrides the retry policy for this failure. Raise a subclass (or
    this class with explicit fields) from a task or step to control whether a failed
    attempt is retried; a plain :class:`Exception` is classified as ``eval_exception``
    and left terminal by default.
    """

    failure_class: str = EVAL_EXCEPTION
    retryable: bool | None = None

    def __init__(
        self,
        message: str = "",
        *,
        failure_class: str | None = None,
        retryable: bool | None = None,
    ):
        super().__init__(message)
        if failure_class is not None:
            self.failure_class = failure_class
        if retryable is not None:
            self.retryable = retryable


class TransientError(DurableEvalError):
    """An ephemeral hiccup (network blip, HTTP 500). Retried by default."""

    failure_class = TRANSIENT
    retryable = True


class ResourceUnavailableError(DurableEvalError):
    """A dependency is temporarily down or exhausted. Retried by default."""

    failure_class = RESOURCE_UNAVAILABLE
    retryable = True


class TerminalError(DurableEvalError):
    """A deterministic failure that should never be retried."""

    failure_class = EVAL_EXCEPTION
    retryable = False


class ArtifactError(DurableEvalError):
    """A deterministic artifact failure (hash mismatch, corrupt content). Terminal."""

    failure_class = ARTIFACT_ERROR
    retryable = False


# A classifier maps an exception to a failure class, a retryable flag, both (as a
# ``(failure_class, retryable)`` tuple or a dict), or ``None`` to decline and fall back
# to the exception's own classification.
Classifier = Callable[[BaseException], Any]


def _apply_classifier(
    exc: BaseException, classify: Classifier | None
) -> tuple[str | None, bool | None]:
    if classify is None:
        return None, None
    verdict = classify(exc)
    if verdict is None:
        return None, None
    if isinstance(verdict, str):
        return verdict, None
    if isinstance(verdict, bool):
        return None, verdict
    if isinstance(verdict, tuple):
        failure_class, retryable = (list(verdict) + [None, None])[:2]
        return failure_class, retryable
    if isinstance(verdict, dict):
        return verdict.get("failure_class"), verdict.get("retryable")
    raise TypeError(
        "classify must return a failure class, a bool, a (class, retryable) tuple, "
        "a dict, or None"
    )


def error_info(exc: BaseException, classify: Classifier | None = None) -> dict[str, Any]:
    """Build the runtime error payload for a failed attempt.

    An explicit ``classify`` hook wins, then the exception's own classification (for
    :class:`DurableEvalError`), then the ``eval_exception`` default. ``retryable`` is
    only included when known; otherwise the runtime's retry policy decides.
    """
    failure_class, retryable = _apply_classifier(exc, classify)
    if isinstance(exc, DurableEvalError):
        if failure_class is None:
            failure_class = exc.failure_class
        if retryable is None:
            retryable = exc.retryable
    payload: dict[str, Any] = {
        "error_type": type(exc).__name__,
        "message": str(exc),
        "failure_class": failure_class or EVAL_EXCEPTION,
        "stack": "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
    }
    if retryable is not None:
        payload["retryable"] = retryable
    return payload
