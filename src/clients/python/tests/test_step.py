import asyncio
import inspect

import pytest

from durable_evals.step import DurableStepInProgress, step


class Runtime:
    def __init__(self, outcome=None):
        self.outcome = outcome or {"type": "execute", "attempt": 1}
        self.async_began = False
        self.async_completed = False
        self.async_failed = False
        self.completed = []
        self.failed = []

    def begin_step(self, payload):
        self.began = payload
        return self.outcome

    def complete_step(self, payload):
        self.completed.append(payload)

    def fail_step(self, payload):
        self.failed.append(payload)

    async def abegin_step(self, payload):
        self.async_began = True
        self.began = payload
        return self.outcome

    async def acomplete_step(self, payload):
        self.async_completed = True
        self.completed.append(payload)

    async def afail_step(self, payload):
        self.async_failed = True
        self.failed.append(payload)


class Eval:
    def __init__(self, runtime=None):
        self.run_id = "run"
        self._runtime = runtime or Runtime()

    @step
    def sync_step(self, value):
        return {"value": value}

    @step
    async def async_step(self, value):
        return {"value": value}


def test_sync_step_remains_sync():
    eval = Eval()

    result = eval.sync_step(1)

    assert not inspect.isawaitable(result)
    assert result == {"value": 1}
    assert eval._runtime.completed[0]["output"] == {"value": 1}


def test_async_step_remains_async():
    eval = Eval()

    result = eval.async_step(1)
    assert inspect.isawaitable(result)

    assert asyncio.run(result) == {"value": 1}
    assert eval._runtime.async_began is True
    assert eval._runtime.async_completed is True
    assert eval._runtime.completed[0]["output"] == {"value": 1}


def test_in_progress_raises_for_sync_step():
    eval = Eval(Runtime({"type": "in_progress"}))

    with pytest.raises(DurableStepInProgress):
        eval.sync_step(1)


def test_sync_step_returns_cached_output():
    eval = Eval(Runtime({"type": "skip_completed", "output": {"value": "cached"}}))

    assert eval.sync_step(1) == {"value": "cached"}
    assert eval._runtime.completed == []


def test_async_step_returns_cached_output():
    eval = Eval(Runtime({"type": "skip_completed", "output": {"value": "cached"}}))

    assert asyncio.run(eval.async_step(1)) == {"value": "cached"}
    assert eval._runtime.completed == []


def test_sync_step_rejects_awaitable_result():
    class BadEval(Eval):
        @step
        def bad_step(self):
            async def inner():
                return {"ok": True}

            return inner()

    with pytest.raises(TypeError, match="sync step returned an awaitable"):
        BadEval().bad_step()
