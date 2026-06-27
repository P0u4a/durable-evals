import asyncio

import pytest

from durable_evals import DurableEval
from durable_evals.eval import _json_digest


class Runtime:
    def __init__(self):
        self.runs = []
        self.tasks = {}
        self.completed = []
        self.failed = []
        self.trace_events = []
        self.memos = {}

    def register_run(self, payload):
        self.runs.append(payload)

    def register_dataset(self, payload):
        key = (payload["run_id"], payload["kind"])
        records = self.tasks.setdefault(key, [])
        by_digest = {record["input_digest"]: record for record in records}
        for task in payload["tasks"]:
            if task["input_digest"] in by_digest:
                continue
            record = {
                "run_id": payload["run_id"],
                "kind": payload["kind"],
                "input_digest": task["input_digest"],
                "label": task.get("label"),
                "category": task.get("category"),
                "status": "pending",
                "attempt": 0,
                "input": task["input"],
                "output": None,
                "error": None,
            }
            records.append(record)
            by_digest[task["input_digest"]] = record
        return {"total": len(records)}

    def _record(self, payload):
        for record in self.tasks[(payload["run_id"], payload["kind"])]:
            if record["input_digest"] == payload["input_digest"]:
                return record
        return None

    def begin(self, payload):
        record = self._record(payload)
        max_attempts = payload.get("retry", {}).get("max_attempts", 2)
        # New/unknown tasks (e.g. steps that were never registered) execute too.
        if record is None or record["status"] in ("pending", "running", "failed"):
            if record is not None and record.get("attempt", 0) >= max_attempts:
                record["status"] = "terminal"
                return {"type": "failed_terminal", "error": record["error"]}
            if record is not None:
                record["status"] = "running"
                record["attempt"] = record.get("attempt", 0) + 1
            return {"type": "execute", "attempt": record["attempt"] if record else 1}
        if record["status"] == "succeeded":
            return {"type": "skip_completed", "output": record["output"]}
        if record["status"] == "terminal":
            return {"type": "failed_terminal", "error": record["error"]}
        return {"type": "execute", "attempt": 1}

    def list(self, payload):
        records = self.tasks[(payload["run_id"], payload["kind"])]
        statuses = set(payload.get("statuses") or [])
        if statuses:
            return [record for record in records if record["status"] in statuses]
        return records

    def complete(self, payload):
        self.completed.append(payload)
        record = self._record(payload)
        if record is not None:
            record["status"] = "succeeded"
            record["output"] = payload["output"]

    def fail(self, payload):
        self.failed.append(payload)
        record = self._record(payload)
        if record is not None:
            record["status"] = "failed"
            record["error"] = payload["error"]

    async def aregister_dataset(self, payload):
        return self.register_dataset(payload)

    async def abegin(self, payload):
        return self.begin(payload)

    async def alist(self, payload):
        return self.list(payload)

    async def asummary(self, payload):
        return self.summary(payload)

    async def acomplete(self, payload):
        self.complete(payload)

    async def afail(self, payload):
        self.fail(payload)

    def memo_get(self, payload):
        key = (payload["run_id"], payload["key_digest"])
        if key in self.memos:
            return {"found": True, "value": self.memos[key]}
        return {"found": False, "value": None}

    def memo_put(self, payload):
        self.memos[(payload["run_id"], payload["key_digest"])] = payload["value"]
        return {"ok": True}

    async def amemo_get(self, payload):
        return self.memo_get(payload)

    async def amemo_put(self, payload):
        return self.memo_put(payload)

    def trace_event(self, payload):
        record = {**payload, "event_index": len(self.trace_events)}
        self.trace_events.append(record)
        return record

    def list_trace_events(self, payload):
        # Mirror the server's filtering: run_id is required, every other field
        # narrows the result, and an empty event_type list means "any".
        kind = payload.get("kind")
        task_id = payload.get("task_id")
        attempt = payload.get("attempt")
        event_types = payload.get("event_type") or []
        return [
            event
            for event in self.trace_events
            if event["run_id"] == payload["run_id"]
            and (kind is None or event["kind"] == kind)
            and (task_id is None or event["task_id"] == task_id)
            and (attempt is None or event["attempt"] == attempt)
            and (not event_types or event["event_type"] in event_types)
        ]

    async def alist_trace_events(self, payload):
        return self.list_trace_events(payload)

    def summary(self, payload):
        return {"run_id": payload["run_id"]}

    def export(self, payload):
        return {"body": "exported"}


def test_dataset_map_returns_results_in_input_order():
    runtime = Runtime()
    eval_run = DurableEval(run_id="run", runtime=runtime)
    tasks = [{"id": "b"}, {"id": "a"}]

    results = eval_run.dataset("tasks", tasks).map(
        run=lambda task: {"task_id": task["id"]},
        id=lambda task: task["id"],
    )

    assert results == [{"task_id": "b"}, {"task_id": "a"}]
    assert [payload["input_digest"] for payload in runtime.completed] == [
        _json_digest(task) for task in tasks
    ]
    assert [record["label"] for record in runtime.tasks[("run", "tasks")]] == ["b", "a"]


def test_dataset_map_works_without_id():
    runtime = Runtime()
    eval_run = DurableEval(run_id="run", runtime=runtime)

    results = eval_run.dataset("tasks", [{"x": 1}, {"x": 2}]).map(run=lambda task: task["x"])

    assert results == [1, 2]
    assert [record["label"] for record in runtime.tasks[("run", "tasks")]] == [None, None]


def test_dataset_map_runs_duplicate_inputs_once_and_fills_all_positions():
    runtime = Runtime()
    eval_run = DurableEval(run_id="run", runtime=runtime)
    calls = []

    def run(task):
        calls.append(task)
        return task["x"] * 10

    results = eval_run.dataset("tasks", [{"x": 1}, {"x": 2}, {"x": 1}]).map(run=run)

    assert results == [10, 20, 10]
    assert calls == [{"x": 1}, {"x": 2}]
    assert len(runtime.tasks[("run", "tasks")]) == 2


def test_dataset_amap_runs_duplicate_inputs_once_and_fills_all_positions():
    runtime = Runtime()
    eval_run = DurableEval(run_id="run", runtime=runtime)
    calls = []

    async def run(task):
        calls.append(task)
        return task["x"] * 10

    results = asyncio.run(
        eval_run.dataset("tasks", [{"x": 1}, {"x": 2}, {"x": 1}]).amap(run=run)
    )

    assert results == [10, 20, 10]
    assert calls == [{"x": 1}, {"x": 2}]


def test_dataset_map_resumes_succeeded_tasks_without_rerunning():
    runtime = Runtime()
    eval_run = DurableEval(run_id="run", runtime=runtime)
    eval_run.dataset("tasks", [{"x": 1}, {"x": 2}]).map(run=lambda task: task["x"])

    rerun = []

    def run(task):
        rerun.append(task)
        return task["x"]

    results = eval_run.dataset("tasks", [{"x": 1}, {"x": 2}]).map(run=run)

    assert results == [1, 2]
    assert rerun == []


def test_dataset_records_callback_failures_and_continues():
    runtime = Runtime()
    eval_run = DurableEval(run_id="run", runtime=runtime)

    # A failing task is recorded durably and leaves its slot empty rather than
    # aborting the whole dataset.
    results = eval_run.dataset("tasks", [{"id": "task"}]).map(
        run=lambda _task: (_ for _ in ()).throw(ValueError("bad")),
        max_attempts=5,
    )

    assert results == [None]
    assert runtime.failed[0]["error"]["failure_class"] == "eval_exception"
    assert runtime.failed[0]["input_digest"] == _json_digest({"id": "task"})
    # fail() no longer carries max_attempts; the retry policy is passed at begin().
    assert "max_attempts" not in runtime.failed[0]


def test_dataset_map_assigns_category_and_filters_by_categories():
    runtime = Runtime()
    eval_run = DurableEval(run_id="run", runtime=runtime)
    ran = []

    results = eval_run.dataset("tasks", [{"x": 1}, {"x": 2}, {"x": 3}]).map(
        run=lambda task: ran.append(task["x"]) or task["x"],
        category=lambda task: "even" if task["x"] % 2 == 0 else "odd",
        categories=["even"],
    )

    # Categories are persisted at registration time for every task.
    assert [record["category"] for record in runtime.tasks[("run", "tasks")]] == [
        "odd",
        "even",
        "odd",
    ]
    # Only the "even" task is actually run; the rest stay pending/empty.
    assert ran == [2]
    assert results == [None, 2, None]


def test_memo_returns_cached_value_without_recomputing():
    runtime = Runtime()
    eval_run = DurableEval(run_id="run", runtime=runtime)
    calls = []

    def fn():
        calls.append(1)
        return {"answer": 42}

    assert eval_run.memo({"prompt": "q"}, fn) == {"answer": 42}
    assert eval_run.memo({"prompt": "q"}, fn) == {"answer": 42}
    assert len(calls) == 1


def test_memo_rejects_awaitable_from_sync_callback():
    runtime = Runtime()
    eval_run = DurableEval(run_id="run", runtime=runtime)

    async def fn():
        return 1

    with pytest.raises(TypeError, match="awaitable"):
        eval_run.memo("key", fn)


def test_amemo_returns_cached_value_without_recomputing():
    runtime = Runtime()
    eval_run = DurableEval(run_id="run", runtime=runtime)
    calls = []

    async def fn():
        calls.append(1)
        return "value"

    async def scenario():
        first = await eval_run.amemo("key", fn)
        second = await eval_run.amemo("key", fn)
        return first, second

    assert asyncio.run(scenario()) == ("value", "value")
    assert len(calls) == 1


def test_trace_task_accepts_task_or_task_id():
    runtime = Runtime()
    eval_run = DurableEval(run_id="run", runtime=runtime)
    task = {"x": 1}

    with eval_run.trace_task("tasks", task=task) as trace:
        trace.model_request({"messages": []})

    assert runtime.trace_events[0]["task_id"] == _json_digest(task)

    with pytest.raises(ValueError, match="exactly one"):
        eval_run.trace_task("tasks")
    with pytest.raises(ValueError, match="exactly one"):
        eval_run.trace_task("tasks", task=task, task_id="x")


def test_list_traces_fetches_all_or_by_task_with_filters():
    runtime = Runtime()
    eval_run = DurableEval(run_id="run", runtime=runtime)
    task = {"x": 1}

    with eval_run.trace_task("tasks", task=task) as first:
        first.model_request({"messages": []})
        first.tool_call({"name": "click"})
    with eval_run.trace_task("tasks", task=task, attempt=2) as second:
        second.model_request({"messages": []})
    with eval_run.trace_task("other", task_id="solo") as other:
        other.scoring_event({"score": 1})

    # Fetch every trace event for the run.
    assert len(eval_run.list_traces()) == 4

    # Fetch a specific task by id (here keyed by the task input digest).
    by_task = eval_run.list_traces(task=task)
    assert [event["event_type"] for event in by_task] == [
        "model_request",
        "tool_call",
        "model_request",
    ]
    assert eval_run.list_traces(task_id="solo") == eval_run.list_traces(kind="other")
    assert eval_run.list_traces(task_id="missing") == []

    # Server-side filters compose: event type, attempt, kind.
    assert len(eval_run.list_traces(event_type="model_request")) == 2
    pair = eval_run.list_traces(
        task=task, event_type=["model_request", "tool_call"], attempt=1
    )
    assert [event["event_type"] for event in pair] == ["model_request", "tool_call"]


def test_list_traces_rejects_task_and_task_id_together():
    eval_run = DurableEval(run_id="run", runtime=Runtime())
    with pytest.raises(ValueError, match="at most one"):
        eval_run.list_traces(task={"x": 1}, task_id="x")


def test_alist_traces_filters_by_event_type():
    runtime = Runtime()
    eval_run = DurableEval(run_id="run", runtime=runtime)

    with eval_run.trace_task("tasks", task_id="task") as trace:
        trace.model_request({"messages": []})
        trace.tool_call({"name": "click"})

    async def scenario():
        return await eval_run.alist_traces(task_id="task", event_type="tool_call")

    events = asyncio.run(scenario())
    assert [event["event_type"] for event in events] == ["tool_call"]


def test_trace_and_export_helpers_are_thin_runtime_calls():
    runtime = Runtime()
    eval_run = DurableEval(run_id="run", runtime=runtime)

    with eval_run.trace_task("tasks", task_id="task") as trace:
        trace.model_request({"messages": []})

    assert runtime.trace_events[0]["event_type"] == "model_request"
    assert eval_run.summary() == {"run_id": "run"}
    assert eval_run.export() == "exported"
