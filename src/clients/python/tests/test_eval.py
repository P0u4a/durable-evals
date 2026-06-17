import asyncio

import pytest

from durable_evals import DurableEval
from durable_evals.eval import _json_digest


class Runtime:
    def __init__(self):
        self.runs = []
        self.cases = {}
        self.completed = []
        self.failed = []
        self.variants_payload = None
        self.trace_events = []
        self.reviews = []
        self.memos = {}

    def register_run(self, payload):
        self.runs.append(payload)

    def register_batch(self, payload):
        key = (payload["run_id"], payload["batch_name"])
        records = self.cases.setdefault(key, [])
        by_digest = {record["input_digest"]: record for record in records}
        for case in payload["cases"]:
            if case["input_digest"] in by_digest:
                continue
            record = {
                "run_id": payload["run_id"],
                "batch_name": payload["batch_name"],
                "input_digest": case["input_digest"],
                "label": case.get("label"),
                "status": "pending",
                "attempt": 0,
                "input": case["input"],
                "output": None,
                "error": None,
            }
            records.append(record)
            by_digest[case["input_digest"]] = record
        return {"total": len(records)}

    def list_cases(self, payload):
        records = self.cases[(payload["run_id"], payload["batch_name"])]
        statuses = set(payload.get("statuses") or [])
        if statuses:
            return [record for record in records if record["status"] in statuses]
        return records

    def complete_case(self, payload):
        self.completed.append(payload)
        for record in self.cases[(payload["run_id"], payload["batch_name"])]:
            if record["input_digest"] == payload["input_digest"]:
                record["status"] = "succeeded"
                record["output"] = payload["output"]

    def fail_case(self, payload):
        self.failed.append(payload)

    async def aregister_batch(self, payload):
        return self.register_batch(payload)

    async def alist_cases(self, payload):
        return self.list_cases(payload)

    async def asummary(self, payload):
        return self.summary(payload)

    async def acomplete_case(self, payload):
        self.complete_case(payload)

    async def afail_case(self, payload):
        self.fail_case(payload)

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

    def register_variants(self, payload):
        self.variants_payload = payload
        return payload["variants"]

    def register_worker(self, payload):
        return {"worker_id": payload["worker_id"], "resources": payload["resources"]}

    def trace_event(self, payload):
        self.trace_events.append(payload)
        return {**payload, "event_index": len(self.trace_events)}

    def mark_reviewed(self, payload):
        self.reviews.append(payload)
        return payload

    def summary(self, payload):
        return {"run_id": payload["run_id"]}

    def export(self, payload):
        return {"body": "exported"}


def test_batch_map_returns_results_in_input_order():
    runtime = Runtime()
    eval_run = DurableEval(run_id="run", runtime=runtime)
    cases = [{"id": "b"}, {"id": "a"}]

    results = eval_run.batch("cases", cases).map(
        run=lambda case: {"case_id": case["id"]},
        id=lambda case: case["id"],
    )

    assert results == [{"case_id": "b"}, {"case_id": "a"}]
    assert [payload["input_digest"] for payload in runtime.completed] == [
        _json_digest(case) for case in cases
    ]
    assert [record["label"] for record in runtime.cases[("run", "cases")]] == ["b", "a"]


def test_batch_map_works_without_id():
    runtime = Runtime()
    eval_run = DurableEval(run_id="run", runtime=runtime)

    results = eval_run.batch("cases", [{"x": 1}, {"x": 2}]).map(run=lambda case: case["x"])

    assert results == [1, 2]
    assert [record["label"] for record in runtime.cases[("run", "cases")]] == [None, None]


def test_batch_map_runs_duplicate_inputs_once_and_fills_all_positions():
    runtime = Runtime()
    eval_run = DurableEval(run_id="run", runtime=runtime)
    calls = []

    def run(case):
        calls.append(case)
        return case["x"] * 10

    results = eval_run.batch("cases", [{"x": 1}, {"x": 2}, {"x": 1}]).map(run=run)

    assert results == [10, 20, 10]
    assert calls == [{"x": 1}, {"x": 2}]
    assert len(runtime.cases[("run", "cases")]) == 2


def test_batch_amap_runs_duplicate_inputs_once_and_fills_all_positions():
    runtime = Runtime()
    eval_run = DurableEval(run_id="run", runtime=runtime)
    calls = []

    async def run(case):
        calls.append(case)
        return case["x"] * 10

    results = asyncio.run(
        eval_run.batch("cases", [{"x": 1}, {"x": 2}, {"x": 1}]).amap(run=run)
    )

    assert results == [10, 20, 10]
    assert calls == [{"x": 1}, {"x": 2}]


def test_batch_map_resumes_succeeded_cases_without_rerunning():
    runtime = Runtime()
    eval_run = DurableEval(run_id="run", runtime=runtime)
    eval_run.batch("cases", [{"x": 1}, {"x": 2}]).map(run=lambda case: case["x"])

    rerun = []

    def run(case):
        rerun.append(case)
        return case["x"]

    results = eval_run.batch("cases", [{"x": 1}, {"x": 2}]).map(run=run)

    assert results == [1, 2]
    assert rerun == []


def test_batch_records_callback_failures_and_continues():
    runtime = Runtime()
    eval_run = DurableEval(run_id="run", runtime=runtime)

    # A failing case is recorded durably and leaves its slot empty rather than
    # aborting the whole batch.
    results = eval_run.batch("cases", [{"id": "case"}]).map(
        run=lambda _case: (_ for _ in ()).throw(ValueError("bad")),
        max_attempts=5,
    )

    assert results == [None]
    assert runtime.failed[0]["error"]["failure_class"] == "user_code_error"
    assert runtime.failed[0]["input_digest"] == _json_digest({"id": "case"})
    assert runtime.failed[0]["max_attempts"] == 5


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


def test_trace_case_and_mark_reviewed_accept_case_or_case_id():
    runtime = Runtime()
    eval_run = DurableEval(run_id="run", runtime=runtime)
    case = {"x": 1}

    with eval_run.trace_case("cases", case=case) as trace:
        trace.model_request({"messages": []})
    eval_run.mark_reviewed(batch_name="cases", case=case, decision="reviewed_pass")

    assert runtime.trace_events[0]["case_id"] == _json_digest(case)
    assert runtime.reviews[0]["case_id"] == _json_digest(case)

    with pytest.raises(ValueError, match="exactly one"):
        eval_run.trace_case("cases")
    with pytest.raises(ValueError, match="exactly one"):
        eval_run.trace_case("cases", case=case, case_id="x")
    with pytest.raises(ValueError, match="exactly one"):
        eval_run.mark_reviewed(batch_name="cases", decision="reviewed_pass")


def test_variants_trace_review_and_export_helpers_are_thin_runtime_calls():
    runtime = Runtime()
    eval_run = DurableEval(run_id="run", runtime=runtime)

    variants = eval_run.variants("model", [{"name": "a", "config": {"model": "a"}}])
    worker = eval_run.worker(id="w1", resources={"gpu": "local"})
    with eval_run.trace_case("cases", case_id="case") as trace:
        trace.model_request({"messages": []})
    review = eval_run.mark_reviewed(
        batch_name="cases", case_id="case", decision="reviewed_fail", note="wrong"
    )

    assert variants[0]["name"] == "a"
    assert worker.id == "w1"
    assert runtime.trace_events[0]["event_type"] == "model_request"
    assert review["decision"] == "reviewed_fail"
    assert eval_run.summary() == {"run_id": "run"}
    assert eval_run.export() == "exported"
