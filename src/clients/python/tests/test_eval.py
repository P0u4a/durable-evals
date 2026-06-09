import pytest

from durable_evals import DurableEval


class Runtime:
    def __init__(self):
        self.runs = []
        self.cases = {}
        self.completed = []
        self.failed = []
        self.variants_payload = None
        self.trace_events = []
        self.reviews = []

    def register_run(self, payload):
        self.runs.append(payload)

    def register_batch(self, payload):
        key = (payload["run_id"], payload["batch_name"])
        self.cases[key] = [
            {
                **case,
                "run_id": payload["run_id"],
                "batch_name": payload["batch_name"],
                "status": "pending",
                "attempt": 0,
                "output": None,
                "error": None,
            }
            for case in payload["cases"]
        ]
        return {"total": len(payload["cases"]), "pending": len(payload["cases"])}

    def list_cases(self, payload):
        records = self.cases[(payload["run_id"], payload["batch_name"])]
        statuses = set(payload.get("statuses") or [])
        if statuses:
            return [record for record in records if record["status"] in statuses]
        return records

    def complete_case(self, payload):
        self.completed.append(payload)
        for record in self.cases[(payload["run_id"], payload["batch_name"])]:
            if record["case_id"] == payload["case_id"]:
                record["status"] = "succeeded"
                record["output"] = payload["output"]

    def fail_case(self, payload):
        self.failed.append(payload)

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

    results = eval_run.batch("cases", [{"id": "b"}, {"id": "a"}]).map(
        id=lambda case: case["id"],
        run=lambda case: {"case_id": case["id"]},
    )

    assert results == [{"case_id": "b"}, {"case_id": "a"}]
    assert [payload["case_id"] for payload in runtime.completed] == ["b", "a"]


def test_batch_records_callback_failures():
    runtime = Runtime()
    eval_run = DurableEval(run_id="run", runtime=runtime)

    with pytest.raises(ValueError, match="bad"):
        eval_run.batch("cases", [{"id": "case"}]).map(
            id=lambda case: case["id"],
            run=lambda _case: (_ for _ in ()).throw(ValueError("bad")),
        )

    assert runtime.failed[0]["error"]["failure_class"] == "user_code_error"


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
