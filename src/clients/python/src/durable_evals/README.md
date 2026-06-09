# Durable Evals Python Client

Python client for Durable Evals.

## Batch Evals

```python
from durable_evals import DurableEval

eval_run = DurableEval(run_id="my-eval", config={"model": "local"})

results = eval_run.batch("infer_cases", cases).map(
    id=lambda case: case["id"],
    run=lambda case: model.infer(case),
    concurrency=8,
)
```

Completed cases are skipped on rerun. Failed retryable cases can be retried, and
terminal cases are preserved unless explicitly reset.

## Durable Steps

```python
from durable_evals import DurableEval, step


class MyEval(DurableEval):
    @step(name="prepare_data")
    def prepare_data(self):
        return [{"id": "case-1"}]

    @step(name="score", retry={"max_attempts": 3})
    def score(self, outputs):
        return {"total": len(outputs)}
```

Callback failures are recorded as workload failures. Serialization, storage, and
completion failures are raised to the caller and are not recorded as case or step
failures.

## Variants, Traces, Reviews, And Reports

```python
eval_run.variants(
    "prompt",
    [
        {"name": "baseline", "config": {"prompt": "v1"}},
        {"name": "candidate", "config": {"prompt": "v2"}},
    ],
)

with eval_run.trace_case("agent_cases", case_id="case-1") as trace:
    trace.model_request({"messages": []})
    trace.model_response({"content": "ok"})
    trace.scoring_event({"score": 1})

eval_run.mark_reviewed(
    batch_name="agent_cases",
    case_id="case-1",
    decision="reviewed_pass",
    note="Correct tool sequence",
)

summary = eval_run.summary()
manifest_json = eval_run.export("manifest_json")
```
