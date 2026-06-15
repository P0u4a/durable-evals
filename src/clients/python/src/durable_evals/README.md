# Durable Evals Python Client

Python client for Durable Evals.

## Batch Evals

```python
from durable_evals import DurableEval

eval_run = DurableEval(run_id="my-eval", config={"model": "local"})

results = eval_run.batch("infer_cases", cases).map(
    run=lambda case: model.infer(case),
    concurrency=8,
)
```

Case identity is the SHA-256 digest of the JSON-canonicalized input. Duplicate
inputs run once, with the shared output placed at every position. Pass
`id=lambda case: case["id"]` to attach an optional human-readable label.

Completed cases are skipped on rerun. Failed retryable cases can be retried, and
terminal cases are preserved unless explicitly reset.

## Memos

Wrap individual requests (for example LLM calls) in a memo so a crashed
multi-turn case replays earlier calls from storage instead of re-paying for them:

```python
reply = eval_run.memo({"turn": 1, "messages": messages}, lambda: model.chat(messages))
reply = await eval_run.amemo({"turn": 1, "messages": messages}, lambda: model.achat(messages))
```

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

with eval_run.trace_case("agent_cases", case=case) as trace:
    trace.model_request({"messages": []})
    trace.model_response({"content": "ok"})
    trace.scoring_event({"score": 1})

eval_run.mark_reviewed(
    batch_name="agent_cases",
    case=case,
    decision="reviewed_pass",
    note="Correct tool sequence",
)

summary = eval_run.summary()
manifest_json = eval_run.export("manifest_json")
```

`trace_case` and `mark_reviewed` take exactly one of `case=` (digested for you)
or `case_id=` (a precomputed digest or free-form id).
