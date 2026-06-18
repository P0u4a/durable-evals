# Durable Evals Python Client

Python client for Durable Evals.

Every unit of work — a dataset task or a durable step — is a single primitive: a
"task" keyed by `(run_id, kind, input_digest)`, where `kind` is the dataset name
or the step name. They share one begin/complete/fail path, so datasets get the
same leasing, single-flight, and retry semantics as steps.

The client auto-spawns the `durable-eval` server (`durable-eval serve`) on first
use unless `DURABLE_EVALS_RUNTIME_URL` is already set (for example when launched
by `durable-eval run`). Override the binary with `DURABLE_EVALS_SERVER_BIN`.

## Dataset Evals

```python
from durable_evals import DurableEval

eval_run = DurableEval(run_id="my-eval", config={"model": "local"})

results = eval_run.dataset("infer_tasks", tasks).map(
    run=lambda task: model.infer(task),
    concurrency=8,
)
```

Task identity is the SHA-256 digest of the JSON-canonicalized input. Duplicate
inputs run once, with the shared output placed at every position. Pass
`id=lambda task: task["id"]` to attach an optional human-readable label, and
`category=lambda task: task["category"]` to tag each task with an optional
category. Restrict a run to selected categories with
`categories=["smoke", "regression"]`; tasks outside that set are left unrun.

Completed tasks are skipped on rerun. Failed retryable tasks can be retried, and
terminal tasks are preserved unless explicitly reset.

## Memos

Wrap individual requests (for example LLM calls) in a memo so a crashed
multi-turn task replays earlier calls from storage instead of re-paying for them:

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
        return [{"id": "task-1"}]

    @step(name="score", retry={"max_attempts": 3})
    def score(self, outputs):
        return {"total": len(outputs)}
```

Callback failures are recorded as workload failures. Serialization, storage, and
completion failures are raised to the caller and are not recorded as task or step
failures.

## Variants, Traces, And Reports

```python
eval_run.variants(
    "prompt",
    [
        {"name": "baseline", "config": {"prompt": "v1"}},
        {"name": "candidate", "config": {"prompt": "v2"}},
    ],
)

with eval_run.trace_task("agent_tasks", task=task) as trace:
    trace.model_request({"messages": []})
    trace.model_response({"content": "ok"})
    trace.scoring_event({"score": 1})

summary = eval_run.summary()
manifest_json = eval_run.export("manifest_json")
```

`trace_task` takes exactly one of `task=` (digested for you) or `task_id=` (a
precomputed digest or free-form id).
