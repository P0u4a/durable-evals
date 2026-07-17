# Durable Evals

> 🚧 WIP

A durable eval harness for performing long-running evals, recovering gracefully from intermittent or transient errors.

## What this is

A problem when running evals on agents is if there's a upstream error during the eval, like an API request that returns a 500, or a write to a file that fails, the process hangs, and now you need to restart the whole eval. When cost is a concern, restarting the eval is expensive (api costs etc.).

The goal of this library is to alleviate some of this pain by providing a mechanism for resuming evals from where they left off, and for automatically retrying upon transient errors.

## How it works

A task is a single datapoint in the eval. Tasks are content-addressed: a task's identity is the hash of its input. Re-running a script reuses completed outputs, editing an input automatically invalidates just that task, and reverting the edit restores the original cached output. Identical
inputs collapse to a single task (add e.g. a `sample` field to the input for repeated sampling). The optional `id` callback only provides a human-readable label.

A dataset is a collection of tasks. Tasks may carry an optional `category`, so you can filter a run to only certain categories.

A step is a single unit of work that is part of an eval's method. For example, scoring is a step.

Under the hood there is just one primitive: a content-addressed **task** keyed by
`(run_id, kind, input_digest)`, where `kind` is a step name or a dataset name. A step
is a group of one; a dataset is a pre-registered group. Both resume and retry the same way.

## Running an eval

Write your eval as a normal script (a "harness") using the SDK, then run it with the CLI:

```
durable-eval run harness.py        # runs your eval, manages the runtime, resumes on rerun
durable-eval run harness.py --fresh # ignore the cache and start over
durable-eval run harness.py -- --only math   # args after the harness are forwarded to it
```

`durable-eval run` starts the runtime, points your harness's client at it, streams its
output, and exits with its status. Re-running resumes from where the last run left off.

## Python

```python
from durable_evals import DurableEval


eval_run = DurableEval(
    run_id="bfcl-simple",
    name="BFCL simple",
    config={"model": "gpt-5.5"},
)

tasks = [{"id": "task-1", "prompt": "hello", "category": "greeting"}]

results = eval_run.dataset("infer_tasks", tasks).map(
    id=lambda task: task["id"],  # optional label
    category=lambda task: task["category"],  # optional category
    run=lambda task: {"task_id": task["id"], "answer": "ok"},
    concurrency=4,
)

print(eval_run.summary())
```

Pass `categories=[...]` to `map` to run only tasks in those categories:

```python
results = eval_run.dataset("infer_tasks", tasks).map(
    run=run_task,
    category=lambda task: task["category"],
    categories=["greeting"],  # only run these categories
)
```

### The task context

If your `run` callback takes a second argument, it receives a `TaskContext` (`ctx`)
that makes durability cheap to opt into without restructuring your task:

```python
def run_task(task, ctx):
    messages = [{"role": "user", "content": task["prompt"]}]
    for turn in range(10):
        # ctx.run memoizes each call by its order in the task, so a crashed multi-turn
        # task replays its earlier calls from storage instead of re-paying for them.
        response = ctx.run(call_model, messages)
        messages = advance(messages, response)
    return {"key": ctx.idempotency_key, "messages": messages}
```

`ctx.idempotency_key` is a stable key for the task, **identical across every retry**
and unique per task. Tag external side effects with it — a container name, an output
path, an API idempotency header — so a retried attempt deduplicates instead of
duplicating work. Derive per-resource sub-keys with `ctx.key("container")`.

`ctx.run(fn, *args)` (and `ctx.arun` for awaitables) is the non-invasive form of
`eval_run.memo(key, fn)`: swap `call_model(x)` for `ctx.run(call_model, x)` and each
call becomes durable, keyed by its position in the task. `ctx.trace` gives a trace
handle already scoped to the task.

### Environment lifecycle

Provision and clean up a task's environment with lifecycle hooks. `setup` runs before
each attempt, `reset` runs after a failed attempt (to roll back partial side effects),
and `teardown` runs once the attempt resolves (to release what `setup` acquired). Pass
them to `map`, or register them on a `DurableEval` subclass with decorators:

```python
from durable_evals import durable_setup, durable_reset, durable_teardown

class MyEval(DurableEval):
    @durable_setup
    def provision(self, task, ctx):
        start_container(name=ctx.idempotency_key)  # idempotent: reused across retries

    @durable_reset
    def rollback(self, task, ctx):
        discard_partial_writes(ctx.idempotency_key)

    @durable_teardown
    def release(self, task, ctx):
        stop_container(name=ctx.idempotency_key)
```

### Failure classification

A failed attempt is retried only if its failure class is retryable under the retry
policy. A plain exception is classified `eval_exception` and is **terminal by
default**. A deterministic bug is not re-run.


Signal a retryable failure by raising a typed error, or classify errors with a hook:

```python
from durable_evals import TransientError, ResourceUnavailableError

def run_task(task, ctx):
    try:
        return call_model(task["prompt"])
    except RateLimitError as exc:
        raise TransientError(str(exc))  # retried per the policy

results = eval_run.dataset("infer_tasks", tasks).map(
    run=run_task,
    classify=lambda exc: "transient" if isinstance(exc, IOError) else None,
    retry={"max_attempts": 5, "retryable": ["transient", "resource_unavailable"]},
)
```

Durable steps are useful for shared setup, scoring, and aggregation:

```python
class MyEval(DurableEval):
    @step(name="prepare_data")
    def prepare_data(self):
        return [{"id": "task-1"}]

    @step(
        name="score",
        retry={
            "max_attempts": 3,
            "retryable": ["transient", "resource_unavailable"],
        },
    )
    def score(self, results):
        return {"total": len(results)}
```

Trace a multi-turn task (pass the task input; its digest keys the trace):

```python
with eval_run.trace_task("browser_tasks", task=task) as trace:
    trace.model_request({"messages": []})
    trace.tool_call({"name": "browser.click"})
    trace.tool_result({"ok": True})
    trace.termination_event({"reason": "done"})
```

Read traces back. `list_traces` returns every trace event for the run by default. You can filter by any combination of `kind`, `task`/`task_id`,
`event_type` (a single type or a list), and `attempt`:

```python
all_events = eval_run.list_traces()                       # every event for the run
task_events = eval_run.list_traces(task=task)             # one task by id
model_calls = eval_run.list_traces(
    kind="browser_tasks", event_type=["model_request", "model_response"]
)
```

## Runtime

`durable-eval run` manages the runtime for you. If you run a harness directly instead,
clients use `DURABLE_EVALS_RUNTIME_URL` when it is set, and otherwise auto-spawn
`durable-eval serve`, storing metadata plus `evals.sqlite` under `.durable/` by default.

Useful environment variables:

- `DURABLE_EVALS_RUNTIME_URL`: Connect to an existing runtime server.
- `DURABLE_EVALS_SERVER_BIN`: Override the `durable-eval` binary path used for auto-spawn.
- `DURABLE_EVALS_DB`: Set the SQLite database path for the server.
- `DURABLE_EVALS_ADDR`: Set the server bind address.
