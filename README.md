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
durable-eval run harness.ts -- --only math   # args after the harness are forwarded to it
```

`durable-eval run` starts the runtime, points your harness's client at it, streams its
output, and exits with its status. Re-running resumes from where the last run left off.
Harness language is detected by extension (`.py` → Python, `.js`/`.mjs` → Node,
`.ts` → Node via `tsx`).

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

Wrap individual model calls in `memo` for sub-task recovery: a crashed multi-turn
task replays its earlier calls from storage instead of re-paying for them.

```python
def run_task(task):
    messages = [{"role": "user", "content": task["prompt"]}]
    for turn in range(10):
        response = eval_run.memo(
            {"task": task, "turn": turn, "messages": messages},
            lambda: call_model(messages),
        )
        messages = advance(messages, response)
    return messages
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

## TypeScript

```ts
import { DurableEval } from "durable-evals";

const evalRun = new DurableEval({
  runId: "bfcl-simple",
  name: "BFCL simple",
  config: { model: "gpt-5.5" },
});

const tasks = [{ id: "task-1", prompt: "hello", category: "greeting" }];

const results = await evalRun.dataset("inferTasks", tasks).map({
  id: (task) => task.id, // optional label
  category: (task) => task.category, // optional category
  categories: ["greeting"], // optional: only run these categories
  run: async (task) => ({ taskId: task.id, answer: "ok" }),
  concurrency: 4,
});

console.log(await evalRun.summary());
```

Memoize individual model calls for sub-task recovery:

```ts
const response = await evalRun.memo(
  { task, turn, messages },
  () => callModel(messages),
);
```

Durable steps:

```ts
const prepareData = evalRun.step("prepareData", async () => [{ id: "task-1" }]);

const score = evalRun.step(
  "score",
  async (results: unknown[]) => ({ total: results.length }),
  {
    retry: {
      max_attempts: 3,
      retryable: ["transient", "resource_unavailable"],
    },
  },
);
```

Trace a multi-turn task:

```ts
const trace = evalRun.traceTask("browserTasks", { task });
await trace.modelRequest({ messages: [] });
await trace.toolCall({ name: "browser.click" });
await trace.toolResult({ ok: true });
await trace.terminationEvent({ reason: "done" });
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
- `DURABLE_EVALS_TOKEN`: Require `Authorization: Bearer <token>` on every
  request except `/health`. Clients read the same variable and attach the
  header automatically.

### Security

The server has no authentication by default and binds to `127.0.0.1:0`, so it is
only reachable locally. If you point `DURABLE_EVALS_ADDR` at a non-loopback
address, also set `DURABLE_EVALS_TOKEN` (and front it with TLS) — otherwise the
mutating API is exposed unauthenticated.
