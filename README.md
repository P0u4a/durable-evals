# Durable Evals

> 🚧 WIP

A durable eval harness for performing long-running evals, recovering gracefully from intermittent or transient errors.

Cases are content-addressed: a case's identity is the hash of its input. Re-running
a script reuses completed outputs, editing an input automatically invalidates just
that case, and reverting the edit restores the original cached output. Identical
inputs collapse to a single case (add e.g. a `sample` field to the input for
repeated sampling). The optional `id` callback only provides a human-readable label.

## Python

```python
from durable_evals import DurableEval


eval_run = DurableEval(
    run_id="bfcl-simple",
    name="BFCL simple",
    config={"model": "gpt-5.5"},
)

cases = [{"id": "case-1", "prompt": "hello"}]

results = eval_run.batch("infer_cases", cases).map(
    id=lambda case: case["id"],  # optional label
    run=lambda case: {"case_id": case["id"], "answer": "ok"},
    concurrency=4,
)

print(eval_run.summary())
```

Wrap individual model calls in `memo` for sub-case recovery: a crashed multi-turn
case replays its earlier calls from storage instead of re-paying for them.

```python
def run_case(case):
    messages = [{"role": "user", "content": case["prompt"]}]
    for turn in range(10):
        response = eval_run.memo(
            {"case": case, "turn": turn, "messages": messages},
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
        return [{"id": "case-1"}]

    @step(
        name="score",
        retry={
            "max_attempts": 3,
            "retryable": ["transient", "resource_unavailable"],
            "terminal": ["terminal_eval"],
        },
    )
    def score(self, results):
        return {"total": len(results)}
```

Trace a multi-turn case (pass the case input; its digest keys the trace):

```python
with eval_run.trace_case("browser_tasks", case=case) as trace:
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

const cases = [{ id: "case-1", prompt: "hello" }];

const results = await evalRun.batch("inferCases", cases).map({
  id: (testCase) => testCase.id, // optional label
  run: async (testCase) => ({ caseId: testCase.id, answer: "ok" }),
  concurrency: 4,
});

console.log(await evalRun.summary());
```

Memoize individual model calls for sub-case recovery:

```ts
const response = await evalRun.memo(
  { case: testCase, turn, messages },
  () => callModel(messages),
);
```

Durable steps:

```ts
const prepareData = evalRun.step("prepareData", async () => [{ id: "case-1" }]);

const score = evalRun.step(
  "score",
  async (results: unknown[]) => ({ total: results.length }),
  {
    retry: {
      max_attempts: 3,
      retryable: ["transient", "resource_unavailable"],
      terminal: ["terminal_eval"],
    },
  },
);
```

Trace a multi-turn case:

```ts
const trace = evalRun.traceCase("browserTasks", { case: testCase });
await trace.modelRequest({ messages: [] });
await trace.toolCall({ name: "browser.click" });
await trace.toolResult({ ok: true });
await trace.terminationEvent({ reason: "done" });
```

## Runtime

Clients use `DURABLE_EVALS_RUNTIME_URL` when it is set. Otherwise they start
`durable-evals-server` automatically and store metadata plus `evals.sqlite`
under `.durable/` by default.

Useful environment variables:

- `DURABLE_EVALS_RUNTIME_URL`: Connect to an existing runtime server.
- `DURABLE_EVALS_SERVER_BIN`: Override the server binary path.
- `DURABLE_EVALS_DB`: Set the SQLite database path for the server.
- `DURABLE_EVALS_ADDR`: Set the server bind address.
