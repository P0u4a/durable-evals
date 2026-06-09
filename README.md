# Durable Evals

A durable eval harness for performing long-running evals on agents, recovering gracefully from intermittent or transient errors.

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
    id=lambda case: case["id"],
    run=lambda case: {"case_id": case["id"], "answer": "ok"},
    concurrency=4,
)

print(eval_run.summary())
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

Trace a multi-turn case:

```python
with eval_run.trace_case("browser_tasks", case_id="task-1") as trace:
    trace.model_request({"messages": []})
    trace.tool_call({"name": "browser.click"})
    trace.tool_result({"ok": True})
    trace.termination_event({"reason": "done"})
```

## Node

```ts
import { DurableEval } from "durable-evals";

const evalRun = new DurableEval({
  runId: "bfcl-simple",
  name: "BFCL simple",
  config: { model: "gpt-5.5" },
});

const cases = [{ id: "case-1", prompt: "hello" }];

const results = await evalRun.batch("inferCases", cases).map({
  id: (testCase) => testCase.id,
  run: async (testCase) => ({ caseId: testCase.id, answer: "ok" }),
  concurrency: 4,
});

console.log(await evalRun.summary());
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
const trace = evalRun.traceCase("browserTasks", { caseId: "task-1" });
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
