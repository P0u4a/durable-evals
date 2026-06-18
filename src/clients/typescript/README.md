# Durable Evals TypeScript Client

TypeScript client for Durable Evals.

## Dataset Evals

```ts
import { DurableEval } from "durable-evals";

const evalRun = new DurableEval({
  runId: "my-eval",
  config: { model: "local" },
});

const results = await evalRun.dataset("inferTasks", tasks).map({
  run: async (task) => model.infer(task),
  id: (task) => task.id, // optional human-readable label
  category: (task) => task.category, // optional category
  categories: ["greeting"], // optional: only run these categories
  concurrency: 8,
});
```

Task identity is the SHA-256 digest of the task input, so completed tasks are
skipped on rerun and a changed input becomes a new pending task. Duplicate
inputs run once and their output is reused at every position. Failed retryable
tasks can be retried, and terminal tasks are preserved unless explicitly reset.

## Memos

```ts
const value = await evalRun.memo({ kind: "embedding", text }, async () =>
  model.embed(text),
);
```

Memo keys are any JSON-serializable value. The callback runs once per key
digest within a run; later calls return the stored value.

## Durable Steps

```ts
const prepareData = evalRun.step("prepareData", async () => [
  { id: "task-1" },
]);

const score = evalRun.step(
  "score",
  async (outputs: unknown[]) => ({ total: outputs.length }),
  { retry: { max_attempts: 3 } },
);
```

Callback failures are recorded as workload failures. Serialization, storage, and
completion failures are raised to the caller and are not recorded as task or step
failures.

## Variants, Traces, And Reports

```ts
await evalRun.variants("prompt", [
  { name: "baseline", config: { prompt: "v1" } },
  { name: "candidate", config: { prompt: "v2" } },
]);

const trace = evalRun.traceTask("agentTasks", { task });
await trace.modelRequest({ messages: [] });
await trace.modelResponse({ content: "ok" });
await trace.scoringEvent({ score: 1 });
```

`traceTask` takes exactly one of `task` (digested to the task id) or `taskId`
(an explicit id, by convention the input digest).

```ts

const summary = await evalRun.summary();
const manifestJson = await evalRun.export("manifest_json");
```
