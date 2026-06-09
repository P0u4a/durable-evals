# Durable Evals Node Client

Node client for Durable Evals.

## Batch Evals

```ts
import { DurableEval } from "durable-evals";

const evalRun = new DurableEval({
  runId: "my-eval",
  config: { model: "local" },
});

const results = await evalRun.batch("inferCases", cases).map({
  id: (testCase) => testCase.id,
  run: async (testCase) => model.infer(testCase),
  concurrency: 8,
});
```

Completed cases are skipped on rerun. Failed retryable cases can be retried, and
terminal cases are preserved unless explicitly reset.

## Durable Steps

```ts
const prepareData = evalRun.step("prepareData", async () => [
  { id: "case-1" },
]);

const score = evalRun.step(
  "score",
  async (outputs: unknown[]) => ({ total: outputs.length }),
  { retry: { max_attempts: 3 } },
);
```

Callback failures are recorded as workload failures. Serialization, storage, and
completion failures are raised to the caller and are not recorded as case or step
failures.

## Variants, Traces, Reviews, And Reports

```ts
await evalRun.variants("prompt", [
  { name: "baseline", config: { prompt: "v1" } },
  { name: "candidate", config: { prompt: "v2" } },
]);

const trace = evalRun.traceCase("agentCases", { caseId: "case-1" });
await trace.modelRequest({ messages: [] });
await trace.modelResponse({ content: "ok" });
await trace.scoringEvent({ score: 1 });

await evalRun.markReviewed({
  batchName: "agentCases",
  caseId: "case-1",
  decision: "reviewed_pass",
  note: "Correct tool sequence",
});

const summary = await evalRun.summary();
const manifestJson = await evalRun.export("manifest_json");
```
