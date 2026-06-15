# Durable Evals TypeScript Client

TypeScript client for Durable Evals.

## Batch Evals

```ts
import { DurableEval } from "durable-evals";

const evalRun = new DurableEval({
  runId: "my-eval",
  config: { model: "local" },
});

const results = await evalRun.batch("inferCases", cases).map({
  run: async (testCase) => model.infer(testCase),
  id: (testCase) => testCase.id, // optional human-readable label
  concurrency: 8,
});
```

Case identity is the SHA-256 digest of the case input, so completed cases are
skipped on rerun and a changed input becomes a new pending case. Duplicate
inputs run once and their output is reused at every position. Failed retryable
cases can be retried, and terminal cases are preserved unless explicitly reset.

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

const trace = evalRun.traceCase("agentCases", { case: testCase });
await trace.modelRequest({ messages: [] });
await trace.modelResponse({ content: "ok" });
await trace.scoringEvent({ score: 1 });

await evalRun.markReviewed({
  batchName: "agentCases",
  case: testCase,
  decision: "reviewed_pass",
  note: "Correct tool sequence",
});
```

`traceCase` and `markReviewed` take exactly one of `case` (digested to the case
id) or `caseId` (an explicit id, by convention the input digest).

```ts

const summary = await evalRun.summary();
const manifestJson = await evalRun.export("manifest_json");
```
