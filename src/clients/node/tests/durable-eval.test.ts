import assert from "node:assert/strict";
import test from "node:test";

import {
  DurableEval,
  DurableStepFailed,
  DurableStepInProgress,
  type Runtime as DurableRuntime,
  type StepOutcome,
} from "../dist/index.js";

type Payload = Record<string, unknown>;

class Runtime implements DurableRuntime {
  outcome: StepOutcome;
  began: Payload[] = [];
  completed: Payload[] = [];
  failed: Payload[] = [];

  constructor(outcome: StepOutcome = { type: "execute", attempt: 1 }) {
    this.outcome = outcome;
  }

  async beginStep(payload: Payload): Promise<StepOutcome> {
    this.began.push(payload);
    return this.outcome;
  }

  async completeStep(payload: Payload): Promise<void> {
    this.completed.push(payload);
  }

  async failStep(payload: Payload): Promise<void> {
    this.failed.push(payload);
  }
}

test("returns a durable callback that completes a step", async () => {
  const runtime = new Runtime();
  const evalRun = new DurableEval({ runId: "run", runtime });

  const runAgent = evalRun.step(
    "runAgent",
    async (testCase: { id: string }) => ({
      caseId: testCase.id,
    }),
  );

  assert.deepEqual(await runAgent({ id: "case-1" }), {
    caseId: "case-1",
  });
  assert.equal(runtime.completed.length, 1);
  assert.equal(runtime.completed[0].step_name, "runAgent");
  assert.deepEqual(runtime.completed[0].output, {
    caseId: "case-1",
  });
});

test("returns cached output without calling callback", async () => {
  const runtime = new Runtime({
    type: "skip_completed",
    output: { value: "cached" },
  });
  const evalRun = new DurableEval({ runId: "run", runtime });

  const cached = evalRun.step("cached", (_input: string) => {
    throw new Error("should not run");
  });

  assert.deepEqual(await cached("input"), { value: "cached" });
  assert.equal(runtime.completed.length, 0);
});

test("raises for in-progress steps", async () => {
  const runtime = new Runtime({ type: "in_progress" });
  const evalRun = new DurableEval({ runId: "run", runtime });
  const busy = evalRun.step("busy", () => ({ ok: true }));

  await assert.rejects(busy(), DurableStepInProgress);
});

test("raises terminal failures", async () => {
  const runtime = new Runtime({
    type: "failed_terminal",
    error: { error_type: "ValueError", message: "bad input" },
  });
  const evalRun = new DurableEval({ runId: "run", runtime });
  const failed = evalRun.step("failed", () => ({ ok: true }));

  await assert.rejects(failed(), DurableStepFailed);
});

test("records callback failures", async () => {
  const runtime = new Runtime();
  const evalRun = new DurableEval({ runId: "run", runtime });
  const explode = evalRun.step("explode", () => {
    throw new TypeError("boom");
  });

  await assert.rejects(explode(), /boom/);
  assert.equal(runtime.failed.length, 1);
  assert.deepEqual(runtime.failed[0].error, {
    error_type: "TypeError",
    message: "boom",
  });
});

test("supports user-owned orchestration", async () => {
  const runtime = new Runtime();
  const evalRun = new DurableEval({ runId: "run", runtime });
  const fetchCases = evalRun.step("fetchCases", async () => [
    { id: "case-1" },
  ]);
  const runAgent = evalRun.step(
    "runAgent",
    async (testCase: { id: string }) => ({
      caseId: testCase.id,
      answer: "ok",
    }),
  );

  const cases = await fetchCases();
  const results: Array<{ caseId: string; answer: string }> = [];
  for (const testCase of cases) {
    results.push(await runAgent(testCase));
  }

  assert.deepEqual(results, [{ caseId: "case-1", answer: "ok" }]);
  assert.deepEqual(
    runtime.completed.map((payload) => payload.step_name),
    ["fetchCases", "runAgent"],
  );
});
