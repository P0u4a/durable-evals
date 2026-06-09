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
  cases = new Map<string, Payload[]>();
  variantsPayload: Payload | null = null;
  traceEvents: Payload[] = [];
  reviews: Payload[] = [];
  completeError: Error | null = null;

  constructor(outcome: StepOutcome = { type: "execute", attempt: 1 }) {
    this.outcome = outcome;
  }

  async beginStep(payload: Payload): Promise<StepOutcome> {
    this.began.push(payload);
    return this.outcome;
  }

  async completeStep(payload: Payload): Promise<void> {
    if (this.completeError !== null) {
      throw this.completeError;
    }
    this.completed.push(payload);
  }

  async failStep(payload: Payload): Promise<void> {
    this.failed.push(payload);
  }

  async registerRun(_payload: Payload): Promise<void> {}

  async registerBatch(payload: Payload): Promise<Payload> {
    const key = `${payload.run_id}:${payload.batch_name}`;
    this.cases.set(
      key,
      (payload.cases as Payload[]).map((testCase) => ({
        ...testCase,
        status: "pending",
        output: null,
      })),
    );
    return { total: (payload.cases as Payload[]).length };
  }

  async listCases(payload: Payload): Promise<Payload[]> {
    const records = this.cases.get(`${payload.run_id}:${payload.batch_name}`) ?? [];
    const statuses = new Set((payload.statuses as string[]) ?? []);
    return statuses.size === 0
      ? records
      : records.filter((record) => statuses.has(String(record.status)));
  }

  async completeCase(payload: Payload): Promise<void> {
    this.completed.push(payload);
    const records = this.cases.get(`${payload.run_id}:${payload.batch_name}`) ?? [];
    const record = records.find((item) => item.case_id === payload.case_id);
    if (record) {
      record.status = "succeeded";
      record.output = payload.output;
    }
  }

  async failCase(payload: Payload): Promise<void> {
    this.failed.push(payload);
  }

  async registerVariants(payload: Payload): Promise<Payload[]> {
    this.variantsPayload = payload;
    return payload.variants as Payload[];
  }

  async registerWorker(payload: Payload): Promise<Payload> {
    return { worker_id: payload.worker_id, resources: payload.resources };
  }

  async traceEvent(payload: Payload): Promise<Payload> {
    this.traceEvents.push(payload);
    return { ...payload, event_index: this.traceEvents.length };
  }

  async markReviewed(payload: Payload): Promise<Payload> {
    this.reviews.push(payload);
    return payload;
  }

  async summary(payload: Payload): Promise<Payload> {
    return { run_id: payload.run_id };
  }

  async export(_payload: Payload): Promise<{ body: string }> {
    return { body: "exported" };
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
    failure_class: "user_code_error",
    retryable: true,
  });
});

test("does not record output serialization failures as step failures", async () => {
  const runtime = new Runtime();
  const evalRun = new DurableEval({ runId: "run", runtime });
  const badOutput = evalRun.step("badOutput", () => ({
    value: BigInt(1),
  }));

  await assert.rejects(
    badOutput(),
    /step output for badOutput must be JSON-serializable/,
  );
  assert.equal(runtime.failed.length, 0);
  assert.equal(runtime.completed.length, 0);
});

test("does not record completion failures as step failures", async () => {
  const runtime = new Runtime();
  runtime.completeError = new Error("completion failed");
  const evalRun = new DurableEval({ runId: "run", runtime });
  const step = evalRun.step("completeFails", () => ({ ok: true }));

  await assert.rejects(step(), /completion failed/);
  assert.equal(runtime.failed.length, 0);
  assert.equal(runtime.completed.length, 0);
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

test("batch maps cases durably in input order", async () => {
  const runtime = new Runtime();
  const evalRun = new DurableEval({ runId: "run", runtime });

  const results = await evalRun.batch("cases", [{ id: "b" }, { id: "a" }]).map({
    id: (testCase) => testCase.id,
    run: async (testCase) => ({ caseId: testCase.id }),
  });

  assert.deepEqual(results, [{ caseId: "b" }, { caseId: "a" }]);
  assert.deepEqual(
    runtime.completed.slice(-2).map((payload) => payload.case_id),
    ["b", "a"],
  );
});

test("batch records callback failures", async () => {
  const runtime = new Runtime();
  const evalRun = new DurableEval({ runId: "run", runtime });

  await assert.rejects(
    evalRun.batch("cases", [{ id: "case" }]).map({
      id: (testCase) => testCase.id,
      run: () => {
        throw new TypeError("bad");
      },
    }),
    /bad/,
  );

  assert.equal(
    (runtime.failed.at(-1)?.error as { failure_class?: string }).failure_class,
    "user_code_error",
  );
});

test("variants trace worker review summary and export helpers call runtime", async () => {
  const runtime = new Runtime();
  const evalRun = new DurableEval({ runId: "run", runtime });

  const variants = await evalRun.variants("model", [
    { name: "a", config: { model: "a" } },
  ]);
  const worker = await evalRun.worker({ id: "w1", resources: { gpu: "local" } });
  await evalRun.traceCase("cases", { caseId: "case" }).modelRequest({ messages: [] });
  const review = await evalRun.markReviewed({
    batchName: "cases",
    caseId: "case",
    decision: "reviewed_fail",
    note: "wrong",
  });

  assert.equal(variants[0].name, "a");
  assert.equal(worker.id, "w1");
  assert.equal(runtime.traceEvents[0].event_type, "model_request");
  assert.equal(review.decision, "reviewed_fail");
  assert.deepEqual(await evalRun.summary(), { run_id: "run" });
  assert.equal(await evalRun.export(), "exported");
});
