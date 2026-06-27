import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";

import {
  DurableEval,
  DurableStepFailed,
  DurableStepInProgress,
  type Runtime as DurableRuntime,
  type Outcome,
} from "../dist/index.js";

type Payload = Record<string, unknown>;

function digestOf(value: unknown): string {
  return createHash("sha256").update(JSON.stringify(value), "utf8").digest("hex");
}

class Runtime implements DurableRuntime {
  outcome: Outcome | null;
  began: Payload[] = [];
  completed: Payload[] = [];
  failed: Payload[] = [];
  tasks = new Map<string, Payload[]>();
  attempts = new Map<string, number>();
  errors = new Map<string, Payload>();
  memos = new Map<string, unknown>();
  memoPuts: Payload[] = [];
  traceEvents: Payload[] = [];
  completeError: Error | null = null;

  // When an explicit outcome is provided (step tests), begin always returns it.
  // Otherwise begin derives the outcome from the unified task store: an already
  // succeeded task -> skip_completed, anything else -> execute.
  constructor(outcome: Outcome | null = null) {
    this.outcome = outcome;
  }

  private recordFor(payload: Payload): Payload | undefined {
    const records = this.tasks.get(`${payload.run_id}:${payload.kind}`) ?? [];
    return records.find((item) => item.input_digest === payload.input_digest);
  }

  async begin(payload: Payload): Promise<Outcome> {
    this.began.push(payload);
    if (this.outcome !== null) {
      return this.outcome;
    }
    const record = this.recordFor(payload);
    const retry = (payload.retry ?? {}) as { max_attempts?: number };
    const maxAttempts = retry.max_attempts ?? 2;
    if (record && record.status === "succeeded") {
      return { type: "skip_completed", output: record.output };
    }
    if (record && record.status === "terminal") {
      return {
        type: "failed_terminal",
        error: { error_type: "Terminal", message: "terminal" },
      };
    }
    const key = `${payload.run_id}:${payload.kind}:${payload.input_digest}`;
    const attempt = Number(record?.attempt ?? this.attempts.get(key) ?? 0);
    if (attempt >= maxAttempts) {
      if (record) {
        record.status = "terminal";
      }
      const error = this.errors.get(key);
      return {
        type: "failed_terminal",
        error: {
          error_type: String(error?.error_type ?? "MaxAttemptsExceeded"),
          message: String(error?.message ?? "terminal"),
        },
      };
    }
    const nextAttempt = attempt + 1;
    if (record) {
      record.status = "running";
      record.attempt = nextAttempt;
    }
    this.attempts.set(key, nextAttempt);
    return { type: "execute", attempt: nextAttempt };
  }

  async complete(payload: Payload): Promise<void> {
    if (this.completeError !== null) {
      throw this.completeError;
    }
    this.completed.push(payload);
    const record = this.recordFor(payload);
    if (record) {
      record.status = "succeeded";
      record.output = payload.output;
    }
  }

  async fail(payload: Payload): Promise<void> {
    this.failed.push(payload);
    this.errors.set(
      `${payload.run_id}:${payload.kind}:${payload.input_digest}`,
      payload.error as Payload,
    );
    const record = this.recordFor(payload);
    if (record) {
      record.status = "failed";
    }
  }

  async registerRun(_payload: Payload): Promise<void> {}

  async registerDataset(payload: Payload): Promise<Payload> {
    const key = `${payload.run_id}:${payload.kind}`;
    const existing = new Map(
      (this.tasks.get(key) ?? []).map((record) => [String(record.input_digest), record]),
    );
    const records: Payload[] = [];
    const seen = new Set<string>();
    for (const task of payload.tasks as Payload[]) {
      const digest = String(task.input_digest);
      if (seen.has(digest)) {
        continue;
      }
      seen.add(digest);
      records.push(
        existing.get(digest) ?? { ...task, status: "pending", output: null },
      );
    }
    this.tasks.set(key, records);
    return { total: records.length };
  }

  async list(payload: Payload): Promise<Payload[]> {
    const records = this.tasks.get(`${payload.run_id}:${payload.kind}`) ?? [];
    const statuses = new Set((payload.statuses as string[]) ?? []);
    return statuses.size === 0
      ? records
      : records.filter((record) => statuses.has(String(record.status)));
  }

  async memoGet(payload: Payload): Promise<{ found: boolean; value: unknown }> {
    const key = `${payload.run_id}:${payload.key_digest}`;
    if (this.memos.has(key)) {
      return { found: true, value: this.memos.get(key) };
    }
    return { found: false, value: null };
  }

  async memoPut(payload: Payload): Promise<{ ok: boolean }> {
    this.memoPuts.push(payload);
    this.memos.set(`${payload.run_id}:${payload.key_digest}`, payload.value);
    return { ok: true };
  }

  async traceEvent(payload: Payload): Promise<Payload> {
    this.traceEvents.push(payload);
    return { ...payload, event_index: this.traceEvents.length };
  }

  async listTraceEvents(payload: Payload): Promise<Payload[]> {
    // Mirror the server's filtering: run_id is required, every other field
    // narrows the result, and an empty event_type list means "any".
    const kind = payload.kind ?? null;
    const taskId = payload.task_id ?? null;
    const attempt = payload.attempt ?? null;
    const eventTypes = (payload.event_type as string[]) ?? [];
    return this.traceEvents.filter(
      (event) =>
        event.run_id === payload.run_id &&
        (kind === null || event.kind === kind) &&
        (taskId === null || event.task_id === taskId) &&
        (attempt === null || event.attempt === attempt) &&
        (eventTypes.length === 0 ||
          eventTypes.includes(String(event.event_type))),
    );
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
  assert.equal(runtime.completed[0].kind, "runAgent");
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
  assert.equal(runtime.failed.length, 2);
  assert.deepEqual(runtime.failed.at(-1)?.error, {
    error_type: "TypeError",
    message: "boom",
    failure_class: "eval_exception",
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
    runtime.completed.map((payload) => payload.kind),
    ["fetchCases", "runAgent"],
  );
});

test("dataset maps tasks durably in input order", async () => {
  const runtime = new Runtime();
  const evalRun = new DurableEval({ runId: "run", runtime });
  const tasks = [{ id: "b" }, { id: "a" }];

  const results = await evalRun.dataset("tasks", tasks).map({
    id: (task) => task.id,
    run: async (task) => ({ taskId: task.id }),
  });

  assert.deepEqual(results, [{ taskId: "b" }, { taskId: "a" }]);
  assert.deepEqual(
    runtime.completed.slice(-2).map((payload) => payload.input_digest),
    tasks.map((task) => digestOf(task)),
  );
});

test("dataset registers tasks with digest identity and optional label", async () => {
  const runtime = new Runtime();
  const evalRun = new DurableEval({ runId: "run", runtime });
  const tasks = [{ id: "b" }, { id: "a" }];

  await evalRun.dataset("labelled", tasks).map({
    id: (task) => task.id,
    run: async (task) => ({ taskId: task.id }),
  });
  await evalRun.dataset("unlabelled", tasks).map({
    run: async (task) => ({ taskId: task.id }),
  });

  const labelled = runtime.tasks.get("run:labelled")!;
  assert.deepEqual(
    labelled.map((record) => [record.input_digest, record.label]),
    tasks.map((task) => [digestOf(task), task.id]),
  );
  const unlabelled = runtime.tasks.get("run:unlabelled")!;
  assert.deepEqual(
    unlabelled.map((record) => record.label),
    [null, null],
  );
  assert.ok(labelled.every((record) => !("task_id" in record)));
});

test("dataset registers an optional category per task", async () => {
  const runtime = new Runtime();
  const evalRun = new DurableEval({ runId: "run", runtime });
  const tasks = [{ id: "b", kind: "math" }, { id: "a", kind: "code" }];

  await evalRun
    .dataset("categorised", tasks, { category: (task) => task.kind })
    .map({ run: async (task) => ({ taskId: task.id }) });

  const records = runtime.tasks.get("run:categorised")!;
  assert.deepEqual(
    records.map((record) => record.category),
    ["math", "code"],
  );
});

test("dataset categories filter restricts which tasks run", async () => {
  const runtime = new Runtime();
  const evalRun = new DurableEval({ runId: "run", runtime });
  const tasks = [{ id: "b", kind: "math" }, { id: "a", kind: "code" }];
  const ran: string[] = [];

  const results = await evalRun
    .dataset("filtered", tasks, { category: (task) => task.kind })
    .map({
      categories: ["math"],
      run: async (task) => {
        ran.push(task.id);
        return { taskId: task.id };
      },
    });

  assert.deepEqual(ran, ["b"]);
  assert.deepEqual(results[0], { taskId: "b" });
  assert.equal(results[1], undefined);
});

test("dataset map works without id and resumes by input digest", async () => {
  const runtime = new Runtime();
  const evalRun = new DurableEval({ runId: "run", runtime });
  let calls = 0;
  const run = async (task: { q: number }) => {
    calls += 1;
    return { answer: task.q };
  };

  const first = await evalRun.dataset("tasks", [{ q: 1 }]).map({ run });
  assert.deepEqual(first, [{ answer: 1 }]);
  assert.equal(calls, 1);

  const second = await evalRun.dataset("tasks", [{ q: 1 }, { q: 2 }]).map({ run });
  assert.deepEqual(second, [{ answer: 1 }, { answer: 2 }]);
  assert.equal(calls, 2);
});

test("dataset runs duplicate inputs once and fills every position", async () => {
  const runtime = new Runtime();
  const evalRun = new DurableEval({ runId: "run", runtime });
  let calls = 0;

  const results = await evalRun
    .dataset("tasks", [{ q: 1 }, { q: 2 }, { q: 1 }])
    .map({
      run: async (task) => {
        calls += 1;
        return { answer: task.q };
      },
    });

  assert.deepEqual(results, [{ answer: 1 }, { answer: 2 }, { answer: 1 }]);
  assert.equal(calls, 2);
  assert.equal(runtime.tasks.get("run:tasks")!.length, 2);
});

test("dataset fills duplicate positions from resumed succeeded tasks", async () => {
  const runtime = new Runtime();
  const evalRun = new DurableEval({ runId: "run", runtime });
  let calls = 0;
  const run = async (task: { q: number }) => {
    calls += 1;
    return { answer: task.q };
  };

  await evalRun.dataset("tasks", [{ q: 1 }]).map({ run });
  const results = await evalRun.dataset("tasks", [{ q: 1 }, { q: 1 }]).map({ run });

  assert.deepEqual(results, [{ answer: 1 }, { answer: 1 }]);
  assert.equal(calls, 1);
});

test("dataset records callback failures and continues", async () => {
  const runtime = new Runtime();
  const evalRun = new DurableEval({ runId: "run", runtime });

  // A failing task is recorded durably and leaves its slot empty rather than
  // aborting the whole dataset.
  const results = await evalRun.dataset("tasks", [{ id: "task" }]).map({
    id: (task) => task.id,
    run: () => {
      throw new TypeError("bad");
    },
    maxAttempts: 5,
  });

  assert.equal(results.length, 1);
  assert.equal(results[0], undefined);
  assert.equal(
    (runtime.failed.at(-1)?.error as { failure_class?: string }).failure_class,
    "eval_exception",
  );
  // fail no longer carries max_attempts; the retry policy is passed to begin.
  assert.equal(runtime.failed.at(-1)?.max_attempts, undefined);
  assert.deepEqual(runtime.began.at(-1)?.retry, { max_attempts: 5 });
});

test("memo caches values by key digest", async () => {
  const runtime = new Runtime();
  const evalRun = new DurableEval({ runId: "run", runtime });
  let calls = 0;
  const fn = async () => {
    calls += 1;
    return { result: 42 };
  };

  assert.deepEqual(await evalRun.memo({ prompt: "v1" }, fn), { result: 42 });
  assert.deepEqual(await evalRun.memo({ prompt: "v1" }, fn), { result: 42 });
  assert.equal(calls, 1);
  assert.equal(runtime.memoPuts.length, 1);
  assert.deepEqual(runtime.memoPuts[0], {
    run_id: "run",
    key_digest: digestOf({ prompt: "v1" }),
    key: { prompt: "v1" },
    value: { result: 42 },
  });

  assert.deepEqual(await evalRun.memo({ prompt: "v2" }, fn), { result: 42 });
  assert.equal(calls, 2);
});

test("traceTask derives task id from task input", async () => {
  const runtime = new Runtime();
  const evalRun = new DurableEval({ runId: "run", runtime });
  const task = { q: 1 };

  await evalRun.traceTask("tasks", { task }).modelRequest({ messages: [] });

  assert.equal(runtime.traceEvents[0].task_id, digestOf(task));
});

test("traceTask requires exactly one of task or taskId", async () => {
  const runtime = new Runtime();
  const evalRun = new DurableEval({ runId: "run", runtime });

  assert.throws(() => evalRun.traceTask("tasks", {}), TypeError);
  assert.throws(
    () => evalRun.traceTask("tasks", { task: { q: 1 }, taskId: "task" }),
    TypeError,
  );
});

test("listTraces fetches all or by task with server-side filters", async () => {
  const runtime = new Runtime();
  const evalRun = new DurableEval({ runId: "run", runtime });
  const task = { q: 1 };

  const first = evalRun.traceTask("tasks", { task });
  await first.modelRequest({ messages: [] });
  await first.toolCall({ name: "click" });
  const second = evalRun.traceTask("tasks", { task, attempt: 2 });
  await second.modelRequest({ messages: [] });
  await evalRun.traceTask("other", { taskId: "solo" }).scoringEvent({ score: 1 });

  // Fetch every trace event for the run.
  assert.equal((await evalRun.listTraces()).length, 4);

  // Fetch a specific task by id (here keyed by the task input digest).
  const byTask = await evalRun.listTraces({ task });
  assert.deepEqual(
    byTask.map((event) => event.event_type),
    ["model_request", "tool_call", "model_request"],
  );
  assert.deepEqual(
    await evalRun.listTraces({ taskId: "solo" }),
    await evalRun.listTraces({ kind: "other" }),
  );
  assert.deepEqual(await evalRun.listTraces({ taskId: "missing" }), []);

  // Server-side filters compose: event type, attempt, kind.
  assert.equal(
    (await evalRun.listTraces({ eventType: "model_request" })).length,
    2,
  );
  const pair = await evalRun.listTraces({
    task,
    eventType: ["model_request", "tool_call"],
    attempt: 1,
  });
  assert.deepEqual(
    pair.map((event) => event.event_type),
    ["model_request", "tool_call"],
  );
});

test("listTraces rejects task and taskId together", async () => {
  const runtime = new Runtime();
  const evalRun = new DurableEval({ runId: "run", runtime });
  await assert.rejects(
    () => evalRun.listTraces({ task: { q: 1 }, taskId: "x" }),
    TypeError,
  );
});

test("trace summary and export helpers call runtime", async () => {
  const runtime = new Runtime();
  const evalRun = new DurableEval({ runId: "run", runtime });

  await evalRun.traceTask("tasks", { taskId: "task" }).modelRequest({ messages: [] });

  assert.equal(runtime.traceEvents[0].event_type, "model_request");
  assert.deepEqual(await evalRun.summary(), { run_id: "run" });
  assert.equal(await evalRun.export(), "exported");
});
