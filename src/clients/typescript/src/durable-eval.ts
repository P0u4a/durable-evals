import { createHash, randomUUID } from "node:crypto";
import { DurableStepFailed, DurableStepInProgress } from "./errors.js";
import { RuntimeClient, type Runtime } from "./runtime.js";

export interface DurableEvalOptions {
  runId?: string;
  name?: string;
  config?: Record<string, unknown>;
  storageDir?: string;
  runtime?: Runtime | Promise<Runtime>;
}

export type StepCallback<TArgs extends unknown[], TOutput> = (
  ...args: TArgs
) => TOutput | Promise<TOutput>;

export type DurableStep<TArgs extends unknown[], TOutput> = (
  ...args: TArgs
) => Promise<Awaited<TOutput>>;

export class DurableEval {
  readonly runId: string;
  readonly name?: string;
  readonly config: Record<string, unknown>;
  private readonly storageDir: string;
  private runtime: Runtime | Promise<Runtime> | null;
  private runRegistered = false;

  constructor(options: DurableEvalOptions = {}) {
    this.runId = options.runId ?? randomUUID();
    this.name = options.name;
    this.config = options.config ?? {};
    this.storageDir = options.storageDir ?? ".durable";
    this.runtime = options.runtime ?? null;
  }

  step<TArgs extends unknown[], TOutput>(
    callback: StepCallback<TArgs, TOutput>,
  ): DurableStep<TArgs, TOutput>;
  step<TArgs extends unknown[], TOutput>(
    name: string,
    callback: StepCallback<TArgs, TOutput>,
    options?: { retry?: Record<string, unknown> },
  ): DurableStep<TArgs, TOutput>;
  step<TArgs extends unknown[], TOutput>(
    nameOrCallback: string | StepCallback<TArgs, TOutput>,
    maybeCallback?: StepCallback<TArgs, TOutput>,
    options: { retry?: Record<string, unknown> } = {},
  ): DurableStep<TArgs, TOutput> {
    const name = typeof nameOrCallback === "string" ? nameOrCallback : null;
    const callback = typeof nameOrCallback === "function" ? nameOrCallback : maybeCallback;
    if (typeof callback !== "function") {
      throw new TypeError("step expects a callback or a name and callback");
    }

    const stepName = (name ?? callback.name) || "anonymous";
    return async (...args: TArgs): Promise<Awaited<TOutput>> => {
      return await this.runStep(stepName, callback, args, options);
    };
  }

  dataset<TTask>(
    datasetName: string,
    tasks: TTask[],
    options: { category?: (task: TTask) => string } = {},
  ): Dataset<TTask> {
    return new Dataset(this, datasetName, tasks, options.category);
  }

  async variants(
    dimension: string,
    variants: Array<{ name: string; config?: Record<string, unknown>; digest?: string }>,
  ): Promise<Array<Record<string, unknown>>> {
    const runtime = await this.getRuntime();
    assertRuntimeMethod(runtime.registerVariants, "registerVariants");
    return await runtime.registerVariants({
      run_id: this.runId,
      dimension,
      variants: variants.map((variant) => {
        const config = variant.config ?? {};
        return {
          name: variant.name,
          config,
          digest: variant.digest ?? digestJson(config),
        };
      }),
    });
  }

  traceTask(
    datasetName: string,
    options: { task?: unknown; taskId?: string; attempt?: number },
  ): TraceTask {
    return new TraceTask(this, datasetName, resolveTaskId(options), options.attempt ?? 1);
  }

  async memo<TValue>(
    key: unknown,
    fn: () => TValue | Promise<TValue>,
  ): Promise<Awaited<TValue>> {
    const runtime = await this.getRuntime();
    assertRuntimeMethod(runtime.memoGet, "memoGet");
    assertRuntimeMethod(runtime.memoPut, "memoPut");
    const keyDigest = digestJson(key);
    const cached = await runtime.memoGet({
      run_id: this.runId,
      key_digest: keyDigest,
    });
    if (cached.found) {
      return cached.value as Awaited<TValue>;
    }
    const value = await fn();
    assertJsonSerializable(value, "memo value");
    await runtime.memoPut({
      run_id: this.runId,
      key_digest: keyDigest,
      key,
      value,
    });
    return value as Awaited<TValue>;
  }

  async summary(): Promise<Record<string, unknown>> {
    const runtime = await this.getRuntime();
    assertRuntimeMethod(runtime.summary, "summary");
    return await runtime.summary({ run_id: this.runId });
  }

  async export(kind = "manifest_json"): Promise<string> {
    const runtime = await this.getRuntime();
    assertRuntimeMethod(runtime.export, "export");
    return (await runtime.export({ run_id: this.runId, kind })).body;
  }

  private async runStep<TArgs extends unknown[], TOutput>(
    stepName: string,
    callback: StepCallback<TArgs, TOutput>,
    args: TArgs,
    options: { retry?: Record<string, unknown> },
  ): Promise<Awaited<TOutput>> {
    const runtime = await this.getRuntime();
    const inputDigest = inputDigestFor(stepName, args);
    const outcome = await runtime.begin({
      run_id: this.runId,
      kind: stepName,
      input_digest: inputDigest,
      retry: options.retry ?? {},
    });

    if (outcome.type === "skip_completed") {
      return outcome.output as Awaited<TOutput>;
    }
    if (outcome.type === "failed_terminal") {
      const error = outcome.error;
      throw new DurableStepFailed(`${error.error_type}: ${error.message}`);
    }
    if (outcome.type === "in_progress") {
      throw new DurableStepInProgress(`step is already running: ${stepName}`);
    }
    if (outcome.type === "retry_later") {
      throw new DurableStepInProgress(`step retry is scheduled at ${outcome.retry_at}: ${stepName}`);
    }
    if (outcome.type !== "execute") {
      throw new Error(`unexpected step outcome: ${(outcome as { type?: string }).type}`);
    }

    let result: TOutput;
    try {
      result = await callback(...args);
    } catch (error) {
      await runtime.fail({
        run_id: this.runId,
        kind: stepName,
        input_digest: inputDigest,
        error: {
          error_type: error instanceof Error ? error.name : "Error",
          message: error instanceof Error ? error.message : String(error),
          failure_class: "eval_exception",
          retryable: true,
        },
      });
      throw error;
    }

    assertJsonSerializable(result, `step output for ${stepName}`);
    await runtime.complete({
      run_id: this.runId,
      kind: stepName,
      input_digest: inputDigest,
      output: result,
    });
    return result as Awaited<TOutput>;
  }

  private async getRuntime(): Promise<Runtime> {
    if (this.runtime === null) {
      this.runtime = RuntimeClient.ensureStarted(this.storageDir);
    }
    const runtime = await this.runtime;
    if (!this.runRegistered && runtime.registerRun) {
      await runtime.registerRun({
        run_id: this.runId,
        name: this.name,
        config: this.config,
      });
      this.runRegistered = true;
    }
    return runtime;
  }
}

export class Dataset<TTask> {
  constructor(
    private readonly evalRun: DurableEval,
    private readonly datasetName: string,
    private readonly tasks: TTask[],
    private readonly category?: (task: TTask) => string,
  ) {}

  async map<TOutput>(options: {
    run: (task: TTask) => TOutput | Promise<TOutput>;
    id?: (task: TTask) => string;
    category?: (task: TTask) => string;
    categories?: string[];
    concurrency?: number;
    progress?: (summary: Record<string, number>) => void;
    maxAttempts?: number;
  }): Promise<TOutput[]> {
    const runtime = await this.runtime();
    const categoryOf = options.category ?? this.category;
    // Register the dataset first so its tasks exist as the canonical primitive,
    // then drive each unique digest through the unified begin/complete/fail path.
    await this.register(options.id, categoryOf);
    const categoryFilter =
      options.categories && options.categories.length > 0
        ? new Set(options.categories)
        : null;
    const positionsByDigest = new Map<string, number[]>();
    for (const [index, task] of this.tasks.entries()) {
      const digest = digestJson(task);
      const positions = positionsByDigest.get(digest);
      if (positions) {
        positions.push(index);
      } else {
        positionsByDigest.set(digest, [index]);
      }
    }
    const outputs = new Array<TOutput>(this.tasks.length);
    const runnable: Array<{ digest: string; positions: number[] }> = [];
    for (const [digest, positions] of positionsByDigest) {
      if (categoryFilter) {
        const task = this.tasks[positions[0]];
        const taskCategory = categoryOf ? categoryOf(task) : undefined;
        if (taskCategory === undefined || !categoryFilter.has(taskCategory)) {
          continue;
        }
      }
      runnable.push({ digest, positions });
    }
    let cursor = 0;
    const concurrency = options.concurrency ?? 1;
    const maxAttempts = options.maxAttempts ?? 3;
    const retry = { max_attempts: maxAttempts };

    const worker = async (): Promise<void> => {
      while (cursor < runnable.length) {
        const { digest, positions } = runnable[cursor++];
        const task = this.tasks[positions[0]];
        const outcome = await runtime.begin({
          run_id: this.evalRun.runId,
          kind: this.datasetName,
          input_digest: digest,
          retry,
        });

        if (outcome.type === "skip_completed") {
          for (const position of positions) {
            outputs[position] = outcome.output as TOutput;
          }
          options.progress?.(await this.summary());
          continue;
        }
        // in_progress / retry_later / failed_terminal: leave positions empty.
        if (outcome.type !== "execute") {
          continue;
        }

        try {
          const output = await options.run(task);
          assertJsonSerializable(output, `dataset output for ${this.datasetName}`);
          await runtime.complete({
            run_id: this.evalRun.runId,
            kind: this.datasetName,
            input_digest: digest,
            output,
          });
          for (const position of positions) {
            outputs[position] = output;
          }
          options.progress?.(await this.summary());
        } catch (error) {
          // Record the failure durably and keep going; aborting here would cancel
          // sibling tasks that are still running. Positions are left empty.
          await runtime.fail({
            run_id: this.evalRun.runId,
            kind: this.datasetName,
            input_digest: digest,
            error: errorPayload(error),
          });
        }
      }
    };

    await Promise.all(
      Array.from({ length: Math.max(1, concurrency) }, () => worker()),
    );
    return outputs;
  }

  async summary(): Promise<Record<string, number>> {
    const records = await this.runtime().then((runtime) =>
      runtime.list!({
        run_id: this.evalRun.runId,
        kind: this.datasetName,
        statuses: [],
      }),
    );
    const counts: Record<string, number> = {
      total: records.length,
      pending: 0,
      running: 0,
      succeeded: 0,
      failed: 0,
      terminal: 0,
    };
    for (const record of records) {
      const status = String(record.status);
      counts[status] = (counts[status] ?? 0) + 1;
    }
    return counts;
  }

  async failed(): Promise<Array<Record<string, unknown>>> {
    return await this.tasksByStatus(["failed"]);
  }

  async terminal(): Promise<Array<Record<string, unknown>>> {
    return await this.tasksByStatus(["terminal"]);
  }

  async missing(): Promise<Array<Record<string, unknown>>> {
    return await this.tasksByStatus(["pending"]);
  }

  private async register(
    id?: (task: TTask) => string,
    category?: (task: TTask) => string,
  ): Promise<Array<Record<string, unknown>>> {
    const runtime = await this.runtime();
    await runtime.registerDataset!({
      run_id: this.evalRun.runId,
      kind: this.datasetName,
      tasks: this.tasks.map((task) => ({
        input_digest: digestJson(task),
        input: task,
        label: id ? id(task) : null,
        category: category ? category(task) : null,
      })),
    });
    return await runtime.list!({
      run_id: this.evalRun.runId,
      kind: this.datasetName,
      statuses: [],
    });
  }

  private async tasksByStatus(statuses: string[]): Promise<Array<Record<string, unknown>>> {
    const runtime = await this.runtime();
    return await runtime.list!({
      run_id: this.evalRun.runId,
      kind: this.datasetName,
      statuses,
    });
  }

  private async runtime(): Promise<Runtime> {
    const runtime = await (this.evalRun as unknown as { getRuntime(): Promise<Runtime> }).getRuntime();
    assertRuntimeMethod(runtime.registerDataset, "registerDataset");
    assertRuntimeMethod(runtime.list, "list");
    return runtime;
  }
}

export class TraceTask {
  constructor(
    private readonly evalRun: DurableEval,
    private readonly datasetName: string,
    private readonly taskId: string,
    private readonly attempt: number,
  ) {}

  async event(
    eventType: string,
    payload: unknown = null,
    options: { artifactIds?: string[] } = {},
  ): Promise<Record<string, unknown>> {
    const runtime = await (this.evalRun as unknown as { getRuntime(): Promise<Runtime> }).getRuntime();
    assertRuntimeMethod(runtime.traceEvent, "traceEvent");
    return await runtime.traceEvent({
      run_id: this.evalRun.runId,
      kind: this.datasetName,
      task_id: this.taskId,
      attempt: this.attempt,
      event_type: eventType,
      payload,
      artifact_ids: options.artifactIds ?? [],
    });
  }

  async modelRequest(payload: unknown): Promise<Record<string, unknown>> {
    return await this.event("model_request", payload);
  }

  async modelResponse(payload: unknown): Promise<Record<string, unknown>> {
    return await this.event("model_response", payload);
  }

  async toolCall(payload: unknown): Promise<Record<string, unknown>> {
    return await this.event("tool_call", payload);
  }

  async toolResult(payload: unknown): Promise<Record<string, unknown>> {
    return await this.event("tool_result", payload);
  }

  async stateSnapshot(payload: unknown, artifactIds: string[] = []): Promise<Record<string, unknown>> {
    return await this.event("state_snapshot", payload, { artifactIds });
  }

  async scoringEvent(payload: unknown): Promise<Record<string, unknown>> {
    return await this.event("scoring_event", payload);
  }

  async terminationEvent(payload: unknown): Promise<Record<string, unknown>> {
    return await this.event("termination_event", payload);
  }
}

function resolveTaskId(options: { task?: unknown; taskId?: string }): string {
  const hasTask = options.task !== undefined;
  const hasTaskId = options.taskId !== undefined;
  if (hasTask === hasTaskId) {
    throw new TypeError("exactly one of task or taskId is required");
  }
  return hasTask ? digestJson(options.task) : String(options.taskId);
}

function inputDigestFor(stepName: string, args: unknown[]): string {
  const inputJson = canonicalJson({ args, kwargs: {} }, `step input for ${stepName}`);
  return createHash("sha256").update(inputJson, "utf8").digest("hex");
}

function digestJson(value: unknown): string {
  return createHash("sha256")
    .update(canonicalJson(value, "durable eval payload"), "utf8")
    .digest("hex");
}

// Canonical form (sorted keys, compact, UTF-8) so object key order doesn't change
// a value's identity across runs.
function canonicalJson(value: unknown, label: string): string {
  try {
    const json = JSON.stringify(value, (_key, val: unknown) =>
      val !== null && typeof val === "object" && !Array.isArray(val)
        ? Object.fromEntries(
            Object.keys(val as Record<string, unknown>)
              .sort()
              .map((key) => [key, (val as Record<string, unknown>)[key]]),
          )
        : val,
    );
    if (json === undefined) {
      throw new TypeError("value is undefined");
    }
    return json;
  } catch (error) {
    throw new TypeError(`${label} must be JSON-serializable`, { cause: error });
  }
}

function assertJsonSerializable(value: unknown, label: string): string {
  try {
    const json = JSON.stringify(value);
    if (json === undefined) {
      throw new TypeError("value is undefined");
    }
    return json;
  } catch (error) {
    throw new TypeError(`${label} must be JSON-serializable`, { cause: error });
  }
}

function assertRuntimeMethod<T>(method: T | undefined, name: string): asserts method is T {
  if (method === undefined) {
    throw new Error(`runtime does not support ${name}`);
  }
}

function errorPayload(error: unknown): Record<string, unknown> {
  return {
    error_type: error instanceof Error ? error.name : "Error",
    message: error instanceof Error ? error.message : String(error),
    failure_class: "eval_exception",
    retryable: true,
  };
}
