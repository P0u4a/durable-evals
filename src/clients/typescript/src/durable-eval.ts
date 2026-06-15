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

  batch<TCase>(batchName: string, cases: TCase[]): Batch<TCase> {
    return new Batch(this, batchName, cases);
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

  async worker(options: { id: string; resources?: Record<string, unknown> }): Promise<Worker> {
    const runtime = await this.getRuntime();
    assertRuntimeMethod(runtime.registerWorker, "registerWorker");
    const record = await runtime.registerWorker({
      worker_id: options.id,
      resources: options.resources ?? {},
    });
    return new Worker(this, record);
  }

  traceCase(
    batchName: string,
    options: { case?: unknown; caseId?: string; attempt?: number },
  ): TraceCase {
    return new TraceCase(this, batchName, resolveCaseId(options), options.attempt ?? 1);
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

  async markReviewed(options: {
    batchName: string;
    case?: unknown;
    caseId?: string;
    decision: string;
    reviewer?: string;
    note?: string;
  }): Promise<Record<string, unknown>> {
    const runtime = await this.getRuntime();
    assertRuntimeMethod(runtime.markReviewed, "markReviewed");
    return await runtime.markReviewed({
      run_id: this.runId,
      batch_name: options.batchName,
      case_id: resolveCaseId(options),
      reviewer: options.reviewer ?? "user",
      decision: options.decision,
      note: options.note,
    });
  }

  private async runStep<TArgs extends unknown[], TOutput>(
    stepName: string,
    callback: StepCallback<TArgs, TOutput>,
    args: TArgs,
    options: { retry?: Record<string, unknown> },
  ): Promise<Awaited<TOutput>> {
    const runtime = await this.getRuntime();
    const inputDigest = inputDigestFor(stepName, args);
    const outcome = await runtime.beginStep({
      run_id: this.runId,
      step_name: stepName,
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
      await runtime.failStep({
        run_id: this.runId,
        step_name: stepName,
        input_digest: inputDigest,
        error: {
          error_type: error instanceof Error ? error.name : "Error",
          message: error instanceof Error ? error.message : String(error),
          failure_class: "user_code_error",
          retryable: true,
        },
      });
      throw error;
    }

    assertJsonSerializable(result, `step output for ${stepName}`);
    await runtime.completeStep({
      run_id: this.runId,
      step_name: stepName,
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

export class Batch<TCase> {
  constructor(
    private readonly evalRun: DurableEval,
    private readonly batchName: string,
    private readonly cases: TCase[],
  ) {}

  async map<TOutput>(options: {
    run: (testCase: TCase) => TOutput | Promise<TOutput>;
    id?: (testCase: TCase) => string;
    concurrency?: number;
    progress?: (summary: Record<string, number>) => void;
  }): Promise<TOutput[]> {
    const runtime = await this.runtime();
    const records = await this.register(options.id);
    const byDigest = new Map(records.map((record) => [String(record.input_digest), record]));
    const positionsByDigest = new Map<string, number[]>();
    for (const [index, testCase] of this.cases.entries()) {
      const digest = digestJson(testCase);
      const positions = positionsByDigest.get(digest);
      if (positions) {
        positions.push(index);
      } else {
        positionsByDigest.set(digest, [index]);
      }
    }
    const outputs = new Array<TOutput>(this.cases.length);
    const runnable: Array<{ digest: string; positions: number[] }> = [];
    for (const [digest, positions] of positionsByDigest) {
      const record = byDigest.get(digest);
      if (!record) {
        throw new Error(`missing registered case: ${digest}`);
      }
      if (record.status === "succeeded") {
        for (const position of positions) {
          outputs[position] = record.output as TOutput;
        }
        continue;
      }
      if (record.status === "terminal") {
        continue;
      }
      runnable.push({ digest, positions });
    }
    let cursor = 0;
    const concurrency = options.concurrency ?? 1;

    const worker = async (): Promise<void> => {
      while (cursor < runnable.length) {
        const { digest, positions } = runnable[cursor++];
        const testCase = this.cases[positions[0]];
        try {
          const output = await options.run(testCase);
          assertJsonSerializable(output, `batch output for ${this.batchName}`);
          await runtime.completeCase!({
            run_id: this.evalRun.runId,
            batch_name: this.batchName,
            input_digest: digest,
            output,
          });
          for (const position of positions) {
            outputs[position] = output;
          }
          options.progress?.(await this.summary());
        } catch (error) {
          await runtime.failCase!({
            run_id: this.evalRun.runId,
            batch_name: this.batchName,
            input_digest: digest,
            error: errorPayload(error),
          });
          throw error;
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
      runtime.listCases!({
        run_id: this.evalRun.runId,
        batch_name: this.batchName,
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
    return await this.casesByStatus(["failed"]);
  }

  async terminal(): Promise<Array<Record<string, unknown>>> {
    return await this.casesByStatus(["terminal"]);
  }

  async missing(): Promise<Array<Record<string, unknown>>> {
    return await this.casesByStatus(["pending"]);
  }

  private async register(
    id?: (testCase: TCase) => string,
  ): Promise<Array<Record<string, unknown>>> {
    const runtime = await this.runtime();
    await runtime.registerBatch!({
      run_id: this.evalRun.runId,
      batch_name: this.batchName,
      cases: this.cases.map((testCase) => ({
        input_digest: digestJson(testCase),
        input: testCase,
        label: id ? id(testCase) : null,
      })),
    });
    return await runtime.listCases!({
      run_id: this.evalRun.runId,
      batch_name: this.batchName,
      statuses: [],
    });
  }

  private async casesByStatus(statuses: string[]): Promise<Array<Record<string, unknown>>> {
    const runtime = await this.runtime();
    return await runtime.listCases!({
      run_id: this.evalRun.runId,
      batch_name: this.batchName,
      statuses,
    });
  }

  private async runtime(): Promise<Runtime> {
    const runtime = await (this.evalRun as unknown as { getRuntime(): Promise<Runtime> }).getRuntime();
    assertRuntimeMethod(runtime.registerBatch, "registerBatch");
    assertRuntimeMethod(runtime.listCases, "listCases");
    assertRuntimeMethod(runtime.completeCase, "completeCase");
    assertRuntimeMethod(runtime.failCase, "failCase");
    return runtime;
  }
}

export class Worker {
  readonly id: string;

  constructor(
    readonly evalRun: DurableEval,
    readonly record: Record<string, unknown>,
  ) {
    this.id = String(record.worker_id);
  }
}

export class TraceCase {
  constructor(
    private readonly evalRun: DurableEval,
    private readonly batchName: string,
    private readonly caseId: string,
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
      batch_name: this.batchName,
      case_id: this.caseId,
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

function resolveCaseId(options: { case?: unknown; caseId?: string }): string {
  const hasCase = options.case !== undefined;
  const hasCaseId = options.caseId !== undefined;
  if (hasCase === hasCaseId) {
    throw new TypeError("exactly one of case or caseId is required");
  }
  return hasCase ? digestJson(options.case) : String(options.caseId);
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

// Digests must match the Python client byte-for-byte (sorted keys, compact, UTF-8)
// so the same logical input has the same identity from either client.
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
    failure_class: "user_code_error",
    retryable: true,
  };
}
