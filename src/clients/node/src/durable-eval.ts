import { createHash, randomUUID } from "node:crypto";
import { DurableStepFailed, DurableStepInProgress } from "./errors.js";
import { RuntimeClient, type Runtime } from "./runtime.js";

export interface DurableEvalOptions {
  runId?: string;
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
  private readonly storageDir: string;
  private runtime: Runtime | Promise<Runtime> | null;

  constructor(options: DurableEvalOptions = {}) {
    this.runId = options.runId ?? randomUUID();
    this.storageDir = options.storageDir ?? ".durable";
    this.runtime = options.runtime ?? null;
  }

  step<TArgs extends unknown[], TOutput>(
    callback: StepCallback<TArgs, TOutput>,
  ): DurableStep<TArgs, TOutput>;
  step<TArgs extends unknown[], TOutput>(
    name: string,
    callback: StepCallback<TArgs, TOutput>,
  ): DurableStep<TArgs, TOutput>;
  step<TArgs extends unknown[], TOutput>(
    nameOrCallback: string | StepCallback<TArgs, TOutput>,
    maybeCallback?: StepCallback<TArgs, TOutput>,
  ): DurableStep<TArgs, TOutput> {
    const name = typeof nameOrCallback === "string" ? nameOrCallback : null;
    const callback = typeof nameOrCallback === "function" ? nameOrCallback : maybeCallback;
    if (typeof callback !== "function") {
      throw new TypeError("step expects a callback or a name and callback");
    }

    const stepName = (name ?? callback.name) || "anonymous";
    return async (...args: TArgs): Promise<Awaited<TOutput>> => {
      return await this.runStep(stepName, callback, args);
    };
  }

  private async runStep<TArgs extends unknown[], TOutput>(
    stepName: string,
    callback: StepCallback<TArgs, TOutput>,
    args: TArgs,
  ): Promise<Awaited<TOutput>> {
    const runtime = await this.getRuntime();
    const inputDigest = inputDigestFor(stepName, args);
    const outcome = await runtime.beginStep({
      run_id: this.runId,
      step_name: stepName,
      input_digest: inputDigest,
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
    if (outcome.type !== "execute") {
      throw new Error(`unexpected step outcome: ${(outcome as { type?: string }).type}`);
    }

    try {
      const result = await callback(...args);
      assertJsonSerializable(result, `step output for ${stepName}`);
      await runtime.completeStep({
        run_id: this.runId,
        step_name: stepName,
        input_digest: inputDigest,
        output: result,
      });
      return result as Awaited<TOutput>;
    } catch (error) {
      await runtime.failStep({
        run_id: this.runId,
        step_name: stepName,
        input_digest: inputDigest,
        error: {
          error_type: error instanceof Error ? error.name : "Error",
          message: error instanceof Error ? error.message : String(error),
        },
      });
      throw error;
    }
  }

  private async getRuntime(): Promise<Runtime> {
    if (this.runtime === null) {
      this.runtime = RuntimeClient.ensureStarted(this.storageDir);
    }
    return await this.runtime;
  }
}

function inputDigestFor(stepName: string, args: unknown[]): string {
  const inputJson = assertJsonSerializable({ args, kwargs: {} }, `step input for ${stepName}`);
  return createHash("sha256").update(inputJson, "utf8").digest("hex");
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
