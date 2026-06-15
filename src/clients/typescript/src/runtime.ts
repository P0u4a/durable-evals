import { spawn } from "node:child_process";
import { createWriteStream } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";

export type StepOutcome =
  | { type: "execute"; attempt: number }
  | { type: "skip_completed"; output: unknown }
  | { type: "in_progress" }
  | { type: "failed_terminal"; error: { error_type: string; message: string } }
  | { type: "retry_later"; retry_at: string };

export interface Runtime {
  beginStep(payload: Record<string, unknown>): Promise<StepOutcome>;
  completeStep(payload: Record<string, unknown>): Promise<void>;
  failStep(payload: Record<string, unknown>): Promise<void>;
  registerRun?(payload: Record<string, unknown>): Promise<void>;
  summary?(payload: Record<string, unknown>): Promise<Record<string, unknown>>;
  export?(payload: Record<string, unknown>): Promise<{ body: string; content_type?: string }>;
  registerBatch?(payload: Record<string, unknown>): Promise<Record<string, unknown>>;
  listCases?(payload: Record<string, unknown>): Promise<Array<Record<string, unknown>>>;
  completeCase?(payload: Record<string, unknown>): Promise<void>;
  failCase?(payload: Record<string, unknown>): Promise<void>;
  memoGet?(payload: Record<string, unknown>): Promise<{ found: boolean; value: unknown }>;
  memoPut?(payload: Record<string, unknown>): Promise<{ ok: boolean }>;
  registerVariants?(payload: Record<string, unknown>): Promise<Array<Record<string, unknown>>>;
  registerWorker?(payload: Record<string, unknown>): Promise<Record<string, unknown>>;
  traceEvent?(payload: Record<string, unknown>): Promise<Record<string, unknown>>;
  markReviewed?(payload: Record<string, unknown>): Promise<Record<string, unknown>>;
}

export class RuntimeClient implements Runtime {
  readonly baseUrl: string;

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl.replace(/\/+$/, "");
  }

  static async ensureStarted(storageDir = ".durable"): Promise<RuntimeClient> {
    const runtimeUrl = process.env.DURABLE_EVALS_RUNTIME_URL;
    if (runtimeUrl) {
      return new RuntimeClient(runtimeUrl);
    }

    await mkdir(storageDir, { recursive: true });
    const metadataPath = join(storageDir, "runtime.json");
    const cached = await readRuntimeMetadata(metadataPath);
    if (cached?.url) {
      const client = new RuntimeClient(cached.url);
      if (await client.isHealthy()) {
        return client;
      }
    }

    const serverBin =
      process.env.DURABLE_EVALS_SERVER_BIN ?? "durable-evals-server";
    const dbPath = join(storageDir, "evals.sqlite");
    const stderr = createWriteStream(join(storageDir, "server.log"), {
      flags: "a",
    });
    const child = spawn(serverBin, {
      env: { ...process.env, DURABLE_EVALS_DB: dbPath },
      stdio: ["ignore", "pipe", "pipe"],
    });
    child.stderr.pipe(stderr);

    const address = await readFirstLine(child);
    if (!address) {
      throw new Error("durable evals server did not print a listening address");
    }

    const client = new RuntimeClient(`http://${address}`);
    await client.waitUntilHealthy();
    await writeFile(
      metadataPath,
      JSON.stringify({ url: client.baseUrl, pid: child.pid }),
    );
    child.unref();
    return client;
  }

  async isHealthy(): Promise<boolean> {
    try {
      const response = await fetch(`${this.baseUrl}/health`, {
        signal: AbortSignal.timeout(200),
      });
      const body = (await response.json()) as { ok?: unknown };
      return response.ok && body.ok === true;
    } catch {
      return false;
    }
  }

  async waitUntilHealthy(): Promise<void> {
    const deadline = Date.now() + 5000;
    while (Date.now() < deadline) {
      if (await this.isHealthy()) {
        return;
      }
      await sleep(50);
    }
    throw new Error("durable evals server did not become healthy");
  }

  async beginStep(payload: Record<string, unknown>): Promise<StepOutcome> {
    return (await this.post("/steps/begin", payload)) as StepOutcome;
  }

  async completeStep(payload: Record<string, unknown>): Promise<void> {
    await this.post("/steps/complete", payload);
  }

  async failStep(payload: Record<string, unknown>): Promise<void> {
    await this.post("/steps/fail", payload);
  }

  async registerRun(payload: Record<string, unknown>): Promise<void> {
    await this.post("/runs/register", payload);
  }

  async summary(payload: Record<string, unknown>): Promise<Record<string, unknown>> {
    return (await this.post("/runs/summary", payload)) as Record<string, unknown>;
  }

  async export(payload: Record<string, unknown>): Promise<{ body: string; content_type?: string }> {
    return (await this.post("/runs/export", payload)) as { body: string; content_type?: string };
  }

  async registerBatch(payload: Record<string, unknown>): Promise<Record<string, unknown>> {
    return (await this.post("/batches/register", payload)) as Record<string, unknown>;
  }

  async listCases(payload: Record<string, unknown>): Promise<Array<Record<string, unknown>>> {
    return (await this.post("/batches/cases/list", payload)) as Array<Record<string, unknown>>;
  }

  async completeCase(payload: Record<string, unknown>): Promise<void> {
    await this.post("/batches/cases/complete", payload);
  }

  async failCase(payload: Record<string, unknown>): Promise<void> {
    await this.post("/batches/cases/fail", payload);
  }

  async memoGet(payload: Record<string, unknown>): Promise<{ found: boolean; value: unknown }> {
    return (await this.post("/memos/get", payload)) as { found: boolean; value: unknown };
  }

  async memoPut(payload: Record<string, unknown>): Promise<{ ok: boolean }> {
    return (await this.post("/memos/put", payload)) as { ok: boolean };
  }

  async registerVariants(payload: Record<string, unknown>): Promise<Array<Record<string, unknown>>> {
    return (await this.post("/variants/register", payload)) as Array<Record<string, unknown>>;
  }

  async registerWorker(payload: Record<string, unknown>): Promise<Record<string, unknown>> {
    return (await this.post("/workers/register", payload)) as Record<string, unknown>;
  }

  async traceEvent(payload: Record<string, unknown>): Promise<Record<string, unknown>> {
    return (await this.post("/traces/events", payload)) as Record<string, unknown>;
  }

  async markReviewed(payload: Record<string, unknown>): Promise<Record<string, unknown>> {
    return (await this.post("/reviews", payload)) as Record<string, unknown>;
  }

  private async post(
    path: string,
    payload: Record<string, unknown>,
  ): Promise<unknown> {
    const response = await fetch(`${this.baseUrl}${path}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(30000),
    });
    const body = (await response.json()) as { message?: string };
    if (!response.ok) {
      throw new Error(
        body?.message ?? `request failed with ${response.status}`,
      );
    }
    return body;
  }
}

async function readRuntimeMetadata(
  path: string,
): Promise<{ url?: string } | null> {
  try {
    return JSON.parse(await readFile(path, "utf8")) as { url?: string };
  } catch {
    return null;
  }
}

async function readFirstLine(child: ReturnType<typeof spawn>): Promise<string> {
  if (child.stdout === null) {
    throw new Error("durable evals server did not expose stdout");
  }

  let buffer = "";
  for await (const chunk of child.stdout) {
    buffer += chunk;
    const newline = buffer.indexOf("\n");
    if (newline >= 0) {
      return buffer.slice(0, newline).trim();
    }
  }
  return buffer.trim();
}
