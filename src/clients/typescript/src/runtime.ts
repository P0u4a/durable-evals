import { spawn } from "node:child_process";
import { createWriteStream } from "node:fs";
import { mkdir, readFile, rmdir, stat, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";

export type Outcome =
  | { type: "execute"; attempt: number }
  | { type: "skip_completed"; output: unknown }
  | { type: "in_progress" }
  | { type: "failed_terminal"; error: { error_type: string; message: string } }
  | { type: "retry_later"; retry_at: string };

export interface Runtime {
  begin(payload: Record<string, unknown>): Promise<Outcome>;
  complete(payload: Record<string, unknown>): Promise<void>;
  fail(payload: Record<string, unknown>): Promise<void>;
  list?(payload: Record<string, unknown>): Promise<Array<Record<string, unknown>>>;
  heartbeat?(payload: Record<string, unknown>): Promise<{ ok: boolean }>;
  registerRun?(payload: Record<string, unknown>): Promise<void>;
  summary?(payload: Record<string, unknown>): Promise<Record<string, unknown>>;
  export?(payload: Record<string, unknown>): Promise<{ body: string; content_type?: string }>;
  registerDataset?(payload: Record<string, unknown>): Promise<Record<string, unknown>>;
  memoGet?(payload: Record<string, unknown>): Promise<{ found: boolean; value: unknown }>;
  memoPut?(payload: Record<string, unknown>): Promise<{ ok: boolean }>;
  registerVariants?(payload: Record<string, unknown>): Promise<Array<Record<string, unknown>>>;
  traceEvent?(payload: Record<string, unknown>): Promise<Record<string, unknown>>;
}

export class RuntimeClient implements Runtime {
  readonly baseUrl: string;
  private readonly headers: Record<string, string>;

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl.replace(/\/+$/, "");
    const token = process.env.DURABLE_EVALS_TOKEN;
    this.headers = token ? { authorization: `Bearer ${token}` } : {};
  }

  static async ensureStarted(storageDir = ".durable"): Promise<RuntimeClient> {
    const runtimeUrl = process.env.DURABLE_EVALS_RUNTIME_URL;
    if (runtimeUrl) {
      return new RuntimeClient(runtimeUrl);
    }

    await mkdir(storageDir, { recursive: true });
    const metadataPath = join(storageDir, "runtime.json");
    const existing = await RuntimeClient.healthyFromMetadata(metadataPath);
    if (existing) {
      return existing;
    }

    // Serialize startup so concurrent first-runs don't each spawn a server against the
    // same SQLite file; the winner writes runtime.json and the rest reuse it.
    return await withStartupLock(storageDir, async () => {
      const cached = await RuntimeClient.healthyFromMetadata(metadataPath);
      if (cached) {
        return cached;
      }
      return await RuntimeClient.spawn(storageDir, metadataPath);
    });
  }

  private static async healthyFromMetadata(
    metadataPath: string,
  ): Promise<RuntimeClient | null> {
    const cached = await readRuntimeMetadata(metadataPath);
    if (cached?.url) {
      const client = new RuntimeClient(cached.url);
      if (await client.isHealthy()) {
        return client;
      }
    }
    return null;
  }

  private static async spawn(
    storageDir: string,
    metadataPath: string,
  ): Promise<RuntimeClient> {
    const serverBin =
      process.env.DURABLE_EVALS_SERVER_BIN ?? "durable-eval";
    const dbPath = join(storageDir, "evals.sqlite");
    const stderr = createWriteStream(join(storageDir, "server.log"), {
      flags: "a",
    });
    const child = spawn(serverBin, ["serve"], {
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
        headers: this.headers,
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

  async begin(payload: Record<string, unknown>): Promise<Outcome> {
    return (await this.post("/tasks/begin", payload)) as Outcome;
  }

  async complete(payload: Record<string, unknown>): Promise<void> {
    await this.post("/tasks/complete", payload);
  }

  async fail(payload: Record<string, unknown>): Promise<void> {
    await this.post("/tasks/fail", payload);
  }

  async list(payload: Record<string, unknown>): Promise<Array<Record<string, unknown>>> {
    return (await this.post("/tasks/list", payload)) as Array<Record<string, unknown>>;
  }

  async heartbeat(payload: Record<string, unknown>): Promise<{ ok: boolean }> {
    return (await this.post("/tasks/heartbeat", payload)) as { ok: boolean };
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

  async registerDataset(payload: Record<string, unknown>): Promise<Record<string, unknown>> {
    return (await this.post("/datasets/register", payload)) as Record<string, unknown>;
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

  async traceEvent(payload: Record<string, unknown>): Promise<Record<string, unknown>> {
    return (await this.post("/traces/events", payload)) as Record<string, unknown>;
  }

  private async post(
    path: string,
    payload: Record<string, unknown>,
  ): Promise<unknown> {
    const response = await fetch(`${this.baseUrl}${path}`, {
      method: "POST",
      headers: { "content-type": "application/json", ...this.headers },
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

async function withStartupLock<T>(
  storageDir: string,
  fn: () => Promise<T>,
  { timeoutMs = 10000, staleMs = 30000 }: { timeoutMs?: number; staleMs?: number } = {},
): Promise<T> {
  const lockDir = join(storageDir, "runtime.lock");
  const deadline = Date.now() + timeoutMs;
  let acquired = false;
  for (;;) {
    try {
      await mkdir(lockDir); // non-recursive: throws EEXIST when another holder exists
      acquired = true;
      break;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") {
        throw error;
      }
      let age = 0;
      try {
        age = Date.now() - (await stat(lockDir)).mtimeMs;
      } catch {
        age = 0;
      }
      if (age > staleMs) {
        // Reclaim a lock orphaned by a crashed process.
        await rmdir(lockDir).catch(() => {});
        continue;
      }
      if (Date.now() > deadline) {
        break; // proceed without the lock rather than hang forever
      }
      await sleep(50);
    }
  }
  try {
    return await fn();
  } finally {
    if (acquired) {
      await rmdir(lockDir).catch(() => {});
    }
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
