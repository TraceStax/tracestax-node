/**
 * TraceStaxClient — batching HTTP client for the TraceStax ingest API.
 *
 * Buffers events in an in-memory queue and flushes them on a timer or when the
 * batch size threshold is reached. Mirrors the Python SDK's design.
 *
 * Resilience features:
 *  - Exponential backoff on flush interval when HTTP calls fail
 *  - Circuit breaker: after 3 consecutive failures the circuit opens and events
 *    are dropped silently until a 30-second cooldown passes, then a single probe
 *    attempt is made. On success the circuit closes and normal flushing resumes.
 *  - X-Retry-After header: honored to pause flushing for the specified seconds
 *  - Backpressure directive: heartbeat response can instruct the SDK to pause
 *    ingest flushing for a configurable duration
 */

import { ZkHasher, type ZkOptions } from "./zk.js";

const INGEST_PATH = "/v1/ingest";
const SNAPSHOT_PATH = "/v1/snapshot";
const LINEAGE_PATH = "/v1/lineage";
const HEARTBEAT_PATH = "/v1/heartbeat";
const DUMP_PATH = "/v1/dump";

const SDK_VERSION = "0.1.0";

const CIRCUIT_OPEN_THRESHOLD = 3;    // consecutive failures before opening
const CIRCUIT_COOLDOWN_MS = 30_000;  // ms to wait before probing again
const MAX_FLUSH_INTERVAL_MS = 60_000;

export interface TraceStaxClientOptions {
  apiKey?: string;
  /**
   * Async function that returns a short-lived token (e.g. an OIDC JWT) to use
   * as the Authorization Bearer value instead of a static apiKey.
   * Called before every HTTP request — implementations should cache and refresh
   * as needed (e.g. check expiry, re-fetch when within 60s of expiry).
   */
  tokenProvider?: () => Promise<string>;
  endpoint?: string;
  flushInterval?: number; // ms — default 5 000
  maxBatchSize?: number; // default 100
  enabled?: boolean; // default true — set false to disable all API calls
  dryRun?: boolean; // default false — log to stdout instead of sending
  workerKey?: string; // set by the monitor layer; used in thread dumps
  /**
   * Zero-Knowledge hashing options. When set, designated payload fields are
   * HMAC-SHA-256 hashed client-side before transmission using the customer's
   * secret key. TraceStax never receives raw values for hashed fields.
   */
  zk?: ZkOptions;
}

interface IngestEvent {
  [key: string]: unknown;
}

export interface HeartbeatDirectives {
  pause_ingest: boolean;
  pause_until_ms: number | null;
  commands: Array<{ id: string; type: string }>;
}

type CircuitState = "CLOSED" | "OPEN" | "HALF_OPEN";

export class TraceStaxClient {
  private readonly apiKey: string;
  private readonly tokenProvider: (() => Promise<string>) | null;
  private readonly endpoint: string;
  private readonly flushInterval: number;
  private readonly maxBatchSize: number;
  private readonly enabled: boolean;
  private readonly dryRun: boolean;
  private readonly zkHasher: ZkHasher | null;

  // Resilience state
  private consecutiveFailures = 0;
  private circuitState: CircuitState = "CLOSED";
  private circuitOpenedAt: number | null = null;
  private currentFlushInterval: number;
  private pauseUntilMs: number | null = null;
  private droppedEvents = 0;

  private queue: IngestEvent[] = [];
  private flushTimer: ReturnType<typeof setTimeout> | null = null;
  private closed = false;

  /** Worker key forwarded from the monitor layer, used in thread dump payloads. */
  workerKey: string | null;

  constructor(options: TraceStaxClientOptions) {
    // RUN-140: Check enabled/dryRun from options or env vars
    this.enabled =
      options.enabled !== undefined
        ? options.enabled
        : process.env.TRACESTAX_ENABLED !== "false";
    this.dryRun =
      options.dryRun !== undefined
        ? options.dryRun
        : process.env.TRACESTAX_DRY_RUN === "true";

    this.workerKey = options.workerKey ?? null;

    if (!this.enabled) {
      this.apiKey = "";
      this.tokenProvider = null;
      this.endpoint = "";
      this.flushInterval = 0;
      this.maxBatchSize = 0;
      this.currentFlushInterval = 0;
      this.zkHasher = null;
      return;
    }

    if (!options.apiKey && !options.tokenProvider && !this.dryRun) {
      throw new Error("apiKey or tokenProvider is required");
    }

    this.apiKey = options.apiKey ?? "";
    this.tokenProvider = options.tokenProvider ?? null;
    this.zkHasher = options.zk ? new ZkHasher(options.zk) : null;
    this.endpoint = (options.endpoint ?? "https://ingest.tracestax.com").replace(
      /\/$/,
      "",
    );
    this.flushInterval = options.flushInterval ?? 5_000;
    this.maxBatchSize = options.maxBatchSize ?? 100;
    this.currentFlushInterval = this.flushInterval;

    if (!this.dryRun) {
      this.scheduleNextFlush();
      this.installExitHook();
    }
  }

  // ── Public API ─────────────────────────────────────────────────────

  /**
   * Enqueue a task event for batched delivery.
   */
  sendEvent(payload: IngestEvent): void {
    if (!this.enabled || this.closed) return;
    const finalPayload = this.zkHasher
      ? this.zkHasher.hashPayload(payload as Record<string, unknown>)
      : payload;

    // Guard against huge payloads that could OOM during serialization at flush
    // time. Serialize now (in a try/catch) so any non-serializable value is
    // caught here rather than poisoning a batch of otherwise-valid events.
    let serialized: string;
    try {
      serialized = JSON.stringify(finalPayload);
    } catch {
      console.warn("[tracestax] sendEvent: payload not serializable, dropping");
      return;
    }
    if (serialized.length > 512 * 1024) {
      console.warn("[tracestax] sendEvent: payload exceeds 512 KB, dropping");
      return;
    }

    if (this.dryRun) {
      const preview = serialized.length > 512 ? serialized.slice(0, 512) + '…' : serialized;
      console.log("[tracestax dry-run] sendEvent:", preview);
      return;
    }
    // Prevent unbounded memory growth — drop oldest events if queue exceeds 10K
    if (this.queue.length >= 10_000) {
      const dropped = this.queue.length - 5_000;
      this.queue.splice(0, dropped);
      this.droppedEvents += dropped;
      console.warn("[tracestax] Queue exceeded 10K events, dropped oldest to 5K");
    }

    this.queue.push(finalPayload);

    if (this.queue.length >= this.maxBatchSize) {
      this.flush().catch(this.onError);
    }
  }

  /**
   * Send a snapshot payload immediately.
   */
  async sendSnapshot(payload: Record<string, unknown>): Promise<void> {
    if (!this.enabled) return;
    const finalPayload = this.zkHasher ? this.zkHasher.hashPayload(payload) : payload;
    if (this.dryRun) {
      const raw = JSON.stringify(finalPayload);
      const preview = raw.length > 512 ? raw.slice(0, 512) + '…' : raw;
      console.log("[tracestax dry-run] sendSnapshot:", preview);
      return;
    }
    await this.postForJson(SNAPSHOT_PATH, finalPayload);
  }

  /**
   * Send a lineage payload immediately.
   */
  async sendLineage(payload: Record<string, unknown>): Promise<void> {
    if (!this.enabled) return;
    const finalPayload = this.zkHasher ? this.zkHasher.hashPayload(payload) : payload;
    if (this.dryRun) {
      console.log("[tracestax dry-run] sendLineage:", JSON.stringify(finalPayload));
      return;
    }
    await this.postForJson(LINEAGE_PATH, finalPayload);
  }

  /**
   * Send a heartbeat payload immediately and return server directives.
   * Returns null when disabled, dry-run, or if the request fails.
   */
  async sendHeartbeat(payload: Record<string, unknown>): Promise<HeartbeatDirectives | null> {
    if (!this.enabled) return null;
    if (this.dryRun) {
      console.log("[tracestax dry-run] sendHeartbeat:", JSON.stringify(payload));
      return null;
    }
    return this.postForJson<HeartbeatDirectives>(HEARTBEAT_PATH, payload);
  }

  /**
   * Returns a snapshot of the client's internal health metrics.
   */
  stats(): {
    queueSize: number;
    droppedEvents: number;
    circuitState: string;
    consecutiveFailures: number;
  } {
    return {
      queueSize: this.queue.length,
      droppedEvents: this.droppedEvents,
      circuitState: this.circuitState.toLowerCase(),
      consecutiveFailures: this.consecutiveFailures,
    };
  }

  /**
   * Instruct the client to pause ingest flushing until the given epoch ms.
   * Called by the monitor layer when the heartbeat response signals backpressure.
   */
  setPauseUntil(ms: number): void {
    this.pauseUntilMs = ms;
  }

  /**
   * Execute a server-issued command (currently only "thread_dump").
   */
  async executeCommand(cmd: { id: string; type: string }): Promise<void> {
    if (cmd.type !== "thread_dump") return;
    const dump = this.captureThreadDump();
    await this.postForJson(DUMP_PATH, {
      cmd_id: cmd.id,
      worker_key: this.workerKey ?? `node:${process.pid}`,
      dump_text: dump,
      language: "typescript",
      sdk_version: SDK_VERSION,
      captured_at: new Date().toISOString(),
    });
  }

  /**
   * Immediately flush all queued events. Clears any active backpressure pause
   * (pauseUntilMs) but still respects the circuit-breaker state — if the circuit
   * is OPEN and the cooldown has not elapsed, the flush is a no-op.
   * Use close() for a best-effort final flush that bypasses the circuit breaker.
   */
  async flush(): Promise<void> {
    // Honor backpressure pause
    if (this.pauseUntilMs !== null && Date.now() < this.pauseUntilMs) return;
    this.pauseUntilMs = null;

    // Circuit breaker check
    if (this.circuitState === "OPEN") {
      const elapsed = Math.max(0, Date.now() - (this.circuitOpenedAt ?? 0));
      if (elapsed < CIRCUIT_COOLDOWN_MS) return; // silent drop
      this.circuitState = "HALF_OPEN";
    }

    while (this.queue.length > 0) {
      const batch = this.queue.splice(0, this.maxBatchSize);
      const ok = await this.postBatch(INGEST_PATH, { events: batch });
      if (!ok) {
        // Restore the batch so events aren't lost
        this.queue.unshift(...batch);
        break;
      }
    }
  }

  /**
   * Flush remaining events and stop the background timer.
   * Bounded to 5 seconds total so a dead server cannot hang the host process.
   */
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;

    if (this.flushTimer !== null) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }

    // Final flush — bypass circuit breaker on shutdown, but cap total time
    // so a dead server cannot cause an indefinite hang (e.g. in K8s with a
    // 30s grace period, 100 batches × 10s each would far exceed the window).
    const deadline = Date.now() + 5_000;
    while (this.queue.length > 0 && Date.now() < deadline) {
      const batch = this.queue.splice(0, this.maxBatchSize);
      await this.postForJson(INGEST_PATH, { events: batch });
    }
  }

  /**
   * Number of events currently queued.
   */
  get pending(): number {
    return this.queue.length;
  }

  // ── Circuit breaker ────────────────────────────────────────────────

  private recordSuccess(): void {
    this.consecutiveFailures = 0;
    this.circuitState = "CLOSED";
    this.circuitOpenedAt = null;
    // Halve the interval back toward the base floor
    this.currentFlushInterval = Math.max(
      this.flushInterval,
      Math.floor(this.currentFlushInterval / 2),
    );
  }

  private recordFailure(): void {
    this.consecutiveFailures++;
    // Exponential backoff
    this.currentFlushInterval = Math.min(
      MAX_FLUSH_INTERVAL_MS,
      this.currentFlushInterval * 2,
    );
    if (
      this.consecutiveFailures >= CIRCUIT_OPEN_THRESHOLD &&
      this.circuitState === "CLOSED"
    ) {
      this.circuitState = "OPEN";
      this.circuitOpenedAt = Date.now();
      console.warn(
        "[tracestax] TraceStax unreachable, circuit open, events dropped",
      );
    } else if (this.circuitState === "HALF_OPEN") {
      // Probe failed — back to OPEN
      this.circuitState = "OPEN";
      this.circuitOpenedAt = Date.now();
    }
  }

  // ── HTTP ───────────────────────────────────────────────────────────

  private async postBatch(path: string, body: unknown): Promise<boolean> {
    return (await this.postForJson(path, body)) !== null;
  }

  /**
   * POST body to path, return parsed JSON response or null on any error.
   * Records success/failure for the circuit breaker.
   */
  async postForJson<T = unknown>(path: string, body: unknown): Promise<T | null> {
    const url = `${this.endpoint}${path}`;

    let token: string;
    if (this.tokenProvider) {
      try {
        token = await this.tokenProvider();
      } catch (err) {
        // tokenProvider threw — auth issue, not a network failure. Log and skip
        // without opening the circuit breaker (which would penalise all future
        // sends for an auth misconfiguration).
        console.warn("[tracestax] tokenProvider() threw, skipping flush:", err);
        return null;
      }
    } else {
      token = this.apiKey;
    }

    // Serialize before entering the fetch try/catch so a non-serializable payload
    // (e.g. BigInt, circular ref in sendSnapshot/sendLineage) is caught here and
    // does NOT increment the circuit-breaker counter — it is a caller bug, not a
    // network failure.
    let bodyJson: string;
    try {
      bodyJson = JSON.stringify(body);
    } catch (err) {
      console.warn("[tracestax] Payload not serializable, dropping:", err);
      return null;
    }

    try {
      const res = await fetch(url, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
          "User-Agent": `tracestax-node/${SDK_VERSION}`,
        },
        body: bodyJson,
        signal: AbortSignal.timeout(10_000),
      });

      // Honor X-Retry-After header
      const retryAfter = res.headers.get("X-Retry-After");
      if (retryAfter) {
        const secs = parseInt(retryAfter, 10);
        if (!isNaN(secs) && secs > 0) {
          this.pauseUntilMs = Date.now() + secs * 1_000;
        }
      }

      if (!res.ok) {
        const errText = await this.readBodyLimited(res, 200);
        if (res.status === 401) {
          // Auth failure is permanent misconfiguration, not a transient network issue.
          // Log prominently without penalising the circuit breaker so that one bad
          // API key does not falsely open the circuit and suppress future sends once
          // the key is corrected.
          console.error(
            `[tracestax] Auth failed (401) — check your API key or tokenProvider. ${errText}`,
          );
          return null;
        }
        this.recordFailure();
        console.warn(`[tracestax] Ingest responded ${res.status}: ${errText}`);
        return null;
      }

      this.recordSuccess();
      const raw = await this.readBodyLimited(res, 1024 * 1024);
      if (!raw) return null;
      try {
        return JSON.parse(raw) as T;
      } catch {
        return null;
      }
    } catch (err) {
      this.recordFailure();
      console.warn("[tracestax] Failed to send to ingest:", err);
      return null;
    }
  }

  /**
   * Read a response body up to maxBytes, then cancel the stream.
   * Prevents an oversized or malicious server response from buffering
   * unbounded memory in the SDK.
   */
  private async readBodyLimited(res: Response, maxBytes: number): Promise<string> {
    if (!res.body) {
      try { return (await res.text()).slice(0, maxBytes); } catch { return ""; }
    }
    const reader = res.body.getReader();
    const chunks: Uint8Array[] = [];
    let total = 0;
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done || !value) break;
        chunks.push(value);
        total += value.length;
        if (total >= maxBytes) {
          await reader.cancel().catch(() => {});
          if (total > maxBytes) {
            console.warn("[tracestax] Ingest response truncated — exceeded size limit");
          }
          break;
        }
      }
    } catch { /* ignore stream errors */ } finally {
      try { reader.releaseLock(); } catch {}
    }
    if (!chunks.length) return "";
    const buf = new Uint8Array(total);
    let off = 0;
    for (const c of chunks) { buf.set(c, off); off += c.length; }
    return new TextDecoder().decode(buf.subarray(0, maxBytes));
  }

  // ── Thread dump ────────────────────────────────────────────────────

  private captureThreadDump(): string {
    const lines: string[] = [
      "=== TraceStax Node.js Thread Dump ===",
      `PID: ${process.pid}  Node: ${process.version}`,
      `Timestamp: ${new Date().toISOString()}`,
      "",
      "=== Current Call Stack ===",
    ];

    const err = new Error("tracestax-dump");
    Error.captureStackTrace(err, this.captureThreadDump);
    lines.push(err.stack ?? "(stack unavailable)");

    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const handles: unknown[] = (process as any)._getActiveHandles?.() ?? [];
      lines.push(`\n=== Active Handles (${handles.length}) ===`);
      for (const h of handles) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        lines.push(`  ${(h as any)?.constructor?.name ?? "unknown"}`);
      }
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const requests: unknown[] = (process as any)._getActiveRequests?.() ?? [];
      lines.push(`\n=== Active Requests (${requests.length}) ===`);
      for (const r of requests) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        lines.push(`  ${(r as any)?.constructor?.name ?? "unknown"}`);
      }
    } catch {
      // _getActiveHandles/_getActiveRequests are internal Node APIs — skip if unavailable
    }

    return lines.join("\n");
  }

  // ── Flush scheduling ───────────────────────────────────────────────

  private scheduleNextFlush(): void {
    if (this.closed) return;
    this.flushTimer = setTimeout(async () => {
      await this.flush().catch(this.onError);
      this.scheduleNextFlush();
    }, this.currentFlushInterval);

    // Don't keep the process alive just for flushing
    if (
      this.flushTimer &&
      typeof this.flushTimer === "object" &&
      "unref" in this.flushTimer
    ) {
      (this.flushTimer as { unref(): void }).unref();
    }
  }

  // ── Lifecycle ──────────────────────────────────────────────────────

  private installExitHook(): void {
    // beforeExit fires when the event loop drains — safe to await async work
    process.on("beforeExit", async () => {
      await this.flush().catch(() => {});
    });

    // SIGTERM/SIGINT: flush on signal, then chain to whatever handler was
    // registered before us. We never call process.exit() — that is the host
    // app's responsibility. Calling process.exit() here would:
    //   1. prevent the customer's own shutdown logic from running
    //   2. race with other TraceStaxClient instances also registered as handlers
    //
    // process.listeners / removeAllListeners may be absent in non-Node runtimes
    // (e.g. test environments with a stubbed process object) — guard defensively.
    if (typeof process.listeners === "function" && typeof process.removeAllListeners === "function") {
      for (const sig of ["SIGTERM", "SIGINT"] as const) {
        const prev = process.listeners(sig).slice() as ((...args: unknown[]) => void)[];
        process.removeAllListeners(sig);
        process.on(sig, async (...args: unknown[]) => {
          await this.close().catch(() => {});
          for (const h of prev) {
            try { await (h(...args) as unknown); } catch {}
          }
        });
      }
    }
  }

  private onError = (err: unknown): void => {
    console.warn("[tracestax] Flush error:", err);
  };
}
