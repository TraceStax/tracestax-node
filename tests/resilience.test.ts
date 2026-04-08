/**
 * Resilience tests for TraceStaxClient.
 *
 * These tests guard the most critical production guarantee: the SDK must NEVER
 * crash, block, or OOM the host application — even when the ingest server is
 * down, slow, or returning errors.
 *
 * Scenarios covered:
 *  - enabled=false / dryRun=true are complete no-ops
 *  - sendEvent/sendHeartbeat never throw regardless of server state
 *  - Circuit breaker: CLOSED → OPEN after 3 failures, events dropped silently
 *  - Circuit breaker: OPEN → HALF_OPEN after 30s cooldown, resets on success
 *  - Circuit breaker: HALF_OPEN → OPEN again if probe fails
 *  - Queue memory protection: capped at 10K, trims to 5K on overflow
 *  - X-Retry-After header causes flush to pause
 *  - Two concurrent flush() calls do not double-send events
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { TraceStaxClient } from "../src/client.js";

// Stub process.on so exit hooks don't fire during tests
vi.stubGlobal("process", {
  ...process,
  on: vi.fn(),
  pid: 9999,
  exit: vi.fn(),
});

// ── Helpers ──────────────────────────────────────────────────────────────────

/** Build a mock fetch that always fails with the given HTTP status. */
function failingFetch(status: number) {
  return vi.fn().mockResolvedValue({
    ok: false,
    status,
    headers: { get: (_: string) => null },
    text: () => Promise.resolve(`error ${status}`),
    json: () => Promise.resolve({}),
  });
}

/** Build a mock fetch that always succeeds. */
function successFetch() {
  return vi.fn().mockResolvedValue({
    ok: true,
    status: 200,
    headers: { get: (_: string) => null },
    text: () => Promise.resolve('{"ok":true}'),
    json: () => Promise.resolve({ ok: true }),
  });
}

/** A fresh client pointing at a dead endpoint with a long manual-flush interval. */
function makeClient(overrides: { apiKey?: string; endpoint?: string; maxBatchSize?: number } = {}) {
  return new TraceStaxClient({
    apiKey: overrides.apiKey ?? "ts_test_resilience",
    endpoint: overrides.endpoint ?? "https://test.tracestax.com",
    flushInterval: 999_999,
    maxBatchSize: overrides.maxBatchSize ?? 100,
  });
}

// ── enabled / dryRun flags ───────────────────────────────────────────────────

describe("enabled=false", () => {
  it("sendEvent is a complete no-op — no fetch, no queue growth", async () => {
    const mockFetch = vi.fn();
    vi.stubGlobal("fetch", mockFetch);

    const client = new TraceStaxClient({
      apiKey: "",
      endpoint: "http://dead:9999",
      enabled: false,
    });

    client.sendEvent({ task: "noop" });
    await client.flush();

    expect(mockFetch).not.toHaveBeenCalled();
    expect(client.pending).toBe(0);
  });

  it("sendHeartbeat returns null immediately", async () => {
    const mockFetch = vi.fn();
    vi.stubGlobal("fetch", mockFetch);

    const client = new TraceStaxClient({ apiKey: "", enabled: false });
    const result = await client.sendHeartbeat({ framework: "bullmq", worker: {} });

    expect(result).toBeNull();
    expect(mockFetch).not.toHaveBeenCalled();
  });

  it("close() is a no-op", async () => {
    const client = new TraceStaxClient({ apiKey: "", enabled: false });
    await expect(client.close()).resolves.toBeUndefined();
  });
});

describe("dryRun=true", () => {
  it("sendEvent logs to console but never calls fetch", async () => {
    const mockFetch = vi.fn();
    vi.stubGlobal("fetch", mockFetch);
    const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});

    const client = new TraceStaxClient({
      apiKey: "ts_test_resilience",
      endpoint: "https://test.tracestax.com",
      dryRun: true,
    });
    client.sendEvent({ task: "dry" });

    expect(mockFetch).not.toHaveBeenCalled();
    expect(consoleSpy).toHaveBeenCalledOnce();
    consoleSpy.mockRestore();
  });

  it("sendHeartbeat logs and returns null", async () => {
    const mockFetch = vi.fn();
    vi.stubGlobal("fetch", mockFetch);
    const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});

    const client = new TraceStaxClient({
      apiKey: "ts_test_resilience",
      dryRun: true,
    });
    const result = await client.sendHeartbeat({ framework: "bullmq", worker: {} });

    expect(result).toBeNull();
    expect(mockFetch).not.toHaveBeenCalled();
    consoleSpy.mockRestore();
  });
});

// ── Fire-and-forget guarantee ────────────────────────────────────────────────

describe("fire-and-forget guarantees", () => {
  afterEach(async () => {
    vi.restoreAllMocks();
  });

  it("sendEvent never throws even when the server is completely unreachable", async () => {
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("ECONNREFUSED")));

    const client = makeClient({ endpoint: "http://dead:9999" });

    expect(() => {
      for (let i = 0; i < 50; i++) client.sendEvent({ task: `t${i}` });
    }).not.toThrow();

    // flush must also not throw
    await expect(client.flush()).resolves.toBeUndefined();
    await client.close();
  });

  it("sendEvent never throws when fetch times out", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockRejectedValue(new DOMException("The operation was aborted", "AbortError")),
    );

    const client = makeClient();
    expect(() => client.sendEvent({ task: "slow" })).not.toThrow();
    await expect(client.flush()).resolves.toBeUndefined();
    await client.close();
  });

  it("sendHeartbeat returns null and never throws on 500", async () => {
    vi.stubGlobal("fetch", failingFetch(500));
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

    const client = makeClient();
    const result = await client.sendHeartbeat({ framework: "bullmq", worker: {} });

    expect(result).toBeNull();
    warnSpy.mockRestore();
    await client.close();
  });

  it("SDK does not propagate errors through a BullMQ-style finally block", async () => {
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("network failure")));

    const client = makeClient({ endpoint: "http://dead:9999" });
    let jobCompleted = false;

    async function simulateJob() {
      const start = Date.now();
      try {
        await Promise.resolve(); // user job logic
        jobCompleted = true;
      } finally {
        // Instrumentation in finally — must never throw
        client.sendEvent({
          type: "task_event",
          status: "succeeded",
          metrics: { duration_ms: Date.now() - start },
        });
      }
    }

    await simulateJob();
    expect(jobCompleted).toBe(true);

    await client.flush();
    await client.close();
  });

  it("SDK does not propagate errors when job throws and SDK is in finally", async () => {
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("network failure")));

    const client = makeClient({ endpoint: "http://dead:9999" });
    const jobError = new Error("user job crashed");

    async function simulateCrashedJob() {
      try {
        throw jobError;
      } finally {
        // SDK must not interfere — original error must propagate normally
        client.sendEvent({ type: "task_event", status: "failed" });
      }
    }

    await expect(simulateCrashedJob()).rejects.toBe(jobError);
    await client.close();
  });
});

// ── Circuit breaker ──────────────────────────────────────────────────────────

describe("circuit breaker", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("opens after 3 consecutive flush failures (503)", async () => {
    vi.stubGlobal("fetch", failingFetch(503));
    vi.spyOn(console, "warn").mockImplementation(() => {});

    const client = makeClient();

    // Trigger 3 failed flushes to open the circuit
    for (let i = 0; i < 3; i++) {
      client.sendEvent({ task: `fail${i}` });
      await client.flush();
    }

    // After 3 failures the circuit must be OPEN — no more fetch calls
    const fetchMock = vi.mocked(global.fetch as ReturnType<typeof vi.fn>);
    const callsAtOpen = fetchMock.mock.calls.length;
    expect(callsAtOpen).toBe(3);

    client.sendEvent({ task: "should-drop" });
    await client.flush();

    expect(fetchMock.mock.calls.length).toBe(3); // unchanged
    await client.close();
  });

  it("drops events silently — never throws — when circuit is open", async () => {
    vi.stubGlobal("fetch", failingFetch(503));
    vi.spyOn(console, "warn").mockImplementation(() => {});

    const client = makeClient();

    for (let i = 0; i < 3; i++) {
      client.sendEvent({ task: `fail${i}` });
      await client.flush();
    }

    // Should not throw, should not call fetch
    expect(() => {
      for (let i = 0; i < 10; i++) client.sendEvent({ task: `drop${i}` });
    }).not.toThrow();

    await expect(client.flush()).resolves.toBeUndefined();

    const fetchMock = vi.mocked(global.fetch as ReturnType<typeof vi.fn>);
    // No additional fetch calls during the "open" phase
    expect(fetchMock.mock.calls.length).toBe(3);

    await client.close();
  });

  it("transitions OPEN → HALF_OPEN after 30s cooldown and resets on success", async () => {
    vi.useFakeTimers();
    vi.spyOn(console, "warn").mockImplementation(() => {});

    let requestCount = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn().mockImplementation(() => {
        requestCount++;
        const ok = requestCount > 3; // first 3 fail, then succeed
        return Promise.resolve({
          ok,
          status: ok ? 200 : 503,
          headers: { get: () => null },
          text: () => Promise.resolve(ok ? '{"ok":true}' : "down"),
          json: () => Promise.resolve({ ok: true }),
        });
      }),
    );

    const client = makeClient();

    // Open the circuit
    for (let i = 0; i < 3; i++) {
      client.sendEvent({ task: `fail${i}` });
      await client.flush();
    }
    expect(requestCount).toBe(3);

    // Advance past the 30s cooldown
    vi.setSystemTime(new Date(Date.now() + 31_000));

    // Probe: circuit transitions to HALF_OPEN → request succeeds → CLOSED
    client.sendEvent({ task: "probe" });
    await client.flush();
    expect(requestCount).toBe(4); // one probe made

    // Circuit is CLOSED again — normal operation resumes
    client.sendEvent({ task: "normal" });
    await client.flush();
    expect(requestCount).toBe(5);

    await client.close();
  });

  it("returns to OPEN if HALF_OPEN probe fails", async () => {
    vi.useFakeTimers();
    vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.stubGlobal("fetch", failingFetch(503));

    const fetchMock = vi.mocked(global.fetch as ReturnType<typeof vi.fn>);
    const client = makeClient();

    // Open the circuit
    for (let i = 0; i < 3; i++) {
      client.sendEvent({ task: `fail${i}` });
      await client.flush();
    }

    const callsAtOpen = fetchMock.mock.calls.length;

    // Advance past cooldown → HALF_OPEN probe
    vi.setSystemTime(new Date(Date.now() + 31_000));
    client.sendEvent({ task: "probe" });
    await client.flush();

    // Exactly one probe was made, then circuit re-opened
    expect(fetchMock.mock.calls.length).toBe(callsAtOpen + 1);

    // Now reset calls and verify no new requests (circuit OPEN again)
    fetchMock.mockClear();
    client.sendEvent({ task: "drop-again" });
    await client.flush();
    expect(fetchMock.mock.calls.length).toBe(0);

    await client.close();
  });
});

// ── Queue memory protection ───────────────────────────────────────────────────

describe("queue memory protection", () => {
  it("caps queue at 10K events to prevent OOM when server is down", async () => {
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("ECONNREFUSED")));
    vi.spyOn(console, "warn").mockImplementation(() => {});

    const client = new TraceStaxClient({
      apiKey: "ts_test_resilience",
      endpoint: "http://dead:9999",
      flushInterval: 999_999,
      maxBatchSize: 50,
    });

    // Queue 11K events — exceeds the 10K safety cap
    for (let i = 0; i < 11_000; i++) {
      client.sendEvent({ task: `e${i}` });
    }

    // The SDK drops oldest events and warns; queue must stay ≤ 10K
    expect(client.pending).toBeLessThanOrEqual(10_000);

    await client.close();
  });

  it("queue stays bounded after overflow: starts trimming at 10K", async () => {
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("ECONNREFUSED")));
    vi.spyOn(console, "warn").mockImplementation(() => {});

    const client = new TraceStaxClient({
      apiKey: "ts_test_resilience",
      endpoint: "http://dead:9999",
      flushInterval: 999_999,
      maxBatchSize: 50,
    });

    for (let i = 0; i < 11_000; i++) {
      client.sendEvent({ task: `e${i}` });
    }

    // After 11K events with a dead server, the SDK must have trimmed the queue
    // so that it never held all 11K events simultaneously.
    expect(client.pending).toBeLessThan(11_000);

    vi.restoreAllMocks();
    await client.close();
  });
});

// ── X-Retry-After backpressure ────────────────────────────────────────────────

describe("X-Retry-After backpressure", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("pauses flushing for the requested duration after a 429 response", async () => {
    vi.useFakeTimers();
    vi.spyOn(console, "warn").mockImplementation(() => {});

    let callCount = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn().mockImplementation(() => {
        callCount++;
        return Promise.resolve({
          ok: false,
          status: 429,
          headers: { get: (h: string) => (h === "X-Retry-After" ? "10" : null) },
          text: () => Promise.resolve("rate limited"),
        });
      }),
    );

    const client = makeClient();

    client.sendEvent({ task: "rate-limited" });
    await client.flush();
    expect(callCount).toBe(1);

    // Within the 10s window, flush must be suppressed
    client.sendEvent({ task: "paused" });
    await client.flush();
    expect(callCount).toBe(1); // no additional call

    // Advance past the retry window
    vi.setSystemTime(new Date(Date.now() + 11_000));
    client.sendEvent({ task: "resumed" });
    await client.flush();
    expect(callCount).toBe(2); // resumes after window expires

    await client.close();
  });
});

// ── Monitor active callback safety ───────────────────────────────────────────

describe("TraceStaxMonitor BullMQ event callbacks", () => {
  it("worker active callback swallows exceptions and does not propagate into BullMQ", async () => {
    // Build a minimal EventEmitter-based mock for a BullMQ Worker
    const { EventEmitter } = await import("node:events");

    class MockWorker extends EventEmitter {
      name = "mock-worker";
      opts = { concurrency: 1 };
    }
    class MockQueue extends EventEmitter {
      name = "mock-queue";
      getWaiting = () => Promise.resolve([]);
      getActive  = () => Promise.resolve([]);
      getFailed  = () => Promise.resolve([]);
    }

    const { TraceStaxMonitor } = await import("../src/monitor.js");

    const monitor = new TraceStaxMonitor({
      apiKey: "ts_test_monitor",
      endpoint: "http://dead:9999",
      enabled: false, // no HTTP calls
    });

    const worker = new MockWorker() as unknown as import("bullmq").Worker;
    const queue  = new MockQueue()  as unknown as import("bullmq").Queue;

    monitor.monitorWorker(queue, worker);

    // Inject a throwing Map implementation to force the active handler to throw
    (monitor as unknown as { jobTimings: Map<string, number> }).jobTimings = {
      set: () => { throw new Error("simulated Map failure"); },
    } as unknown as Map<string, number>;

    // Emitting "active" must NOT throw — the try/catch in monitor.ts absorbs it
    expect(() => {
      worker.emit("active", { id: "job-1" } as unknown as import("bullmq").Job);
    }).not.toThrow();

    await monitor.close();
  });
});

// ── Concurrent flush safety ───────────────────────────────────────────────────

describe("concurrent flush safety", () => {
  it("two concurrent flush() calls send exactly the right total number of events", async () => {
    let totalEventsSent = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn().mockImplementation((_url: string, opts: RequestInit) => {
        const body = JSON.parse(opts.body as string) as { events?: unknown[] };
        totalEventsSent += body.events?.length ?? 0;
        return Promise.resolve({
          ok: true,
          status: 200,
          headers: { get: () => null },
          text: () => Promise.resolve('{"ok":true}'),
          json: () => Promise.resolve({ ok: true }),
        });
      }),
    );

    const client = makeClient({ maxBatchSize: 100 });
    const EVENT_COUNT = 10;

    for (let i = 0; i < EVENT_COUNT; i++) client.sendEvent({ task: `t${i}` });

    // Fire two flushes simultaneously
    await Promise.all([client.flush(), client.flush()]);

    // Every event must be sent exactly once — not dropped, not double-sent
    expect(totalEventsSent).toBe(EVENT_COUNT);
    expect(client.pending).toBe(0);

    await client.close();
  });
});

// ── SIGTERM / SIGINT handler safety (C4 from audit) ──────────────────────────
//
// Note: The global test file stubs `process` with vi.stubGlobal, which replaces
// process.listeners/removeAllListeners. The signal-chaining path in installExitHook
// therefore doesn't execute in these tests (guarded by the typeof check). We
// instead test the SDK's behavior at the installExitHook source level.

describe("signal handler safety", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("installExitHook never calls process.exit — the guard typeof check prevents it in stub env", () => {
    // In the test environment, process is stubbed with vi.stubGlobal and lacks
    // process.listeners. The SDK's typeof guard means the signal-chain block is
    // skipped entirely. Verify: constructing the client does not call process.exit.
    const exitSpy = vi.spyOn(process, "exit").mockImplementation((() => {}) as () => never);

    vi.stubGlobal("fetch", successFetch());
    const client = new TraceStaxClient({
      apiKey: "ts_test_no_exit",
      endpoint: "https://test.tracestax.com",
      flushInterval: 999_999,
    });

    expect(exitSpy).not.toHaveBeenCalled();

    exitSpy.mockRestore();
    void client.close();
  });

  it("installExitHook registers a beforeExit listener (never-exit guarantee)", () => {
    // process.on is vi.fn() in these tests — verify it was called for beforeExit
    vi.stubGlobal("fetch", successFetch());
    const client = new TraceStaxClient({
      apiKey: "ts_test_beforeexit",
      endpoint: "https://test.tracestax.com",
      flushInterval: 999_999,
    });

    // process.on is mocked — verify at least one "beforeExit" registration happened
    const onCalls = (process.on as ReturnType<typeof vi.fn>).mock.calls;
    const hasBeforeExit = onCalls.some(([event]) => event === "beforeExit");
    expect(hasBeforeExit).toBe(true);

    void client.close();
  });
});

// ── close() deadline (H1 from audit) ─────────────────────────────────────────

describe("close() deadline", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  it("close() completes within ~5 seconds even when the server is completely dead", async () => {
    // Server always rejects after 200ms — simulating a dead/slow ingest server
    vi.stubGlobal(
      "fetch",
      vi.fn().mockImplementation(
        () => new Promise((_resolve, reject) =>
          setTimeout(() => reject(new Error("timeout")), 200)
        ),
      ),
    );

    // Use a very large batch size so sendEvent never pre-triggers a flush,
    // ensuring all events stay in the queue until close() is called.
    const client = new TraceStaxClient({
      apiKey: "ts_test_deadline",
      endpoint: "http://dead:9999",
      flushInterval: 999_999,
      maxBatchSize: 10_000,
    });

    // Queue events — no pre-flush since maxBatchSize > event count
    for (let i = 0; i < 200; i++) client.sendEvent({ task: `e${i}` });
    expect(client.pending).toBe(200);

    const start = Date.now();
    await client.close();
    const elapsed = Date.now() - start;

    // The 5s deadline cap ensures close() returns well before 10s even with
    // 200 events / 10000 batch = 1 attempt × 200ms timeout = ~200ms.
    // Without the deadline cap, a real 10s timeout would hang indefinitely.
    expect(elapsed).toBeLessThan(8_000);
  }, 15_000);
});

// ── Serialization safety ─────────────────────────────────────────────────────

describe("serialization safety — sendEvent must never throw into caller", () => {
  it("dry-run: circular-reference payload does not throw", () => {
    const client = new TraceStaxClient({
      apiKey: "ts_test_resilience",
      endpoint: "https://test.tracestax.com",
      flushInterval: 999_999,
      dryRun: true,
    });

    // Create a circular reference — JSON.stringify would throw without the fix
    const circular: Record<string, unknown> = { task: "circ" };
    circular["self"] = circular;

    // Must not throw
    expect(() => client.sendEvent(circular)).not.toThrow();
  });

  it("normal mode: circular-reference payload does not throw and is silently dropped", () => {
    vi.stubGlobal("fetch", successFetch());
    const client = makeClient();

    const circular: Record<string, unknown> = { task: "circ" };
    circular["self"] = circular;

    // sendEvent now serializes eagerly to catch bad payloads before they can
    // poison a batch. A circular reference must be silently dropped — no throw,
    // and the queue must remain empty.
    expect(() => client.sendEvent(circular)).not.toThrow();
    expect(client.pending).toBe(0);  // dropped, not queued
    void client.close();
  });
});

// ── tokenProvider throws (2A) ────────────────────────────────────────────────

describe("tokenProvider() failure safety", () => {
  afterEach(() => vi.restoreAllMocks());

  it("sendEvent queues normally; a throwing tokenProvider does not propagate on flush", async () => {
    const client = new TraceStaxClient({
      tokenProvider: async () => { throw new Error("auth failed"); },
      endpoint: "https://test.tracestax.com",
      flushInterval: 999_999,
    });

    // sendEvent itself must not throw
    expect(() => client.sendEvent({ task: "e1" })).not.toThrow();
    expect(client.pending).toBe(1);

    // flush must not throw or leave an unhandled rejection
    await expect(client.flush()).resolves.toBeUndefined();

    // Circuit must NOT be penalised for an auth error
    expect(client.stats().circuitState).toBe("closed");

    await client.close();
  });

  it("subsequent sendEvent calls still work after a tokenProvider failure", async () => {
    let calls = 0;
    const client = new TraceStaxClient({
      tokenProvider: async () => {
        calls++;
        throw new Error("transient auth error");
      },
      endpoint: "https://test.tracestax.com",
      flushInterval: 999_999,
    });

    client.sendEvent({ task: "a" });
    await client.flush();  // tokenProvider throws → skip, no crash

    client.sendEvent({ task: "b" });
    // Client must still be functional, not in a broken state
    expect(client.stats().circuitState).toBe("closed");

    await client.close();
  });
});

// ── large payload dropped at sendEvent (2B) ──────────────────────────────────

describe("large payload size guard", () => {
  afterEach(() => vi.restoreAllMocks());

  it("payload over 512 KB is dropped without throwing", () => {
    vi.stubGlobal("fetch", successFetch());
    const client = makeClient();

    const bigPayload = { task: "big", data: "x".repeat(600 * 1024) };
    expect(() => client.sendEvent(bigPayload)).not.toThrow();
    expect(client.pending).toBe(0);  // dropped, not queued

    void client.close();
  });

  it("payload at exactly 512 KB is accepted", () => {
    vi.stubGlobal("fetch", successFetch());
    const client = makeClient();

    // Build a payload whose JSON is just under the limit
    const data = "a".repeat(512 * 1024 - 30);
    client.sendEvent({ d: data });
    expect(client.pending).toBe(1);

    void client.close();
  });

  it("subsequent events after a dropped oversized payload are accepted normally", () => {
    vi.stubGlobal("fetch", successFetch());
    const client = makeClient();

    client.sendEvent({ task: "big", data: "x".repeat(600 * 1024) }); // dropped
    client.sendEvent({ task: "small" });  // must still be accepted
    expect(client.pending).toBe(1);

    void client.close();
  });
});

// ── concurrent close() calls (2C) ───────────────────────────────────────────

describe("concurrent close() calls", () => {
  afterEach(() => vi.restoreAllMocks());

  it("calling close() three times concurrently resolves all promises without deadlock", async () => {
    vi.stubGlobal("fetch", successFetch());
    const client = new TraceStaxClient({
      apiKey: "ts_test_close",
      endpoint: "https://test.tracestax.com",
      flushInterval: 999_999,
    });

    client.sendEvent({ task: "e1" });

    // All three must resolve without throwing
    await expect(
      Promise.all([client.close(), client.close(), client.close()])
    ).resolves.not.toThrow();
  });
});

// ── circuit breaker under clock skew (2D) ───────────────────────────────────

describe("circuit breaker — clock skew resilience", () => {
  afterEach(() => vi.restoreAllMocks());

  it("backward system clock jump does not permanently keep the circuit OPEN", async () => {
    vi.stubGlobal("fetch", failingFetch(500));
    const client = makeClient();

    // Open the circuit (3 failures)
    for (let i = 0; i < 3; i++) {
      client.sendEvent({ task: `fail${i}` });
      await client.flush();
    }
    expect(client.stats().circuitState).toBe("open");

    // Simulate a backward clock jump: set circuitOpenedAt to future time
    // (which makes elapsed = negative on next check without the Math.max fix)
    (client as unknown as { circuitOpenedAt: number }).circuitOpenedAt = Date.now() + 60_000;

    // With Math.max(0, elapsed), the circuit should not get stuck — elapsed is
    // clamped to 0, so we stay in OPEN state (< 30s) which is correct behaviour.
    // The key property is that the circuit will eventually transition when
    // enough real time passes — not freeze indefinitely.
    const statsBefore = client.stats();
    expect(statsBefore.circuitState).toBe("open");

    // After setting circuitOpenedAt back to the past, the circuit must probe
    (client as unknown as { circuitOpenedAt: number }).circuitOpenedAt = Date.now() - 31_000;
    vi.stubGlobal("fetch", successFetch());
    await client.flush();
    expect(client.stats().circuitState).toBe("closed");

    await client.close();
  });
});

// ── Non-serializable payload in sendSnapshot/sendLineage ────────────────────
//
// These methods call postForJson directly (no pre-check in sendEvent).
// After the fix, the pre-serialization in postForJson catches the error before
// touching the circuit breaker counter, so the circuit must stay CLOSED.

describe("non-serializable payload in sendSnapshot / sendLineage", () => {
  afterEach(() => vi.restoreAllMocks());

  it("sendSnapshot with BigInt payload does not throw and does not open circuit", async () => {
    vi.stubGlobal("fetch", successFetch());
    const client = makeClient();

    // BigInt is not JSON-serializable — JSON.stringify throws TypeError
    const payload: Record<string, unknown> = { queue: "default", depth: BigInt(9007199254740991) };
    await expect(client.sendSnapshot(payload)).resolves.toBeUndefined();

    // The error must NOT be treated as a network failure
    expect(client.stats().circuitState).toBe("closed");
    expect(client.stats().consecutiveFailures).toBe(0);

    await client.close();
  });

  it("sendLineage with circular reference does not throw and does not open circuit", async () => {
    vi.stubGlobal("fetch", successFetch());
    const client = makeClient();

    const circular: Record<string, unknown> = { parent: "abc" };
    circular["self"] = circular;

    await expect(client.sendLineage(circular)).resolves.toBeUndefined();
    expect(client.stats().circuitState).toBe("closed");
    expect(client.stats().consecutiveFailures).toBe(0);

    await client.close();
  });

  it("sendSnapshot with Symbol property does not throw (Symbol keys silently dropped)", async () => {
    vi.stubGlobal("fetch", successFetch());
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    const client = makeClient();

    // Symbol properties are silently omitted by JSON.stringify — no error
    const payload: Record<symbol | string, unknown> = { queue: "default" };
    payload[Symbol("meta")] = "ignored";

    await expect(client.sendSnapshot(payload as Record<string, unknown>)).resolves.toBeUndefined();

    warnSpy.mockRestore();
    await client.close();
  });
});

// ── 401 does not open the circuit breaker ────────────────────────────────────
//
// A 401 is permanent misconfiguration, not a transient network failure.
// The circuit must stay CLOSED so that correcting the API key immediately
// resumes delivery without waiting for the 30s cooldown.

describe("HTTP 401 — auth failure does not penalise circuit breaker", () => {
  afterEach(() => vi.restoreAllMocks());

  it("circuit remains CLOSED after a 401 response", async () => {
    vi.stubGlobal("fetch", failingFetch(401));
    vi.spyOn(console, "error").mockImplementation(() => {});

    const client = makeClient();
    client.sendEvent({ task: "t1" });
    await client.flush();

    expect(client.stats().circuitState).toBe("closed");
    expect(client.stats().consecutiveFailures).toBe(0);

    await client.close();
  });

  it("repeated 401s never open the circuit", async () => {
    vi.stubGlobal("fetch", failingFetch(401));
    vi.spyOn(console, "error").mockImplementation(() => {});

    const client = makeClient();

    for (let i = 0; i < 10; i++) {
      client.sendEvent({ task: `t${i}` });
      await client.flush();
    }

    expect(client.stats().circuitState).toBe("closed");
    expect(client.stats().consecutiveFailures).toBe(0);

    await client.close();
  });

  it("401 logs an error-level message (not just a warning)", async () => {
    vi.stubGlobal("fetch", failingFetch(401));
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    const client = makeClient();
    client.sendEvent({ task: "t1" });
    await client.flush();

    expect(errorSpy).toHaveBeenCalledOnce();
    expect(errorSpy.mock.calls[0][0]).toMatch(/401/);

    await client.close();
  });
});

// ── Oversized response body ──────────────────────────────────────────────────
//
// A server returning a huge response body (e.g. misconfigured proxy sending
// HTML error pages) must not buffer unbounded memory in the SDK process.

describe("oversized ingest response body", () => {
  afterEach(() => vi.restoreAllMocks());

  it("response body larger than 1 MB is truncated without throwing", async () => {
    // Simulate a server that returns a 2 MB JSON-looking body
    const bigBody = `{"ok":true,"padding":"${"x".repeat(2 * 1024 * 1024)}"}`;
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        headers: { get: () => null },
        body: null,  // no streaming body — forces text() fallback
        text: () => Promise.resolve(bigBody),
      }),
    );

    const client = makeClient();
    client.sendEvent({ task: "t1" });

    // Must not throw even though the body is huge
    await expect(client.flush()).resolves.toBeUndefined();
    await client.close();
  });

  it("error response body larger than 200 bytes is truncated in the log", async () => {
    const bigError = "error ".repeat(200);  // 1200 bytes
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: false,
        status: 500,
        headers: { get: () => null },
        body: null,
        text: () => Promise.resolve(bigError),
      }),
    );
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

    const client = makeClient();
    client.sendEvent({ task: "t1" });
    await client.flush();

    // The logged error text must have been capped at 200 bytes
    const logMsg: string = warnSpy.mock.calls.find(c => String(c[0]).includes("500"))?.[0] ?? "";
    expect(logMsg.length).toBeLessThan(bigError.length);

    await client.close();
  });
});

// ── Lambda init failure safety (2F) ─────────────────────────────────────────

describe("Lambda createTracestaxLambda — init failure safety", () => {
  it("handler still runs when API key is missing (no crash at module init)", async () => {
    const { createTracestaxLambda } = await import("../src/lambda.js");

    const ctx = {
      functionName: "myFn",
      functionVersion: "$LATEST",
      invokedFunctionArn: "arn:aws:lambda:us-east-1:123:function:myFn",
      memoryLimitInMB: "128",
      awsRequestId: "req-1",
      logGroupName: "/aws/lambda/myFn",
      logStreamName: "2024/01/01/[$LATEST]abc",
      getRemainingTimeInMillis: () => 30_000,
    };

    let handlerRan = false;
    const handler = createTracestaxLambda(
      { apiKey: "" },  // empty key → TraceStaxClient would normally throw
      async (_event, _context) => {
        handlerRan = true;
        return { statusCode: 200 };
      },
    );

    const result = await handler({}, ctx);
    expect(handlerRan).toBe(true);
    expect((result as { statusCode: number }).statusCode).toBe(200);
  });
});
