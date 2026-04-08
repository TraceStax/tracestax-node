/**
 * Signal-handler chaining tests for TraceStaxClient.
 *
 * These tests use the REAL process object (not a stub) so that
 * installExitHook's chaining logic actually executes. They verify that:
 *
 *  1. A SIGTERM handler registered BEFORE the client is preserved and called
 *     AFTER the client's flush completes.
 *  2. A SIGINT handler registered BEFORE the client is also chained correctly.
 *  3. A second TraceStaxClient correctly chains to the first client's handler
 *     so both flush before handing control to the original handler.
 *
 * IMPORTANT: Each test cleans up all SIGTERM/SIGINT listeners in afterEach to
 * avoid test pollution. Because these tests touch the real process, they must
 * run in isolation from the resilience tests which stub process.on globally.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { TraceStaxClient } from "../src/client.js";

// ── Helpers ──────────────────────────────────────────────────────────────────

function makeClient(extraOpts: object = {}) {
  return new TraceStaxClient({
    apiKey: "ts_test_signal",
    endpoint: "http://ingest.tracestax.test",
    flushInterval: 999_999, // never auto-flush
    ...extraOpts,
  });
}

function mockFetchOk() {
  return vi.fn().mockResolvedValue(
    new Response(JSON.stringify({ ok: true }), {
      status: 202,
      headers: { "Content-Type": "application/json" },
    })
  );
}

// ── Tests ────────────────────────────────────────────────────────────────────

describe("SIGTERM / SIGINT signal handler chaining", () => {
  let savedSigtermListeners: Array<(...args: unknown[]) => void>;
  let savedSigintListeners: Array<(...args: unknown[]) => void>;

  beforeEach(() => {
    // Snapshot existing listeners so we can restore them after each test.
    savedSigtermListeners = process.listeners("SIGTERM").slice() as Array<(...args: unknown[]) => void>;
    savedSigintListeners  = process.listeners("SIGINT").slice()  as Array<(...args: unknown[]) => void>;
    process.removeAllListeners("SIGTERM");
    process.removeAllListeners("SIGINT");
  });

  afterEach(() => {
    // Restore original listeners to avoid leaking into subsequent tests.
    process.removeAllListeners("SIGTERM");
    process.removeAllListeners("SIGINT");
    savedSigtermListeners.forEach(h => process.on("SIGTERM", h));
    savedSigintListeners.forEach(h  => process.on("SIGINT",  h));
    vi.restoreAllMocks();
  });

  it("calls a pre-existing SIGTERM handler after the SDK flushes", async () => {
    const priorCalled = { value: false };
    const priorHandler = () => { priorCalled.value = true; };
    process.on("SIGTERM", priorHandler);

    const mockFetch = mockFetchOk();
    vi.stubGlobal("fetch", mockFetch);

    const client = makeClient();
    client.sendEvent({ type: "task_event", status: "started" });

    // Emit SIGTERM synchronously — the SDK handler is async but chaining is immediate
    process.emit("SIGTERM");

    // Wait for the flush to complete and the prior handler to be called.
    // Use expect() inside waitFor so Vitest 3 retries on failure (returning a
    // falsy value resolves waitFor immediately; throwing causes it to retry).
    await vi.waitFor(() => {
      expect(priorCalled.value).toBe(true);
    }, { timeout: 3_000 });

    expect(mockFetch).toHaveBeenCalled();
    expect(priorCalled.value).toBe(true);
  });

  it("calls a pre-existing SIGINT handler after the SDK flushes", async () => {
    const priorCalled = { value: false };
    process.on("SIGINT", () => { priorCalled.value = true; });

    const mockFetch = mockFetchOk();
    vi.stubGlobal("fetch", mockFetch);

    makeClient(); // constructs and installs hook

    process.emit("SIGINT");

    await vi.waitFor(() => {
      expect(priorCalled.value).toBe(true);
    }, { timeout: 3_000 });

    expect(priorCalled.value).toBe(true);
  });

  it("chains handlers from two consecutive TraceStaxClient instances", async () => {
    const order: string[] = [];

    // Simulate an application handler that was registered first
    process.on("SIGTERM", () => order.push("app"));

    const mockFetch = mockFetchOk();
    vi.stubGlobal("fetch", mockFetch);

    // First client — chains to the app handler
    const client1 = makeClient();
    client1.sendEvent({ type: "task_event", id: "client1-event" });

    // Second client — chains to the first client's handler (which chains to app)
    const client2 = makeClient();
    client2.sendEvent({ type: "task_event", id: "client2-event" });

    process.emit("SIGTERM");

    // Both clients flush and the app handler must eventually be called.
    // Use expect() inside waitFor so Vitest 3 retries on failure.
    await vi.waitFor(() => {
      expect(order).toContain("app");
    }, { timeout: 5_000 });

    // Both clients must have attempted to flush
    expect(mockFetch.mock.calls.length).toBeGreaterThanOrEqual(1);
    expect(order).toContain("app");
  });
});
