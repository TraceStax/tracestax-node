/**
 * Vitest unit tests for TraceStaxClient: batching, flush, close.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { TraceStaxClient } from "../src/client.js";

// Stub global fetch
const mockFetch = vi.fn().mockResolvedValue({
  ok: true,
  status: 200,
  headers: { get: () => null },
  text: () => Promise.resolve('{"ok":true}'),
  json: () => Promise.resolve({ ok: true }),
});

vi.stubGlobal("fetch", mockFetch);

// Prevent exit hooks from interfering with tests
vi.stubGlobal("process", {
  ...process,
  on: vi.fn(),
  pid: 12345,
  exit: vi.fn(),
});

describe("TraceStaxClient", () => {
  let client: TraceStaxClient;

  beforeEach(() => {
    mockFetch.mockClear();
    client = new TraceStaxClient({
      apiKey: "ts_test_abc",
      endpoint: "https://test.tracestax.com",
      flushInterval: 60_000, // long interval — we flush manually
      maxBatchSize: 10,
    });
  });

  afterEach(async () => {
    await client.close();
  });

  describe("constructor", () => {
    it("throws if apiKey is empty", () => {
      expect(() => new TraceStaxClient({ apiKey: "" })).toThrow(
        "apiKey or tokenProvider is required",
      );
    });

    it("strips trailing slash from endpoint", () => {
      const c = new TraceStaxClient({
        apiKey: "ts_test_x",
        endpoint: "https://example.com/",
        flushInterval: 999_999,
      });
      // Verify by flushing an event and checking the URL
      c.sendEvent({ test: true });
      c.flush().then(() => {
        const url = mockFetch.mock.calls[mockFetch.mock.calls.length - 1][0];
        expect(url).toBe("https://example.com/v1/ingest");
      });
      c.close();
    });
  });

  describe("sendEvent", () => {
    it("queues events without calling fetch immediately", () => {
      client.sendEvent({ task: "add" });
      client.sendEvent({ task: "mul" });

      expect(client.pending).toBe(2);
      expect(mockFetch).not.toHaveBeenCalled();
    });
  });

  describe("flush", () => {
    it("sends all queued events in a single batch", async () => {
      for (let i = 0; i < 5; i++) {
        client.sendEvent({ task: `t${i}` });
      }

      await client.flush();

      expect(mockFetch).toHaveBeenCalledTimes(1);
      const [url, opts] = mockFetch.mock.calls[0];
      expect(url).toBe("https://test.tracestax.com/v1/ingest");
      expect(opts.method).toBe("POST");

      const body = JSON.parse(opts.body);
      expect(body.events).toHaveLength(5);
      expect(client.pending).toBe(0);
    });

    it("splits into multiple batches when exceeding maxBatchSize", async () => {
      for (let i = 0; i < 25; i++) {
        client.sendEvent({ task: `t${i}` });
      }

      await client.flush();

      // 25 events / 10 per batch = 3 batches (10 + 10 + 5)
      expect(mockFetch).toHaveBeenCalledTimes(3);

      const batch1 = JSON.parse(mockFetch.mock.calls[0][1].body);
      const batch2 = JSON.parse(mockFetch.mock.calls[1][1].body);
      const batch3 = JSON.parse(mockFetch.mock.calls[2][1].body);

      expect(batch1.events).toHaveLength(10);
      expect(batch2.events).toHaveLength(10);
      expect(batch3.events).toHaveLength(5);
    });

    it("is a no-op when queue is empty", async () => {
      await client.flush();
      expect(mockFetch).not.toHaveBeenCalled();
    });
  });

  describe("close", () => {
    it("flushes remaining events", async () => {
      client.sendEvent({ task: "final" });
      await client.close();

      expect(mockFetch).toHaveBeenCalledTimes(1);
      const body = JSON.parse(mockFetch.mock.calls[0][1].body);
      expect(body.events[0].task).toBe("final");
    });

    it("is idempotent", async () => {
      await client.close();
      await client.close(); // should not throw
    });

    it("drops events sent after close", async () => {
      await client.close();
      client.sendEvent({ task: "dropped" });
      expect(client.pending).toBe(0);
    });
  });

  describe("direct send methods", () => {
    it("sendHeartbeat posts to /v1/heartbeat", async () => {
      await client.sendHeartbeat({ framework: "bullmq", worker: {} });
      const url = mockFetch.mock.calls[0][0];
      expect(url).toBe("https://test.tracestax.com/v1/heartbeat");
    });

    it("sendSnapshot posts to /v1/snapshot", async () => {
      await client.sendSnapshot({ queues: [] });
      const url = mockFetch.mock.calls[0][0];
      expect(url).toBe("https://test.tracestax.com/v1/snapshot");
    });

    it("sendLineage posts to /v1/lineage", async () => {
      await client.sendLineage({
        parent_id: "abc",
        children: [],
        chain_id: "c1",
      });
      const url = mockFetch.mock.calls[0][0];
      expect(url).toBe("https://test.tracestax.com/v1/lineage");
    });
  });

  describe("auth header", () => {
    it("sends Bearer token in Authorization header", async () => {
      client.sendEvent({ task: "auth-test" });
      await client.flush();

      const opts = mockFetch.mock.calls[0][1];
      expect(opts.headers.Authorization).toBe("Bearer ts_test_abc");
    });
  });

  describe("error handling", () => {
    it("does not throw when fetch fails", async () => {
      mockFetch.mockRejectedValueOnce(new Error("network down"));

      client.sendEvent({ task: "err" });
      // Should not throw
      await client.flush();
    });

    it("warns on non-ok response", async () => {
      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 429,
        text: () => Promise.resolve("rate limited"),
      });

      client.sendEvent({ task: "rate" });
      await client.flush();

      expect(warnSpy).toHaveBeenCalled();
      warnSpy.mockRestore();
    });
  });

  describe("circuit breaker", () => {
    beforeEach(() => {
      // Return 503 for all requests in this suite
      mockFetch.mockResolvedValue({
        ok: false,
        status: 503,
        headers: { get: () => null },
        text: () => Promise.resolve("service unavailable"),
      });
    });

    it("opens after 3 consecutive failures", async () => {
      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

      for (let i = 0; i < 3; i++) {
        client.sendEvent({ task: `fail${i}` });
        await client.flush();
      }

      const callsAtOpen = mockFetch.mock.calls.length;
      expect(callsAtOpen).toBe(3);

      // Circuit open — next flush must not call fetch
      client.sendEvent({ task: "drop" });
      await client.flush();

      expect(mockFetch).toHaveBeenCalledTimes(3);
      warnSpy.mockRestore();
    });

    it("does not throw when circuit is open and events are queued", async () => {
      vi.spyOn(console, "warn").mockImplementation(() => {});

      // Open the circuit
      for (let i = 0; i < 3; i++) {
        client.sendEvent({ task: `fail${i}` });
        await client.flush();
      }

      // Must not throw
      expect(() => {
        for (let i = 0; i < 10; i++) client.sendEvent({ task: `drop${i}` });
      }).not.toThrow();

      await expect(client.flush()).resolves.toBeUndefined();
    });
  });
});
