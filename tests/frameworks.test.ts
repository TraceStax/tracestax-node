/**
 * Integration tests that simulate real framework usage against mock-ingest.
 * Verifies that each framework integration sends correctly-shaped events.
 */

import { describe, it, expect, beforeEach } from "vitest";

const INGEST_URL = process.env.TRACESTAX_INGEST_URL || "http://localhost:4001";

async function resetIngest(): Promise<void> {
  await fetch(`${INGEST_URL}/test/reset`, { method: "POST" });
}

async function fetchEvents(): Promise<Record<string, unknown>[]> {
  const res = await fetch(`${INGEST_URL}/test/events`);
  return res.json() as Promise<Record<string, unknown>[]>;
}

async function ingestAvailable(): Promise<boolean> {
  try {
    const res = await fetch(`${INGEST_URL}/test/health`, {
      signal: AbortSignal.timeout(2000),
    });
    return res.status === 200;
  } catch {
    return false;
  }
}

async function waitForEvents(
  predicate: (e: Record<string, unknown>) => boolean,
  timeout = 10_000,
): Promise<Record<string, unknown>[]> {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const events = await fetchEvents();
    if (events.some(predicate)) return events;
    await new Promise((r) => setTimeout(r, 250));
  }
  return fetchEvents();
}

function getTask(e: Record<string, unknown>): Record<string, unknown> {
  return (e.task as Record<string, unknown>) ?? {};
}

function getError(e: Record<string, unknown>): Record<string, unknown> {
  return (e.error as Record<string, unknown>) ?? {};
}

function getMetrics(e: Record<string, unknown>): Record<string, unknown> {
  return (e.metrics as Record<string, unknown>) ?? {};
}

// ── BullMQ simulation ─────────────────────────────────────────────────

describe("BullMQ simulation", async () => {
  const available = await ingestAvailable();
  if (!available) {
    it.skip("mock-ingest not available", () => {});
    return;
  }

  beforeEach(async () => {
    await resetIngest();
  });

  it("sends a succeeded task event", async () => {
    const { TraceStaxClient } = await import("../src/client.js");
    const client = new TraceStaxClient({
      apiKey: "ts_test_abc",
      endpoint: INGEST_URL,
      flushInterval: 60_000,
    });

    client.sendEvent({
      type: "task_event",
      framework: "bullmq",
      language: "typescript",
      sdk_version: "0.1.0",
      status: "succeeded",
      worker: { key: "bullmq@test:1", hostname: "test", pid: 1, concurrency: 5, queues: ["orders"] },
      task: { name: "processOrder", id: "bullmq-sim-001", queue: "orders", attempt: 1 },
      metrics: { duration_ms: 250 },
    });

    await client.flush();

    const events = await waitForEvents((e) => getTask(e).id === "bullmq-sim-001");
    const match = events.filter((e) => getTask(e).id === "bullmq-sim-001");
    expect(match.length).toBeGreaterThanOrEqual(1);
    expect(match[0].framework).toBe("bullmq");
    expect(match[0].status).toBe("succeeded");
    expect(getMetrics(match[0]).duration_ms).toBe(250);

    await client.close();
  });

  it("sends a failed task event with error", async () => {
    const { TraceStaxClient } = await import("../src/client.js");
    const client = new TraceStaxClient({
      apiKey: "ts_test_abc",
      endpoint: INGEST_URL,
      flushInterval: 60_000,
    });

    client.sendEvent({
      type: "task_event",
      framework: "bullmq",
      language: "typescript",
      sdk_version: "0.1.0",
      status: "failed",
      worker: { key: "bullmq@test:1", hostname: "test", pid: 1, concurrency: 5, queues: ["orders"] },
      task: { name: "processOrder", id: "bullmq-sim-002", queue: "orders", attempt: 3 },
      metrics: { duration_ms: 15 },
      error: { type: "TypeError", message: "Cannot read property of undefined" },
    });

    await client.flush();

    const events = await waitForEvents((e) => getTask(e).id === "bullmq-sim-002");
    const match = events.filter((e) => getTask(e).id === "bullmq-sim-002");
    expect(match.length).toBeGreaterThanOrEqual(1);
    expect(match[0].status).toBe("failed");
    expect(getError(match[0]).type).toBe("TypeError");

    await client.close();
  });

  it("sends a stalled task event", async () => {
    const { TraceStaxClient } = await import("../src/client.js");
    const client = new TraceStaxClient({
      apiKey: "ts_test_abc",
      endpoint: INGEST_URL,
      flushInterval: 60_000,
    });

    client.sendEvent({
      type: "task_event",
      framework: "bullmq",
      language: "typescript",
      sdk_version: "0.1.0",
      status: "stalled",
      worker: { key: "bullmq@test:1", hostname: "test", pid: 1, concurrency: 5, queues: ["orders"] },
      task: { name: "unknown", id: "bullmq-sim-003", queue: "orders", attempt: 1 },
      metrics: { duration_ms: 0 },
    });

    await client.flush();

    const events = await waitForEvents((e) => getTask(e).id === "bullmq-sim-003");
    const match = events.filter((e) => getTask(e).id === "bullmq-sim-003");
    expect(match.length).toBeGreaterThanOrEqual(1);
    expect(match[0].status).toBe("stalled");

    await client.close();
  });
});

// ── Bull (legacy) simulation ──────────────────────────────────────────

describe("Bull (legacy) simulation", async () => {
  const available = await ingestAvailable();
  if (!available) {
    it.skip("mock-ingest not available", () => {});
    return;
  }

  beforeEach(async () => {
    await resetIngest();
  });

  it("sends a succeeded task event", async () => {
    const { TraceStaxClient } = await import("../src/client.js");
    const client = new TraceStaxClient({
      apiKey: "ts_test_abc",
      endpoint: INGEST_URL,
      flushInterval: 60_000,
    });

    client.sendEvent({
      type: "task_event",
      framework: "bull",
      language: "typescript",
      sdk_version: "0.1.0",
      status: "succeeded",
      worker: { key: "bull@test:1", hostname: "test", pid: 1, concurrency: 1, queues: ["emails"] },
      task: { name: "sendEmail", id: "bull-sim-001", queue: "emails", attempt: 1 },
      metrics: { duration_ms: 180 },
    });

    await client.flush();

    const events = await waitForEvents((e) => getTask(e).id === "bull-sim-001");
    const match = events.filter((e) => getTask(e).id === "bull-sim-001");
    expect(match.length).toBeGreaterThanOrEqual(1);
    expect(match[0].framework).toBe("bull");
    expect(match[0].status).toBe("succeeded");

    await client.close();
  });
});

// ── Lambda wrapper integration ────────────────────────────────────────

describe("Lambda wrapper", async () => {
  const available = await ingestAvailable();
  if (!available) {
    it.skip("mock-ingest not available", () => {});
    return;
  }

  beforeEach(async () => {
    await resetIngest();
  });

  it("reports succeeded handler invocation", async () => {
    const { TraceStaxClient } = await import("../src/client.js");
    const { tracestaxLambda } = await import("../src/lambda.js");

    const client = new TraceStaxClient({
      apiKey: "ts_test_abc",
      endpoint: INGEST_URL,
      flushInterval: 60_000,
    });

    const wrap = tracestaxLambda(client);
    const handler = wrap(async (_event: unknown, _context: any) => {
      return { statusCode: 200, body: "OK" };
    });

    const fakeContext = {
      functionName: "node-process-orders",
      functionVersion: "$LATEST",
      invokedFunctionArn: "arn:aws:lambda:us-east-1:123:function:node-process-orders",
      memoryLimitInMB: "128",
      awsRequestId: "lambda-node-001",
      logGroupName: "/aws/lambda/test",
      logStreamName: "2024/01/01/[$LATEST]abc",
      getRemainingTimeInMillis: () => 30000,
    };

    const result = await handler({}, fakeContext);
    expect(result.statusCode).toBe(200);

    await client.flush();
    await new Promise((r) => setTimeout(r, 500));

    const events = await fetchEvents();
    const lambdaEvents = events.filter(
      (e) => getTask(e).name === "node-process-orders",
    );
    expect(lambdaEvents.length).toBeGreaterThanOrEqual(1);
    expect(lambdaEvents.some((e) => e.status === "succeeded")).toBe(true);
    expect(lambdaEvents[0].framework).toBe("lambda");

    await client.close();
  });

  it("reports failed handler invocation", async () => {
    const { TraceStaxClient } = await import("../src/client.js");
    const { tracestaxLambda } = await import("../src/lambda.js");

    const client = new TraceStaxClient({
      apiKey: "ts_test_abc",
      endpoint: INGEST_URL,
      flushInterval: 60_000,
    });

    const wrap = tracestaxLambda(client);
    const handler = wrap(async () => {
      throw new Error("handler crashed");
    });

    const fakeContext = {
      functionName: "node-validate-input",
      functionVersion: "$LATEST",
      invokedFunctionArn: "arn:aws:lambda:us-east-1:123:function:node-validate-input",
      memoryLimitInMB: "128",
      awsRequestId: "lambda-node-002",
      logGroupName: "/aws/lambda/test",
      logStreamName: "2024/01/01/[$LATEST]def",
      getRemainingTimeInMillis: () => 30000,
    };

    await expect(handler({}, fakeContext)).rejects.toThrow("handler crashed");

    await client.flush();
    await new Promise((r) => setTimeout(r, 500));

    const events = await fetchEvents();
    const failed = events.filter(
      (e) =>
        e.status === "failed" &&
        getTask(e).name === "node-validate-input",
    );
    expect(failed.length).toBeGreaterThanOrEqual(1);
    expect(getError(failed[0]).type).toBe("Error");

    await client.close();
  });
});

// ── Cloud Functions wrapper integration ───────────────────────────────

describe("Cloud Functions wrapper", async () => {
  const available = await ingestAvailable();
  if (!available) {
    it.skip("mock-ingest not available", () => {});
    return;
  }

  beforeEach(async () => {
    await resetIngest();
  });

  it("reports succeeded HTTP function", async () => {
    const { TraceStaxClient } = await import("../src/client.js");
    const { tracestaxCloudFunction } = await import("../src/cloud-functions.js");

    const client = new TraceStaxClient({
      apiKey: "ts_test_abc",
      endpoint: INGEST_URL,
      flushInterval: 60_000,
    });

    const wrap = tracestaxCloudFunction(client);
    let sentBody: unknown = null;
    const handler = wrap("myHttpFunction", async (_req: any, res: any) => {
      sentBody = "hello";
      res.send("hello");
    });

    const fakeReq = { method: "GET", url: "/test", headers: {} };
    const fakeRes = {
      status: () => fakeRes,
      send: () => {},
      json: () => {},
    };

    await handler(fakeReq, fakeRes as any);

    await client.flush();
    await new Promise((r) => setTimeout(r, 500));

    const events = await fetchEvents();
    const cfEvents = events.filter(
      (e) => e.framework === "cloud-functions",
    );
    expect(cfEvents.length).toBeGreaterThanOrEqual(1);
    expect(cfEvents.some((e) => e.status === "succeeded")).toBe(true);

    await client.close();
  });

  it("reports failed HTTP function", async () => {
    const { TraceStaxClient } = await import("../src/client.js");
    const { tracestaxCloudFunction } = await import("../src/cloud-functions.js");

    const client = new TraceStaxClient({
      apiKey: "ts_test_abc",
      endpoint: INGEST_URL,
      flushInterval: 60_000,
    });

    const wrap = tracestaxCloudFunction(client);
    const handler = wrap("myBrokenFunction", async () => {
      throw new TypeError("null reference");
    });

    const fakeReq = { method: "POST", url: "/crash", headers: {} };
    const fakeRes = {
      status: () => fakeRes,
      send: () => {},
      json: () => {},
    };

    await expect(handler(fakeReq, fakeRes as any)).rejects.toThrow("null reference");

    await client.flush();
    await new Promise((r) => setTimeout(r, 500));

    const events = await fetchEvents();
    const failed = events.filter(
      (e) => e.status === "failed" && e.framework === "cloud-functions",
    );
    expect(failed.length).toBeGreaterThanOrEqual(1);
    expect(getError(failed[0]).type).toBe("TypeError");

    await client.close();
  });
});

// ── SQS wrapper integration ──────────────────────────────────────────

describe("SQS consumer simulation", async () => {
  const available = await ingestAvailable();
  if (!available) {
    it.skip("mock-ingest not available", () => {});
    return;
  }

  beforeEach(async () => {
    await resetIngest();
  });

  it("sends succeeded and failed SQS events", async () => {
    const { TraceStaxClient } = await import("../src/client.js");
    const client = new TraceStaxClient({
      apiKey: "ts_test_abc",
      endpoint: INGEST_URL,
      flushInterval: 60_000,
    });

    // Simulate succeeded
    client.sendEvent({
      type: "task_event",
      framework: "sqs",
      language: "typescript",
      sdk_version: "0.1.0",
      status: "succeeded",
      worker: { key: "sqs-test-1", hostname: "test", pid: 1, concurrency: 1, queues: ["order-queue"] },
      task: { name: "order-queue", id: "sqs-sim-001", queue: "order-queue", attempt: 1 },
      metrics: { duration_ms: 90 },
    });

    // Simulate failed
    client.sendEvent({
      type: "task_event",
      framework: "sqs",
      language: "typescript",
      sdk_version: "0.1.0",
      status: "failed",
      worker: { key: "sqs-test-1", hostname: "test", pid: 1, concurrency: 1, queues: ["order-queue"] },
      task: { name: "order-queue", id: "sqs-sim-002", queue: "order-queue", attempt: 1 },
      metrics: { duration_ms: 5 },
      error: { type: "Error", message: "message processing failed" },
    });

    await client.flush();

    const events = await waitForEvents((e) => getTask(e).id === "sqs-sim-002");
    const succeeded = events.filter((e) => getTask(e).id === "sqs-sim-001");
    const failed = events.filter((e) => getTask(e).id === "sqs-sim-002");

    expect(succeeded.length).toBeGreaterThanOrEqual(1);
    expect(succeeded[0].framework).toBe("sqs");
    expect(succeeded[0].status).toBe("succeeded");

    expect(failed.length).toBeGreaterThanOrEqual(1);
    expect(failed[0].status).toBe("failed");
    expect(getError(failed[0]).type).toBe("Error");

    await client.close();
  });
});

// ── Temporal simulation ──────────────────────────────────────────────

describe("Temporal simulation", async () => {
  const available = await ingestAvailable();
  if (!available) {
    it.skip("mock-ingest not available", () => {});
    return;
  }

  beforeEach(async () => {
    await resetIngest();
  });

  it("sends a succeeded workflow event", async () => {
    const { TraceStaxClient } = await import("../src/client.js");
    const client = new TraceStaxClient({
      apiKey: "ts_test_abc",
      endpoint: INGEST_URL,
      flushInterval: 60_000,
    });

    client.sendEvent({
      type: "task_event",
      framework: "temporal",
      language: "typescript",
      sdk_version: "0.1.0",
      status: "succeeded",
      worker: { key: "temporal@test:1", hostname: "test", pid: 1, concurrency: 1, queues: ["default"] },
      task: { name: "OrderWorkflow", id: "temporal-sim-001", queue: "default", attempt: 1 },
      metrics: { duration_ms: 1500 },
    });

    await client.flush();

    const events = await waitForEvents((e) => getTask(e).id === "temporal-sim-001");
    const match = events.filter((e) => getTask(e).id === "temporal-sim-001");
    expect(match.length).toBeGreaterThanOrEqual(1);
    expect(match[0].framework).toBe("temporal");

    await client.close();
  });
});

// ── Event structure validation ────────────────────────────────────────

describe("Event structure validation", async () => {
  const available = await ingestAvailable();
  if (!available) {
    it.skip("mock-ingest not available", () => {});
    return;
  }

  beforeEach(async () => {
    await resetIngest();
  });

  it("verifies all required fields are present", async () => {
    const { TraceStaxClient } = await import("../src/client.js");
    const client = new TraceStaxClient({
      apiKey: "ts_test_abc",
      endpoint: INGEST_URL,
      flushInterval: 60_000,
    });

    client.sendEvent({
      type: "task_event",
      framework: "bullmq",
      language: "typescript",
      sdk_version: "0.1.0",
      status: "succeeded",
      worker: { key: "test:1", hostname: "test", pid: 1, concurrency: 1, queues: ["default"] },
      task: { name: "structTest", id: "struct-node-001", queue: "default", attempt: 1 },
      metrics: { duration_ms: 42 },
    });

    await client.flush();

    const events = await waitForEvents((e) => getTask(e).id === "struct-node-001");
    const match = events.filter((e) => getTask(e).id === "struct-node-001");
    expect(match.length).toBeGreaterThanOrEqual(1);

    const event = match[0];

    // Top-level required fields
    for (const field of ["type", "framework", "language", "sdk_version", "status", "worker", "task", "metrics"]) {
      expect(event).toHaveProperty(field);
    }

    // Worker sub-fields
    const worker = event.worker as Record<string, unknown>;
    for (const field of ["key", "hostname", "pid", "queues"]) {
      expect(worker).toHaveProperty(field);
    }

    // Task sub-fields
    const task = event.task as Record<string, unknown>;
    for (const field of ["name", "id", "queue", "attempt"]) {
      expect(task).toHaveProperty(field);
    }

    // Metrics
    const metrics = event.metrics as Record<string, unknown>;
    expect(metrics).toHaveProperty("duration_ms");

    await client.close();
  });
});
