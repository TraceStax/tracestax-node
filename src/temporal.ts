/**
 * TraceStaxTemporalInterceptor — Temporal activity interceptor.
 *
 * Instruments Temporal activity executions and reports lifecycle events to
 * the TraceStax ingest API.
 *
 * Usage::
 *
 *   import { Worker } from "@temporalio/worker";
 *   import { TraceStaxTemporalInterceptor } from "@tracestax/node/temporal";
 *
 *   const worker = await Worker.create({
 *     taskQueue: "my-task-queue",
 *     interceptors: {
 *       activityInbound: [() => new TraceStaxTemporalInterceptor({
 *         apiKey: "ts_live_xxx",
 *         taskQueue: "my-task-queue",
 *       })],
 *     },
 *   });
 */

import type {
  ActivityExecuteInput,
  ActivityInboundCallsInterceptor,
  Next,
} from "@temporalio/worker";
import * as os from "os";
import { TraceStaxClient, type TraceStaxClientOptions } from "./client.js";

const SDK_VERSION = "0.1.0";

export interface TemporalInterceptorOptions extends TraceStaxClientOptions {
  /** The Temporal task queue name (used in worker info). */
  taskQueue?: string;
  /** Worker concurrency (informational, default 1). */
  concurrency?: number;
}

export class TraceStaxTemporalInterceptor
  implements ActivityInboundCallsInterceptor
{
  private readonly client: TraceStaxClient;
  private readonly taskQueue: string;
  private readonly concurrency: number;
  private readonly workerKey: string;

  constructor(options: TemporalInterceptorOptions) {
    this.client = new TraceStaxClient(options);
    this.taskQueue = options.taskQueue ?? "default";
    this.concurrency = options.concurrency ?? 1;
    this.workerKey = `temporal-${os.hostname()}-${process.pid}`;
  }

  async execute(
    input: ActivityExecuteInput,
    next: Next<ActivityInboundCallsInterceptor, "execute">,
  ): Promise<unknown> {
    const taskName = String(input.headers?.activityType ?? "unknown");
    const taskId = String(input.headers?.workflowRunId ?? crypto.randomUUID());
    const startTime = Date.now();
    const worker = this.buildWorkerInfo();

    this.client.sendEvent({
      type: "task_event",
      framework: "temporal",
      language: "typescript",
      sdk_version: SDK_VERSION,
      task: { name: taskName, id: taskId, queue: this.taskQueue, attempt: 1 },
      status: "started",
      metrics: { duration_ms: 0 },
      worker,
    });

    try {
      const result = await next(input);
      this.client.sendEvent({
        type: "task_event",
        framework: "temporal",
        language: "typescript",
        sdk_version: SDK_VERSION,
        task: { name: taskName, id: taskId, queue: this.taskQueue, attempt: 1 },
        status: "succeeded",
        metrics: { duration_ms: Date.now() - startTime },
        worker,
      });
      return result;
    } catch (err) {
      this.client.sendEvent({
        type: "task_event",
        framework: "temporal",
        language: "typescript",
        sdk_version: SDK_VERSION,
        task: { name: taskName, id: taskId, queue: this.taskQueue, attempt: 1 },
        status: "failed",
        metrics: { duration_ms: Date.now() - startTime },
        error: {
          type: (err as Error).name ?? "Error",
          message: (err as Error).message ?? String(err),
        },
        worker,
      });
      throw err;
    }
  }

  async close(): Promise<void> {
    await this.client.close();
  }

  private buildWorkerInfo(): Record<string, unknown> {
    return {
      key: this.workerKey,
      hostname: os.hostname(),
      pid: process.pid,
      concurrency: this.concurrency,
      queues: [this.taskQueue],
    };
  }
}
