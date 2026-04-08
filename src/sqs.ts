/**
 * TraceStaxSQSConsumer — AWS SQS message handler wrapper.
 *
 * Wraps your SQS message handler to automatically report started/succeeded/failed
 * events to TraceStax. Works with any SQS consumer library (sqs-consumer, @aws-sdk/client-sqs, etc).
 */

import * as os from "os";
import { TraceStaxClient, type TraceStaxClientOptions } from "./client.js";

const SDK_VERSION = "0.1.0";

export interface SQSMessage {
  MessageId?: string;
  Body?: string;
  [key: string]: unknown;
}

export interface SQSConsumerOptions extends TraceStaxClientOptions {
  /** Name of the SQS queue (used in event payloads). */
  queueName: string;
  /** Task name to use in the dashboard (defaults to queueName). */
  taskName?: string;
  /** Heartbeat interval in ms (default 30 000). */
  heartbeatInterval?: number;
}

export class TraceStaxSQSConsumer {
  private readonly client: TraceStaxClient;
  private readonly queueName: string;
  private readonly taskName: string;
  private readonly workerKey: string;
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;

  constructor(options: SQSConsumerOptions) {
    this.client = new TraceStaxClient(options);
    this.queueName = options.queueName;
    this.taskName = options.taskName ?? options.queueName;
    this.workerKey = `sqs-${os.hostname()}-${process.pid}`;
  }

  /**
   * Wrap an SQS message handler. Reports started/succeeded/failed events
   * around the handler execution.
   *
   * Usage::
   *
   *   const consumer = new TraceStaxSQSConsumer({ apiKey: "ts_live_xxx", queueName: "orders" });
   *
   *   // With sqs-consumer:
   *   const app = Consumer.create({
   *     queueUrl: "https://sqs.eu-west-1.amazonaws.com/...",
   *     handleMessage: (msg) => consumer.wrapHandler(msg, () => processOrder(msg)),
   *   });
   */
  async wrapHandler<T>(
    message: SQSMessage,
    handler: () => Promise<T>,
  ): Promise<T> {
    const taskId = message.MessageId ?? crypto.randomUUID();
    const startTime = Date.now();
    const worker = this.buildWorkerInfo();

    this.client.sendEvent({
      type: "task_event",
      framework: "sqs",
      language: "typescript",
      sdk_version: SDK_VERSION,
      task: { name: this.taskName, id: taskId, queue: this.queueName, attempt: 1 },
      status: "started",
      metrics: { duration_ms: 0 },
      worker,
    });

    try {
      const result = await handler();
      this.client.sendEvent({
        type: "task_event",
        framework: "sqs",
        language: "typescript",
        sdk_version: SDK_VERSION,
        task: { name: this.taskName, id: taskId, queue: this.queueName, attempt: 1 },
        status: "succeeded",
        metrics: { duration_ms: Date.now() - startTime },
        worker,
      });
      return result;
    } catch (err) {
      this.client.sendEvent({
        type: "task_event",
        framework: "sqs",
        language: "typescript",
        sdk_version: SDK_VERSION,
        task: { name: this.taskName, id: taskId, queue: this.queueName, attempt: 1 },
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

  startHeartbeat(intervalMs?: number): void {
    const interval = intervalMs ?? 30_000;
    this.heartbeatTimer = setInterval(async () => {
      try {
        await this.client.sendHeartbeat({
          framework: "sqs",
          worker: this.buildWorkerInfo(),
          timestamp: new Date().toISOString(),
        });
      } catch {
        // skip
      }
    }, interval);
    this.heartbeatTimer?.unref?.();
  }

  async close(): Promise<void> {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    await this.client.close();
  }

  private buildWorkerInfo(): Record<string, unknown> {
    return {
      key: this.workerKey,
      hostname: os.hostname(),
      pid: process.pid,
      concurrency: 1,
      queues: [this.queueName],
    };
  }
}
