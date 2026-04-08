/**
 * TraceStaxBullMonitor — Bull (legacy v3/v4) queue event listener.
 *
 * Subscribes to Bull queue events and forwards them to the TraceStax ingest API.
 * For BullMQ (v5+) use TraceStaxMonitor from the main export instead.
 */

import type Bull from "bull";
import * as os from "os";
import { TraceStaxClient, type TraceStaxClientOptions } from "./client.js";

const SDK_VERSION = "0.1.0";

export interface BullMonitorOptions extends TraceStaxClientOptions {
  /** Heartbeat interval in ms (default 30 000). */
  heartbeatInterval?: number;
  /** Queue snapshot interval in ms (default 60 000). */
  snapshotInterval?: number;
}

export class TraceStaxBullMonitor {
  private readonly client: TraceStaxClient;
  private readonly heartbeatInterval: number;
  private readonly snapshotInterval: number;
  private readonly workerKey: string;

  private queues: Bull.Queue[] = [];
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private snapshotTimer: ReturnType<typeof setInterval> | null = null;

  constructor(options: BullMonitorOptions) {
    this.client = new TraceStaxClient(options);
    this.heartbeatInterval = options.heartbeatInterval ?? 30_000;
    this.snapshotInterval = options.snapshotInterval ?? 60_000;
    this.workerKey = `bull-${os.hostname()}-${process.pid}`;
  }

  monitor(queue: Bull.Queue): void {
    this.queues.push(queue);

    queue.on("completed", (job) => {
      this.client.sendEvent({
        type: "task_event",
        framework: "bull",
        language: "typescript",
        sdk_version: SDK_VERSION,
        task: {
          name: job.name || queue.name,
          id: String(job.id),
          queue: queue.name,
          attempt: job.attemptsMade,
        },
        status: "succeeded",
        metrics: {
          duration_ms:
            job.finishedOn && job.processedOn
              ? job.finishedOn - job.processedOn
              : 0,
        },
        worker: this.buildWorkerInfo([queue.name]),
      });
    });

    queue.on("failed", (job, err) => {
      this.client.sendEvent({
        type: "task_event",
        framework: "bull",
        language: "typescript",
        sdk_version: SDK_VERSION,
        task: {
          name: job.name || queue.name,
          id: String(job.id),
          queue: queue.name,
          attempt: job.attemptsMade,
        },
        status: "failed",
        metrics: {
          duration_ms: job.processedOn ? Date.now() - job.processedOn : 0,
        },
        error: { type: err.name, message: err.message },
        worker: this.buildWorkerInfo([queue.name]),
      });
    });
  }

  startHeartbeat(): void {
    this.heartbeatTimer = setInterval(async () => {
      try {
        await this.client.sendHeartbeat({
          framework: "bull",
          worker: this.buildWorkerInfo(this.queues.map((q) => q.name)),
          timestamp: new Date().toISOString(),
        });
      } catch {
        // skip
      }
    }, this.heartbeatInterval);
    this.heartbeatTimer?.unref?.();
  }

  startSnapshots(): void {
    this.snapshotTimer = setInterval(async () => {
      try {
        const queues = [];
        for (const q of this.queues) {
          const counts = await q.getJobCounts();
          queues.push({
            name: q.name,
            depth: (counts.waiting ?? 0) + (counts.delayed ?? 0),
            active: counts.active ?? 0,
            failed: counts.failed ?? 0,
            throughput_per_min: 0,
          });
        }
        await this.client.sendSnapshot({
          framework: "bull",
          worker_key: this.workerKey,
          queues,
          timestamp: new Date().toISOString(),
        });
      } catch {
        // skip
      }
    }, this.snapshotInterval);
    this.snapshotTimer?.unref?.();
  }

  async close(): Promise<void> {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    if (this.snapshotTimer) clearInterval(this.snapshotTimer);
    await this.client.close();
  }

  private buildWorkerInfo(queues: string[]): Record<string, unknown> {
    return {
      key: this.workerKey,
      hostname: os.hostname(),
      pid: process.pid,
      concurrency: 1,
      queues,
    };
  }
}
