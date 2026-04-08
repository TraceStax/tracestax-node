/**
 * TraceStaxMonitor — BullMQ queue and worker event listener.
 *
 * Subscribes to BullMQ Queue/Worker events and forwards them to the TraceStax
 * ingest API as IngestPayload, HeartbeatPayload, and SnapshotPayload.
 */

import type { Queue, Worker, Job } from "bullmq";
import { TraceStaxClient, type TraceStaxClientOptions } from "./client.js";
import { parseTraceparent, createTraceparent, childTraceparent } from "./trace.js";

const SDK_VERSION = "0.1.0";

export interface MonitorOptions extends TraceStaxClientOptions {
  /** Heartbeat interval in ms (default 60 000). */
  heartbeatInterval?: number;
  /** Queue snapshot interval in ms (default 60 000). */
  snapshotInterval?: number;
  /** Disable snapshot collection. */
  disableSnapshots?: boolean;
}

interface TimedJob {
  startedAt: number;
}

/**
 * Convenience entry point — configure monitoring for a BullMQ queue and
 * optional worker in a single call.
 */
export function configure(
  queue: Queue,
  options: MonitorOptions,
  worker?: Worker,
): TraceStaxMonitor {
  const monitor = new TraceStaxMonitor(options);
  monitor.monitorQueue(queue);
  if (worker) {
    monitor.monitorWorker(queue, worker);
  }
  return monitor;
}

export class TraceStaxMonitor {
  private client: TraceStaxClient;
  private heartbeatInterval: number;
  private snapshotInterval: number;
  private disableSnapshots: boolean;

  private heartbeatTimers: ReturnType<typeof setInterval>[] = [];
  private snapshotTimers: ReturnType<typeof setInterval>[] = [];

  /** Track job start times for duration calculation. */
  private jobTimings = new Map<string, number>();

  constructor(options: MonitorOptions) {
    this.client = new TraceStaxClient(options);
    this.heartbeatInterval = options.heartbeatInterval ?? 60_000;
    this.snapshotInterval = options.snapshotInterval ?? 60_000;
    this.disableSnapshots = options.disableSnapshots ?? false;
  }

  // ── Queue monitoring ───────────────────────────────────────────────

  /**
   * Subscribe to BullMQ queue events for snapshot collection.
   */
  monitorQueue(queue: Queue): void {
    if (!this.disableSnapshots) {
      this.startSnapshotTimer(queue);
    }
  }

  // ── Worker monitoring ──────────────────────────────────────────────

  /**
   * Subscribe to BullMQ worker events for task lifecycle tracking.
   */
  monitorWorker(queue: Queue, worker: Worker): void {
    const workerInfo = this.buildWorkerInfo(worker, queue);

    // Track job start
    worker.on("active", (job: Job) => {
      try {
        this.jobTimings.set(job.id ?? "", Date.now());
      } catch {
        // swallow — instrumentation must never propagate into the BullMQ event loop
      }
    });

    // Job completed
    worker.on("completed", (job: Job) => {
      const durationMs = this.popDuration(job.id ?? "");
      this.client.sendEvent(
        this.buildTaskPayload({
          job,
          queueName: queue.name,
          status: "succeeded",
          durationMs,
          workerInfo,
        }),
      );
    });

    // Job failed
    worker.on("failed", (job: Job | undefined, err: Error) => {
      const jobId = job?.id ?? "unknown";
      const durationMs = this.popDuration(jobId);
      this.client.sendEvent(
        this.buildTaskPayload({
          job,
          queueName: queue.name,
          status: "failed",
          durationMs,
          workerInfo,
          error: {
            type: err.name,
            message: err.message,
            stack_trace: err.stack,
          },
        }),
      );
    });

    // Job stalled (claimed by another worker)
    worker.on("stalled", (jobId: string) => {
      const durationMs = this.popDuration(jobId);
      this.client.sendEvent({
        framework: "bullmq",
        language: "typescript",
        sdk_version: SDK_VERSION,
        type: "task_event",
        worker: workerInfo,
        task: {
          name: "unknown",
          id: jobId,
          queue: queue.name,
          attempt: 1,
        },
        status: "stalled",
        metrics: { duration_ms: durationMs },
      });
    });

    // Set worker key on client for thread dumps
    this.client.workerKey = workerInfo.key as string;

    // Heartbeat
    this.startHeartbeatTimer(workerInfo);

    // Send initial heartbeat and process any immediate directives
    this.client
      .sendHeartbeat({
        framework: "bullmq",
        worker: workerInfo,
        timestamp: new Date().toISOString(),
      })
      .then((directives) => {
        if (directives?.pause_ingest) {
          this.client.setPauseUntil(
            directives.pause_until_ms ?? Date.now() + 60_000,
          );
        }
        for (const cmd of directives?.commands ?? []) {
          this.client.executeCommand(cmd).catch(() => {});
        }
      })
      .catch(() => {});
  }

  // ── Snapshot collection ────────────────────────────────────────────

  private startSnapshotTimer(queue: Queue): void {
    const timer = setInterval(async () => {
      try {
        const counts = await queue.getJobCounts(
          "waiting",
          "active",
          "failed",
          "completed",
        );

        const snapshot = {
          framework: "bullmq" as const,
          worker_key: `node:${process.pid}`,
          queues: [
            {
              name: queue.name,
              depth: counts.waiting ?? 0,
              active: counts.active ?? 0,
              failed: counts.failed ?? 0,
              throughput_per_min: 0, // computed server-side from completed count delta
            },
          ],
          timestamp: new Date().toISOString(),
        };

        await this.client.sendSnapshot(snapshot);
      } catch {
        // Silently skip failed snapshot — the next tick will retry
      }
    }, this.snapshotInterval);

    if (timer && typeof timer === "object" && "unref" in timer) {
      timer.unref();
    }

    this.snapshotTimers.push(timer);
  }

  // ── Heartbeat ──────────────────────────────────────────────────────

  private startHeartbeatTimer(
    workerInfo: Record<string, unknown>,
  ): void {
    const runHeartbeat = async () => {
      try {
        const directives = await this.client.sendHeartbeat({
          framework: "bullmq",
          worker: workerInfo,
          timestamp: new Date().toISOString(),
        });
        if (directives) {
          if (directives.pause_ingest) {
            this.client.setPauseUntil(
              directives.pause_until_ms ?? Date.now() + 60_000,
            );
          }
          for (const cmd of directives.commands ?? []) {
            this.client.executeCommand(cmd).catch(() => {});
          }
        }
      } catch {
        // skip
      }
    };

    const timer = setInterval(runHeartbeat, this.heartbeatInterval);

    if (timer && typeof timer === "object" && "unref" in timer) {
      timer.unref();
    }

    this.heartbeatTimers.push(timer);
  }

  // ── Payload builders ───────────────────────────────────────────────

  private buildTaskPayload(opts: {
    job: Job | undefined;
    queueName: string;
    status: string;
    durationMs: number;
    workerInfo: Record<string, unknown>;
    error?: { type: string; message: string; stack_trace?: string };
  }): Record<string, unknown> {
    const { job, queueName, status, durationMs, workerInfo, error } = opts;

    // W3C trace context: prefer job headers, then job data field, then generate new
    const rawTraceparent =
      (job?.opts as any)?.headers?.traceparent ??
      job?.data?.__traceparent ??
      null;
    const parentCtx = parseTraceparent(rawTraceparent);
    const traceparent = parentCtx
      ? childTraceparent(parentCtx)
      : createTraceparent();

    const payload: Record<string, unknown> = {
      framework: "bullmq",
      language: "typescript",
      sdk_version: SDK_VERSION,
      type: "task_event",
      worker: workerInfo,
      task: {
        name: job?.name ?? "unknown",
        id: job?.id ?? "unknown",
        queue: queueName,
        attempt: job?.attemptsMade ?? 1,
        ...(job?.data?._tracestax_parent_id && {
          parent_id: job.data._tracestax_parent_id,
        }),
        ...(job?.data?._tracestax_chain_id && {
          chain_id: job.data._tracestax_chain_id,
        }),
      },
      status,
      metrics: {
        duration_ms: Math.round(durationMs * 100) / 100,
        ...(job?.timestamp && job?.processedOn
          ? {
              queued_ms:
                Math.round((job.processedOn - job.timestamp) * 100) / 100,
            }
          : {}),
      },
      trace_context: { traceparent },
    };

    if (error) {
      payload.error = error;
    }

    return payload;
  }

  private buildWorkerInfo(
    worker: Worker,
    queue: Queue,
  ): Record<string, unknown> {
    const hostname =
      typeof globalThis !== "undefined" && "os" in globalThis
        ? // eslint-disable-next-line @typescript-eslint/no-require-imports
          require("os").hostname()
        : "unknown";

    return {
      key: `${hostname}:${process.pid}`,
      hostname,
      pid: process.pid,
      concurrency: (worker as any).opts?.concurrency ?? 1,
      queues: [queue.name],
    };
  }

  // ── Utilities ──────────────────────────────────────────────────────

  private popDuration(jobId: string): number {
    const start = this.jobTimings.get(jobId);
    this.jobTimings.delete(jobId);
    if (start) {
      return Date.now() - start;
    }
    return 0;
  }

  // ── Shutdown ───────────────────────────────────────────────────────

  async close(): Promise<void> {
    for (const t of this.heartbeatTimers) clearInterval(t);
    for (const t of this.snapshotTimers) clearInterval(t);
    this.heartbeatTimers = [];
    this.snapshotTimers = [];
    await this.client.close();
  }
}
