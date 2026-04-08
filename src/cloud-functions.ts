/**
 * TraceStaxCloudFunction — GCP Cloud Functions middleware.
 *
 * Wraps a Cloud Function handler to automatically report started/succeeded/failed
 * events to the TraceStax ingest API.
 *
 * Usage::
 *
 *   import { TraceStaxClient } from "@tracestax/node";
 *   import { tracestaxCloudFunction } from "@tracestax/node/cloud-functions";
 *
 *   const client = new TraceStaxClient({ apiKey: "ts_live_xxx" });
 *   const wrap = tracestaxCloudFunction(client);
 *
 *   export const myFunction = wrap("myFunction", async (req, res) => {
 *     res.send("OK");
 *   });
 */

import { TraceStaxClient } from "./client.js";

const SDK_VERSION = "0.1.0";

/** Minimal Cloud Functions HTTP request shape. */
export interface CloudFunctionRequest {
  method?: string;
  url?: string;
  headers?: Record<string, string | string[] | undefined>;
  body?: unknown;
  [key: string]: unknown;
}

/** Minimal Cloud Functions HTTP response shape. */
export interface CloudFunctionResponse {
  status(code: number): CloudFunctionResponse;
  send(body?: unknown): void;
  json(body: unknown): void;
  [key: string]: unknown;
}

/** Cloud Functions event-driven (background/CloudEvent) context. */
export interface CloudFunctionContext {
  eventId?: string;
  eventType?: string;
  resource?: string | { service: string; name: string; type?: string };
  [key: string]: unknown;
}

/**
 * Returns a wrapper for HTTP-triggered Cloud Functions.
 *
 * The wrapper never swallows errors — if the handler throws, the error is
 * reported to TraceStax and then re-thrown so Cloud Functions sees the failure.
 */
export function tracestaxCloudFunction(client: TraceStaxClient) {
  return function wrap<T>(
    functionName: string,
    handler: (req: CloudFunctionRequest, res: CloudFunctionResponse) => Promise<T>,
  ) {
    return async (req: CloudFunctionRequest, res: CloudFunctionResponse): Promise<T> => {
      const taskId = crypto.randomUUID();
      const start = Date.now();

      const worker = {
        key: `gcf-${functionName}`,
        hostname: process.env.K_SERVICE ?? functionName,
        pid: process.pid,
        concurrency: 1,
        queues: [functionName],
      };

      try {
        client.sendEvent({
          type: "task_event",
          framework: "cloud-functions",
          language: "typescript",
          sdk_version: SDK_VERSION,
          task: { name: functionName, id: taskId, queue: functionName, attempt: 1 },
          status: "started",
          metrics: { duration_ms: 0 },
          worker,
        });
      } catch {
        // Never crash the host app
      }

      try {
        const result = await handler(req, res);

        try {
          client.sendEvent({
            type: "task_event",
            framework: "cloud-functions",
            language: "typescript",
            sdk_version: SDK_VERSION,
            task: { name: functionName, id: taskId, queue: functionName, attempt: 1 },
            status: "succeeded",
            metrics: { duration_ms: Date.now() - start },
            worker,
          });
        } catch {
          // Never crash the host app
        }

        return result;
      } catch (err) {
        try {
          client.sendEvent({
            type: "task_event",
            framework: "cloud-functions",
            language: "typescript",
            sdk_version: SDK_VERSION,
            task: { name: functionName, id: taskId, queue: functionName, attempt: 1 },
            status: "failed",
            metrics: { duration_ms: Date.now() - start },
            error: {
              type: (err as Error).name ?? "Error",
              message: (err as Error).message ?? String(err),
            },
            worker,
          });
        } catch {
          // Never crash the host app
        }

        throw err;
      }
    };
  };
}

/**
 * Returns a wrapper for event-driven (background/CloudEvent) Cloud Functions.
 *
 * Same lifecycle tracking pattern as the HTTP variant.
 */
export function tracestaxCloudEvent(client: TraceStaxClient) {
  return function wrap<T>(
    functionName: string,
    handler: (data: unknown, context: CloudFunctionContext) => Promise<T>,
  ) {
    return async (data: unknown, context: CloudFunctionContext): Promise<T> => {
      const taskId = context.eventId ?? crypto.randomUUID();
      const start = Date.now();

      const worker = {
        key: `gcf-${functionName}`,
        hostname: process.env.K_SERVICE ?? functionName,
        pid: process.pid,
        concurrency: 1,
        queues: [functionName],
      };

      try {
        client.sendEvent({
          type: "task_event",
          framework: "cloud-functions",
          language: "typescript",
          sdk_version: SDK_VERSION,
          task: { name: functionName, id: taskId, queue: functionName, attempt: 1 },
          status: "started",
          metrics: { duration_ms: 0 },
          worker,
        });
      } catch {
        // Never crash the host app
      }

      try {
        const result = await handler(data, context);

        try {
          client.sendEvent({
            type: "task_event",
            framework: "cloud-functions",
            language: "typescript",
            sdk_version: SDK_VERSION,
            task: { name: functionName, id: taskId, queue: functionName, attempt: 1 },
            status: "succeeded",
            metrics: { duration_ms: Date.now() - start },
            worker,
          });
        } catch {
          // Never crash the host app
        }

        return result;
      } catch (err) {
        try {
          client.sendEvent({
            type: "task_event",
            framework: "cloud-functions",
            language: "typescript",
            sdk_version: SDK_VERSION,
            task: { name: functionName, id: taskId, queue: functionName, attempt: 1 },
            status: "failed",
            metrics: { duration_ms: Date.now() - start },
            error: {
              type: (err as Error).name ?? "Error",
              message: (err as Error).message ?? String(err),
            },
            worker,
          });
        } catch {
          // Never crash the host app
        }

        throw err;
      }
    };
  };
}
