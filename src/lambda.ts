/**
 * TraceStaxLambda — AWS Lambda middleware.
 *
 * Wraps a Lambda handler function to automatically report started/succeeded/failed
 * events to the TraceStax ingest API.
 *
 * Usage::
 *
 *   import { TraceStaxClient } from "@tracestax/node";
 *   import { tracestaxLambda } from "@tracestax/node/lambda";
 *
 *   const client = new TraceStaxClient({ apiKey: "ts_live_xxx" });
 *   const wrap = tracestaxLambda(client);
 *
 *   export const handler = wrap(async (event, context) => {
 *     // your handler logic
 *     return { statusCode: 200, body: "OK" };
 *   });
 */

import { TraceStaxClient, type TraceStaxClientOptions } from "./client.js";

const SDK_VERSION = "0.1.0";

export interface LambdaContext {
  functionName: string;
  functionVersion: string;
  invokedFunctionArn: string;
  memoryLimitInMB: string;
  awsRequestId: string;
  logGroupName: string;
  logStreamName: string;
  getRemainingTimeInMillis(): number;
  [key: string]: unknown;
}

/**
 * Creates a Lambda wrapper that safely initialises a TraceStaxClient from
 * options. If client construction fails (e.g. missing API key at cold start),
 * the handler still runs — tracing is silently disabled for that instance.
 *
 * Prefer this over constructing the client at module scope, where a throw
 * would abort the Lambda cold start and prevent the function from running.
 *
 * Usage:
 *   export const handler = createTracestaxLambda(
 *     { apiKey: process.env.TRACESTAX_API_KEY },
 *     async (event, context) => { ... },
 *   );
 */
export function createTracestaxLambda(
  options: TraceStaxClientOptions,
  handler: (event: unknown, context: LambdaContext) => Promise<unknown>,
): (event: unknown, context: LambdaContext) => Promise<unknown> {
  let client: TraceStaxClient | null = null;
  try {
    client = new TraceStaxClient(options);
  } catch {
    console.warn("[tracestax] Failed to initialise client; tracing disabled for this Lambda");
  }
  if (client === null) {
    return handler;
  }
  return tracestaxLambda(client)(handler);
}

/**
 * Returns a wrapper function that instruments a Lambda handler with TraceStax
 * lifecycle events.
 *
 * The wrapper never swallows errors — if the handler throws, the error is
 * reported to TraceStax and then re-thrown so Lambda sees the failure.
 */
export function tracestaxLambda(client: TraceStaxClient) {
  return function wrap<T>(
    handler: (event: unknown, context: LambdaContext) => Promise<T>,
  ) {
    return async (event: unknown, context: LambdaContext): Promise<T> => {
      const taskName = context.functionName;
      const taskId = context.awsRequestId;
      const start = Date.now();

      const worker = {
        key: `lambda-${taskName}`,
        hostname: taskName,
        pid: process.pid,
        concurrency: 1,
        queues: [taskName],
      };

      try {
        client.sendEvent({
          type: "task_event",
          framework: "lambda",
          language: "typescript",
          sdk_version: SDK_VERSION,
          task: { name: taskName, id: taskId, queue: taskName, attempt: 1 },
          status: "started",
          metrics: { duration_ms: 0 },
          worker,
        });
      } catch {
        // Never crash the host app
      }

      try {
        const result = await handler(event, context);

        try {
          client.sendEvent({
            type: "task_event",
            framework: "lambda",
            language: "typescript",
            sdk_version: SDK_VERSION,
            task: { name: taskName, id: taskId, queue: taskName, attempt: 1 },
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
            framework: "lambda",
            language: "typescript",
            sdk_version: SDK_VERSION,
            task: { name: taskName, id: taskId, queue: taskName, attempt: 1 },
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
