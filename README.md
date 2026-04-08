# @tracestax/node

TraceStax SDK for Node.js. Automatically captures task lifecycle events, queue depth snapshots, and task lineage from your background job workers.

Supports **BullMQ**, **Bull**, **AWS SQS**, **Temporal**, **AWS Lambda**, and **Google Cloud Functions**.

## Installation

```bash
npm install @tracestax/node
# or
pnpm add @tracestax/node
```

## Quickstart - BullMQ

```typescript
import { Queue, Worker } from "bullmq";
import { configure } from "@tracestax/node";

const queue = new Queue("tasks", { connection });
const worker = new Worker("tasks", processor, { connection });

const monitor = configure(queue, { apiKey: "ts_live_xxx" }, worker);
```

BullMQ is a peer dependency - you must have `bullmq >= 5.0.0` installed.

## Quickstart - Bull

```typescript
import Bull from "bull";
import { configureBull } from "@tracestax/node";

const queue = new Bull("tasks", { redis: { host: "localhost", port: 6379 } });
configureBull(queue, { apiKey: "ts_live_xxx" });
```

## Quickstart - AWS SQS

```typescript
import { SQSClient } from "@aws-sdk/client-sqs";
import { configureSqs } from "@tracestax/node";

const sqs = new SQSClient({ region: "us-east-1" });
const monitor = configureSqs(sqs, { apiKey: "ts_live_xxx", queueUrl: "https://sqs..." });
```

## Quickstart - Temporal

```typescript
import { Worker } from "@temporalio/worker";
import { makeTraceStaxInterceptors } from "@tracestax/node";

const worker = await Worker.create({
  taskQueue: "my-task-queue",
  interceptors: { activityInbound: [makeTraceStaxInterceptors({ apiKey: "ts_live_xxx" })] },
});
```

## Quickstart - AWS Lambda / Google Cloud Functions

Wrap your handler with zero code changes:

```typescript
import { wrapLambda } from "@tracestax/node";

export const handler = wrapLambda(
  async (event, context) => {
    // your handler logic
  },
  { apiKey: "ts_live_xxx", taskName: "my-function" }
);
```

## FlowProducer Lineage (BullMQ)

Track parent-child relationships in BullMQ flows:

```typescript
import { FlowProducer } from "bullmq";
import { interceptFlowProducer } from "@tracestax/node";

const flow = new FlowProducer({ connection });
interceptFlowProducer(flow, { apiKey: "ts_live_xxx" });

await flow.add({
  name: "parent-job",
  queueName: "main",
  children: [
    { name: "child-a", queueName: "sub", data: {} },
    { name: "child-b", queueName: "sub", data: {} },
  ],
});
```

## Configuration

```typescript
import { configure } from "@tracestax/node";

const monitor = configure(queue, {
  apiKey: "ts_live_xxx",                        // Required
  endpoint: "https://ingest.tracestax.com",     // Override ingest endpoint
  flushInterval: 5000,                          // ms between batch flushes
  maxBatchSize: 100,                            // Max events per HTTP request
  heartbeatInterval: 60000,                     // ms between heartbeats
  snapshotInterval: 60000,                      // ms between queue snapshots
  disableSnapshots: false,                      // Disable snapshot collection
}, worker);
```

## How It Works

The SDK buffers task events in memory and flushes them to `https://ingest.tracestax.com/v1/ingest` using the `Authorization: Bearer` header. Events are flushed:

- Every `flushInterval` ms (default 5,000)
- When the batch buffer reaches `maxBatchSize` (default 100)
- On `beforeExit`, `SIGTERM`, and `SIGINT`

## Authentication

All requests use your API key as a Bearer token:

```
Authorization: Bearer ts_live_xxx
```

Get your project API key from the TraceStax dashboard under **Project → API Key**.

## License

MIT
