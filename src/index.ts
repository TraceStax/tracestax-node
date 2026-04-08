// Core
export { TraceStaxClient } from "./client.js";
export type { TraceStaxClientOptions } from "./client.js";

// BullMQ (v5+)
export { TraceStaxMonitor, configure } from "./monitor.js";
export type { MonitorOptions } from "./monitor.js";
export { interceptFlowProducer } from "./lineage.js";

// Bull (legacy v3/v4)
export { TraceStaxBullMonitor } from "./bull.js";
export type { BullMonitorOptions } from "./bull.js";

// AWS SQS
export { TraceStaxSQSConsumer } from "./sqs.js";
export type { SQSConsumerOptions, SQSMessage } from "./sqs.js";

// Temporal
export { TraceStaxTemporalInterceptor } from "./temporal.js";
export type { TemporalInterceptorOptions } from "./temporal.js";

// AWS Lambda
export { tracestaxLambda } from "./lambda.js";
export type { LambdaContext } from "./lambda.js";

// GCP Cloud Functions
export { tracestaxCloudFunction, tracestaxCloudEvent } from "./cloud-functions.js";
export type {
  CloudFunctionRequest,
  CloudFunctionResponse,
  CloudFunctionContext,
} from "./cloud-functions.js";

// Zero-Knowledge hashing
export { ZkHasher } from "./zk.js";
export type { ZkOptions } from "./zk.js";

// W3C Trace Context
export {
  generateTraceId,
  generateSpanId,
  parseTraceparent,
  formatTraceparent,
  createTraceparent,
  childTraceparent,
} from "./trace.js";
export type { TraceContext } from "./trace.js";
