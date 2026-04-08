/**
 * BullMQ FlowProducer lineage tracking for TraceStax.
 *
 * Wraps FlowProducer.add() and FlowProducer.addBulk() to automatically
 * capture parent-child task relationships and report them as LineagePayload.
 */

import type { FlowProducer, FlowJob } from "bullmq";
import { TraceStaxClient } from "./client.js";

/**
 * Intercept a FlowProducer instance to automatically track lineage.
 *
 * @example
 * ```ts
 * import { FlowProducer } from "bullmq";
 * import { interceptFlowProducer } from "@tracestax/bullmq";
 *
 * const flow = new FlowProducer({ connection });
 * interceptFlowProducer(flow, { apiKey: "ts_live_xxx" });
 *
 * // All subsequent .add() / .addBulk() calls will report lineage
 * await flow.add({ name: "parent", queueName: "main", children: [...] });
 * ```
 */
export function interceptFlowProducer(
  producer: FlowProducer,
  options: { apiKey: string; endpoint?: string } | TraceStaxClient,
): void {
  const client =
    options instanceof TraceStaxClient
      ? options
      : new TraceStaxClient({
          apiKey: (options as { apiKey: string; endpoint?: string }).apiKey,
          endpoint: (options as { apiKey: string; endpoint?: string }).endpoint,
        });

  const originalAdd = producer.add.bind(producer);
  const originalAddBulk = producer.addBulk.bind(producer);

  producer.add = async function patchedAdd(flow: FlowJob) {
    reportFlowLineage(client, flow);
    return originalAdd(flow);
  };

  producer.addBulk = async function patchedAddBulk(flows: FlowJob[]) {
    for (const flow of flows) {
      reportFlowLineage(client, flow);
    }
    return originalAddBulk(flows);
  };
}

/**
 * Walk a FlowJob tree and report each parent-children relationship.
 */
function reportFlowLineage(client: TraceStaxClient, flow: FlowJob): void {
  if (!flow.children || flow.children.length === 0) return;

  const parentId = flow.opts?.jobId ?? generateId();
  const chainId = generateId();

  const children = flow.children.map((child) => ({
    id: child.opts?.jobId ?? generateId(),
    task_name: child.name,
  }));

  client
    .sendLineage({
      parent_id: parentId,
      children,
      chain_id: chainId,
    })
    .catch(() => {});

  // Recurse into children that themselves have children
  for (const child of flow.children) {
    if (child.children && child.children.length > 0) {
      reportFlowLineage(client, child);
    }
  }
}

let counter = 0;
function generateId(): string {
  return `rw_${Date.now()}_${++counter}`;
}
