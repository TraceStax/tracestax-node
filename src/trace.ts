/**
 * W3C Trace Context utilities (https://www.w3.org/TR/trace-context/).
 *
 * Provides helpers for generating, parsing, and formatting traceparent headers
 * so TraceStax can propagate trace context across polyglot job pipelines.
 *
 * traceparent format: 00-<traceId:32hex>-<spanId:16hex>-<flags:2hex>
 */

import { randomBytes } from 'node:crypto';

export interface TraceContext {
  traceId: string;
  spanId: string;
  flags: string;
}

const TRACEPARENT_REGEX = /^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/i;

/** Generate a new 128-bit trace ID (32 hex chars). */
export function generateTraceId(): string {
  return randomBytes(16).toString('hex');
}

/** Generate a new 64-bit span ID (16 hex chars). */
export function generateSpanId(): string {
  return randomBytes(8).toString('hex');
}

/**
 * Parse a W3C traceparent header string.
 * Returns null if the header is absent, malformed, or uses an unsupported version.
 */
export function parseTraceparent(header: string | null | undefined): TraceContext | null {
  if (!header) return null;
  const m = TRACEPARENT_REGEX.exec(header.trim());
  if (!m) return null;
  const [, traceId, spanId, flags] = m;
  // Reject all-zero IDs per spec
  if (traceId === '0'.repeat(32) || spanId === '0'.repeat(16)) return null;
  return { traceId, spanId: spanId.toLowerCase(), flags };
}

/**
 * Format a W3C traceparent header string.
 * @param traceId  32-char hex trace ID
 * @param spanId   16-char hex span ID
 * @param flags    2-char hex flags (default '01' = sampled)
 */
export function formatTraceparent(traceId: string, spanId: string, flags = '01'): string {
  return `00-${traceId.toLowerCase()}-${spanId.toLowerCase()}-${flags}`;
}

/**
 * Create a fresh traceparent for a new root span.
 */
export function createTraceparent(): string {
  return formatTraceparent(generateTraceId(), generateSpanId());
}

/**
 * Create a child traceparent that inherits the traceId but gets a new spanId.
 */
export function childTraceparent(parent: TraceContext): string {
  return formatTraceparent(parent.traceId, generateSpanId(), parent.flags);
}
