/**
 * Zero-Knowledge SDK hashing utilities.
 *
 * When ZK mode is enabled on a project, the customer supplies an HMAC secret
 * that they control. The SDK hashes designated string fields before transmission
 * so TraceStax never sees raw values.
 *
 * Usage:
 *   const hasher = new ZkHasher({ secret: process.env.ZK_SECRET! });
 *   const client = new TraceStaxClient({ apiKey: '...', zk: { secret: process.env.ZK_SECRET! } });
 *
 * To verify a stored hash against a known plaintext:
 *   const match = await hasher.verify('sensitive-value', storedHash);
 */

import { createHmac, timingSafeEqual } from 'node:crypto';

export interface ZkOptions {
  /**
   * HMAC secret key — must be kept private and never sent to TraceStax.
   * Generate with: openssl rand -hex 32
   */
  secret: string;
  /**
   * Top-level payload field names whose string values will be hashed.
   * Defaults to ['task_name', 'error_message', 'error_type'].
   * Nested paths are not supported — only top-level keys.
   * All string leaves under a configured key are hashed.
   */
  fields?: string[];
}

const DEFAULT_FIELDS = ['task_name', 'error_message', 'error_type'] as const;

export class ZkHasher {
  private readonly secret: string;
  private readonly fields: Set<string>;

  constructor(options: ZkOptions) {
    if (!options.secret) throw new Error('[tracestax-zk] secret is required');
    this.secret = options.secret;
    this.fields = new Set(options.fields ?? DEFAULT_FIELDS);
  }

  /**
   * HMAC-SHA-256 hash of a string value using the customer's secret key.
   * Returns a 64-character hex string.
   */
  hashString(value: string): string {
    return createHmac('sha256', this.secret).update(value).digest('hex');
  }

  /**
   * Apply hashing to all configured fields in a payload object.
   * Only strings are hashed — numbers, booleans, and nulls are left as-is.
   * Arrays of strings have each element hashed.
   */
  hashPayload(payload: Record<string, unknown>): Record<string, unknown> {
    const result: Record<string, unknown> = {};
    for (const [key, val] of Object.entries(payload)) {
      if (this.fields.has(key)) {
        result[key] = this.hashField(val);
      } else {
        result[key] = val;
      }
    }
    return result;
  }

  /**
   * Constant-time comparison: verify that `plaintext` hashes to `hash`.
   * Use this locally to look up stored hashed values.
   */
  verify(plaintext: string, hash: string): boolean {
    try {
      const expected = this.hashString(plaintext);
      const a = Buffer.from(expected, 'hex');
      const b = Buffer.from(hash, 'hex');
      if (a.length !== b.length) return false;
      return timingSafeEqual(a, b);
    } catch {
      return false;
    }
  }

  private hashField(val: unknown): unknown {
    if (typeof val === 'string') return this.hashString(val);
    if (Array.isArray(val)) return val.map((item) => this.hashField(item));
    return val;
  }
}
