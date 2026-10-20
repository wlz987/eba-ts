import type { Envelope } from "../envelope/envelope.ts";
import { hasResultShape } from "../envelope/result.ts";
import type { Job, State } from "./job.ts";
import { Buffer } from "./context/buffer.ts";

export class Intent {
  readonly stage: string;
  readonly request: Envelope;
  readonly timeout: number | null;
  constructor(stage: string, request: Envelope, timeout: number | null) {
    this.stage = stage;
    this.request = request;
    this.timeout = timeout;
  }
}

export class Control {
  readonly body: Record<string, unknown> | null;
  constructor(body: Record<string, unknown> | null) {
    this.body = body;
  }
}

export const states = new WeakMap<Job, State>();
export { Commit, sealContext } from "./context/commit.ts";
export const buffers = new WeakMap<Context, Buffer>();
import { Context } from "./context/context.ts";
export { Context };

export function stateOf(job: Job): State {
  const state = states.get(job);
  if (state === undefined) throw new Error("Job constructor must run");
  return state;
}

export function bufferOf(ctx: Context): Buffer {
  const buffer = buffers.get(ctx);
  if (buffer === undefined) throw new Error("invalid Context");
  return buffer;
}

export function requireActive(ctx: Context): Buffer {
  const buffer = bufferOf(ctx);
  if (buffer.idGen === null) {
    throw new Error("Context is valid only during its callback");
  }
  return buffer;
}

export function putReply(buffer: Buffer, body: unknown): void {
  if (buffer.job.root.header.address.kind !== "request") {
    throw new TypeError("root is not a request");
  }
  if (buffer.reply !== null || buffer.job.replied) {
    throw new Error("root already replied");
  }
  if (!hasResultShape(body)) {
    throw new TypeError("invalid result body");
  }
  buffer.reply = body;
}

