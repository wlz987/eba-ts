import { Envelope } from "../envelope/envelope.ts";
import type { EnvelopeId } from "../envelope/ids.ts";
import type { Queue } from "../mail/queue.ts";
import { Core } from "./core.ts";
import type { JobHost } from "./host.ts";

export const cores = new WeakMap<JobHost, Core>();

export function coreOf(host: JobHost): Core {
  const core = cores.get(host);
  if (core === undefined) throw new Error("invalid JobHost");
  return core;
}

export function queued(
  queue: Queue,
  envelopeId: EnvelopeId | string,
): Envelope | null {
  for (const item of queue) {
    if (item instanceof Envelope && item.header.id === envelopeId) return item;
  }
  return null;
}

