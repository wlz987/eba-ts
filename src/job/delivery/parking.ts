import type { Envelope } from "../../envelope/envelope.ts";
import type { Core } from "../core.ts";
import type { Job } from "../job.ts";

export function park(core: Core, job: Job | null, envelope: Envelope): void {
  if (core.pending === null) {
    core.pending = envelope;
    core.pendingJob = job;
  } else {
    core.rest.push([envelope, job]);
  }
}

export function holdsLetter(core: Core, envelope: Envelope): boolean {
  if (core.pending === envelope) return true;
  return core.rest.some(([held]) => held === envelope);
}

export function owns(core: Core, envelope: Envelope): boolean {
  return holdsLetter(core, envelope) || core.outbox.contains(envelope);
}

export function parkedJob(core: Core, envelope: Envelope): Job | null {
  if (core.pending === envelope) return core.pendingJob;
  for (const [held, owner] of core.rest) {
    if (held === envelope) return owner;
  }
  return null;
}

export function unpark(core: Core, envelope: Envelope): void {
  if (core.pending === envelope) {
    core.pending = null;
    core.pendingJob = null;
  }
  core.rest = core.rest.filter(([held]) => held !== envelope);
}

