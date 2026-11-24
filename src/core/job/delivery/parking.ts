import type { Envelope } from "#src/core/envelope.js";
import type { Core } from "#src/core/job/core.js";
import type { Job } from "#src/core/job/job.js";

/** Envelope identity is its **id**, never its object reference. */
function sameLetter(left: Envelope, right: Envelope): boolean {
  return left.header.id === right.header.id;
}

export function park(core: Core, job: Job | null, envelope: Envelope): void {
  core.parkLetter(envelope, job);
}

export function holdsLetter(core: Core, envelope: Envelope): boolean {
  if (core.pending !== null && sameLetter(core.pending, envelope)) return true;
  return core.rest.some(([held]) => sameLetter(held, envelope));
}

export function owns(core: Core, envelope: Envelope): boolean {
  return holdsLetter(core, envelope) || core.mailbox.contains(envelope);
}

export function parkedJob(core: Core, envelope: Envelope): Job | null {
  if (core.pending !== null && sameLetter(core.pending, envelope))
    return core.pendingJob;
  for (const [held, owner] of core.rest) {
    if (sameLetter(held, envelope)) return owner;
  }
  return null;
}

export function unpark(core: Core, envelope: Envelope): void {
  if (core.pending !== null && sameLetter(core.pending, envelope)) {
    core.takeParked();
  }
  core.dropRestLetter(envelope);
}
