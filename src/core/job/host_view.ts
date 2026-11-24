import type { Envelope, ActorId, EnvelopeId, IdGen } from "#src/core/envelope.js";
import type { Bus } from "#src/core/mail/bus.js";
import type { Clock } from "#src/core/time.js";
import type { Inbox } from "#src/core/mail/inbox.js";
import { find } from "#src/core/mail/inbox.js";
import type { Job } from "#src/core/job/job.js";
import { resume } from "#src/core/job/execution.js";
import { queued } from "#src/core/job/host.js";
import type { Core } from "#src/core/job/core.js";

export function driveResume(
  host: HostView,
  requestId: EnvelopeId,
  failure: Envelope,
  bus: Bus,
  clock: Clock,
  idGen: IdGen,
  after: () => void,
): boolean {
  const core = host.core;
  if (!core.leases.holds(requestId)) return false;
  if (core.busy || (core.state !== "running" && core.state !== "draining")) return true;
  try {
    resume(core, bus, clock, idGen, failure);
  } finally {
    after();
  }
  return true;
}

export abstract class HostView {
  abstract get core(): Core;
  get actorId(): ActorId {
    return this.core.sub.actorId;
  }
  get inbox(): Inbox {
    return this.core.sub.inbox;
  }
  makeJob(_root: Envelope): Job {
    throw new Error("make_job");
  }
  onRejected(_root: Envelope, _reason: string): void {}
  crowded(): boolean {
    const core = this.core;
    const inbox = core.sub.inbox;
    const sitting = inbox.isClosed ? 0 : inbox.capacity - inbox.remaining();
    return core.jobs.size + core.queue.size + sitting >= core.activeLimit;
  }
  acceptedRoot(rootId: EnvelopeId | string): Envelope | null {
    const core = this.core;
    return (
      find(core.sub.inbox, rootId as EnvelopeId) ?? queued(core.queue.items, rootId)
    );
  }
  deliverySpent(envelope: Envelope): boolean {
    const core = this.core;
    const key = envelope.header.id;
    return core.attempts.has(key) && !core.again.has(key);
  }

}
