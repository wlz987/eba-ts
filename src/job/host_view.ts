import type { Envelope } from "../envelope/envelope.ts";
import type { ActorId, EnvelopeId } from "../envelope/ids.ts";
import type { Bus } from "../mail/bus.ts";
import type { Clock } from "../time/clock.ts";
import type { IdGen } from "../envelope/idgen.ts";
import type { Inbox } from "../mail/inbox.ts";
import { find } from "../mail/inbox.ts";
import type { Job } from "./job.ts";
import type { HostState } from "./core.ts";
import { resume } from "./execution.ts";
import { coreOf, queued } from "./host_core.ts";
import { hasWork } from "./pump.ts";
import type { JobHost } from "./host.ts";

export abstract class HostView {
  get actorId(): ActorId {
    return coreOf(this as unknown as JobHost).sub.actorId;
  }
  get inbox(): Inbox {
    return coreOf(this as unknown as JobHost).sub.inbox;
  }
  get state(): HostState {
    return coreOf(this as unknown as JobHost).state;
  }
  get admitted(): boolean {
    return coreOf(this as unknown as JobHost).signal.isBound;
  }
  makeJob(_root: Envelope): Job {
    throw new Error("make_job");
  }
  onRejected(_root: Envelope, _reason: string): void {}
  job(requestId: EnvelopeId): Job | null {
    return coreOf(this as unknown as JobHost).jobs.get(requestId) ?? null;
  }
  get activeCount(): number {
    return coreOf(this as unknown as JobHost).jobs.size;
  }
  get queuedCount(): number {
    return coreOf(this as unknown as JobHost).queue.size;
  }
  get pendingCount(): number {
    return coreOf(this as unknown as JobHost).book.size;
  }
  get unsentCount(): number {
    return coreOf(this as unknown as JobHost).outbox.size;
  }
  get rejectedCount(): number {
    return coreOf(this as unknown as JobHost).rejectedCount;
  }
  get nextDeadlineMs(): number | null {
    return coreOf(this as unknown as JobHost).book.nextDeadlineMs;
  }
  crowded(): boolean {
    const core = coreOf(this as unknown as JobHost);
    const inbox = core.sub.inbox;
    const sitting = inbox.isClosed ? 0 : inbox.capacity - inbox.remaining();
    return core.jobs.size + core.queue.size + sitting >= core.activeLimit;
  }
  acceptedRoot(rootId: EnvelopeId | string): Envelope | null {
    const core = coreOf(this as unknown as JobHost);
    return (
      find(core.sub.inbox, rootId as EnvelopeId) ?? queued(core.queue, rootId)
    );
  }
  get isQuiescent(): boolean {
    const core = coreOf(this as unknown as JobHost);
    return !core.busy && !hasWork(core);
  }
  deliverySpent(envelope: Envelope): boolean {
    const core = coreOf(this as unknown as JobHost);
    return core.attempts.has(envelope) && !core.again.has(envelope);
  }
  resumeHeld(
    requestId: EnvelopeId,
    failure: Envelope,
    bus: Bus,
    clock: Clock,
    idGen: IdGen,
    after: () => void,
  ): boolean {
    const core = coreOf(this as unknown as JobHost);
    if (!core.book.holds(requestId)) return false;
    if (core.busy || (core.state !== "running" && core.state !== "draining"))
      return true;
    try {
      resume(core, bus, clock, idGen, failure);
    } finally {
      after();
    }
    return true;
  }
}
