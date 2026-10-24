import type { Bus } from "../mail/bus.ts";
import { Envelope } from "../envelope/envelope.ts";
import { LeasePending, combineFailures } from "../errors.ts";
import type { Core } from "./core.ts";
import { Job, stateOf } from "./job.ts";
import { releaseAttempts } from "./delivery.ts";
import { heldCount, takeSeized } from "../mail/inbox.ts";
import { finishJobs, settleInbound, settleLetter } from "./close/settle.ts";
import { closeHeld } from "./held.ts";
import { SIGNAL_MASK, enter, idle, recomputePause } from "./pump.ts";

export function closeHost(core: Core, bus: Bus): Envelope[] {
  closeHeld(core);
  if (core.state === "closed") return [...(core.abandoned ?? [])];
  enter(core, true);
  try {
    const abandoned = core.beginClose();
    const [hookError, leaseError] = finishJobs(core, abandoned);
    settleInbound(core, abandoned, core.queue.size + heldCount(core.sub.inbox));
    recomputePause(core);
    if (leaseError !== null) {
      if (hookError !== null) throw combineFailures(hookError, leaseError);
      throw leaseError;
    }
    core.book.recover();
    if (core.book.unreleased > 0 || core.book.size > 0) {
      const pending = new LeasePending();
      if (hookError !== null) throw combineFailures(hookError, pending);
      throw pending;
    }
    core.outbox.drain(abandoned);
    while (core.pending !== null || core.rest.length > 0) {
      let envelope: Envelope;
      let job: Job | null;
      if (core.pending === null) {
        [envelope, job] = core.rest.shift()!;
      } else {
        envelope = core.pending;
        job = core.pendingJob;
        core.pending = null;
        core.pendingJob = null;
      }
      if (job !== null) stateOf(job).pendingControl = null;
      if (job !== null && stateOf(job).notice === "finished")
        abandoned.push(envelope);
      else settleLetter(core, envelope, abandoned, 1);
    }
    core.sub.close(bus);
    for (const envelope of takeSeized(core.sub.inbox)) abandoned.push(envelope);
    core.signal.clear(SIGNAL_MASK);
    core.state = "closed";
    releaseAttempts(core);
    if (hookError !== null) throw hookError;
    return abandoned;
  } finally {
    idle(core);
  }
}
