import type { Bus } from "#src/core/mail/bus.js";
import type { IdGen } from "#src/core/envelope.js";
import { Envelope } from "#src/core/envelope.js";
import { LeasePending, combineFailures } from "#src/core/errors.js";
import type { Core } from "#src/core/job/core.js";
import { closeCore } from "#src/core/job/core.js";
import { Job, setPendingControl, stateOf } from "#src/core/job/job.js";
import { releaseAttempts } from "#src/core/job/delivery.js";
import { heldCount, takeSeized } from "#src/core/mail/inbox.js";
import { finishJobs, settleInbound, settleLetter } from "#src/core/job/close/settle.js";
import { closeHeld } from "#src/core/job/held.js";
import { SIGNAL_MASK, enter, idle, recomputePause } from "#src/core/job/pump.js";

export function closeHost(
  core: Core,
  bus: Bus,
  settle = false,
  idGen: IdGen | null = null,
  /** Collects the final flush so `Runtime.closeAsync` can await it. */
  flushes?: Promise<unknown>[],
): Envelope[] {
  closeHeld(core);
  if (core.state === "closed") return [...(core.abandoned ?? [])];
  enter(core, true);
  try {
    const abandoned = core.abandonedMut();
    const [hookError, leaseError] = finishJobs(core, abandoned, bus, idGen, settle);
    settleInbound(core, abandoned, core.queue.size + heldCount(core.sub.inbox));
    recomputePause(core);
    // The record ring is *staging*, and this host is about to be retired. Move
    // its pending facts into the sink before that happens — otherwise close
    // silently drops them, violating Record rule 6 (the log is the only
    // recovery fact). `drainRecords` is synchronous (append only); the flush
    // advances the durable watermark. A synchronous sink error is reported
    // through `hookError`; an async rejection is armed on `core.fault` (the
    // append already happened, so no fact is lost either way).
    let recordError: Error | null = null;
    try {
      core.drainRecords();
      const target = core.records.lastSeq;
      if (target > core.records.durable) {
        const flush = core.flushRecords(target);
        flushes?.push(flush);
        flush.catch((error: unknown) => {
          core.recordFault(error instanceof Error ? error : new Error(String(error)));
        });
      }
    } catch (error) {
      recordError = error instanceof Error ? error : new Error(String(error));
    }
    if (recordError !== null) {
      if (hookError !== null) throw combineFailures(hookError, recordError);
      throw recordError;
    }
    if (leaseError !== null) {
      if (hookError !== null) throw combineFailures(hookError, leaseError);
      throw leaseError;
    }
    core.leases.recover();
    if (core.leases.unreleased > 0 || core.leases.size > 0) {
      const pending = new LeasePending();
      if (hookError !== null) throw combineFailures(hookError, pending);
      throw pending;
    }
    core.mailbox.drain(abandoned);
    while (core.pending !== null || core.rest.length > 0) {
      const parked = core.takeParked();
      let envelope: Envelope;
      let job: Job | null;
      if (parked === null) {
        [envelope, job] = core.shiftRest();
      } else {
        [envelope, job] = parked;
      }
      if (job !== null) setPendingControl(stateOf(job), null);
      if (job !== null && stateOf(job).notice === "finished")
        abandoned.push(envelope);
      else settleLetter(core, envelope, abandoned, 1);
    }
    core.sub.close(bus);
    for (const envelope of takeSeized(core.sub.inbox)) abandoned.push(envelope);
    // No job survives this host, so no timer may either. `drop` retracts
    // per-job timers; this catches anything armed by a hook whose job was
    // already removed (e.g. a failed hook that armed a timer then threw).
    core.clearTimers();
    core.signal.clear(SIGNAL_MASK);
    closeCore(core);
    releaseAttempts(core);
    if (hookError !== null) throw hookError;
    return abandoned;
  } finally {
    idle(core);
  }
}
