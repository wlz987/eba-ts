import type { Envelope } from "#src/core/envelope.js";
import { LeasePending, noteSettled } from "#src/core/errors.js";
import type { Core } from "#src/core/job/core.js";
import { become, setPendingControl, stateOf } from "#src/core/job/job.js";
import type { Job, State } from "#src/core/job/job.js";
import { drop, notify } from "#src/core/job/ending.js";

export function releaseEnding(core: Core, job: Job): Envelope {
  const state = stateOf(job);
  const ending = state.pendingControl;
  try {
    core.leases.cancel(state.root.header.id);
  } catch (error) {
    const pending = new LeasePending();
    pending.cause = error;
    throw pending;
  }
  setPendingControl(state, null);
  become(state, "finished");
  if (ending === null) {
    drop(core, job);
    return state.root;
  }
  try {
    notify(core, job, ending);
  } catch (error) {
    if (stateOf(job).notice === "finished") noteSettled(error as Error);
    throw error;
  } finally {
    drop(core, job);
  }
  return state.root;
}

export function releaseDeferred(core: Core): boolean {
  // `releaseDeferred` runs once per host per `advanceHost`, i.e. on the pump's
  // hot path, and the copy it needs (`[...core.jobs.values()]`, because the loop
  // may `drop` a job) costs 43ns even when the book is empty — which is the
  // common case for a host between roots. Measured `[...emptyMap]` **43ns** vs
  // the `size === 0` guard **3.7ns** (12×).
  if (core.jobs.size === 0) return false;
  let released = false;
  for (const job of [...core.jobs.values()]) {
    const state = stateOf(job);
    if (!waitsForBook(core, state)) continue;
    try {
      releaseEnding(core, job);
    } catch (error) {
      if (error instanceof LeasePending) {
        continue;
      }
      throw error;
    }
    released = true;
  }
  return released;
}

export function waitsForBook(core: Core, state: State): boolean {
  return (
    state.pendingControl !== null && core.leases.count(state.root.header.id) > 0
  );
}

export function waitsForLease(core: Core): boolean {
  if (core.leases.size === 0) return false;
  for (const job of core.jobs.values()) {
    if (waitsForBook(core, stateOf(job))) return true;
  }
  return false;
}

