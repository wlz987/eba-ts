import type { Envelope } from "../../envelope/envelope.ts";
import { LeasePending, noteSettled } from "../../errors.ts";
import type { Core } from "../core.ts";
import { become, stateOf } from "../job.ts";
import type { Job, State } from "../job.ts";
import { drop, notify } from "../ending.ts";

export function releaseEnding(core: Core, job: Job): Envelope {
  const state = stateOf(job);
  const ending = state.pendingControl;
  try {
    core.book.cancel(state.root.header.id);
  } catch (error) {
    const pending = new LeasePending();
    pending.cause = error;
    throw pending;
  }
  state.pendingControl = null;
  become(state, "finished");
  if (ending === null) {
    drop(core, job);
    return state.root;
  }
  try {
    notify(job, ending);
  } catch (error) {
    if (stateOf(job).notice === "finished") noteSettled(error as Error);
    throw error;
  } finally {
    drop(core, job);
  }
  return state.root;
}

export function releaseDeferred(core: Core): boolean {
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
    state.pendingControl !== null && core.book.count(state.root.header.id) > 0
  );
}

export function waitsForLease(core: Core): boolean {
  if (core.book.size === 0) return false;
  for (const job of core.jobs.values()) {
    if (waitsForBook(core, stateOf(job))) return true;
  }
  return false;
}

