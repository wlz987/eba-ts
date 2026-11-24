import { noteSettled } from "#src/core/errors.js";
import type { Core } from "#src/core/job/core.js";
import { Control, Job, become, setNotice, setPendingControl, stateOf } from "#src/core/job/job.js";
import { invoke } from "#src/core/job/execution.js";
import { applyFinished } from "#src/core/job/finished.js";

export function refused(core: Core): string | null {
  return core.state !== "running" ? "host_draining" : null;
}

export function seal(core: Core, job: Job): void {
  const state = stateOf(job);
  if (state.lifecycle === "active") become(state, "delivering");
  core.leases.cancel(state.root.header.id);
}

export function drop(core: Core, job: Job): void {
  const requestId = stateOf(job).root.header.id;
  const reply = core.takeHeld(requestId);
  if (reply !== null) reply.close();
  core.dropJob(requestId, job);
  // The job is gone, so its continuation can never run: retract any timer it
  // armed. Leaving them behind makes the agenda wake forever for a dead job
  // and grows `timers`/`timerWaiters` without bound.
  core.cancelTimers(requestId);
}

export function notify(core: Core, job: Job, control: Control): void {
  const state = stateOf(job);
  become(state, "finished");
  setNotice(state, "finished");
  const returned = invoke(() => job.onFinished(control.body));
  if (returned === undefined) return;
  if (!Array.isArray(returned)) throw new TypeError("onFinished must return Effect[] or None");
  applyFinished(core, returned);
}

export function leaveEnding(job: Job, control: Control | null): void {
  if (control !== null) setPendingControl(stateOf(job), control);
}

export function settleEnding(core: Core, job: Job, control: Control | null): void {
  if (control === null) return;
  try {
    notify(core, job, control);
  } catch (error) {
    if (stateOf(job).notice === "finished") noteSettled(error as Error);
    throw error;
  } finally {
    drop(core, job);
  }
}

export function remove(core: Core, job: Job): void {
  seal(core, job);
  drop(core, job);
}
export { releaseDeferred, releaseEnding, waitsForBook, waitsForLease } from "#src/core/job/ending/release.js";
