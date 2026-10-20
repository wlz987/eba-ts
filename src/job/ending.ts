import { noteSettled } from "../errors.ts";
import type { Core } from "./core.ts";
import { Control, Job, become, stateOf } from "./job.ts";
import { invoke } from "./execution.ts";

export function refused(core: Core): string | null {
  return core.state !== "running" ? "host_draining" : null;
}

export function seal(core: Core, job: Job): void {
  const state = stateOf(job);
  if (state.lifecycle === "active") become(state, "delivering");
  core.book.cancel(state.root.header.id);
}

export function drop(core: Core, job: Job): void {
  const requestId = stateOf(job).root.header.id;
  if (core.jobs.get(requestId) === job) core.jobs.delete(requestId);
}

export function notify(job: Job, control: Control): void {
  const state = stateOf(job);
  become(state, "finished");
  state.notice = "finished";
  invoke(() => job.onFinished(control.body));
}

export function leaveEnding(job: Job, control: Control | null): void {
  if (control !== null) stateOf(job).pendingControl = control;
}

export function settleEnding(core: Core, job: Job, control: Control | null): void {
  if (control === null) return;
  try {
    notify(job, control);
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
export { releaseDeferred, releaseEnding, waitsForBook, waitsForLease } from "./ending/release.ts";
