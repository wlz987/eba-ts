import { errPayload } from "../envelope/result.ts";
import type { Core } from "./core.ts";
import { stateOf } from "./context.ts";
import { drop, seal } from "./ending.ts";
import { invoke } from "./execution.ts";
import { become } from "./job.ts";
import type { Job } from "./job.ts";

export function cancelJob(core: Core, job: Job): boolean {
  const state = stateOf(job);
  if (state.lifecycle !== "active" || state.held) return false;
  const body = errPayload("cancelled");
  become(state, "finished");
  state.notice = "finished";
  let error: unknown = null;
  try {
    invoke(() => job.onFinished(body));
  } catch (cause) {
    error = cause;
  }
  try {
    seal(core, job);
  } catch (cause) {
    if (error === null) error = cause;
  } finally {
    drop(core, job);
  }
  if (error !== null) {
    core.cancelErrors += 1;
    core.lastCancelError = error instanceof Error ? error.message : String(error);
  }
  return true;
}
