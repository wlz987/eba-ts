import { errPayload } from "../envelope/result.ts";
import type { Core } from "./core.ts";
import { stateOf } from "./context.ts";
import { drop } from "./ending.ts";
import { invoke } from "./execution.ts";
import { become } from "./job.ts";
import type { Job } from "./job.ts";

export function cancelJob(core: Core, job: Job): boolean {
  const state = stateOf(job);
  if (state.lifecycle !== "active" || state.held) return false;
  try {
    core.book.cancel(job.root.header.id);
  } catch {
    return false;
  }
  const body = errPayload("cancelled");
  become(state, "finished");
  state.notice = "finished";
  let error: unknown = null;
  try {
    invoke(() => job.onFinished(body));
  } catch (cause) {
    error = cause;
  } finally {
    drop(core, job);
  }
  if (error !== null) {
    core.cancelErrors += 1;
    core.lastCancelError = error instanceof Error ? error.message : String(error);
  }
  return true;
}
