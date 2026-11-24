import { errPayload } from "#src/core/envelope.js";
import type { Core } from "#src/core/job/core.js";
import { stateOf } from "#src/core/job/context.js";
import { drop } from "#src/core/job/ending.js";
import { invoke } from "#src/core/job/execution.js";
import { become, setNotice } from "#src/core/job/job.js";
import { applyFinished } from "#src/core/job/finished.js";
import { toEffectArray } from "#src/core/job/effect.js";
import type { Job } from "#src/core/job/job.js";

export function cancelJob(core: Core, job: Job): boolean {
  const state = stateOf(job);
  if (state.lifecycle !== "active" || state.held) return false;
  try {
    core.leases.cancel(job.root.header.id);
  } catch {
    return false;
  }
  const body = errPayload("cancelled");
  become(state, "finished");
  setNotice(state, "finished");
  try {
    const returned = invoke(() => job.onFinished(body));
    const extra = toEffectArray(returned);
    if (extra !== null) applyFinished(core, extra);
  } catch {
    // cleanup error is contained: the job is dropped either way
  } finally {
    drop(core, job);
  }
  return true;
}
