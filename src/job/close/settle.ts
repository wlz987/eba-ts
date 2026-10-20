import { Envelope } from "../../envelope/envelope.ts";
import { LeasePending, combineFailures } from "../../errors.ts";
import type { Core } from "../core.ts";
import { stateOf } from "../job.ts";
import { invoke } from "../execution.ts";
import { releaseEnding } from "../ending.ts";

export function finishJobs(
  core: Core,
  abandoned: Envelope[],
): [Error | null, Error | null] {
  let hookError: Error | null = null;
  for (const job of [...core.jobs.values()]) {
    const state = stateOf(job);
    const root = state.root;
    if (state.pendingControl === null && state.notice === "open") {
      try {
        invoke(() => core.onRejected(root, "host_draining"));
      } catch (error) {
        const failure = error as Error;
        hookError =
          hookError === null ? failure : combineFailures(hookError, failure);
      }
      state.notice = "rejected";
    }
    try {
      releaseEnding(core, job);
    } catch (error) {
      if (error instanceof LeasePending) return [hookError, error];
      if (core.jobs.get(root.header.id) !== job) abandoned.push(root);
      const failure = error as Error;
      hookError =
        hookError === null ? failure : combineFailures(hookError, failure);
      continue;
    }
    abandoned.push(root);
  }
  return [hookError, null];
}

export function settleLetter(
  core: Core,
  envelope: Envelope,
  abandoned: Envelope[],
  notice: number,
): number {
  abandoned.push(envelope);
  if (notice <= 0) return 0;
  invoke(() => core.onRejected(envelope, "host_draining"));
  return notice - 1;
}

export function settleInbound(
  core: Core,
  abandoned: Envelope[],
  notice: number,
): void {
  while (core.queue.size > 0) {
    const item = core.queue.shift();
    if (item instanceof Envelope)
      notice = settleLetter(core, item, abandoned, Math.max(notice, 1));
  }
  if (core.sub.inbox.isClosed) return;
  while (true) {
    const envelope = core.sub.tryRecv();
    if (envelope === null) break;
    notice = settleLetter(core, envelope, abandoned, notice);
  }
}

