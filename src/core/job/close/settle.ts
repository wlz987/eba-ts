import type { Envelope, IdGen } from "#src/core/envelope.js";
import { LeasePending, combineFailures } from "#src/core/errors.js";
import { errPayload } from "#src/core/envelope.js";
import type { Bus } from "#src/core/mail/bus.js";
import type { Core } from "#src/core/job/core.js";
import { Control, markReplied, setNotice, setPendingControl, stateOf } from "#src/core/job/job.js";
import { invoke, respond } from "#src/core/job/execution.js";
import { releaseEnding } from "#src/core/job/ending.js";

/** End every in-flight job so close never drops an ending (I2). */
export function finishJobs(
  core: Core,
  abandoned: Envelope[],
  bus: Bus,
  idGen: IdGen | null,
  settle: boolean,
): [Error | null, Error | null] {
  let hookError: Error | null = null;
  const reason = settle ? "host_closing" : "host_draining";
  for (const job of [...core.jobs.values()]) {
    const state = stateOf(job);
    const root = state.root;
    if (state.pendingControl === null && state.notice === "open") {
      try {
        invoke(() => core.onRejected(root, reason));
      } catch (error) {
        const failure = error as Error;
        hookError =
          hookError === null ? failure : combineFailures(hookError, failure);
      }
      setNotice(state, "rejected");
    }
    if (settle && state.pendingControl === null) {
      const body = errPayload(reason);
      setPendingControl(state, new Control(body));
      if (!state.replied && idGen !== null && root.header.address.kind === "request") {
        try {
          respond(core, bus, idGen, root, body);
          markReplied(state);
        } catch (error) {
          const failure = error as Error;
          hookError =
            hookError === null ? failure : combineFailures(hookError, failure);
        }
      }
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
    const item = core.queueShift();
    if (item !== null)
      notice = settleLetter(core, item, abandoned, Math.max(notice, 1));
  }
  if (core.sub.inbox.isClosed) return;
  while (true) {
    const envelope = core.sub.tryRecv();
    if (envelope === null) break;
    notice = settleLetter(core, envelope, abandoned, notice);
  }
}

