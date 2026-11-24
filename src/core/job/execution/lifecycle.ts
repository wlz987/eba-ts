import type { Bus } from "#src/core/mail/bus.js";
import type { Clock } from "#src/core/time.js";
import { readClock } from "#src/core/time.js";
import type { Envelope, IdGen } from "#src/core/envelope.js";
import { errPayload } from "#src/core/envelope.js";
import { replyTopic } from "#src/core/mail/inbox.js";
import type { Core } from "#src/core/job/core.js";
import { rejectRoot } from "#src/core/job/core.js";
import { Job, Control, stateOf } from "#src/core/job/job.js";
import { invoke, observe, replyFor, respond } from "#src/core/job/execution.js";

export function reject(
  core: Core,
  bus: Bus,
  idGen: IdGen,
  root: Envelope,
  reason: string,
): void {
  rejectRoot(core);
  const send = () => {
    if (root.header.address.kind === "request") {
      respond(core, bus, idGen, root, errPayload(reason));
    }
  };
  const observeReject = () => {
    invoke(() => core.onRejected(root, reason));
  };
  observe(send, observeReject);
}

export function deadline(
  core: Core,
  clock: Clock,
  timeout: number | null = null,
): number | null {
  const chosen = timeout === null ? core.timeout : timeout;
  if (chosen === null) return null;
  const now = readClock(clock);
  return now + chosen;
}

export function silence(
  core: Core,
  job: Job,
  idGen: IdGen,
  control: Control,
): [Envelope | null, Control] {
  const state = stateOf(job);
  const replyTo =
    state.root.header.address.kind === "request"
      ? state.root.header.address.replyTo
      : null;
  if (
    state.replied ||
    state.root.header.address.kind !== "request" ||
    (replyTo === replyTopic(core.sub.inbox) &&
      !core.leases.holds(state.root.header.id))
  ) {
    return [null, control];
  }
  const body = errPayload("job_silent");
  return [
    replyFor(state.root, body, core, idGen),
    new Control(body),
  ];
}

