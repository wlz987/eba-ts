import type { Bus } from "../../mail/bus.ts";
import type { Clock } from "../../time/clock.ts";
import { readClock } from "../../time/clock.ts";
import type { Envelope } from "../../envelope/envelope.ts";
import type { IdGen } from "../../envelope/idgen.ts";
import { Matchmaker } from "../../envelope/reply.ts";
import { errPayload } from "../../envelope/result.ts";
import { nonnegativeInt } from "../../time/numbers.ts";
import { replyTopic } from "../../mail/inbox.ts";
import type { Core } from "../core.ts";
import { Job } from "../job.ts";
import { Control, stateOf } from "../job.ts";
import { invoke, observe, respond } from "../execution.ts";

export function reject(
  core: Core,
  bus: Bus,
  idGen: IdGen,
  root: Envelope,
  reason: string,
): void {
  core.rejectedCount += 1;
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
  return nonnegativeInt(now + chosen, "deadline_ms");
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
      !core.book.holds(state.root.header.id))
  ) {
    return [null, control];
  }
  const body = errPayload("job_silent");
  return [
    new Matchmaker(state.root.header).reply(body, core.sub.actorId, idGen),
    new Control(body),
  ];
}

