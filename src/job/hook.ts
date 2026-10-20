import type { Bus } from "../mail/bus.ts";
import type { Clock } from "../time/clock.ts";
import type { Envelope } from "../envelope/envelope.ts";
import type { IdGen } from "../envelope/idgen.ts";
import { combineFailures, noteSettled } from "../errors.ts";
import { Matchmaker } from "../envelope/reply.ts";
import { errPayload } from "../envelope/result.ts";
import { replyTopic } from "../mail/inbox.ts";
import type { Core } from "./core.ts";
import { Commit, Context, Control, Job, become, sealContext, stateOf } from "./job.ts";
import {
  deadline,
  holdOrSend,
  holdsLetter,
  invoke,
  remove,
  seal,
  silence,
} from "./execution.ts";
import { deferEnding, deliverEnding } from "./hook/deliver.ts";
export { deliverEnding } from "./hook/deliver.ts";

export function runHook(
  core: Core,
  bus: Bus,
  clock: Clock,
  idGen: IdGen,
  job: Job,
  callback: (ctx: Context) => unknown,
): void {
  const state = stateOf(job);
  const root = state.root;
  const ctx = new Context(job, {
    actorId: core.sub.actorId,
    replyTo: replyTopic(core.sub.inbox),
    idGen,
    pendingCount: core.book.count(root.header.id),
  });
  let reply: Envelope | null = null;
  let control: Control | null = null;
  let commit: Commit;
  try {
    try {
      invoke(() => callback(ctx));
    } finally {
      commit = sealContext(ctx);
    }
    [reply, control] = commitTo(core, bus, clock, idGen, job, commit);
    if (control !== null) {
      if (reply === null) {
        [reply, control] = silence(core, job, idGen, control);
      }
      seal(core, job);
    }
  } catch (error) {
    let raised: Error =
      error instanceof Error ? error : new Error(String(error));
    if (control !== null) {
      if (core.state === "running") noteSettled(raised);
      deferEnding(core, bus, job, state, control, reply, raised);
      throw raised;
    }
    become(state, "finished");
    const failure = errPayload("job_failed", { message: String(error) });
    let current: Error = raised;
    try {
      invoke(() => job.onFinished(failure));
    } catch (hookError) {
      current = combineFailures(current, hookError);
    } finally {
      state.notice = "finished";
    }
    try {
      remove(core, job);
    } catch (secondary) {
      core.beginClose();
      current = combineFailures(current, secondary);
    }
    if (root.header.address.kind === "request" && !state.replied) {
      let replyEnvelope: Envelope | null = null;
      try {
        replyEnvelope = new Matchmaker(root.header).reply(
          failure,
          core.sub.actorId,
          idGen,
        );
        holdOrSend(core, bus, job, replyEnvelope);
        state.replied = true;
      } catch (deliveryError) {
        if (replyEnvelope !== null && holdsLetter(core, replyEnvelope)) {
          state.replied = true;
        }
        current = combineFailures(current, deliveryError);
      }
    }
    throw noteSettled(current);
  }
  deliverEnding(core, bus, job, reply, control);
}

function commitTo(
  core: Core,
  bus: Bus,
  clock: Clock,
  idGen: IdGen,
  job: Job,
  commit: Commit,
): [Envelope | null, Control | null] {
  const root = stateOf(job).root;
  const state = stateOf(job);
  let reply: Envelope | null = null;
  if (commit.reply !== null) {
    reply = new Matchmaker(root.header).reply(
      commit.reply,
      core.sub.actorId,
      idGen,
    );
    state.replyBody = commit.reply;
  }
  let control = commit.control;
  if (control === null) {
    for (const intent of commit.requests) {
      core.book.submit(
        bus,
        core.sub.inbox,
        intent.request,
        root.header.id,
        intent.stage,
        deadline(core, clock, intent.timeout),
      );
    }
    if (core.book.count(root.header.id) === 0)
      control = new Control(state.replyBody);
  } else if (control.body === null) {
    control = new Control(state.replyBody);
  }
  return [reply, control];
}
