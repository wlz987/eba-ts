import type { Bus } from "#src/core/mail/bus.js";
import type { Clock } from "#src/core/time.js";
import { readClock } from "#src/core/time.js";
import type { Envelope, EnvelopeId, IdGen } from "#src/core/envelope.js";
import { BackpressureError, combineFailures, noteSettled } from "#src/core/errors.js";
import { errPayload, makeEnvelope } from "#src/core/envelope.js";
import { replyTopic } from "#src/core/mail/inbox.js";
import type { Core } from "#src/core/job/core.js";
import { Commit, Context, Control, Job, become, markHeld, markReplied, sealContext, setNotice, setReplyBody, stateOf } from "#src/core/job/job.js";
import {
  deadline,
  holdOrSend,
  holdsLetter,
  invoke,
  remove,
  seal,
  replyFor,
  silence,
} from "#src/core/job/execution.js";
import { applyEffects, toEffectArray } from "#src/core/job/effect.js";
import { bufferMint } from "#src/core/job/context/buffer.js";
import { bufferOf } from "#src/core/job/context.js";
import { applyFinished } from "#src/core/job/finished.js";
import type { Effect, EffectHost } from "#src/core/job/effect.js";
import { deferEnding, deliverEnding } from "#src/core/job/hook/deliver.js";
export { deliverEnding } from "#src/core/job/hook/deliver.js";

function makeHost(
  core: Core,
  bus: Bus,
  clock: Clock,
  idGen: IdGen,
  owner: EnvelopeId,
): EffectHost {
  // Stamp `record` with the clock of the turn that *emits* it, not the one
  // captured when this host was built. The read goes through `readClock` like
  // every other clock observation, so a broken or fractional `Clock` surfaces as
  // `ClockError` here too instead of a bare `RangeError` from `RecordLog.record`.
  const now = (): number => readClock(core.clock ?? clock);
  return {
    tell: (topic, payload) => {
      bus.tell(makeEnvelope({
        topic,
        payload,
        from: core.sub.actorId,
        kind: "event",
        idGen,
      }));
    },
    record: (topic, payload) => {
      const envelope = makeEnvelope({
        topic,
        payload,
        from: core.sub.actorId,
        kind: "event",
        idGen,
      });
      const written = core.recordFact(envelope, now());
      if (written === null) {
        // The ring is a bounded staging area, not a reason to kill the job:
        // move the staged facts to the sink, then retry the current fact.
        core.drainRecords();
        const retried = core.recordFact(envelope, now());
        if (retried === null) throw new BackpressureError("record ring full");
      }
    },
    barrier: (upTo) => {
      // A barrier may only wait for seqs that have already been recorded
      // (or will be: the next one). A target beyond the recorded range can
      // never become durable, so reject it with an explicit error instead of
      // parking a promise that never resolves.
      const last = core.records.lastSeq;
      if (upTo !== undefined && upTo > last) {
        throw new Error(`barrier target beyond recorded seq: ${upTo} > ${last}`);
      }
      const target = upTo ?? last;
      core.deferWait(core.recordBarrier(target));
    },
    timer: (deadlineMs) => {
      core.deferWait(core.waitTimer(deadlineMs, owner));
    },
    close: () => { core.requestClose(); },
  };
}

function runFinishedEffects(
  core: Core,
  bus: Bus,
  clock: Clock,
  idGen: IdGen,
  job: Job,
  body: Record<string, unknown> | null,
): void {
  // Build a Context only so the callback can read its root/ids; the effects it
  // returns are applied through `applyFinished`, the *single* validator for
  // what a finished job may still emit. Routing the failure path through
  // `applyEffects` used to accept `barrier`/`timer` here (leaving a stale
  // `core.pendingWait` that no continuation would ever consume) while the
  // normal path rejected them — same job state, two different rules.
  const ctx = new Context(job, {
    actorId: core.sub.actorId,
    replyTo: replyTopic(core.sub.inbox),
    idGen,
    pendingCount: core.leases.count(job.root.header.id),
    core,
    host: makeHost(core, bus, clock, idGen, job.root.header.id),
  });
  const returned = job.onFinished(body);
  const extra = toEffectArray(returned);
  applyFinished(core, [...ctx.takeEffects(), ...(extra ?? [])]);
}

export function runHook(
  core: Core,
  bus: Bus,
  clock: Clock,
  idGen: IdGen,
  job: Job,
  callback: (ctx: Context) => unknown,
  mint?: object,
): void {
  const state = stateOf(job);
  const root = state.root;
  const host = makeHost(core, bus, clock, idGen, root.header.id);
  core.attachHost(host);

  const ctx = new Context(job, {
    actorId: core.sub.actorId,
    replyTo: replyTopic(core.sub.inbox),
    idGen,
    pendingCount: core.leases.count(root.header.id),
    core,
    host,
    ...(mint === undefined ? {} : { mint }),
  });
  let reply: Envelope | null = null;
  let control: Control | null = null;
  let post: readonly Effect[] = [];
  let wait: Promise<void> | null;
  let commit: Commit;
  try {
    try {
      const returned = invoke(() => callback(ctx));
      const extra = toEffectArray(returned);
      if (extra !== null) ctx.pushEffects(extra);
      const all = ctx.takeEffects();
      const at = all.findIndex((effect) =>
        effect.kind === "barrier" || effect.kind === "timer");
      const pre = at < 0 ? all : all.slice(0, at + 1);
      // No barrier/timer ⇒ nothing is deferred; `post` must be empty. (Slicing
      // at `at + 1` with `at === -1` would yield the *whole* effect list, so a
      // stale `pendingWait` left by another job would replay this job's effects.)
      post = at < 0 ? [] : all.slice(at + 1);
      // A finish/settle/revoke/close BEFORE the checkpoint retires the job in
      // this turn, so the post list could
      // never run — silently swallowing it would be a fraud. Name the effect.
      // `hold` is deliberately not terminal (a held checkpoint with post is the
      // legal shape), and a terminal IN the post list is legal too.
      if (post.length > 0 && at >= 0) {
        const terminal = all.slice(0, at).find((effect) =>
          effect.kind === "finish" || effect.kind === "settle" ||
          effect.kind === "revoke" || effect.kind === "close");
        if (terminal !== undefined) {
          throw new Error(
            `unsupported ${terminal.kind} effect before a checkpoint with post effects`,
          );
        }
      }
      applyEffects(ctx, pre);
    } finally {
      commit = sealContext(ctx);
    }
    wait = core.takePendingWait();
    [reply, control] = commitTo(core, bus, clock, idGen, job, commit, wait !== null);
    if (control !== null) {
      if (reply === null) {
        [reply, control] = silence(core, job, idGen, control);
      }
      seal(core, job);
    }
  } catch (error) {
    const raised: Error =
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
      runFinishedEffects(core, bus, clock, idGen, job, failure);
    } catch (hookError) {
      current = combineFailures(current, hookError);
    } finally {
      setNotice(state, "finished");
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
        replyEnvelope = replyFor(root, failure, core, idGen);
        holdOrSend(core, bus, job, replyEnvelope);
        markReplied(state);
      } catch (deliveryError) {
        if (replyEnvelope !== null && holdsLetter(core, replyEnvelope)) {
          markReplied(state);
        }
        current = combineFailures(current, deliveryError);
      }
    }
    throw noteSettled(current);
  }
  deliverEnding(core, bus, job, reply, control);

  if (wait !== null) {
    const owner = root.header.id;
    core.defer(wait, () => {
      if (core.jobs.get(owner) !== job) return;
      // The resume runs *outside* a turn, long after the hook that armed it.
      // `clock` here is the per-turn pinned clock captured at hook time, so
      // reusing it would compute every deadline from a stale instant (an
      // `ask(timeout)` made at t=500 would get deadline 0+timeout and expire
      // immediately). Use the runtime's live clock, which is the same source
      // the next turn will pin.
      const live = core.clock ?? clock;
      // The post list was built by the outer turn's context, so its Intents
      // carry that buffer's mint token. Replay must reuse the same token — a
      // fresh Context mints its own and would reject them as forged.
      const outerMint = bufferMint(bufferOf(ctx));
      if (post.length > 0) {
        runHook(core, bus, live, idGen, job, () => post, outerMint);
      } else {
        runHook(core, bus, live, idGen, job, (next) => job.onResume(next));
      }
    });
  }
}

function commitTo(
  core: Core,
  bus: Bus,
  clock: Clock,
  idGen: IdGen,
  job: Job,
  commit: Commit,
  waiting: boolean,
): [Envelope | null, Control | null] {
  const root = stateOf(job).root;
  const state = stateOf(job);
  if (commit.hold !== null) return [null, null];
  let reply: Envelope | null = null;
  if (commit.reply !== null) {
    reply = replyFor(root, commit.reply, core, idGen);
    setReplyBody(state, commit.reply);
  } else if (state.held) {
    // A held job answers through its lease: settle/revoke in a later turn wrote
    // the body into the held lease, not into `commit.reply`. Pull that body out
    // and answer the caller with it, retracting the slot so `resolveHeld` cannot
    // deliver it a second time.
    const requestId = root.header.id;
    const slot = core.held.get(requestId);
    // Only a slot that was actually settled this turn answers the caller. A
    // held turn that merely asked/told/deadlined has an empty slot and must
    // keep it installed — retracting it here strands a later settle.
    if (slot !== undefined && slot.isSettled) {
      const body = slot.take();
      // Retract from the table only: `resolveHeld` walks `core.held`, so a
      // retracted slot cannot be delivered twice.
      core.releaseHeld(requestId);
      markHeld(state, false);
      if (body !== null) {
        reply = replyFor(root, body, core, idGen);
        setReplyBody(state, body);
      }
    }
  }
  let control = commit.control;
  if (control === null) {
    for (const intent of commit.requests) {
      core.leases.submit(
        bus,
        core.sub.inbox,
        intent.request,
        root.header.id,
        intent.stage,
        deadline(core, clock, intent.timeout),
      );
    }
    if (!waiting && core.leases.count(root.header.id) === 0)
      control = new Control(state.replyBody);
  } else if (control.body === null) {
    control = new Control(state.replyBody);
  }
  return [reply, control];
}
