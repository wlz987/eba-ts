import type { Bus } from "../mail/bus.ts";
import type { Clock } from "../time/clock.ts";
import type { Envelope } from "../envelope/envelope.ts";
import type { IdGen } from "../envelope/idgen.ts";
import { combineFailures } from "../errors.ts";
import { Continuation } from "./book.ts";
import type { Core } from "./core.ts";
import { Job, become, stateOf } from "./job.ts";
import { UnreadableResult } from "../rendezvous/records.ts";
import { reject, runHook } from "./execution.ts";

export function adopt(
  core: Core,
  bus: Bus,
  clock: Clock,
  idGen: IdGen,
  root: Envelope,
): void {
  if (core.jobs.has(root.header.id)) {
    reject(core, bus, idGen, root, "adoption_failed");
    return;
  }
  let job: Job;
  try {
    const produced = core.makeJob(root);
    if (produced instanceof Promise) {
      void Promise.resolve(produced).catch(() => undefined);
      throw new TypeError(
        "make_job must be synchronous and return a fresh Job",
      );
    }
    job = produced;
    if (!(job instanceof Job)) {
      throw new TypeError(
        "make_job must be synchronous and return a fresh Job",
      );
    }
    const state = stateOf(job);
    if (state.root !== root || state.lifecycle !== "new") {
      throw new Error("factory must return a fresh Job for this root");
    }
  } catch (error) {
    try {
      reject(core, bus, idGen, root, "adoption_failed");
    } catch (secondary) {
      throw combineFailures(error, secondary);
    }
    throw error;
  }
  become(stateOf(job), "active");
  core.jobs.set(root.header.id, job);
  runHook(core, bus, clock, idGen, job, (ctx) => job.begin(ctx));
}

export function keepUnread(core: Core, envelope: Envelope): void {
  if (core.abandoned === null) core.abandoned = [];
  core.abandoned.push(envelope);
}

export function resume(
  core: Core,
  bus: Bus,
  clock: Clock,
  idGen: IdGen,
  source: Envelope | number,
): void {
  let claim;
  try {
    claim =
      typeof source === "number"
        ? core.book.takeDue(source)
        : core.book.takeResult(source);
  } catch (error) {
    if (error instanceof UnreadableResult) {
      if (typeof source !== "number") keepUnread(core, source);
      return;
    }
    const abandoned = core.beginClose();
    if (typeof source !== "number") abandoned.push(source);
    throw error;
  }
  if (claim === null) return;
  const continuation = claim.continuation;
  if (!(continuation instanceof Continuation)) return;
  const job = core.jobs.get(continuation.owner);
  if (job === undefined) return;
  runHook(core, bus, clock, idGen, job, (ctx) =>
    job.onStageResult(ctx, continuation.stage, claim.completion),
  );
}
