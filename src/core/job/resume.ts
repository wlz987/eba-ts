import type { Bus } from "#src/core/mail/bus.js";
import type { Clock } from "#src/core/time.js";
import type { Envelope, IdGen } from "#src/core/envelope.js";
import { combineFailures } from "#src/core/errors.js";
import { LeaseContinuation } from "#src/core/job/lease_registry.js";
import type { Core } from "#src/core/job/core.js";
import { Job, become, stateOf } from "#src/core/job/job.js";
import { UnreadableResult } from "#src/core/rendezvous/records.js";
import { reject, runHook } from "#src/core/job/execution.js";

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
    job = core.makeJob(root);
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
  core.admitJob(root.header.id, job);
  runHook(core, bus, clock, idGen, job, (ctx) => job.begin(ctx));
}

export function keepUnread(core: Core, envelope: Envelope): void {
  core.keepAbandoned(envelope);
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
        ? core.leases.expireDue(source)
        : core.leases.takeResult(source);
  } catch (error) {
    if (error instanceof UnreadableResult) {
      if (typeof source !== "number") keepUnread(core, source);
      return;
    }
    const abandoned = core.abandonedMut();
    if (typeof source !== "number") abandoned.push(source);
    throw error;
  }
  if (claim === null) return;
  const continuation = claim.continuation;
  if (!(continuation instanceof LeaseContinuation)) return;
  const job = core.jobs.get(continuation.owner);
  if (job === undefined) return;
  runHook(core, bus, clock, idGen, job, (ctx) =>
    job.onStageResult(ctx, continuation.stage, claim.completion),
  );
}
