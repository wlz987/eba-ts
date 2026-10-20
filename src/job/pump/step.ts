import type { Bus } from "../../mail/bus.ts";
import type { Clock } from "../../time/clock.ts";
import { readClock } from "../../time/clock.ts";
import { Envelope } from "../../envelope/envelope.ts";
import type { IdGen } from "../../envelope/idgen.ts";
import { DeliveryError } from "../../errors.ts";
import type { Core } from "../core.ts";
import type { Step } from "../outbox.ts";
import { DELIVERY } from "../../notify/signal.ts";
import { pullKind } from "../../mail/inbox.ts";
import { adopt, noteDeliverySuccess, raiseDeliveryFailure } from "../execution.ts";
import { releaseResult, resume, retryDue, refused, reject } from "../execution.ts";
import {
  PHASE_FLAG,
  dispatch,
  holding,
  recv,
  refresh,
  snapshotOrKeep,
} from "../pump.ts";

export function step(
  core: Core,
  bus: Bus,
  clock: Clock,
  idGen: IdGen,
  phase: number,
): Step {
  if (holding(core) && phase === 2) return "idle";
  if (holding(core) && phase === 0) {
    const envelope = pullKind(core.sub.inbox, "result");
    if (envelope !== null) {
      resume(core, bus, clock, idGen, envelope);
      return "advanced";
    }
    if (core.queue.full) {
      return "idle";
    }
    const received = recv(core);
    if (received === null) return "idle";
    const frozen = snapshotOrKeep(core, received);
    if (frozen === null || frozen.header.address.kind === "diagnostic") {
      return "advanced";
    }
    core.queue.push(frozen);
    return "advanced";
  }
  if (phase === 3 && (retryDue(core) || core.signal.has(DELIVERY))) {
    let stepValue: Step = "idle";
    try {
      const [flushStep, sent] = core.outbox.flushOne(bus);
      stepValue = flushStep;
      if (sent !== null) noteDeliverySuccess(core, sent);
    } catch (error) {
      if (error instanceof DeliveryError) raiseDeliveryFailure(core, error);
      throw error;
    }
    if (releaseResult(core, bus) && stepValue === "idle")
      stepValue = "advanced";
    return stepValue;
  }
  if (!core.signal.has(PHASE_FLAG[phase]!)) return "idle";
  if (phase === 0) {
    const envelope = recv(core);
    if (envelope === null) return "idle";
    if (envelope.header.address.kind === "result") {
      resume(core, bus, clock, idGen, envelope);
    } else {
      dispatch(core, bus, idGen, envelope);
    }
    return "advanced";
  }
  if (phase === 1) {
    resume(core, bus, clock, idGen, readClock(clock));
    return "advanced";
  }
  const root = core.queue.shift();
  if (!(root instanceof Envelope)) return "idle";
  let reason = refused(core);
  if (reason === null && core.jobs.size >= core.activeLimit)
    reason = "active_full";
  if (reason !== null) reject(core, bus, idGen, root, reason);
  else adopt(core, bus, clock, idGen, root);
  return "advanced";
}

export function pump(
  core: Core,
  bus: Bus,
  clock: Clock,
  idGen: IdGen,
  budget: number,
): boolean {
  let progressed = false;
  let idle = 0;
  while (budget > 0 && idle < 4) {
    const phase = core.phase;
    core.phase = (phase + 1) % 4;
    const now = readClock(clock);
    let advanced: Step;
    try {
      advanced = step(core, bus, clock, idGen, phase);
    } finally {
      refresh(core, now);
    }
    if (advanced === "idle") {
      idle += 1;
      continue;
    }
    budget -= 1;
    progressed = progressed || advanced === "advanced";
    idle = 0;
  }
  return progressed;
}

