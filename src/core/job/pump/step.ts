import type { Bus } from "#src/core/mail/bus.js";
import type { Clock } from "#src/core/time.js";
import { readClock } from "#src/core/time.js";
import type { IdGen } from "#src/core/envelope.js";
import { DeliveryError } from "#src/core/errors.js";
import type { Core } from "#src/core/job/core.js";
import { stepPhase } from "#src/core/job/core.js";
import type { Step } from "#src/core/job/mailbox.js";
import { DELIVERY } from "#src/core/notify.js";
import { pullKind } from "#src/core/mail/inbox.js";
import { adopt, noteDeliverySuccess, raiseDeliveryFailure, releaseResult, resume, retryDue, refused, reject } from "#src/core/job/execution.js";
import {
  PHASE_FLAG,
  dispatch,
  holding,
  recv,
  refresh,
  snapshotOrKeep,
} from "#src/core/job/pump.js";

export function step(
  core: Core,
  bus: Bus,
  clock: Clock,
  idGen: IdGen,
  phase: number,
): Step {
  if (holding(core) && phase === 2) {
    const root = core.queueShiftIf(
      (item) => item.header.address.kind === "event",
    );
    if (root === null) return "idle";
    let reason = refused(core);
    if (reason === null && core.jobs.size >= core.activeLimit) reason = "active_full";
    if (reason !== null) reject(core, bus, idGen, root, reason);
    else adopt(core, bus, clock, idGen, root);
    return "advanced";
  }
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
    core.queuePush(frozen);
    return "advanced";
  }
  if (phase === 3 && (retryDue(core) || core.signal.has(DELIVERY))) {
    let stepValue: Step;
    try {
      const [flushStep, sent] = core.mailbox.flushOne(bus);
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
  const root = core.queueShift();
  if (root === null) return "idle";
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
    const phase = stepPhase(core);
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

