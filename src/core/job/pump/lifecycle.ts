import type { Core } from "#src/core/job/core.js";
import { busy as markBusy, idle as markIdle } from "#src/core/job/core.js";
import { REENTRANT } from "#src/core/errors.js";
import { holdsKind, pauseWork } from "#src/core/mail/inbox.js";
import { retryDue, waitsForLease } from "#src/core/job/execution.js";
import { holding, SIGNAL_MASK } from "#src/core/job/pump.js";
import { DELIVERY, READ, TIMER, WORK } from "#src/core/notify.js";

export function recomputePause(core: Core): void {
  pauseWork(core.sub.inbox, holding(core) && core.queue.full);
}

export function armClosedWaits(core: Core): void {
  if (core.state !== "running" && core.state !== "draining") return;
  if (!core.sub.inbox.isClosed) return;
  core.leases.elapseOpen();
}

export function scheduled(core: Core): boolean {
  if (waitsForLease(core) || retryDue(core) || core.leases.unreleased > 0)
    return true;
  if (!holding(core)) return core.signal.has(SIGNAL_MASK);
  if (core.queue.full) {
    if (holdsKind(core.sub.inbox, "result")) return true;
    return core.signal.has(TIMER | DELIVERY);
  }
  return core.signal.has(READ | TIMER | DELIVERY);
}

export function refresh(core: Core, nowMs: number): void {
  const isHolding = holding(core);
  recomputePause(core);
  armClosedWaits(core);
  core.signal.set(WORK, !isHolding && core.queue.size > 0);
  const deadline = core.leases.nextDeadlineMs;
  core.signal.set(TIMER, deadline !== null && deadline <= nowMs);
}

export function enter(core: Core, closing = false): void {
  if (!closing) {
    if (core.state === "closed") throw new Error("host closed");
    if (core.state === "closing") throw new Error("host closing");
  }
  if (core.busy) throw new Error(REENTRANT);
  markBusy(core);
}

export function idle(core: Core): void {
  markIdle(core);
}
