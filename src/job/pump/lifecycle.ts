import type { Core } from "../core.ts";
import { REENTRANT } from "../../errors.ts";
import { holdsKind, pauseWork } from "../../mail/inbox.ts";
import { retryDue, waitsForLease } from "../execution.ts";
import { holding, SIGNAL_MASK } from "../pump.ts";
import { DELIVERY, READ, TIMER, WORK } from "../../notify/signal.ts";

export function recomputePause(core: Core): void {
  pauseWork(core.sub.inbox, holding(core) && core.queue.full);
}

export function armClosedWaits(core: Core): void {
  if (core.state !== "running" && core.state !== "draining") return;
  if (!core.sub.inbox.isClosed) return;
  core.book.elapseOpen();
}

export function scheduled(core: Core): boolean {
  if (waitsForLease(core) || retryDue(core) || core.book.unreleased > 0)
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
  const deadline = core.book.nextDeadlineMs;
  core.signal.set(TIMER, deadline !== null && deadline <= nowMs);
}

export function enter(core: Core, closing = false): void {
  if (!closing) {
    if (core.state === "closed") throw new Error("host closed");
    if (core.state === "closing") throw new Error("host closing");
  }
  if (core.busy) throw new Error(REENTRANT);
  core.busy = true;
}

export function idle(core: Core): void {
  core.busy = false;
}
