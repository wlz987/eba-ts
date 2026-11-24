import type { Bus } from "#src/core/mail/bus.js";
import type { Envelope } from "#src/core/envelope.js";
import type { Core } from "#src/core/job/core.js";
import { DELIVERY, READ, TIMER, WORK } from "#src/core/notify.js";
import { enter, idle, refresh } from "#src/core/job/pump/lifecycle.js";
export { step } from "#src/core/job/pump/step.js";
export { advanceHost, hasWork } from "#src/core/job/pump/advance.js";
export { dispatch, snapshotOrKeep } from "#src/core/job/pump/dispatch.js";

export const PHASE_FLAG = [READ, TIMER, WORK, DELIVERY];
export const SIGNAL_MASK = READ | TIMER | WORK | DELIVERY;

export function recv(core: Core): Envelope | null {
  if (core.sub.inbox.isClosed) return null;
  return core.sub.tryRecv();
}

export function holding(core: Core): boolean {
  return core.pending !== null || core.rest.length > 0;
}

export function attach(core: Core, bus: Bus): void {
  enter(core);
  try {
    core.sub.start(bus);
    refresh(core, 0);
  } finally {
    idle(core);
  }
}

export { armClosedWaits, enter, idle, recomputePause, refresh, scheduled } from "#src/core/job/pump/lifecycle.js";
