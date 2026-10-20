import type { Bus } from "../mail/bus.ts";
import type { Envelope } from "../envelope/envelope.ts";
import type { Core } from "./core.ts";
import { DELIVERY, READ, TIMER, WORK } from "../notify/signal.ts";
export { step } from "./pump/step.ts";
export { advanceHost, hasWork } from "./pump/advance.ts";
export { dispatch, snapshotOrKeep } from "./pump/dispatch.ts";

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

import { enter, idle, refresh } from "./pump/lifecycle.ts";
export { armClosedWaits, enter, idle, recomputePause, refresh, scheduled } from "./pump/lifecycle.ts";
