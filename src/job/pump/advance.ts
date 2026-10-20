import type { Bus } from "../../mail/bus.ts";
import type { Clock } from "../../time/clock.ts";
import { readClock } from "../../time/clock.ts";
import type { IdGen } from "../../envelope/idgen.ts";
import type { Core } from "../core.ts";
import { releaseDeferred } from "../execution.ts";
import { READ } from "../../notify/signal.ts";
import { pump } from "./step.ts";
import { enter, holding, idle, refresh } from "../pump.ts";

export function advanceHost(
  core: Core,
  bus: Bus,
  clock: Clock,
  idGen: IdGen,
): boolean {
  const now = readClock(clock);
  enter(core);
  try {
    const released = releaseDeferred(core);
    const progressed = pump(core, bus, clock, idGen, core.workBudget);
    const held = core.book.unreleased;
    core.book.recover();
    return progressed || released || core.book.unreleased < held;
  } finally {
    idle(core);
    refresh(core, now);
  }
}

export function hasWork(core: Core): boolean {
  return (
    core.state === "closing" ||
    core.jobs.size > 0 ||
    core.queue.size > 0 ||
    core.book.size > 0 ||
    core.outbox.size > 0 ||
    holding(core) ||
    core.book.unreleased > 0 ||
    core.signal.has(READ) ||
    (core.abandoned !== null && core.abandoned.length > 0)
  );
}
