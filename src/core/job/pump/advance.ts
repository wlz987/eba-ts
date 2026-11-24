import type { Bus } from "#src/core/mail/bus.js";
import type { Clock } from "#src/core/time.js";
import { readClock } from "#src/core/time.js";
import type { IdGen } from "#src/core/envelope.js";
import type { Core } from "#src/core/job/core.js";
import { releaseDeferred } from "#src/core/job/execution.js";
import { heldCount } from "#src/core/mail/inbox.js";
import { pump } from "#src/core/job/pump/step.js";
import { enter, holding, idle, refresh } from "#src/core/job/pump.js";

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
    const held = core.leases.unreleased;
    core.leases.recover();
    return progressed || released || core.leases.unreleased < held;
  } finally {
    idle(core);
    refresh(core, now);
  }
}

export function hasWork(core: Core): boolean {
  return (
    core.state === "closing" ||
    core.closeRequested ||
    core.jobs.size > 0 ||
    core.queue.size > 0 ||
    core.leases.size > 0 ||
    core.mailbox.size > 0 ||
    holding(core) ||
    core.leases.unreleased > 0 ||
    heldCount(core.sub.inbox) > 0 ||
    (core.abandoned !== null && core.abandoned.length > 0)
  );
}
