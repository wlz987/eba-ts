import type { Core } from "#src/core/job/core.js";
import type { JobHost } from "#src/core/job/host.js";
import { coreOf } from "#src/core/job/host.js";
import { attach, hasWork, scheduled } from "#src/core/job/pump.js";
import { combineFailures } from "#src/core/errors.js";
import {
  RuntimeBase,
  agendaOf,
  deleteHost,
  hostsOf,
  markBusy,
  markIdle,
  setHost,
} from "#src/core/runtime/base.js";

/**
 * Enter a turn: check the reentrancy guard, then set it.
 *
 * `advance`/`add`/`close` share this free function rather than a class method,
 * so the guard has a single owner that every entry point goes through.
 */
export function enter(runtime: RuntimeBase): void {
  if (runtime.busy || runtime.readingClock || runtime.state === "closed") {
    throw new Error("closed or reentrant runtime");
  }
  markBusy(runtime);
}

/**
 * Recompute one actor's scheduling footprint on the runtime's agenda.
 *
 */
export function sync(runtime: RuntimeBase, core: Core): void {
  const closing = core.state === "closing";
  const retired = core.state === "closed";
  const leaseDeadline = core.leases.nextDeadlineMs;
  // A held job whose deadline lives in its reply slot (not in the lease
  // registry book) must still wake the runtime at expiry; otherwise an idle
  // Drive never settles the held job and it hangs past its deadline.
  const heldDeadline = core.nextHeldLeaseDeadlineMs;
  let deadline: number | null =
    leaseDeadline !== null && heldDeadline !== null
      ? Math.min(leaseDeadline, heldDeadline)
      : (leaseDeadline ?? heldDeadline);
  if (!closing && !retired) {
    const timerDeadline = core.nextTimerMs;
    if (timerDeadline !== null && (deadline === null || timerDeadline < deadline)) {
      deadline = timerDeadline;
    }
  }
  const work = !retired && hasWork(core);
  agendaOf(runtime).update(
    core.sub.actorId,
    closing || retired ? null : deadline,
    closing || work,
    !closing && work && scheduled(core),
  );
}

export class RuntimeAdmission extends RuntimeBase {
  add(host: JobHost): void {
    enter(this);
    try {
      if (this.state !== "running")
        throw new Error("runtime cannot accept hosts");
      const core = coreOf(host);
      if (core.signal.handed) throw new Error("host already admitted");
      const actor = String(core.sub.actorId);
      if (hostsOf(this).has(actor)) throw new Error("duplicate host actor");
      attach(core, this.bus);
      // Deferred continuations read `core.clock` (see `runHook`): a resume that
      // runs outside a turn must use the live clock, not the pinned instant
      // captured when its hook was created.
      core.attachClock(this.clock);
      setHost(this, actor, host);
      core.signal.bind(agendaOf(this), actor);
      sync(this, core);
      if (this.state !== "running") {
        const refused = new Error("runtime cannot accept hosts");
        deleteHost(this, actor);
        agendaOf(this).remove(actor);
        core.signal.detach();
        try {
          core.sub.stop();
        } catch (stuck) {
          throw combineFailures(refused, stuck);
        }
        throw refused;
      }
    } finally {
      markIdle(this);
    }
  }
}
