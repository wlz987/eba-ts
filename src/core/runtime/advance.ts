import type { Envelope, EnvelopeId } from "#src/core/envelope.js";
import { correlationIdOf, makeEnvelope, errPayload } from "#src/core/envelope.js";
import type { Clock } from "#src/core/time.js";
import { pinClock, readClock } from "#src/core/time.js";
import type { Core } from "#src/core/job/core.js";
import type { JobHost } from "#src/core/job/host.js";
import { coreOf } from "#src/core/job/host.js";
import { driveResume } from "#src/core/job/host_view.js";
import { advanceHost, armClosedWaits } from "#src/core/job/pump.js";
import { resolveHeld } from "#src/core/job/held.js";
import { closeHost } from "#src/core/job/close.js";
import { closeCount } from "#src/core/mail/inbox.js";
import { DeliveryError, RecordSinkError, WakeError } from "#src/core/errors.js";
import { RuntimeAdmission, enter, sync } from "#src/core/runtime/admission.js";
import {
  clearObserved,
  deleteHost,
  agendaOf,
  hostsOf,
  markClockRead,
  markIdle,
  markReadingClock,
  noteCloses,
  pinTurn,
} from "#src/core/runtime/base.js";

/**
 * The host's core, or a rejection when the host is bound to another agenda.
 * `advance` calls this before touching a host's books: the runtime is the only
 * writer.
 */
export function written(runtime: RuntimeAdvance, host: JobHost): Core {
  const core = coreOf(host);
  if (!core.signal.boundTo(agendaOf(runtime)))
    throw new Error("runtime is the only writer");
  return core;
}

/**
 * Hand a `delivery_failed` notice to the consumer whose answer was spent.
 *
 * Same seam as `written` above: as a `protected abstract` method it was a
 * public prototype method, and it published a synthetic result envelope into the
 * runtime's own bus, resuming a held job with a body nobody produced. It belongs
 * inside `advance`'s error branch, not on the class.
 */
export function answerSpent(
  runtime: RuntimeAdvance,
  error: DeliveryError,
  clock: Clock,
): void {
  const envelope = error.envelope;
  const requestId = correlationIdOf(envelope.header.address);
  if (
    envelope.header.address.kind !== "result" ||
    requestId === undefined ||
    !spent(runtime, envelope)
  ) {
    return;
  }
  const failure = makeEnvelope({
    topic: envelope.header.topic,
    payload: errPayload("delivery_failed"),
    from: envelope.header.from,
    idGen: runtime.idGen,
    cause: envelope.header.cause,
    kind: "result",
    correlationId: requestId,
  });
  let held = false;
  for (const host of hostsOf(runtime).values()) {
    if (
      driveResume(host, requestId, failure, runtime.bus, clock, runtime.idGen, () =>
        sync(runtime, coreOf(host)),
      )
    )
      held = true;
  }
  if (!held) runtime.bus.deliver(failure, agendaOf(runtime));
}

function spent(runtime: RuntimeAdvance, envelope: Envelope): boolean {
  for (const host of hostsOf(runtime).values()) {
    if (host.deliverySpent(envelope)) return true;
  }
  return false;
}

function drainCancelledAll(runtime: RuntimeAdvance): boolean {
  let progressed = false;
  let seen: Set<EnvelopeId> | null = null;
  for (let round = 0; round < 256; round += 1) {
    const cancelled = drainCancelled(runtime);
    if (cancelled.length === 0) return progressed;
    progressed = true;
    if (seen === null) seen = new Set<EnvelopeId>();
    let fresh = false;
    for (const id of cancelled) {
      if (!seen.has(id)) {
        seen.add(id);
        fresh = true;
      }
    }
    for (const host of hostsOf(runtime).values()) sync(runtime, coreOf(host));
    // A revoke that cannot make progress must not spin this turn forever.
    if (!fresh) return progressed;
  }
  return progressed;
}

function closeRequestedHosts(runtime: RuntimeAdvance): boolean {
  // Two phases: collect first, then close. Closing mutates `hosts`/`agenda`,
  // so the scan must not hold a snapshot — but a snapshot is also an
  // allocation per turn even when nobody asked to close (measured: this method
  // was 4.1% of a 20 000-ask profile, mostly the `[...hostsOf(runtime)]` copy).
  // Collecting the *actors* (usually none) costs nothing in the common case.
  const closing: string[] = [];
  for (const [actor, host] of hostsOf(runtime)) {
    if (coreOf(host).takeCloseRequest()) closing.push(actor);
  }
  let closed = false;
  for (const actor of closing) {
    const host = hostsOf(runtime).get(actor);
    if (host === undefined) continue;
    const core = coreOf(host);
    closed = true;
    try {
      closeHost(core, runtime.bus, true, runtime.idGen);
    } finally {
      core.signal.detach();
      agendaOf(runtime).remove(actor);
      deleteHost(runtime, actor);
      core.clearAbandoned();
    }
  }
  return closed;
}

function drainCancelled(runtime: RuntimeAdvance): EnvelopeId[] {
  const cancelled: EnvelopeId[] = [];
  for (const host of hostsOf(runtime).values()) {
    const core = coreOf(host);
    // `takeCancelled` already returns `[]` without allocating when its own
    // book is empty, but `push(...[])` still pays the spread — so guard both
    // sides. `takePendingCancels` has its own `size === 0` short-circuit
    // (see `Core`), which is what makes this cheap on the common turn.
    const taken = core.takeCancelled();
    if (taken.length > 0) for (const id of taken) cancelled.push(id);
    const pending = core.takePendingCancels();
    if (pending.length > 0) for (const id of pending) cancelled.push(id);
  }
  if (cancelled.length > 0) {
    for (const host of hostsOf(runtime).values()) {
      const core = coreOf(host);
      core.revoke(cancelled);
    }
  }
  return cancelled;
}

export abstract class RuntimeAdvance extends RuntimeAdmission {
  /**
   * The clock view handed to the per-turn pin. Sealed as `#` so a stray
   * `runtime.view = fake` cannot reroute `now`, and with it every lease deadline
   * and timer, through a source the runtime never validated.
   */
  readonly $view: Clock = { nowMs: () => this.#now() };

  /** Hand `$view` to `scheduleNow` below (a module binding, not an accessor). */

  advance(): boolean {
    enter(this);
    try {
      const now = scheduleNow(this);
      // `observed` is a per-turn pin reset in `finally`, so it cannot be the
      // budget reference; the persistent `lastNowMs` carries the real
      // wall-clock elapsed time across turns.
      const delta = pinTurn(this, now);
      const pinned = pinClock(now);
      const noticed = closeCount();
      const closesChanged = noticed !== this.noticedCloses;
      if (closesChanged) noteCloses(this, noticed);
      // One pass instead of four: charge, close-notify, expire/timer/settle and
      // fault collection all touch the same hosts and none mutates the map.
      for (const host of hostsOf(this).values()) {
        const core = coreOf(host);
        core.chargeLeases(delta);
        if (closesChanged && core.sub.inbox.isClosed) {
          armClosedWaits(core);
          sync(this, core);
        }
        core.expireLeases(now);
        core.fireDueTimers(now);
        if (core.settleRecords()) sync(this, core);
        // Surface a record-sink fault armed by `settleRecords`. A persistent
        // failure must abort the turn rather than leave a silent barrier hang.
        const deferred = core.takeDeferFault();
        if (deferred !== null) {
          const error = new WakeError(String(deferred));
          error.cause = deferred;
          throw error;
        }
        const fault = core.takeFault();
        if (fault !== null) {
          const error = new RecordSinkError(fault.message);
          error.cause = fault;
          throw error;
        }
      }
      agendaOf(this).due(now);
      let progressed = drainCancelledAll(this);
      const count = agendaOf(this).readySize;
      for (let index = 0; index < count; index += 1) {
        const actor = agendaOf(this).pop();
        if (actor === null) break;
        const host = hostsOf(this).get(actor)!;
        const core = written(this, host);
        try {
          if (core.state === "running" || core.state === "draining") {
            host.onStep(pinned);
            progressed =
              advanceHost(core, this.bus, pinned, this.idGen) || progressed;
          }
        } catch (error) {
          if (error instanceof DeliveryError) {
            answerSpent(this, error, pinned);
          }
          throw error;
        } finally {
          sync(this, core);
        }
      }
      progressed = drainCancelledAll(this) || progressed;
      for (const host of hostsOf(this).values()) {
        const core = coreOf(host);
        if (core.state !== "running" && core.state !== "draining") continue;
        try {
          if (resolveHeld(core, this.bus, this.idGen)) {
            sync(this, core);
            progressed = true;
          }
        } catch (error) {
          if (error instanceof DeliveryError) answerSpent(this, error, pinned);
          throw error;
        }
      }
      if (closeRequestedHosts(this)) progressed = true;
      // Recompute the agenda once more after the turn's jobs/leases/hosts are
      // gone. Without this, a stale active entry can keep `isQuiescent` false
      // even though every host reports zero work.
      for (const host of hostsOf(this).values()) sync(this, coreOf(host));
      const fault = agendaOf(this).takeFault();
      if (fault !== null) {
        const error = new WakeError(String(fault));
        error.cause = fault;
        throw error;
      }
      return progressed;
    } finally {
      clearObserved(this);
      markIdle(this);
    }
  }

  #now(): number {
    if (this.readingClock) throw new Error("reentrant clock observation");
    markReadingClock(this);
    try {
      return this.clock.nowMs();
    } finally {
      markClockRead(this);
    }
  }

}

/**
 * The instant a scheduling question is asked about.
 *
 * A `protected` method is erased at runtime, so `runtime.scheduleNow()` was an
 * ordinary prototype method on the published `Runtime` — and it reads the
 * runtime's *clock*, which is the one thing `advance` pins per turn. It is an
 * internal helper alongside `enter`/`sync`/`written`, not a public method.
 */
export function scheduleNow(runtime: RuntimeAdvance): number {
  const pinned = runtime.observed;
  if (pinned !== null) return pinned;
  return readClock((runtime.$view));
}
