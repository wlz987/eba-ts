import type { Envelope, ActorId } from "#src/core/envelope.js";
import { closeHost } from "#src/core/job/close.js";
import type { Core } from "#src/core/job/core.js";
import { combineFailures, toError } from "#src/core/errors.js";
import { Schedule } from "#src/core/time.js";
import { RuntimeAdvance, scheduleNow, written } from "#src/core/runtime/advance.js";
import {
  deleteHost,
  agendaOf,
  flushSink,
  hostsOf,
  markBusy,
  markClosed,
  markClosing,
  markDraining,
  markIdle,
  releaseClaimOf,
  stashRoots,
  takeFlushes,
  takeStashed,
} from "#src/core/runtime/base.js";
import { enter, sync } from "#src/core/runtime/admission.js";

export abstract class RuntimeLifecycle extends RuntimeAdvance {
  schedule(): Schedule {
    const now = scheduleNow(this);
    const snapshot = this.agenda.schedule(now);
    return new Schedule(
      !this.busy && snapshot.ready,
      snapshot.nextDeadlineMs,
      !this.busy && snapshot.isQuiescent,
      now,
    );
  }

  shutdown(actorId?: ActorId | string | null): void {
    if (this.state === "closed" || this.state === "closing") return;
    if (this.readingClock) throw new Error("reentrant clock observation");
    const selected = this.#select(actorId ?? null);
    if (this.busy) {
      this.#drain(selected, actorId ?? null);
      return;
    }
    markBusy(this);
    try {
      this.#drain(selected, actorId ?? null);
    } finally {
      markIdle(this);
    }
  }

  /** The two internal steps `shutdown`/`close` are built from, pinned as `#` */
  #drain(selected: Core[], actorId: ActorId | string | null): void {
    if (actorId === null) markDraining(this);
    for (const core of selected) {
      core.beginDrain();
    }
  }

  close(actorId?: ActorId | string | null): Envelope[] {
    return this.#closeWith(actorId);
  }

  /**
   * Retire the runtime and await the final record flush. `close()` reports a
   * synchronous fault; an async sink rejection is only observable here — the
   * append already happened, so the abandoned roots survive it either way.
   */
  async closeAsync(
    actorId?: ActorId | string | null,
    timeoutMs?: number,
  ): Promise<Envelope[]> {
    // A close failure and a flush failure/timeout are independent: both must
    // stay visible, so collect them and combine at the end instead of letting
    // the first one short-circuit the other.
    let closeError: unknown = null;
    let abandoned: Envelope[];
    try {
      abandoned = this.#closeWith(actorId);
    } catch (error) {
      closeError = error;
      abandoned = takeStashed(this);
    }
    const pending = takeFlushes(this);
    let flushError: unknown = null;
    if (timeoutMs === undefined) {
      for (const flush of pending) {
        try { await flush; } catch (error) { if (flushError === null) flushError = error; }
      }
    } else {
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        await Promise.race([
          Promise.all(pending),
          new Promise<never>((_resolve, reject) => {
            timer = setTimeout(
              () => reject(new Error("closeAsync timed out")),
              timeoutMs,
            );
          }),
        ]);
      } catch (error) {
        flushError = error;
      } finally {
        if (timer !== undefined) clearTimeout(timer);
      }
    }
    if (closeError !== null || flushError !== null) {
      // An async failure does not undo the retire, but the roots must be
      // retrievable by the next close (they stay in the stash).
      stashRoots(this, abandoned);
      const combined = closeError !== null && flushError !== null
        ? combineFailures(closeError, flushError)
        : toError(closeError ?? flushError);
      throw combined;
    }
    return abandoned;
  }

  #closeWith(
    actorId: ActorId | string | null | undefined,
  ): Envelope[] {
    const abandoned: Envelope[] = takeStashed(this);
    if (this.state === "closed") return abandoned;
    enter(this);
    try {
      const selected = this.#select(actorId ?? null);
      if (actorId === undefined || actorId === null) {
        markClosing(this);
        // Outgoing offers owned by this runtime die with it; hand their letters
        // back as abandoned roots so a closed host can never deliver them.
        abandoned.push(...this.bus.abandonDeliveries(agendaOf(this)));
        for (const core of selected) {
          core.beginClose();
        }
      }
      let failed: Error | null = null;
      for (const core of selected) {
        try {
          closeHost(core, this.bus, false, null, flushSink(this));
        } catch (error) {
          const raised =
            error instanceof Error ? error : new Error(String(error));
          failed = failed === null ? raised : combineFailures(failed, raised);
        } finally {
          sync(this, core);
        }
      }
      // Retire every selected host even when one failed: a zombie would keep
      // the actor reserved and let a failed close poison the next one.
      for (const core of selected) {
        // A host only leaves the registry once `closeHost` retired it. A sink
        // fault throws before that point, so the host stays open for a retry.
        if (core.state !== "closed") continue;
        abandoned.push(...(core.abandoned ?? []));
        core.clearAbandoned();
        core.signal.detach();
        const actor = String(core.sub.actorId);
        agendaOf(this).remove(actor);
        deleteHost(this, actor);
      }
      if (actorId === undefined || actorId === null) {
        // A failed full close frees the bus but stays "closing", so a retry
        // can still hand back the stashed abandoned roots.
        releaseClaimOf(this);
        if (failed === null) markClosed(this);
      }
      if (failed !== null) throw failed;
      return abandoned;
    } catch (error) {
      stashRoots(this, abandoned);
      throw error;
    } finally {
      markIdle(this);
      agendaOf(this).wakeFn();
    }
  }

  #select(actorId: ActorId | string | null): Core[] {
    if (actorId === null)
      return [...hostsOf(this).values()].map((host) => written(this, host));
    const host = hostsOf(this).get(String(actorId));
    if (host === undefined) throw new Error(`unknown host: ${String(actorId)}`);
    return [written(this, host)];
  }

}
