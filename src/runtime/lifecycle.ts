import type { Envelope } from "../envelope/envelope.ts";
import type { ActorId } from "../envelope/ids.ts";
import { closeHost } from "../job/close.ts";
import type { Core } from "../job/core.ts";
import { combineFailures } from "../errors.ts";
import { Schedule } from "../time/schedule.ts";
import { RuntimeAdvance } from "./advance.ts";

export abstract class RuntimeLifecycle extends RuntimeAdvance {
  schedule(): Schedule {
    const now = this.scheduleNow();
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
    const selected = this.select(actorId ?? null);
    if (this.busy) {
      this.drain(selected, actorId ?? null);
      return;
    }
    this.busy = true;
    try {
      this.drain(selected, actorId ?? null);
    } finally {
      this.busy = false;
    }
  }

  private drain(selected: Core[], actorId: ActorId | string | null): void {
    if (actorId === null) this.state = "draining";
    for (const core of selected) {
      if (core.state === "running") core.state = "draining";
    }
  }

  close(actorId?: ActorId | string | null): Envelope[] {
    if (this.state === "closed") return [];
    this.enter();
    try {
      const selected = this.select(actorId ?? null);
      if (actorId === undefined || actorId === null) {
        this.state = "closing";
        for (const core of selected) {
          core.beginClose();
        }
      }
      let failed: Error | null = null;
      for (const core of selected) {
        try {
          closeHost(core, this.bus);
        } catch (error) {
          const raised =
            error instanceof Error ? error : new Error(String(error));
          failed = failed === null ? raised : combineFailures(failed, raised);
          if (core.state !== "closed") throw failed;
        } finally {
          this.sync(core);
        }
      }
      if (failed !== null) throw failed;
      const abandoned: Envelope[] = [];
      for (const core of selected) {
        abandoned.push(...(core.abandoned ?? []));
        core.abandoned = null;
        core.signal.detach();
        const actor = String(core.sub.actorId);
        this.agenda.remove(actor);
        this.hosts.delete(actor);
      }
      if (actorId === undefined || actorId === null) {
        this.state = "closed";
        this.bus.release(this);
      }
      return abandoned;
    } finally {
      this.busy = false;
      this.agenda.wakeFn();
    }
  }

  private select(actorId: ActorId | string | null): Core[] {
    if (actorId === null)
      return [...this.hosts.values()].map((host) => this.written(host));
    const host = this.hosts.get(String(actorId));
    if (host === undefined) throw new Error(`unknown host: ${String(actorId)}`);
    return [this.written(host)];
  }

}
