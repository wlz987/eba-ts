import type { Clock } from "../time/clock.ts";
import { pinClock, readClock } from "../time/clock.ts";
import type { Core } from "../job/core.ts";
import type { JobHost } from "../job/host.ts";
import { coreOf } from "../job/host.ts";
import { advanceHost, armClosedWaits } from "../job/pump.ts";
import { closeCount } from "../mail/inbox.ts";
import { DeliveryError, WakeError } from "../errors.ts";
import { RuntimeAdmission } from "./admission.ts";

export abstract class RuntimeAdvance extends RuntimeAdmission {
  private view: Clock = { nowMs: () => this.now() };

  protected abstract answerSpent(error: DeliveryError, clock: Clock): void;
  protected abstract written(host: JobHost): Core;

  advance(): boolean {
    this.enter();
    try {
      const now = this.scheduleNow();
      this.observed = now;
      const pinned = pinClock(now);
      const noticed = closeCount();
      if (noticed !== this.noticedCloses) {
        this.noticedCloses = noticed;
        for (const host of [...this.hosts.values()]) {
          const core = coreOf(host);
          if (core.sub.inbox.isClosed) {
            armClosedWaits(core);
            this.sync(core);
          }
        }
      }
      this.agenda.due(now);
      const count = this.agenda.readySize;
      let progressed = false;
      for (let index = 0; index < count; index += 1) {
        const actor = this.agenda.pop();
        if (actor === null) break;
        const host = this.hosts.get(actor)!;
        const core = this.written(host);
        try {
          if (core.state === "running" || core.state === "draining") {
            host.onStep(pinned);
            progressed =
              advanceHost(core, this.bus, pinned, this.idGen) || progressed;
          }
        } catch (error) {
          if (error instanceof DeliveryError) {
            this.answerSpent(error, pinned);
          }
          throw error;
        } finally {
          this.sync(core);
        }
      }
      const fault = this.agenda.takeFault();
      if (fault !== null) {
        const error = new WakeError(String(fault));
        error.cause = fault;
        throw error;
      }
      return progressed;
    } finally {
      this.observed = null;
      this.busy = false;
    }
  }

  private now(): number {
    if (this.readingClock) throw new Error("reentrant clock observation");
    this.readingClock = true;
    try {
      return this.clock.nowMs();
    } finally {
      this.readingClock = false;
    }
  }

  protected scheduleNow(): number {
    if (this.observed !== null) return this.observed;
    return readClock(this.view);
  }
}
