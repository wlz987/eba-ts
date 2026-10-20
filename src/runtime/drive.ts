import { DeliveryError, MailboxFull, isSettled, toError } from "../errors.ts";
import type { Caller } from "../job/caller.ts";
import type { Runtime } from "./runtime.ts";
import { Pending } from "./signals.ts";
export { Pending };
import { DriveOpen } from "./drive/open.ts";

export class Drive extends DriveOpen {
  constructor(runtime: Runtime) {
    super(runtime);
  }

  protected async runLoop(): Promise<void> {
    while (!this.closed) {
      this.release();
      let moved = false;
      this.pumping = true;
      try {
        try {
          moved = this.runtime.advance();
        } catch (error) {
          if (!(error instanceof DeliveryError) && !isSettled(error)) {
            throw toError(error);
          }
          moved = true;
        }
      } finally {
        this.pumping = false;
      }
      if (this.closed) return;
      let delay = this.delay();
      if (delay === 0 && moved) {
        await yieldTurn();
        continue;
      }
      if (delay === 0) {
        delay = null;
      }
      if (this.wake!.consume()) continue;
      await this.pause(delay);
    }
    this.release();
  }

  protected release(): void {
    this.releasing = true;
    try {
      const kept: Array<{ caller: Caller; rootId: string }> = [];
      for (const stop of this.stops) {
        try {
          stop.caller.cancel(stop.rootId);
        } catch (error) {
          if (error instanceof MailboxFull) {
            kept.push(stop);
            this.armSpace(stop.caller);
          } else if (
            stop.caller.state === "closing" ||
            stop.caller.state === "closed" ||
            stop.caller.inbox.isClosed
          ) {
          } else {
            throw error;
          }
        }
      }
      this.stops = kept;
    } finally {
      this.releasing = false;
    }
  }

  private delay(): number | null {
    const snapshot = this.runtime.schedule();
    const wait = snapshot.waitMs();
    if (wait === 0) return 0;
    if (wait === null || this.runtime.clockRef.armsTimer?.() === false)
      return null;
    return wait;
  }

  private async pause(delayMs: number | null): Promise<void> {
    const wake = this.wake;
    if (wake === null) return;
    let settle!: () => void;
    const parked = new Promise<void>((resolve) => {
      settle = resolve;
    });
    const alarm =
      delayMs === null ? null : setTimeout(() => wake.expire(settle), delayMs);
    wake.arm(settle);
    try {
      await parked;
    } finally {
      wake.disarm(settle);
      if (alarm !== null) clearTimeout(alarm);
    }
  }

  protected failWaiters(error: Error): void {
    const pending: Array<(e: Error) => void> = [];
    for (const waiter of [...this.waiters.values()]) {
      if (!waiter.done) {
        waiter.done = true;
        pending.push(waiter.reject);
      }
    }
    this.waiters.clear();
    for (const reject of pending) reject(error);
  }
}

function yieldTurn(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}
