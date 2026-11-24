import { DeliveryError, MailboxFull, RecordSinkError, isSettled, toError } from "#src/core/errors.js";
import { MAX_TIMER_DELAY_MS } from "#src/core/time.js";
import type { Caller } from "#src/core/job/caller.js";
import type { Runtime } from "#src/core/runtime/runtime.js";
import { Pending } from "#src/core/runtime/signals.js";
export { Pending };
import { DriveOpen } from "#src/core/runtime/drive/open.js";
import {
  armSpace,
  clearWaiters,
  driveClosed,
  driveRuntime,
  driveStops,
  driveWaiters,
  driveWake,
  retainStops,
  setPumping,
  setReleasing,
} from "#src/core/runtime/drive/base.js";
import { coreOf } from "#src/core/job/host.js";

export function runDrive(drive: Drive): Promise<void> {
  return drive.$runLoop();
}

export function failWaiters(drive: Drive, error: Error): void {
  drive.$failWaiters(error);
}

function shutDoor(caller: Caller): boolean {
  const core = coreOf(caller);
  return (
    core.state === "closing" ||
    core.state === "closed" ||
    core.sub.inbox.isClosed
  );
}

export class Drive extends DriveOpen {
  constructor(runtime: Runtime) {
    super(runtime);
  }

  async $runLoop(): Promise<void> {
    while (!driveClosed(this)) {
      this.$release();
      let moved: boolean;
      setPumping(this, true);
      try {
        try {
          moved = driveRuntime(this).advance();
        } catch (error) {
          // A record-sink fault is a hard failure: fail the Drive over rather
          // than looping on a sink that keeps failing.
          if (error instanceof RecordSinkError) throw toError(error);
          if (!(error instanceof DeliveryError) && !isSettled(error)) {
            throw toError(error);
          }
          moved = true;
        }
      } finally {
        setPumping(this, false);
      }
      if (driveClosed(this)) return;
      let delay = this.$delay();
      if (delay === 0 && moved) {
        await yieldTurn();
        continue;
      }
      if (delay === 0) {
        delay = null;
      }
      if (driveWake(this)!.consume()) continue;
      await this.$pause(delay);
    }
    this.$release();
  }

  $release(): void {
    setReleasing(this, true);
    try {
      const kept: Array<{ caller: Caller; rootId: string }> = [];
      for (const stop of driveStops(this)) {
        try {
          stop.caller.revoke(stop.rootId);
        } catch (error) {
          if (error instanceof MailboxFull) {
            kept.push(stop);
            armSpace(this, stop.caller);
          } else if (shutDoor(stop.caller)) {
            // 目标 host 正在关闭或已关闭：该 call 不再重试，静默丢弃。
          } else {
            throw error;
          }
        }
      }
      retainStops(this, kept);
    } finally {
      setReleasing(this, false);
    }
  }

  $delay(): number | null {
    const snapshot = driveRuntime(this).schedule();
    const wait = snapshot.waitMs();
    if (wait === 0) return 0;
    if (wait === null || driveRuntime(this).clock.armsTimer?.() === false)
      return null;
    return wait;
  }

  async $pause(delayMs: number | null): Promise<void> {
    const wake = driveWake(this);
    if (wake === null) return;
    let settle!: () => void;
    const parked = new Promise<void>((resolve) => {
      settle = resolve;
    });
    // Same 32-bit clamp as `Wake.wait`: an unrepresentable delay is silently
    // shortened to 1 ms by the host, so the alarm would fire ~1000×/s and the
    // loop would busy-turn. Cap it; the next turn recomputes the (still future)
    // deadline and re-arms.
    const alarm =
      delayMs === null
        ? null
        : setTimeout(() => wake.expire(settle), Math.min(delayMs, MAX_TIMER_DELAY_MS));
    wake.arm(settle);
    try {
      await parked;
    } finally {
      wake.disarm(settle);
      if (alarm !== null) clearTimeout(alarm);
    }
  }

  $failWaiters(error: Error): void {
    const pending: Array<(e: Error) => void> = [];
    const waiters = driveWaiters(this);
    for (const waiter of [...waiters.values()]) {
      if (!waiter.done) {
        waiter.done = true;
        pending.push(waiter.reject);
      }
    }
    clearWaiters(this);
    for (const reject of pending) reject(error);
  }

}

function yieldTurn(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}
