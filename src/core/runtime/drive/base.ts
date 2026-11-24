import type { Caller } from "#src/core/job/caller.js";
import type { Runtime } from "#src/core/runtime/runtime.js";
import { watchSpace } from "#src/core/mail/inbox.js";
import { SpaceKick, WakeTicket } from "#src/core/runtime/signals.js";
import type { PendingWait } from "#src/core/runtime/signals.js";
import { failWaiters, runDrive } from "#src/core/runtime/drive.js";
import { bindWakeOf } from "#src/core/runtime/base.js";

const attached = new WeakMap<Runtime, DriveBase>();

export function driveRuntime(drive: DriveBase): Runtime {
  return drive.$runtime;
}

export function driveWaiters(drive: DriveBase): ReadonlyMap<string, PendingWait> {
  return drive.$waiters;
}

export function driveStops(
  drive: DriveBase,
): ReadonlyArray<{ caller: Caller; rootId: string }> {
  return drive.$stops;
}

export function driveWake(drive: DriveBase): WakeTicket | null {
  return drive.$wake;
}

export function driveFault(drive: DriveBase): Error | null {
  return drive.$fault;
}

export function driveEnded(drive: DriveBase): boolean {
  return drive.$ended;
}

export function driveClosed(drive: DriveBase): boolean {
  return drive.$closed;
}

export function driveTask(drive: DriveBase): Promise<void> | null {
  return drive.$task;
}

export function parkWaiter(
  drive: DriveBase,
  key: string,
  waiter: PendingWait,
): void {
  drive.$waiters.set(key, waiter);
}

export function claimWaiter(
  drive: DriveBase,
  key: string,
): PendingWait | undefined {
  const waiters = drive.$waiters;
  const waiter = waiters.get(key);
  if (waiter !== undefined) waiters.delete(key);
  return waiter;
}

export function queueStop(drive: DriveBase, caller: Caller, rootId: string): void {
  drive.$stops.push({ caller, rootId });
}

export function retainStops(
  drive: DriveBase,
  kept: Array<{ caller: Caller; rootId: string }>,
): void {
  const stops = drive.$stops;
  stops.length = 0;
  stops.push(...kept);
}

export function clearSpaceKick(drive: DriveBase, inboxId: number): void {
  drive.$spaceKicks.delete(inboxId);
}

export function watchSpaceFor(
  drive: DriveBase,
  inboxId: number,
  kick: SpaceKick,
): void {
  drive.$spaceKicks.set(inboxId, kick);
}

export function setPumping(drive: DriveBase, pumping: boolean): void {
  drive.$pumping = pumping;
}

export function setReleasing(drive: DriveBase, releasing: boolean): void {
  drive.$releasing = releasing;
}

export function markClosed(drive: DriveBase): void {
  drive.$closed = true;
}

export function markEnded(drive: DriveBase, fault: Error | null): void {
  drive.$ended = true;
  drive.$fault = fault;
}

export function setTask(drive: DriveBase, task: Promise<void>): void {
  drive.$task = task;
}

export function clearTask(drive: DriveBase): void {
  drive.$task = null;
}

export function clearWaiters(drive: DriveBase): void {
  drive.$waiters.clear();
}

// A `protected` armSpace was an ordinary prototype method on the published
// `Drive`, so any caller could fire the wake ticket with mail it never sent.
export function armSpace(drive: DriveBase, caller: Caller): void {
  const inbox = caller.inbox;
  const kicks = drive.$spaceKicks;
  if (kicks.has(inbox.id)) return;
  const kick = new SpaceKick(() => {
    clearSpaceKick(drive, inbox.id);
    drive.$wake?.fire();
  });
  watchSpaceFor(drive, inbox.id, kick);
  watchSpace(inbox, kick);
}

export function resetDrive(drive: DriveBase): void {
  drive.$waiters.clear();
  drive.$stops.length = 0;
  drive.$spaceKicks.clear();
  drive.$wake = new WakeTicket();
  drive.$closed = false;
  drive.$pumping = false;
  drive.$releasing = false;
  drive.$fault = null;
  drive.$ended = false;
}

export abstract class DriveBase {
  readonly $runtime: Runtime;
  $waiters = new Map<string, PendingWait>();
  $stops: Array<{ caller: Caller; rootId: string }> = [];
  $wake: WakeTicket | null = new WakeTicket();
  $spaceKicks = new Map<number, SpaceKick>();
  $ended = false;
  $fault: Error | null = null;
  $closed = false;
  $pumping = false;
  $releasing = false;
  $task: Promise<void> | null = null;

  // Not `protected abstract`: that modifier is erased at runtime, so the loop
  // body stayed a callable prototype method. They are `#` methods in `drive.ts`,

  constructor(runtime: Runtime) {
    this.$runtime = runtime;
    resetDrive(this);
  }

  /** True while the drive is open and its loop task is alive. */
  get running(): boolean {
    return !this.$closed && this.$task !== null && !this.$ended;
  }

  async start(): Promise<void> {
    if (this.$task !== null && !this.$ended)
      throw new Error("drive already running");
    const prior = attached.get(this.$runtime);
    if (
      prior !== undefined &&
      prior !== this &&
      prior.$task !== null &&
      !prior.$ended
    ) {
      throw new Error("runtime already has a drive");
    }
    resetDrive(this);
    bindWakeOf(this.$runtime, () => this.kick());
    attached.set(this.$runtime, this);
    setTask(
      this,
      (async () => {
        let fault: Error | null = null;
        try {
          await runDrive(this as never);
        } catch (error) {
          fault = error instanceof Error ? error : new Error(String(error));
          failWaiters(this as never, fault);
        } finally {
          markEnded(this, fault);
          this.$wake?.fire();
        }
      })(),
    );
  }

  async closeTimeout(timeoutMs: number): Promise<string> {
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 0) {
      throw new RangeError("timeout_ms must be a safe integer");
    }
    const task = this.$task;
    if (task === null) {
      await this.close();
      return "closed";
    }
    const settled = await Promise.race([
      task.then(
        () => true,
        () => true,
      ),
      new Promise<boolean>((resolve) => setTimeout(() => resolve(false), timeoutMs)),
    ]);
    if (!settled) return "timeout";
    await this.close();
    return "closed";
  }

  async close(): Promise<void> {
    markClosed(this);
    this.$wake?.fire();
    const task = this.$task;
    bindWakeOf(this.$runtime, null);
    if (attached.get(this.$runtime) === this) attached.delete(this.$runtime);
    try {
      if (task !== null) await task;
    } finally {
      clearTask(this);
      // Pass the fault back in: `markEnded(this, null)` would wipe the verdict
      // the loop died with before the `throw fault` below could read it.
      markEnded(this, driveFault(this));
      failWaiters(this as never, new Error("drive closed"));
    }
    const fault = this.$fault;
    if (fault !== null) throw fault;
  }

  kick(): void {
    if (!this.$pumping && !this.$releasing) this.$wake?.fire();
  }

}
