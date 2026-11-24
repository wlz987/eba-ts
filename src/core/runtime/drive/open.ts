import type { Envelope, Completion, ActorId } from "#src/core/envelope.js";
import type { Caller } from "#src/core/job/caller.js";
import { coreOf } from "#src/core/job/host.js";
import { Pending } from "#src/core/runtime/signals.js";
import type { PendingWait } from "#src/core/runtime/signals.js";
import {
  DriveBase,
  claimWaiter,
  driveClosed,
  driveEnded,
  driveRuntime,
  driveTask,
  parkWaiter,
  queueStop,
} from "#src/core/runtime/drive/base.js";

export abstract class DriveOpen extends DriveBase {
  open(
    caller: Caller,
    options: {
      topic: string;
      payload: unknown;
      from: ActorId | string;
      timeoutMs?: number | null;
    },
  ): Pending {
    this.#requireRunning();
    if (coreOf(caller).signal.handed) {
      if (!driveRuntime(this).owns(caller))
        throw new Error("caller belongs to another runtime");
    } else {
      driveRuntime(this).add(caller);
    }
    let resolveFn: (completion: Completion) => void = () => {};
    let rejectFn: (error: Error) => void = () => {};
    const ready = new Promise<Completion>((resolve, reject) => {
      resolveFn = resolve;
      rejectFn = reject;
    });
    const waiter: PendingWait = {
      resolve: resolveFn,
      reject: rejectFn,
      done: false,
    };
    let key = "";
    const envelope = caller.admit({
      topic: options.topic,
      payload: options.payload,
      from: options.from,
      timeoutMs: options.timeoutMs ?? null,
      pay: (completion) => {
        if (waiter.done) return;
        waiter.done = true;
        claimWaiter(this, key);
        waiter.resolve(completion);
      },
    });
    key = String(envelope.header.id);
    parkWaiter(this, key, waiter);
    return new Pending(envelope, ready);
  }

  abandon(caller: Caller, envelope: Envelope): void {
    const key = String(envelope.header.id);
    const waiter = claimWaiter(this, key);
    if (waiter === undefined) return;
    if (waiter.done) return;
    waiter.done = true;
    waiter.reject(new Error("cancelled"));
    queueStop(this, caller, key);
    this.kick();
  }

  /**
   *
   * A `#` method, not `private`: the TS modifier is erased at runtime, so a
   * `private` method stays a reachable prototype method. A `#` method is
   * unreachable outside the class body.
   */
  #requireRunning(): void {
    if (driveClosed(this) || driveTask(this) === null || driveEnded(this)) {
      throw new Error("drive is not running");
    }
  }

}
