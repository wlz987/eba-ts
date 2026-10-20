import type { Envelope } from "../../envelope/envelope.ts";
import type { Completion } from "../../envelope/result.ts";
import type { Caller } from "../../job/caller.ts";
import { coreOf } from "../../job/host.ts";
import type { ActorId } from "../../envelope/ids.ts";
import { Pending } from "../signals.ts";
import type { PendingWait } from "../signals.ts";
import { DriveBase } from "./base.ts";

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
    this.requireRunning();
    if (coreOf(caller).signal.handed) {
      if (!this.runtime.owns(caller))
        throw new Error("caller belongs to another runtime");
    } else {
      this.runtime.add(caller);
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
        this.waiters.delete(key);
        waiter.resolve(completion);
      },
    });
    key = String(envelope.header.id);
    this.waiters.set(key, waiter);
    return new Pending(envelope, ready);
  }

  abandon(caller: Caller, envelope: Envelope): void {
    const key = String(envelope.header.id);
    const waiter = this.waiters.get(key);
    if (waiter === undefined) return;
    this.waiters.delete(key);
    if (waiter.done) return;
    waiter.done = true;
    waiter.reject(new Error("cancelled"));
    this.stops.push({ caller, rootId: key });
    this.kick();
  }

  private requireRunning(): void {
    if (this.closed || this.task === null || this.ended) {
      throw new Error("drive is not running");
    }
  }

}
