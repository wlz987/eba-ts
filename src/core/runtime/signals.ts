import type { Envelope, Completion } from "#src/core/envelope.js";
import { Signal, Interest } from "#src/core/notify.js";

export class WakeTicket {
  /** The one-shot wake latch, pinned as ECMAScript `#` fields. */
  #fired = false;
  #waiter: (() => void) | null = null;
  fire(): void {
    this.#fired = true;
    const waiter = this.#waiter;
    if (waiter !== null) waiter();
  }
  consume(): boolean {
    const fired = this.#fired;
    this.#fired = false;
    return fired;
  }
  arm(waiter: () => void): void {
    this.#waiter = waiter;
    if (this.#fired) waiter();
  }
  disarm(waiter: () => void): void {
    if (this.#waiter === waiter) this.#waiter = null;
  }
  expire(waiter: () => void): void {
    if (this.#waiter === waiter) waiter();
  }
}

export class SpaceKick extends Interest {
  constructor(onFree: () => void) {
    // A pure wake-up: `notify` is replaced through the hook rather than by
    // overriding the method, so nothing has to be stored on the
    // instance at all.
    super(new Signal(), {}, "");
    this.setOnNotify(onFree);
  }
}

export interface PendingWait {
  resolve: (completion: Completion) => void;
  reject: (error: Error) => void;
  done: boolean;
}

export class Pending {
  /** The parked-ask handle: its request envelope and one-shot completion promise. */
  readonly envelope: Envelope;
  readonly ready: Promise<Completion>;
  constructor(envelope: Envelope, ready: Promise<Completion>) {
    this.envelope = envelope;
    this.ready = ready;
  }
}

