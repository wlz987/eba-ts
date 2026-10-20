import type { Envelope } from "../envelope/envelope.ts";
import type { Completion } from "../envelope/result.ts";
import { Signal } from "../notify/signal.ts";
import { Interest } from "../notify/watch.ts";

export class WakeTicket {
  private fired = false;
  private waiter: (() => void) | null = null;
  fire(): void {
    this.fired = true;
    const waiter = this.waiter;
    if (waiter !== null) waiter();
  }
  consume(): boolean {
    const fired = this.fired;
    this.fired = false;
    return fired;
  }
  arm(waiter: () => void): void {
    this.waiter = waiter;
    if (this.fired) waiter();
  }
  disarm(waiter: () => void): void {
    if (this.waiter === waiter) this.waiter = null;
  }
  expire(waiter: () => void): void {
    if (this.waiter === waiter) waiter();
  }
}

export class SpaceKick extends Interest {
  private readonly onFree: () => void;
  constructor(onFree: () => void) {
    super(new Signal(), {}, "");
    this.onFree = onFree;
  }
  override notify(): void {
    this.onFree();
  }
}

export interface PendingWait {
  resolve: (completion: Completion) => void;
  reject: (error: Error) => void;
  done: boolean;
}

export class Pending {
  readonly envelope: Envelope;
  readonly ready: Promise<Completion>;
  constructor(envelope: Envelope, ready: Promise<Completion>) {
    this.envelope = envelope;
    this.ready = ready;
  }
}

