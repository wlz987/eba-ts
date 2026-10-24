import type { Bus } from "../../mail/bus.ts";
import type { Envelope } from "../../envelope/envelope.ts";
import { correlationIdOf } from "../../envelope/header.ts";
import { positiveInt } from "../../time/numbers.ts";
import type { Signal } from "../../notify/signal.ts";
import { Interest } from "../../notify/watch.ts";

export function awaited(bus: Bus, envelope: Envelope): boolean {
  return bus.waiting(correlationIdOf(envelope.header.address));
}

export class Group {
  readonly interest: Interest;
  readonly items: Envelope[] = [];
  constructor(interest: Interest) {
    this.interest = interest;
  }
}

export class OutboxBase {
  protected limit: number;
  protected signal: Signal;
  protected candidates: Record<string, null> = {};
  protected groups = new Map<string, Group>();
  protected buried: Envelope[] = [];
  protected count = 0;

  constructor(limit: number, signal: Signal) {
    this.limit = positiveInt(limit, "outbox_limit");
    this.signal = signal;
  }

  get size(): number {
    return this.count;
  }

  get buriedCount(): number {
    return this.buried.length;
  }

  protected rememberBuried(envelope: Envelope): void {
    this.buried.push(envelope);
    while (this.buried.length > this.limit) {
      this.buried.shift();
      this.count -= 1;
    }
  }

  protected queued(): number {
    return this.count - this.buried.length;
  }

  hasRoom(): boolean {
    return this.queued() < this.limit;
  }

  contains(envelope: Envelope): boolean {
    if (this.buried.some((item) => item === envelope)) return true;
    for (const group of this.groups.values()) {
      if (group.items.some((item) => item === envelope)) return true;
    }
    return false;
  }

}
