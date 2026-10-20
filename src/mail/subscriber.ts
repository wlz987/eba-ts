import type { Envelope } from "../envelope/envelope.ts";
import { actorId } from "../envelope/ids.ts";
import type { ActorId } from "../envelope/ids.ts";
import { combineFailures } from "../errors.ts";
import { parsePattern } from "../pattern/pattern.ts";
import type { Bus } from "./bus.ts";
import type { Subscription } from "./bus.ts";
import type { Inbox } from "./inbox.ts";
import type { Receiver } from "./inbox.ts";

const UNSET = Symbol("unset");
const CHANGING = Symbol("changing");

export class Subscriber {
  readonly actorId: ActorId;
  readonly patterns: readonly string[];
  readonly receiver: Receiver;
  readonly inbox: Inbox;
  private subscriptions: Subscription[] | symbol = UNSET;
  private readonly owed: Subscription[] = [];

  constructor(
    actor: ActorId | string,
    receiver: Receiver,
    patterns: readonly string[] = [],
  ) {
    this.actorId = actorId(actor);
    this.patterns = [...patterns];
    for (const pattern of this.patterns) parsePattern(pattern);
    this.receiver = receiver.take();
    this.inbox = this.receiver.inbox;
  }

  start(bus: Bus): void {
    if (this.subscriptions !== UNSET) {
      if (this.subscriptions === CHANGING)
        throw new Error("reentrant subscriber mutation");
      throw new Error("subscriber already started");
    }
    if (this.inbox.isClosed) throw new Error("inbox closed");
    const releaseReceiver = this.receiver.borrow();
    const subscriptions: Subscription[] = [];
    this.subscriptions = CHANGING;
    try {
      release(this.owed);
      for (const pattern of this.patterns) {
        subscriptions.push(bus.subscribe(pattern, this.inbox));
      }
      if (this.inbox.isClosed) throw new Error("inbox closed");
      this.subscriptions = subscriptions;
    } catch (error) {
      try {
        release(subscriptions);
      } catch (secondary) {
        this.owed.push(...subscriptions);
        throw combineFailures(error, secondary);
      }
      throw error;
    } finally {
      releaseReceiver();
      if (this.subscriptions === CHANGING) this.subscriptions = UNSET;
    }
  }

  stop(): void {
    this.release_();
  }

  close(bus: Bus): void {
    this.release_(bus);
  }

  tryRecv(): Envelope | null {
    return this.receiver.tryRecv();
  }

  private release_(bus?: Bus): void {
    if (this.subscriptions === CHANGING)
      throw new Error("reentrant subscriber mutation");
    const subscriptions = Array.isArray(this.subscriptions)
      ? this.subscriptions
      : null;
    this.subscriptions = CHANGING;
    try {
      if (subscriptions !== null) release(subscriptions);
      release(this.owed);
      if (bus !== undefined) this.receiver.close();
    } finally {
      this.subscriptions =
        subscriptions !== null && subscriptions.length > 0
          ? subscriptions
          : UNSET;
    }
  }
}

function release(subscriptions: Subscription[]): void {
  while (subscriptions.length > 0) {
    subscriptions[subscriptions.length - 1]!.close();
    subscriptions.pop();
  }
}
