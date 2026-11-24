import type { Envelope, ActorId } from "#src/core/envelope.js";
import { actorId } from "#src/core/envelope.js";
import { combineFailures } from "#src/core/errors.js";
import { parsePattern } from "#src/core/pattern.js";
import type { Bus, Subscription } from "#src/core/mail/bus.js";
import type { Inbox, Receiver } from "#src/core/mail/inbox.js";

const UNSET = Symbol("unset");
const CHANGING = Symbol("changing");

export class Subscriber {
  /** Identity and wiring, pinned as ECMAScript `#` fields. */
  readonly actorId: ActorId;
  readonly #patterns: readonly string[];
  readonly receiver: Receiver;
  readonly inbox: Inbox;
  /**
   * Held subscriptions and release debt, kept in `#` fields so a JS caller
   * cannot swap the subscription list mid-flight.
   */
  #subscriptions: Subscription[] | symbol = UNSET;
  #owed: Subscription[] = [];

  constructor(
    actor: ActorId | string,
    receiver: Receiver,
    patterns: readonly string[] = [],
  ) {
    this.actorId = actorId(actor);
    this.#patterns = [...patterns];
    for (const pattern of this.#patterns) parsePattern(pattern);
    this.receiver = receiver.take();
    this.inbox = this.receiver.inbox;
  }

  /** The declared patterns */
  get patterns(): readonly string[] {
    return [...this.#patterns];
  }

  start(bus: Bus): void {
    if (this.#subscriptions !== UNSET) {
      if (this.#subscriptions === CHANGING)
        throw new Error("reentrant subscriber mutation");
      throw new Error("subscriber already started");
    }
    if (this.inbox.isClosed) throw new Error("inbox closed");
    const releaseReceiver = this.receiver.borrow();
    const subscriptions: Subscription[] = [];
    this.#subscriptions = CHANGING;
    try {
      release(this.#owed);
      for (const pattern of this.#patterns) {
        subscriptions.push(bus.subscribe(pattern, this.inbox));
      }
      if (this.inbox.isClosed) throw new Error("inbox closed");
      this.#subscriptions = subscriptions;
    } catch (error) {
      try {
        release(subscriptions);
      } catch (secondary) {
        this.#owed.push(...subscriptions);
        throw combineFailures(error, secondary);
      }
      throw error;
    } finally {
      releaseReceiver();
      if (this.#subscriptions === CHANGING) this.#subscriptions = UNSET;
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
    if (this.#subscriptions === CHANGING)
      throw new Error("reentrant subscriber mutation");
    const subscriptions = Array.isArray(this.#subscriptions)
      ? this.#subscriptions
      : null;
    this.#subscriptions = CHANGING;
    try {
      if (subscriptions !== null) release(subscriptions);
      release(this.#owed);
      if (bus !== undefined) this.receiver.close();
    } finally {
      this.#subscriptions =
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
