import type { Envelope } from "#src/core/envelope.js";
import type { AgendaLike } from "#src/core/notify.js";
import { Signal, Interest } from "#src/core/notify.js";
import type { Bus } from "#src/core/mail/bus.js";
import type { Inbox } from "#src/core/mail/inbox.js";

// A delivery offer remembers the targets it armed so a later re-offer can
// release exactly those watches, even if routing changed in between.
const armedTargets = new WeakMap<DeliveryWatch, readonly Inbox[]>();
export function setArmedTargets(watch: DeliveryWatch, targets: readonly Inbox[]): void {
  armedTargets.set(watch, targets);
}
export function armedTargetsOf(watch: DeliveryWatch): readonly Inbox[] {
  return armedTargets.get(watch) ?? [];
}

let finishHook: ((bus: Bus, watch: DeliveryWatch) => void) | null = null;
export function installFinishHook(hook: (bus: Bus, watch: DeliveryWatch) => void): void {
  finishHook = hook;
}

/**
 * Disarm a live `Bus` grant. Sibling modules reach the `#` field through this
 * accessor instead of putting a setter on the published class.
 */

export function disarmSubscription(right: Subscription): void {
  right.$release = null;
}

export class Subscription {
  $release: (() => boolean) | null;
  constructor(release: () => boolean) {
    this.$release = release;
  }
  active(): boolean {
    return this.$release !== null;
  }

  close(): boolean {
    const release = this.$release;
    if (release === null) return false;
    const released = release();
    this.$release = null;
    return released;
  }
}

export class DeliveryWatch extends Interest {
  declare readonly envelope: Envelope;
  declare readonly home: AgendaLike;
  declare private readonly source: Bus;
  constructor(source: Bus, envelope: Envelope, home: AgendaLike) {
    super(new Signal(), {}, envelope.header.topic, { envelope, home, source });
  }
  override notify(): void {
    finishHook?.(this.source, this);
  }

  override dispose(): void {
    // A no-op, not a throw: `finishDelivery` calls this to detach an offer after
    // a re-offer and `deliver`'s catch path to abandon one. Refusing would break
    // both; what must not happen is a *caller* reaching it and detaching a live
    // `finishDelivery`; its lifetime is the bus's, not the caller's.
  }

  override setOnNotify(_hook: () => void): void {
    // Same reasoning: the bus installs `notify` by overriding the method, so a
    // caller-supplied hook would replace the `finishDelivery` behaviour with
    // anything at all.
  }
}

