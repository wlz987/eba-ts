import type { Envelope, TrustPolicy } from "#src/core/envelope.js";
import { snapshotEnvelope, splitTopic } from "#src/core/envelope.js";
import type { SchemaRegistry } from "#src/core/schema.js";
import { canEnqueue, enqueue, markRead, readSignal, stateOf, watchSpace } from "#src/core/mail/inbox.js";
import type { Inbox } from "#src/core/mail/inbox.js";
import { AmbiguousRoute, MailboxFull } from "#src/core/errors.js";
import { Interest, disposeInterest } from "#src/core/notify.js";
import type { AgendaLike } from "#src/core/notify.js";
import {
  BusBase,
  addWait,
  armDelivery,
  busDedup,
  busRoutes,
  deliveriesOf,
  dropWait,
  retainDeliveries,
  retireDelivery,
} from "#src/core/mail/bus/base.js";
import { dedupKey } from "#src/core/record.js";
import {
  DeliveryWatch,
  armedTargetsOf,
  installFinishHook,
  setArmedTargets,
} from "#src/core/mail/bus/types.js";
export { Subscription } from "#src/core/mail/bus/types.js";

function elsewhere(_bus: Bus, inbox: Inbox, home: AgendaLike): boolean {
  const signal = readSignal(inbox);
  return !signal.boundTo(home) && !inbox.isClosed;
}

function targetsFor(bus: Bus, envelope: Envelope, home: AgendaLike): Inbox[] {
  const parts = splitTopic(envelope.header.topic);
  return busRoutes(bus)
    .snapshotMatch(parts)
    .filter((box) => elsewhere(bus, box, home));
}

function place(bus: Bus, envelope: Envelope, home: AgendaLike): boolean {
  const targets = targetsFor(bus, envelope, home);
  if (targets.length === 0) return false;
  const kind = envelope.header.address.kind;
  if (!targets.every((box) => canEnqueue(box, kind))) return false;
  for (const box of targets) enqueue(box, envelope);
  for (const box of targets) markRead(box);
  return true;
}

function arm(bus: Bus, watch: DeliveryWatch): void {
  const kind = watch.envelope.header.address.kind;
  const targets = targetsFor(bus, watch.envelope, watch.home);
  // Remember only the inboxes that actually parked this offer. A later
  // re-offer may find a different blocked target; stale ones must not receive
  // the letter when the route finally disappears.
  const blocked = targets.filter((box) => !canEnqueue(box, kind));
  setArmedTargets(watch, blocked);
  for (const box of blocked) watchSpace(box, watch);
}

/** The routing-table installation step (internal, never on `Bus`). */
export function readyOrWatchOf(
  bus: Bus,
  envelope: Envelope,
  interest: Interest,
): boolean {
  busRoutes(bus).changes.watch(envelope.header.topic, interest);
  const targets = busRoutes(bus).snapshotMatch(
    splitTopic(envelope.header.topic),
  );
  let ready = true;
  for (const box of targets) {
    if (!canEnqueue(box, envelope.header.address.kind)) {
      watchSpace(box, interest);
      ready = false;
    }
  }
  return ready;
}

/** Re-offer step: land, re-arm, or seize back to the last armed targets. */
export function finishDeliveryOf(bus: Bus, watch: DeliveryWatch): void {
  if (place(bus, watch.envelope, watch.home)) {
    retireDelivery(bus, watch);
    disposeInterest(watch);
    return;
  }
  if (targetsFor(bus, watch.envelope, watch.home).length === 0) {
    // No route can ever take it: hand the letter back to the inboxes that
    // armed this offer instead of silently dropping it.
    for (const box of armedTargetsOf(watch)) {
      stateOf(box).seized.push(watch.envelope);
    }
    retireDelivery(bus, watch);
    disposeInterest(watch);
    return;
  }
  disposeInterest(watch);
  arm(bus, watch);
}

installFinishHook(finishDeliveryOf);

export class Bus extends BusBase {
  /** The single-writer claim, pinned as an ECMAScript `#` field. */
  owner: object | null = null;

  /** The capability `claim()` hands back, and the only thing `release()` accepts. */
  #claimToken: symbol | null = null;

  /** Optional schema registry; when set, ingress migrates older payloads. */
  readonly #schemas: SchemaRegistry | null;
  /** Optional identity policy checked on every ingress. */
  #trust: TrustPolicy | null = null;

  constructor(schemas?: SchemaRegistry) {
    super();
    this.#schemas = schemas ?? null;
  }

  /** Install an optional identity policy for every ingress on this bus. */
  setTrustPolicy(policy: TrustPolicy | null): void {
    this.#trust = policy;
  }

  // Ingress order: migrate first (a rejected migration never reaches
  // the trust gate), then check identity. Both propagate on `publish`/`tell`.
  #migrateIncoming(envelope: Envelope): Envelope {
    return this.#schemas === null
      ? envelope
      : this.#schemas.migrateEnvelope(envelope);
  }

  #checkTrust(envelope: Envelope): void {
    if (this.#trust !== null) envelope.checkTrust(this.#trust);
  }

  claim(_owner: object): symbol {
    if (this.owner !== null) {
      throw new Error("bus already belongs to a runtime");
    }
    this.owner = _owner;
    const token = Symbol("eba.bus.claim");
    this.#claimToken = token;
    return token;
  }

  release(token: symbol): void {
    // A token is spent exactly once: it is only the live capability while it is
    // still the one this bus minted for its *current* claim. An old token (or a
    // token minted by another bus) matches nothing and frees nothing.
    if (token !== this.#claimToken) return;
    this.#claimToken = null;
    this.owner = null;
  }

  publish(envelope: unknown): boolean {
    const snapshot = this.#migrateIncoming(snapshotEnvelope(envelope));
    this.#checkTrust(snapshot);
    const targets = busRoutes(this).snapshotMatch(
      splitTopic(snapshot.header.topic),
    );
    if (targets.length === 0) return false;
    // A request is a single-owner operation. Fanning it out would run the same
    // work in several hosts and let the first reply win. Events intentionally
    // fan out; requests fail closed on an ambiguous route.
    if (snapshot.header.address.kind === "request" && targets.length > 1) {
      throw new AmbiguousRoute(
        `request topic ${snapshot.header.topic} matches ${targets.length} subscribers`,
      );
    }
    // Ingress dedup: the same (from, id) lands once per bounded FIFO window.
    // Only a letter that actually landed is remembered — a refused publish
    // (ambiguous route, full target) stays retryable.
    const dedup = busDedup(this);
    const key = dedupKey(snapshot.header.from, snapshot.header.id);
    if (dedup.has(key)) return false;
    for (const box of targets) {
      if (!canEnqueue(box, snapshot.header.address.kind)) {
        throw new MailboxFull("target inbox full");
      }
    }
    for (const box of targets) enqueue(box, snapshot);
    for (const box of targets) markRead(box);
    dedup.accept(key);
    return true;
  }

  tell(envelope: unknown): number {
    const snapshot = this.#migrateIncoming(snapshotEnvelope(envelope));
    this.#checkTrust(snapshot);
    const kind = snapshot.header.address.kind;
    const targets = busRoutes(this).snapshotMatch(
      splitTopic(snapshot.header.topic),
    );
    const ready = targets.filter((box) => canEnqueue(box, kind));
    for (const box of ready) enqueue(box, snapshot);
    for (const box of ready) markRead(box);
    return targets.length - ready.length;
  }

  holdWait(requestId: string): boolean {
    if (this.owner !== null) return false;
    addWait(this, requestId);
    return true;
  }

  releaseWait(requestId: string): boolean {
    if (this.owner !== null) return false;
    dropWait(this, requestId);
    return true;
  }

  /** True when `requestId` is awaited — a plain read (it cannot move anything). */
  waiting(requestId: string | undefined): boolean {
    return requestId !== undefined && this.waits.has(requestId);
  }

  deliver(envelope: Envelope, home: AgendaLike): void {
    // dropped silently and never arms an offer.
    let held: Envelope;
    try {
      held = this.#migrateIncoming(snapshotEnvelope(envelope));
    } catch {
      return;
    }
    if (place(this, held, home)) return;
    const watch = new DeliveryWatch(this, held, home);
    armDelivery(this, watch);
    arm(this, watch);
  }

  /** Abandon every armed offer owned by `home`, returning their letters. */
  abandonDeliveries(home: AgendaLike): Envelope[] {
    const kept: DeliveryWatch[] = [];
    const abandoned: Envelope[] = [];
    for (const watch of deliveriesOf(this)) {
      if (watch.home !== home) {
        kept.push(watch);
        continue;
      }
      disposeInterest(watch);
      abandoned.push(watch.envelope);
    }
    retainDeliveries(this, kept);
    return abandoned;
  }

}
