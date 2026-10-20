import type { Envelope } from "../envelope/envelope.ts";
import { snapshotEnvelope } from "../envelope/envelope.ts";
import { splitTopic } from "../envelope/topic.ts";
import { canEnqueue, enqueue, markRead, readSignal, watchSpace } from "./inbox.ts";
import type { Inbox } from "./inbox.ts";
import { MailboxFull } from "../errors.ts";
import { Interest } from "../notify/watch.ts";
import type { AgendaLike } from "../notify/signal.ts";
import { BusBase } from "./bus/base.ts";
import { Offer } from "./bus/types.ts";
export { Subscription } from "./bus/types.ts";

export class Bus extends BusBase {
  canPublish(envelope: unknown): boolean {
    let snapshot: Envelope;
    let parts: string[];
    try {
      snapshot = snapshotEnvelope(envelope);
      parts = splitTopic(snapshot.header.topic);
    } catch {
      return false;
    }
    const kind = snapshot.header.address.kind;
    return this.routes
      .snapshotMatch(parts)
      .every((box) => canEnqueue(box, kind));
  }

  readyOrWatch(envelope: Envelope, interest: Interest): boolean {
    this.routes.changes.watch(envelope.header.topic, interest);
    const targets = this.routes.snapshotMatch(
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

  publish(envelope: unknown): boolean {
    const snapshot = snapshotEnvelope(envelope);
    const targets = this.routes.snapshotMatch(
      splitTopic(snapshot.header.topic),
    );
    if (targets.length === 0) return false;
    for (const box of targets) {
      if (!canEnqueue(box, snapshot.header.address.kind)) {
        throw new MailboxFull("target inbox full");
      }
    }
    for (const box of targets) enqueue(box, snapshot);
    for (const box of targets) markRead(box);
    return true;
  }

  holdWait(requestId: string): void {
    this.waits.add(requestId);
  }

  releaseWait(requestId: string): void {
    this.waits.delete(requestId);
  }

  waiting(requestId: string | undefined): boolean {
    return requestId !== undefined && this.waits.has(requestId);
  }

  deliver(envelope: Envelope, home: AgendaLike): void {
    const held = snapshotEnvelope(envelope);
    if (this.place(held, home)) return;
    const offer = new Offer(this, held, home);
    this.offers.push(offer);
    this.arm(offer);
  }

  finishOffer(offer: Offer): void {
    if (!this.place(offer.envelope, offer.home)) {
      this.arm(offer);
      return;
    }
    const index = this.offers.indexOf(offer);
    if (index !== -1) this.offers.splice(index, 1);
    offer.dispose();
  }

  private elsewhere(inbox: Inbox, home: AgendaLike): boolean {
    const signal = readSignal(inbox);
    return !signal.boundTo(home) && !inbox.isClosed;
  }

  private targetsFor(envelope: Envelope, home: AgendaLike): Inbox[] {
    const parts = splitTopic(envelope.header.topic);
    return this.routes
      .snapshotMatch(parts)
      .filter((box) => this.elsewhere(box, home));
  }

  private place(envelope: Envelope, home: AgendaLike): boolean {
    const targets = this.targetsFor(envelope, home);
    const kind = envelope.header.address.kind;
    if (!targets.every((box) => canEnqueue(box, kind))) return false;
    for (const box of targets) enqueue(box, envelope);
    for (const box of targets) markRead(box);
    return true;
  }

  private arm(offer: Offer): void {
    const kind = offer.envelope.header.address.kind;
    for (const box of this.targetsFor(offer.envelope, offer.home)) {
      if (!canEnqueue(box, kind)) watchSpace(box, offer);
    }
  }
}
